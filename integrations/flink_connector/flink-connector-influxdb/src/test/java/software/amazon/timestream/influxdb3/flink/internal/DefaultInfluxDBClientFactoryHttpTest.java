/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.influxdb.v3.client.InfluxDBPartialWriteException;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.write.WritePrecision;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.timestream.influxdb3.flink.InfluxDB3Sink;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkTestAccess;
import software.amazon.timestream.influxdb3.flink.TestTls;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.absent;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToIgnoreCase;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DefaultInfluxDBClientFactoryHttpTest {
    private WireMockServer server;

    @BeforeEach
    void startServer() {
        server = new WireMockServer(TestTls.wireMockHttps());
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop();
    }

    @Test
    void sendsV3BearerRequestWithGzipAndWriteOptions() throws Exception {
        server.stubFor(
                post(urlPathEqualTo("/api/v3/write_lp"))
                        .willReturn(aResponse().withStatus(204)));
        InfluxDBWriteClient client = createClient(true, false, true);
        try {
            client.writeRecords(List.of("cpu,host=a value=1i", "cpu,host=b value=2i"));
        } finally {
            client.close();
        }

        server.verify(
                postRequestedFor(urlPathEqualTo("/api/v3/write_lp"))
                        .withQueryParam("db", equalTo("metrics"))
                        .withQueryParam("precision", equalTo("nanosecond"))
                        .withQueryParam("no_sync", equalTo("true"))
                        .withQueryParam("accept_partial", equalTo("false"))
                        .withHeader("Authorization", equalTo("Bearer test-token"))
                        .withHeader("Content-Encoding", equalTo("gzip"))
                        .withHeader(
                                "Content-Type", equalToIgnoreCase("text/plain; charset=UTF-8")));
        LoggedRequest request = server.getAllServeEvents().get(0).getRequest();
        assertThat(new String(request.getBody(), StandardCharsets.UTF_8))
                .isEqualTo("cpu,host=a value=1i\ncpu,host=b value=2i");
    }

    @Test
    void canDisableGzipAndUsesDefaultPartialWriteSetting() throws Exception {
        server.stubFor(
                post(urlPathEqualTo("/api/v3/write_lp"))
                        .willReturn(aResponse().withStatus(204)));
        InfluxDBWriteClient client = createClient(false, true, false);
        try {
            client.writeRecords(List.of("cpu value=1i"));
        } finally {
            client.close();
        }

        server.verify(
                postRequestedFor(urlPathEqualTo("/api/v3/write_lp"))
                        .withQueryParam("accept_partial", absent())
                        .withQueryParam("no_sync", absent())
                        .withHeader("Content-Encoding", absent())
                        .withRequestBody(equalTo("cpu value=1i")));
    }

    @Test
    void exposesParsedLineErrorsFromPartialWriteResponse() throws Exception {
        server.stubFor(
                post(urlPathEqualTo("/api/v3/write_lp"))
                        .willReturn(
                                aResponse()
                                        .withStatus(400)
                                        .withHeader("Content-Type", "application/json")
                                        .withBody(
                                                "{\"error\":\"partial write of line protocol occurred\","
                                                        + "\"data\":[{\"error_message\":\"field type conflict\","
                                                        + "\"line_number\":2,"
                                                        + "\"original_line\":\"cpu value=\\\"bad\\\"\"}]}")));
        InfluxDBWriteClient client = createClient(false, true, false);
        try {
            assertThatThrownBy(
                            () ->
                                    client.writeRecords(
                                            List.of(
                                                    "cpu value=1i",
                                                    "cpu value=\"bad\"")))
                    .isInstanceOf(InfluxDBPartialWriteException.class)
                    .satisfies(
                            error -> {
                                InfluxDBPartialWriteException partial =
                                        (InfluxDBPartialWriteException) error;
                                assertThat(partial.lineErrors()).singleElement()
                                        .satisfies(
                                                line -> {
                                                    assertThat(line.lineNumber()).isEqualTo(2);
                                                    assertThat(line.errorMessage())
                                                            .isEqualTo("field type conflict");
                                                    assertThat(line.originalLine())
                                                            .isEqualTo("cpu value=\"bad\"");
                                                });
                            });
        } finally {
            client.close();
        }
    }

    @Test
    void clearsCopiedTokenAndKeepsWritePolicyOutOfClientConfigOnConstructionFailure() {
        char[] callerToken = "test-token".toCharArray();
        AtomicReference<ClientConfig> capturedConfig = new AtomicReference<>();
        InfluxDB3SinkConfig config =
                createConfig(true, false, true, WritePrecision.MS);

        assertThatThrownBy(
                        () ->
                                DefaultInfluxDBClientFactory.INSTANCE.create(
                                        config,
                                        callerToken,
                                        clientConfig -> {
                                            capturedConfig.set(clientConfig);
                                            throw new IllegalStateException(
                                                    "client construction failed");
                                        }))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("client construction failed");

        assertThat(callerToken).containsExactly("test-token".toCharArray());
        assertThat(capturedConfig.get().getToken()).containsOnly('\0');
        assertThat(capturedConfig.get().getDatabase()).isNull();
        assertThat(capturedConfig.get().getWritePrecision()).isEqualTo(WritePrecision.NS);
        assertThat(capturedConfig.get().getGzipThreshold()).isEqualTo(1_000);
        assertThat(capturedConfig.get().getWriteNoSync()).isFalse();
        assertThat(capturedConfig.get().getWriteAcceptPartial()).isTrue();
        assertThat(capturedConfig.get().getWriteUseV2Api()).isTrue();
    }

    private InfluxDBWriteClient createClient(
            boolean gzipEnabled, boolean acceptPartial, boolean noSync) {
        InfluxDB3SinkConfig config = createConfig(gzipEnabled, acceptPartial, noSync);
        return DefaultInfluxDBClientFactory.INSTANCE.create(config, "test-token".toCharArray());
    }

    private InfluxDB3SinkConfig createConfig(
            boolean gzipEnabled, boolean acceptPartial, boolean noSync) {
        return createConfig(
                gzipEnabled,
                acceptPartial,
                noSync,
                WritePrecision.NS);
    }

    private InfluxDB3SinkConfig createConfig(
            boolean gzipEnabled,
            boolean acceptPartial,
            boolean noSync,
            WritePrecision writePrecision) {
        return InfluxDB3SinkTestAccess.configurationOf(
                InfluxDB3Sink.<String>builder()
                        .setEndpoint(TestTls.baseUrl(server))
                        .setDatabase("metrics")
                        .setToken("unused-by-factory")
                        .setConverter(value -> value)
                        .setWritePrecision(writePrecision)
                        .setGzipEnabled(gzipEnabled)
                        .setAcceptPartial(acceptPartial)
                        .setNoSync(noSync)
                        .build());
    }
}
