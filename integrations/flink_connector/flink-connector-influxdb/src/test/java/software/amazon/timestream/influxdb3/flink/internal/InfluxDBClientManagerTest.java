/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import software.amazon.timestream.influxdb3.flink.InfluxDB3Sink;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkException;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkTestAccess;
import software.amazon.timestream.influxdb3.flink.auth.TokenProvider;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class InfluxDBClientManagerTest {

    @Test
    void reportsRetiredClientCloseFailureOnceAndRetriesItDuringShutdown() throws Exception {
        WriterTestContext.CapturedContext testContext = WriterTestContext.capture();
        InfluxDBSinkMetrics metrics =
                new InfluxDBSinkMetrics(
                        testContext.context(),
                        new PayloadSizeTrackingRequestBuffer<>(),
                        false,
                        null);
        FailOnceOnCloseClient staleClient = new FailOnceOnCloseClient();
        InfluxDBWriteClient refreshedClient = new NoOpClient();
        AtomicInteger clientsCreated = new AtomicInteger();
        List<Exception> asynchronousFailures = new CopyOnWriteArrayList<>();
        InfluxDBClientManager manager =
                new InfluxDBClientManager(
                        config(new RotatingTokenProvider()),
                        (config, token) ->
                                clientsCreated.getAndIncrement() == 0
                                        ? staleClient
                                        : refreshedClient,
                        metrics,
                        asynchronousFailures::add);

        manager.initialize();
        try {
            InfluxDBClientManager.ClientLease lease = manager.acquire();
            assertThat(manager.refresh(lease.generation())).isTrue();

            lease.release();

            assertThat(asynchronousFailures).singleElement()
                    .satisfies(
                            failure ->
                                    assertThat(failure)
                                            .isInstanceOf(InfluxDB3SinkException.class)
                                            .hasMessageContaining("retired InfluxDB client")
                                            .hasRootCauseMessage("first close failed"));
            assertThat(staleClient.closeCalls).hasValue(1);
            assertThat(manager.clientCount()).isEqualTo(2);
            assertThat(testContext.gaugeValue("numActiveClientGenerations")).isEqualTo(2);

            assertThat(manager.beginClose()).isTrue();
            assertThat(manager.closeResources(null)).isNull();

            assertThat(staleClient.closeCalls).hasValue(2);
            assertThat(manager.clientCount()).isZero();
            assertThat(testContext.gaugeValue("numActiveClientGenerations")).isZero();
        } finally {
            manager.beginClose();
            manager.closeResources(null);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTokens")
    void rejectsInvalidInitialTokens(InvalidToken invalidToken) {
        InvalidTokenProvider tokenProvider =
                new InvalidTokenProvider(invalidToken.value(), "new-token".toCharArray());
        AtomicInteger clientsCreated = new AtomicInteger();
        InfluxDBClientManager manager =
                manager(
                        tokenProvider,
                        (config, token) -> {
                            clientsCreated.incrementAndGet();
                            return new NoOpClient();
                        });

        assertThatThrownBy(manager::initialize)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("empty InfluxDB token");
        assertThat(clientsCreated).hasValue(0);
        assertThat(tokenProvider.closeCalls).hasValue(1);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTokens")
    void rejectsInvalidRefreshedTokensWithoutReplacingTheActiveClient(
            InvalidToken invalidToken) throws Exception {
        InvalidTokenProvider tokenProvider =
                new InvalidTokenProvider("old-token".toCharArray(), invalidToken.value());
        AtomicInteger clientsCreated = new AtomicInteger();
        InfluxDBClientManager manager =
                manager(
                        tokenProvider,
                        (config, token) -> {
                            clientsCreated.incrementAndGet();
                            return new NoOpClient();
                        });

        manager.initialize();
        try {
            InfluxDBClientManager.ClientLease lease = manager.acquire();
            try {
                assertThatThrownBy(() -> manager.refresh(lease.generation()))
                        .isInstanceOf(IOException.class)
                        .hasMessageContaining("empty InfluxDB token");
                assertThat(clientsCreated).hasValue(1);
                assertThat(manager.clientCount()).isEqualTo(1);
            } finally {
                lease.release();
            }
        } finally {
            manager.beginClose();
            manager.closeResources(null);
        }
        assertThat(tokenProvider.closeCalls).hasValue(1);
    }

    private static InfluxDBClientManager manager(
            TokenProvider tokenProvider, InfluxDBClientFactory clientFactory) {
        WriterTestContext.CapturedContext testContext = WriterTestContext.capture();
        InfluxDBSinkMetrics metrics =
                new InfluxDBSinkMetrics(
                        testContext.context(),
                        new PayloadSizeTrackingRequestBuffer<>(),
                        false,
                        null);
        return new InfluxDBClientManager(
                config(tokenProvider), clientFactory, metrics, ignored -> {});
    }

    private static InfluxDB3SinkConfig config(TokenProvider tokenProvider) {
        return InfluxDB3SinkTestAccess.configurationOf(
                InfluxDB3Sink.<String>builder()
                        .setEndpoint("https://localhost:8181")
                        .setDatabase("metrics")
                        .setTokenProvider(tokenProvider)
                        .setConverter(value -> value)
                        .build());
    }

    private static Stream<InvalidToken> invalidTokens() {
        return Stream.of(
                new InvalidToken("null token", null),
                new InvalidToken("empty token", new char[0]));
    }

    private record InvalidToken(String description, char[] value) {

        @Override
        public String toString() {
            return description;
        }
    }

    private static final class RotatingTokenProvider implements TokenProvider {
        private static final long serialVersionUID = 1L;

        @Override
        public char[] getToken() {
            return "old-token".toCharArray();
        }

        @Override
        public char[] refreshToken() {
            return "new-token".toCharArray();
        }
    }

    private static final class FailOnceOnCloseClient implements InfluxDBWriteClient {
        private final AtomicInteger closeCalls = new AtomicInteger();

        @Override
        public void writeRecords(List<String> records) {}

        @Override
        public void close() throws Exception {
            if (closeCalls.incrementAndGet() == 1) {
                throw new IOException("first close failed");
            }
        }
    }

    private static final class NoOpClient implements InfluxDBWriteClient {
        @Override
        public void writeRecords(List<String> records) {}

        @Override
        public void close() {}
    }

    private static final class InvalidTokenProvider implements TokenProvider {
        private static final long serialVersionUID = 1L;

        private final char[] initialToken;
        private final char[] refreshedToken;
        private final AtomicInteger closeCalls = new AtomicInteger();

        private InvalidTokenProvider(char[] initialToken, char[] refreshedToken) {
            this.initialToken = copy(initialToken);
            this.refreshedToken = copy(refreshedToken);
        }

        @Override
        public char[] getToken() {
            return copy(initialToken);
        }

        @Override
        public char[] refreshToken() {
            return copy(refreshedToken);
        }

        @Override
        public void close() {
            closeCalls.incrementAndGet();
        }

        private static char[] copy(char[] token) {
            return token == null ? null : Arrays.copyOf(token, token.length);
        }
    }
}
