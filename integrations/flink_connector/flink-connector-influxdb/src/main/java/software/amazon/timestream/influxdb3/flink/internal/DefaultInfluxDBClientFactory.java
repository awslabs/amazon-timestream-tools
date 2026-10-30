/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.internal;

import org.apache.flink.annotation.Internal;

import com.influxdb.v3.client.InfluxDBClient;
import com.influxdb.v3.client.config.ClientConfig;
import com.influxdb.v3.client.write.WriteOptions;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/** Production client factory backed by {@code influxdb3-java}. */
@Internal
public enum DefaultInfluxDBClientFactory implements InfluxDBClientFactory {
    INSTANCE;

    @Override
    public InfluxDBWriteClient create(InfluxDB3SinkConfig config, char[] token) {
        return create(config, token, InfluxDBClient::getInstance);
    }

    InfluxDBWriteClient create(
            InfluxDB3SinkConfig config,
            char[] token,
            Function<ClientConfig, InfluxDBClient> clientCreator) {
        char[] clientToken = Arrays.copyOf(token, token.length);
        try {
            Duration clientTimeout =
                    config.getConnectionTimeout().compareTo(config.getWriteTimeout()) <= 0
                            ? config.getConnectionTimeout()
                            : config.getWriteTimeout();
            ClientConfig clientConfig =
                    new ClientConfig.Builder()
                            .host(config.getEndpoint().toString())
                            .token(clientToken)
                            .authScheme("Bearer")
                            // influxdb3-java 1.10.0 applies this only as
                            // HttpClient.connectTimeout.
                            .writeTimeout(clientTimeout)
                            .build();
            WriteOptions writeOptions =
                    new WriteOptions.Builder()
                            .database(config.getDatabase())
                            .precision(config.getWritePrecision())
                            .gzipThreshold(config.isGzipEnabled() ? 0 : Integer.MAX_VALUE)
                            .noSync(config.isNoSync())
                            .acceptPartial(config.isAcceptPartial())
                            .useV2Api(false)
                            .build();
            InfluxDBClient client =
                    Objects.requireNonNull(
                            clientCreator.apply(clientConfig),
                            "clientCreator returned null");
            return new OfficialInfluxDBWriteClient(client, writeOptions, clientToken);
        } catch (RuntimeException | Error error) {
            Arrays.fill(clientToken, '\0');
            throw error;
        }
    }

    private static final class OfficialInfluxDBWriteClient implements InfluxDBWriteClient {
        private final InfluxDBClient client;
        private final WriteOptions writeOptions;
        private final char[] token;

        private OfficialInfluxDBWriteClient(
                InfluxDBClient client, WriteOptions writeOptions, char[] token) {
            this.client = client;
            this.writeOptions = writeOptions;
            this.token = token;
        }

        @Override
        public void writeRecords(List<String> records) {
            client.writeRecords(records, writeOptions);
        }

        @Override
        public void close() throws Exception {
            try {
                client.close();
            } finally {
                Arrays.fill(token, '\0');
            }
        }
    }
}
