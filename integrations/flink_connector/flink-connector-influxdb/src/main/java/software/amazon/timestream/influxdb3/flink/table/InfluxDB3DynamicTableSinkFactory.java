/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
package software.amazon.timestream.influxdb3.flink.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.base.table.AsyncSinkConnectorOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.logical.RowType;

import software.amazon.timestream.influxdb3.flink.InfluxDB3Sink;
import software.amazon.timestream.influxdb3.flink.InfluxDB3SinkBuilder;
import software.amazon.timestream.influxdb3.flink.auth.SecretsManagerTokenProvider;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.ACCEPT_PARTIAL;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.DATABASE;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.ENDPOINT;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.FAIL_ON_ERROR;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.FAIL_ON_REJECTED;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.FAIL_ON_VALIDATION;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.GZIP_ENABLED;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.MANAGED_FLINK_METRICS_ENABLED;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.MANAGED_FLINK_METRICS_SINK_NAME;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.MAX_RETRIES;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.NO_SYNC;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.PRECISION;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.REQUEST_TIMEOUT;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.TAG_COLUMNS;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.TIMESTAMP_COLUMN;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.TOKEN;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.TOKEN_SECRET_ID;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.TOKEN_SECRET_JSON_FIELD;
import static software.amazon.timestream.influxdb3.flink.table.InfluxDB3ConnectorOptions.TOKEN_SECRET_REGION;

/**
 * Creates insert-only Flink SQL sinks for Amazon Timestream for InfluxDB 3.
 *
 * <p>Registered under the connector identifier {@value #IDENTIFIER}. Every row is written to the
 * measurement named after the sink table. See {@link InfluxDB3ConnectorOptions} for the supported
 * options.
 */
@Internal
public final class InfluxDB3DynamicTableSinkFactory implements DynamicTableSinkFactory {

    /** Connector identifier used in {@code WITH ('connector' = ...)}. */
    public static final String IDENTIFIER = "timestream-influxdb3";

    /** Creates the factory; invoked through service discovery. */
    public InfluxDB3DynamicTableSinkFactory() {}

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Set.of(ENDPOINT, DATABASE, TIMESTAMP_COLUMN);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Set.of(
                TAG_COLUMNS,
                PRECISION,
                TOKEN,
                TOKEN_SECRET_ID,
                TOKEN_SECRET_JSON_FIELD,
                TOKEN_SECRET_REGION,
                FAIL_ON_VALIDATION,
                FAIL_ON_REJECTED,
                FAIL_ON_ERROR,
                MAX_RETRIES,
                GZIP_ENABLED,
                ACCEPT_PARTIAL,
                NO_SYNC,
                REQUEST_TIMEOUT,
                MANAGED_FLINK_METRICS_ENABLED,
                MANAGED_FLINK_METRICS_SINK_NAME,
                FactoryUtil.SINK_PARALLELISM,
                AsyncSinkConnectorOptions.MAX_BATCH_SIZE,
                AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS,
                AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS,
                AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE,
                AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT);
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        ReadableConfig options = helper.getOptions();

        ResolvedCatalogTable table = context.getCatalogTable();
        if (table.isPartitioned()) {
            throw new ValidationException(
                    "The '" + IDENTIFIER + "' connector does not support partitioned tables.");
        }
        if (context.getPrimaryKeyIndexes().length > 0) {
            throw new ValidationException(
                    "The '"
                            + IDENTIFIER
                            + "' connector is insert-only and does not support PRIMARY KEY "
                            + "declarations. Points with the same measurement, tags, and "
                            + "timestamp overwrite each other in InfluxDB 3.");
        }

        RowDataLineProtocolConverter converter =
                RowDataLineProtocolConverter.create(
                        (RowType) context.getPhysicalRowDataType().getLogicalType(),
                        context.getObjectIdentifier().getObjectName(),
                        parseTagColumns(options.getOptional(TAG_COLUMNS)),
                        options.get(TIMESTAMP_COLUMN),
                        options.get(PRECISION));

        InfluxDB3Sink<RowData> sink;
        try {
            InfluxDB3SinkBuilder<RowData> builder =
                    InfluxDB3Sink.<RowData>builder()
                            .setEndpoint(options.get(ENDPOINT))
                            .setDatabase(options.get(DATABASE))
                            .setConverter(converter)
                            .setWritePrecision(options.get(PRECISION))
                            .setFailOnValidation(options.get(FAIL_ON_VALIDATION))
                            .setGzipEnabled(options.get(GZIP_ENABLED))
                            .setAcceptPartial(options.get(ACCEPT_PARTIAL))
                            .setNoSync(options.get(NO_SYNC))
                            .setRequestTimeout(options.get(REQUEST_TIMEOUT))
                            .setMaxRetries(options.get(MAX_RETRIES))
                            .setManagedFlinkMetricsEnabled(
                                    options.get(MANAGED_FLINK_METRICS_ENABLED));
            configureToken(options, builder);
            options.getOptional(FAIL_ON_REJECTED).ifPresent(builder::setFailOnRejected);
            options.getOptional(FAIL_ON_ERROR).ifPresent(builder::setFailOnError);
            options.getOptional(MANAGED_FLINK_METRICS_SINK_NAME)
                    .ifPresent(builder::setManagedFlinkMetricsSinkName);
            options.getOptional(AsyncSinkConnectorOptions.MAX_BATCH_SIZE)
                    .ifPresent(builder::setMaxBatchSize);
            options.getOptional(AsyncSinkConnectorOptions.MAX_IN_FLIGHT_REQUESTS)
                    .ifPresent(builder::setMaxInFlightRequests);
            options.getOptional(AsyncSinkConnectorOptions.MAX_BUFFERED_REQUESTS)
                    .ifPresent(builder::setMaxBufferedRequests);
            options.getOptional(AsyncSinkConnectorOptions.FLUSH_BUFFER_SIZE)
                    .ifPresent(builder::setMaxBatchSizeInBytes);
            options.getOptional(AsyncSinkConnectorOptions.FLUSH_BUFFER_TIMEOUT)
                    .ifPresent(builder::setMaxTimeInBufferMS);
            sink = builder.build();
        } catch (IllegalArgumentException | NullPointerException error) {
            throw new ValidationException(
                    "Invalid '" + IDENTIFIER + "' sink configuration: " + error.getMessage(),
                    error);
        }
        return new InfluxDB3DynamicTableSink(
                sink, options.getOptional(FactoryUtil.SINK_PARALLELISM).orElse(null));
    }

    private static void configureToken(
            ReadableConfig options, InfluxDB3SinkBuilder<RowData> builder) {
        Optional<String> token = options.getOptional(TOKEN);
        Optional<String> secretId = options.getOptional(TOKEN_SECRET_ID);
        if (token.isPresent() == secretId.isPresent()) {
            throw new ValidationException(
                    "Exactly one of '"
                            + TOKEN.key()
                            + "' or '"
                            + TOKEN_SECRET_ID.key()
                            + "' must be set.");
        }
        if (token.isPresent()) {
            for (ConfigOption<String> secretOption :
                    List.of(TOKEN_SECRET_JSON_FIELD, TOKEN_SECRET_REGION)) {
                if (options.getOptional(secretOption).isPresent()) {
                    throw new ValidationException(
                            "Option '"
                                    + secretOption.key()
                                    + "' requires '"
                                    + TOKEN_SECRET_ID.key()
                                    + "'.");
                }
            }
            builder.setToken(token.get());
            return;
        }
        builder.setTokenProvider(
                SecretsManagerTokenProvider.builder()
                        .setSecretId(secretId.get())
                        .setJsonField(options.getOptional(TOKEN_SECRET_JSON_FIELD).orElse(null))
                        .setRegion(options.getOptional(TOKEN_SECRET_REGION).orElse(null))
                        .build());
    }

    private static List<String> parseTagColumns(Optional<String> tagColumns) {
        if (tagColumns.isEmpty() || tagColumns.get().isBlank()) {
            return List.of();
        }
        List<String> tags =
                Arrays.stream(tagColumns.get().split(",", -1))
                        .map(String::trim)
                        .collect(Collectors.toList());
        if (tags.contains("")) {
            throw new ValidationException(
                    "Option '"
                            + TAG_COLUMNS.key()
                            + "' must be a comma-separated list of column names.");
        }
        return tags;
    }
}
