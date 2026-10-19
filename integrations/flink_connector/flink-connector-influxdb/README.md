# Apache Flink Connector for Amazon Timestream for InfluxDB 3

An Apache Flink DataStream and SQL sink that writes line protocol to Amazon
Timestream for InfluxDB 3 through the `/api/v3/write_lp` endpoint, with
pluggable token authentication including AWS Secrets Manager.

Requires Java 17+ and Apache Flink 2.3.x (not compatible with Flink 1.x).
Built on `influxdb3-java` 1.10.0.

## Installation

```xml
<dependency>
    <groupId>software.amazon.timestream</groupId>
    <artifactId>flink-connector-timestream-influxdb3</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

Until a release is published, run `mvn clean install` to install the snapshot
locally. The artifact is a library, not a runnable Flink application; its
dependencies resolve from its POM.

## Quick start

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import software.amazon.timestream.influxdb3.flink.InfluxDB3Sink;
import software.amazon.timestream.influxdb3.flink.auth.SecretsManagerTokenProvider;

record SensorReading(String deviceId, double temperature, long timestampNanos) {}

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<SensorReading> readings = ...; // e.g. a Kafka or Kinesis source

InfluxDB3Sink<SensorReading> sink =
        InfluxDB3Sink.<SensorReading>builder()
                .setEndpoint(
                        "https://cluster-id.timestream-influxdb.us-east-1.on.aws:8181")
                .setDatabase("sensors")
                .setTokenProvider(
                        SecretsManagerTokenProvider.builder()
                                .setSecretId("timestream/influxdb3/token")
                                .setJsonField("token")
                                .setRegion("us-east-1")
                                .build())
                .setConverter(
                        reading ->
                                String.format(
                                        "sensor,device=%s temperature=%s %d",
                                        reading.deviceId(),
                                        reading.temperature(),
                                        reading.timestampNanos()))
                .build();

readings.sinkTo(sink).name("timestream-influxdb3");
env.execute("sensor-ingest");
```

- Use the base endpoint; the connector appends `/api/v3/write_lp`.
- The database must exist, and the token must be allowed to write to it.
- The converter must return exactly one non-blank, correctly escaped
  [line protocol](https://docs.influxdata.com/influxdb3/core/reference/line-protocol/)
  row per record. Rows starting with `#` are rejected as comments.

## Flink SQL

The insert-only Table API and SQL sink is named `timestream-influxdb3`. Add the
connector as a dependency of your application, as shown in
[Installation](#installation).

```sql
CREATE TABLE cpu (
    host STRING,
    region STRING,
    usage DOUBLE,
    cores INT,
    ts TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'timestream-influxdb3',
    'endpoint' = 'https://cluster-id.timestream-influxdb.us-east-1.on.aws:8181',
    'database' = 'sensors',
    'tag-columns' = 'host,region',
    'timestamp-column' = 'ts',
    'token.secret-id' = 'timestream/influxdb3/token',
    'token.secret-json-field' = 'token'
);

INSERT INTO cpu SELECT host, region, usage, cores, event_time FROM readings;
```

Each row becomes a point in the measurement named after the table: tag
columns become tags, the timestamp column the timestamp, and the rest fields.
The connector escapes all names and values; quote non-identifier table names,
for example ``CREATE TABLE `cpu.usage` (...)``.

| Flink type | Written as |
| --- | --- |
| `BOOLEAN` | Boolean field |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | Integer field |
| `FLOAT`, `DOUBLE`, `DECIMAL` | Float field; `DECIMAL` may lose precision |
| `CHAR`, `VARCHAR`, `STRING` | String field, or a tag when listed in `tag-columns` |

- Tag columns must be string types; cast others in the query.
- `ROW`, `ARRAY`, and `MAP` are unsupported; declare JSON as `STRING` with the
  `json` format to write compacted JSON text.
- Unsigned (`u`) fields are never written. Values beyond `BIGINT` range fail
  in `BIGINT` and lose precision in `DECIMAL`/`DOUBLE`; use `STRING` or the
  DataStream sink to keep them exact.
- The timestamp column can be `TIMESTAMP(p)` (UTC), `TIMESTAMP_LTZ(p)`, or
  `BIGINT` in `precision` units. A `precision` coarser than the column
  truncates timestamps, so distinct rows can overwrite each other.
- Null or empty tags and null, `NaN`, or infinite fields are omitted. A row
  with no remaining fields, a null timestamp, or a line break in a tag or
  string value fails validation, which fails the job unless
  `'sink.fail-on-validation' = 'false'` (rows are dropped and counted in
  `numRecordsDropped`).
- The planner rejects updating queries, such as an unwindowed `GROUP BY`.
  `PRIMARY KEY` and partitioned tables are unsupported.
- Tag and field columns cannot be named `time`. The table, tag, and field
  names cannot start with `_` or `iox_`.
- Options are validated when an `INSERT` is planned, not at `CREATE TABLE`.

### SQL options

| Option | Default | Purpose |
| --- | --- | --- |
| `connector` | Required | `timestream-influxdb3` |
| `endpoint` | Required | Base HTTPS endpoint, without `/api/v3/write_lp`; plaintext `http` is rejected because the token is sent with every request |
| `database` | Required | Target database |
| `timestamp-column` | Required | Column providing each row's timestamp |
| `tag-columns` | None | Comma-separated string columns written as tags |
| `token.secret-id` | | Secrets Manager secret name or ARN containing the token |
| `token.secret-json-field` | None | Top-level JSON field; omit when the whole secret is the token |
| `token.secret-region` | AWS SDK region chain | Region containing the secret |
| `token` | | Static token stored in the table definition; development only |
| `sink.parallelism` | Planner default | Sink parallelism |

Exactly one of `token` or `token.secret-id` is required. The following options
map to [builder options](#configuration) and share their defaults; other
builder settings use their defaults, so `request-timeout` must exceed 40 s.

| SQL option | Builder option |
| --- | --- |
| `precision` (`ns`, `us`, `ms`, `s`) | `setWritePrecision` |
| `gzip.enabled` | `setGzipEnabled` |
| `accept-partial` | `setAcceptPartial` |
| `no-sync` | `setNoSync` |
| `request-timeout` | `setRequestTimeout` |
| `sink.fail-on-validation` | `setFailOnValidation` |
| `sink.fail-on-rejected` | `setFailOnRejected` |
| `sink.fail-on-error` | `setFailOnError` |
| `sink.max-retries` | `setMaxRetries` |
| `sink.batch.max-size` | `setMaxBatchSize` |
| `sink.flush-buffer.size` | `setMaxBatchSizeInBytes` |
| `sink.flush-buffer.timeout` | `setMaxTimeInBufferMS` |
| `sink.requests.max-inflight` | `setMaxInFlightRequests` |
| `sink.requests.max-buffered` | `setMaxBufferedRequests` |
| `sink.metrics.managed-flink.enabled` | `setManagedFlinkMetricsEnabled` |
| `sink.metrics.managed-flink.sink-name` | `setManagedFlinkMetricsSinkName` |

## Authentication

Use `setTokenProvider(...)` in deployed applications. `setToken(...)`
serializes the token with the job and is meant for development. `TokenProvider`
is a serializable interface, initialized in each sink subtask, that you can
implement for any token source.

The built-in `SecretsManagerTokenProvider` accepts a secret that is the token
itself (`apiv3_AAA...`) or JSON with the token in a top-level field
(`{"token":"apiv3_AAA..."}`).

| Provider option | Default | Purpose |
| --- | --- | --- |
| `setSecretId(String)` | Required | Secret name or ARN |
| `setJsonField(String)` | None | Top-level textual JSON field; omit when the whole secret is the token |
| `setRegion(String)` | AWS SDK region chain | Region containing the secret |
| `setVersionStage(String)` | `AWSCURRENT` | Secrets Manager version stage |

Each subtask reads the secret at startup and does not poll for rotation. A
`401` or `403` re-reads the secret, recreates the client, and retries the
affected records once; a second rejection fails the job.

The execution role needs `secretsmanager:GetSecretValue` on the secret, plus
`kms:Decrypt` for a customer-managed KMS key. VPC-bound applications need
network access to Secrets Manager and the InfluxDB endpoint. The AWS SDK
dependencies are always declared, even when another token source is used.

## Managed Flink deployment

Build an application JAR containing your job and the connector, upload it to
S3, and configure runtime properties, networking, and role permissions. The
connector does not read runtime properties; your application passes them to
`InfluxDB3Sink.builder()`. Do not put the token in runtime properties.

The InfluxDB client initializes Apache Arrow Flight even though the connector
writes over HTTP, so Java 17 deployments need:

```text
--add-opens=java.base/java.nio=ALL-UNNAMED
```

The [Managed Flink example](example/managed-flink/README.md) provides a
Kinesis-to-InfluxDB application and a CDK app that deploys it with a Kinesis
stream, logging, a dashboard and alarm, and automatic start and stop. It uses
an existing InfluxDB endpoint, database, and token secret.

## Configuration

`InfluxDB3Sink.builder()` validates all options in `build()`. See
[`InfluxDB3SinkBuilder`](src/main/java/software/amazon/timestream/influxdb3/flink/InfluxDB3SinkBuilder.java)
for the complete API.

### Required options

| Builder option | Purpose |
| --- | --- |
| `setEndpoint(String\|URI)` | Base HTTPS endpoint, without `/api/v3/write_lp`; plaintext `http` is rejected because the token is sent with every request |
| `setDatabase(String)` | Target database |
| `setTokenProvider(TokenProvider)` or `setToken(String)` | Token source; exactly one |
| `setConverter(LineProtocolConverter<T>)` | Converts each record to one line protocol row |

### Batching and backpressure

Limits apply per sink subtask.

| Builder option | Default | Purpose |
| --- | --- | --- |
| `setMaxBatchSize(int)` | 10,000 rows | Maximum rows per request |
| `setMaxBatchSizeInBytes(long)` | 10 MiB | Maximum uncompressed bytes per request |
| `setMaxRecordSizeInBytes(long)` | 1 MiB | Maximum uncompressed bytes per row |
| `setMaxTimeInBufferMS(long)` | 5,000 ms | Maximum age before a partial batch is sent |
| `setMaxInFlightRequests(int)` | 8 | Concurrent blocking writes and writer threads |
| `setMaxBufferedRequests(int)` | At least 20,000 rows | Row-count backpressure bound; always greater than the batch size |
| `setMaxBufferedBytes(long)` | 64 MiB | Buffered-bytes backpressure bound |

Backpressure applies when either buffer bound is reached. Requires
`maxRecordSizeInBytes <= maxBatchSizeInBytes <= maxBufferedBytes`.

Each subtask starts with `maxInFlightRequests` full batches allowed in flight
(Flink's default starts at one batch and grows by ten records per success). A
failed or retried request, such as a `429`, halves the allowance; each success
adds one batch back.

Raw payload memory per subtask is bounded by
`maxBufferedBytes + maxInFlightRequests * maxBatchSizeInBytes` (about 144 MiB
by default), before object, HTTP, compression, and checkpoint overhead.
Multiply by sink parallelism for the job.

### Timeouts and retries

| Builder option | Default | Purpose |
| --- | --- | --- |
| `setConnectionTimeout(Duration)` | 10 s | Establishing the HTTP connection |
| `setWriteTimeout(Duration)` | 10 s | Hard deadline for one write attempt |
| `setRequestTimeout(Duration)` | 60 s | Flink result deadline, including retry delay |
| `setFailOnRequestTimeout(boolean)` | `true` | Fail instead of re-buffering when the request timeout expires |
| `setMaxRetries(int)` | 8 | Retries after the initial attempt |
| `setRetryBackoff(initial, max, jitter)` | 200 ms, 30 s, 0.2 | Exponential backoff, cap, and proportional jitter |

`requestTimeout` must exceed `writeTimeout + maxRetryBackoff` by at least 2 ms.
`Retry-After` is honored up to `maxRetryBackoff`.

### Write and failure controls

| Builder option | Default | Purpose |
| --- | --- | --- |
| `setWritePrecision(WritePrecision)` | `NS` | Timestamp precision of the line protocol |
| `setGzipEnabled(boolean)` | `true` | Compress request payloads |
| `setAcceptPartial(boolean)` | `true` | Commit valid rows of a partially invalid batch |
| `setNoSync(boolean)` | `false` | Acknowledge before durable sync (lower latency, less durability) |
| `setFailOnRejected(boolean)` | `false` | Fail instead of dropping lines rejected in a partial write |
| `setFailOnValidation(boolean)` | `true` | Fail on local conversion errors and non-partial `400`/`422` |
| `setFailOnError(boolean)` | `true` | Fail on other terminal errors and retry exhaustion |
| `setPrintFailedRequests(boolean)` | `false` | Log failed line protocol, escaped and truncated to 4,096 chars but **not redacted** |
| `setFailureHandler(WriteFailureHandler)` | Built-in policy | Classify eligible failures as `RETRY`, `DROP`, or `FAIL` |

A custom `WriteFailureHandler` replaces `failOnRejected` and `failOnError`,
which cannot be set alongside it. Validation, failed-request logging, and the
`maxRetries` bound remain connector-controlled.

## Delivery and failure semantics

Buffered requests are checkpointed, and in-flight writes resolve before a
checkpoint completes. Writes are not transactional, so delivery is
at-least-once (except for dropped records) and records may be replayed after
recovery.

| Default behavior | Failures |
| --- | --- |
| Retry with backoff and jitter | `408`, `425`, `429`, `500`, `502`, `503`, `504`; connection, socket, DNS, premature EOF, or timeout errors; write deadline with confirmed cancellation |
| Refresh token, retry once, then fail | `401`, `403` |
| Keep accepted lines, drop rejected ones | Partial write with invalid lines |
| Fail the job | Converter exception or invalid output; non-partial `400`/`422`; `413`; write deadline without confirmed cancellation; Flink request timeout; retry exhaustion |
| Terminal-error policy (fail by default) | TLS handshake, peer verification, HTTP protocol, or unclassified I/O errors; other HTTP statuses |

Always fatal, regardless of policy: authentication rejection after refresh,
`413`, connector state or serialization failures, failure-handler errors, and
unconfirmed write cancellation. See
[`DefaultWriteFailureHandler`](src/main/java/software/amazon/timestream/influxdb3/flink/failure/DefaultWriteFailureHandler.java).

## Metrics

Registered per sink subtask, alongside Flink's standard async sink metrics.

| Metric | Type | Description |
| --- | --- | --- |
| `numRecordsWritten` | Counter | Records accepted by InfluxDB |
| `numBytesWritten` | Counter | Uncompressed bytes accepted by InfluxDB |
| `numRecordsSendErrors` | Counter | Record send or validation errors; may recount across retries |
| `numRecordsDropped` | Counter | Records dropped by the failure policy |
| `numPartialWriteFailures` | Counter | Lines rejected in partial writes |
| `numRetries` | Counter | Records submitted for retry |
| `numRetryDelayMillis` | Counter | Total delay scheduled before retries |
| `maxRetryAfterMillis` | Gauge | Largest `Retry-After` received since the writer started |
| `numAuthenticationRefreshes` | Counter | Authentication refreshes |
| `numThrottledRequests` | Counter | Attempts rejected with `429` |
| `numWriteTimeouts` | Counter | Attempts that exceeded the write timeout |
| `numServerErrors`, `numClientErrors` | Counter | Attempts receiving `5xx`, or `4xx` other than `429` (including partial writes) |
| `numConnectionErrors` | Counter | Attempts failing without a response, excluding write timeouts |
| `numBatchesSuccess`, `numBatchesCompletedWithDrops`, `numBatchesRetried`, `numBatchesFailed` | Counter | Batch outcomes; each batch updates exactly one |
| `batchSizeLines`, `batchSizeBytes` | Gauge | Lines and UTF-8 bytes in the latest batch |
| `writeLatencyMillis` | Histogram | Sliding-window write-attempt latency (last 1,024 attempts) |
| `lastWriteLatencyMillis` | Gauge | Latency of the latest write attempt |
| `numBufferedBytes` | Gauge | Bytes currently buffered |
| `numInFlightWriteRequests` | Gauge | Write requests in flight |
| `numActiveClientGenerations` | Gauge | Open client generations |

### Managed Flink CloudWatch metrics

Managed Flink publishes to CloudWatch only its built-in metrics and the
`KinesisAnalytics` group. Enable publishing with
`.setManagedFlinkMetricsEnabled(true)` or
`'sink.metrics.managed-flink.enabled' = 'true'` to also register the metrics
under `KinesisAnalytics` with dimension `Connector=TimestreamInfluxDB3`
(namespace `AWS/KinesisAnalytics`). Changes from the table above:

- Not published: `writeLatencyMillis` (CloudWatch rejects histograms),
  `numActiveClientGenerations`.
- Added: `numBytesOut` (Flink's uncompressed bytes attempted, including
  retries); per-second meters over the last minute for `numRecordsWritten`,
  `numBytesWritten`, `numRecordsDropped`, `numRecordsSendErrors`, and
  `numBatchesFailed` (suffix `PerSecond`); and gauges
  `writeLatencyP50Millis`, `writeLatencyP95Millis`, `writeLatencyP99Millis`.

Notes:

- Counters reset on job restart; alarm on meters, such as
  `numRecordsDroppedPerSecond > 0`.
- Percentiles cover the subtask's last 60 seconds (up to 4,096 attempts) and
  report `0` when idle. Subtask percentiles can't be combined exactly; use the
  `Maximum` statistic as a conservative p95/p99.
- With several InfluxDB sinks in a job, give each a distinct `Sink` dimension
  via `setManagedFlinkMetricsSinkName` or
  `'sink.metrics.managed-flink.sink-name'` (letters, digits, underscores);
  otherwise they share metrics.
- Off by default: each sink adds 32 custom metrics at `APPLICATION` level
  (one set per subtask at `TASK`, `OPERATOR`, or `PARALLELISM`), billed at
  [CloudWatch pricing](https://aws.amazon.com/cloudwatch/pricing/), and other
  platforms would report every metric twice.

## Build and test

Requires JDK 17 and Maven; infrastructure tests also need Node.js 20+ and
Python 3, and the container test needs Docker.

```bash
# Connector unit, packaging, and coverage checks
mvn clean verify

# Example application
mvn clean install
mvn -f example/managed-flink/app/pom.xml clean verify

# Docker-backed end-to-end test (override image with -Dinfluxdb3.test.image=...)
mvn clean verify -Pintegration-tests -Dit.test=InfluxDB3SinkContainerIT

# CDK infrastructure
cd example/managed-flink/infra
npm ci
npm test
npm run synth -- \
  -c applicationJarPath=../app/target/managed-flink-influxdb3-example-0.1.0-SNAPSHOT-all.jar
```

`verify` enforces 80% line and 75% branch coverage (reports in
`target/site/jacoco/`). The [CI workflow](.github/workflows/ci.yml) runs all
of the above.

## License

Licensed under the [Apache License 2.0](LICENSE).
