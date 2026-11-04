# Changelog

All notable changes to the Apache Flink Connector for Amazon Timestream for
InfluxDB 3 are documented in this file. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-11-02

Initial release, published to
[Maven Central](https://central.sonatype.com/artifact/software.amazon.timestream/flink-connector-timestream-influxdb3/0.1.0)
as `software.amazon.timestream:flink-connector-timestream-influxdb3:0.1.0`.
Requires Java 17+ and Apache Flink 2.3.x; built on `influxdb3-java` 1.10.0.

### Added

- `InfluxDB3Sink` DataStream sink, built on Flink's async sink, that writes
  line protocol to the `/api/v3/write_lp` endpoint over HTTPS.
- Insert-only Table API and SQL sink, `'connector' = 'timestream-influxdb3'`,
  with tag columns, a timestamp column, and configurable precision.
- Pluggable `TokenProvider` authentication, including
  `SecretsManagerTokenProvider` for AWS Secrets Manager (plain or JSON
  secrets) and automatic token refresh on `401`/`403`.
- Configurable batching, byte- and row-based backpressure, gzip compression,
  partial writes, and no-sync writes.
- Exponential backoff with jitter, `Retry-After` support, adaptive in-flight
  request limits, and a pluggable `WriteFailureHandler` to retry, drop, or fail
  on errors.
- At-least-once delivery through checkpointed buffered requests.
- Per-subtask sink metrics, with optional publishing to Amazon CloudWatch
  through Amazon Managed Service for Apache Flink.
- Managed Flink example: a Kinesis-to-InfluxDB 3 application and a CDK app
  that deploys it with logging, a dashboard, an alarm, and automatic start and
  stop.

[0.1.0]: https://github.com/awslabs/amazon-timestream-tools/tree/mainline/integrations/flink_connector/flink-connector-influxdb
