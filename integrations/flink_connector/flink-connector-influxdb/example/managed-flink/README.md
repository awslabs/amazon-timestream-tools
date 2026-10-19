# Managed Flink Connector Example

This Flink application demonstrates the following data path:

```text
Kinesis Data Streams -> Managed Flink -> Timestream for InfluxDB 3
```

The application does not generate records. It runs one of two pipelines,
selected by the `pipeline` runtime property:

| Pipeline | Kinesis record | Sink |
| --- | --- | --- |
| `datastream` (default) | One line protocol row, passed unchanged | DataStream `InfluxDB3Sink` |
| `sql` | One JSON CPU reading | Flink SQL `timestream-influxdb3` table |

The SQL pipeline expects records such as:

```json
{"host":"web-1","region":"us-east-1","usage":41.7,"ts":"2026-10-08T19:00:04.120Z"}
```

The conversion has two steps. Flink's `json` format parses each record into a
row of the `readings` table, and the `timestream-influxdb3` sink writes each
row of the `cpu` table as one point:

| JSON field | `readings` column | Point |
| --- | --- | --- |
| `host` | `host STRING` | Tag |
| `region` | `region STRING` | Tag |
| `usage` | `usage DOUBLE` | Float field |
| `ts` (ISO-8601, UTC `Z`) | `ts TIMESTAMP_LTZ(3)` | Timestamp, in milliseconds |

The measurement is `cpu`, the name of the sink table. The record above becomes
this line protocol:

```text
cpu,host=web-1,region=us-east-1 usage=41.7 1791486004120
```

JSON fields not listed are ignored. A missing `host` or `region` omits that
tag. A missing `usage` or `ts`, or a record that is not valid JSON, fails the
job. To write other tags or fields, add columns to both tables and the
`INSERT` in `SqlPipeline.java`; to write another measurement, rename the `cpu`
table there. The connector README describes the full column-to-point rules.

## Layout

```text
example/managed-flink/
├── app/      # Java Flink application and Maven packaging
├── infra/    # TypeScript CDK deployment and lifecycle management
└── README.md
```

The application and infrastructure are separate build projects owned by the
same example. Infrastructure consumes a supplied application fat JAR; it does
not build the application or the connector.

## Relationship To The Connector

The connector project at
`integrations/flink_connector/flink-connector-influxdb/` is a Maven library.
This application is a consumer of that library:

```xml
<dependency>
    <groupId>software.amazon.timestream</groupId>
    <artifactId>flink-connector-timestream-influxdb3</artifactId>
    <version>0.1.0-SNAPSHOT</version>
</dependency>
```

During development, `mvn install` places the connector in the local Maven
repository. After release, the same dependency is resolved from Maven Central.
The application's Maven Shade configuration embeds the connector and its
runtime dependencies in the deployable application JAR. Flink itself and
`aws-kinesisanalytics-runtime` remain `provided` by Managed Flink.

The application fat JAR has the `main()` entry point and is the artifact
uploaded to S3.

## 1. Build

From `integrations/flink_connector/flink-connector-influxdb/`, install the
connector and package the application:

```bash
mvn clean install
mvn -f example/managed-flink/app/pom.xml clean verify
```

The deployable artifact is:

```text
example/managed-flink/app/target/
  managed-flink-influxdb3-example-0.1.0-SNAPSHOT-all.jar
```

## 2. Create The Input Stream

```bash
aws kinesis create-stream \
  --stream-name influxdb3-example-input \
  --stream-mode-details StreamMode=ON_DEMAND \
  --region us-east-1

aws kinesis wait stream-exists \
  --stream-name influxdb3-example-input \
  --region us-east-1

aws kinesis describe-stream-summary \
  --stream-name influxdb3-example-input \
  --region us-east-1 \
  --query 'StreamDescriptionSummary.StreamARN' \
  --output text
```

Use the returned ARN as `KinesisSource.streamArn`.

## 3. Use The Cluster Authentication Secret

Timestream for InfluxDB 3 creates a regional Secrets Manager secret containing
the cluster operator token. Use that generated secret as `secretId` and
configure `secretJsonField=token`.

Do not put the token in application runtime properties, source code, the JAR,
CloudFormation outputs, or deployment logs. Grant the Managed Flink execution
role access only to the generated cluster secret.

When the application runs in private subnets, create a Secrets Manager interface
VPC endpoint with private DNS enabled. Its security group must allow inbound TCP
443 from the Flink application's security group.

## 4. Upload The Application JAR

Use an S3 bucket in the same Region as the Managed Flink application:

```bash
aws s3 cp \
  example/managed-flink/app/target/managed-flink-influxdb3-example-0.1.0-SNAPSHOT-all.jar \
  s3://YOUR_BUCKET/flink/managed-flink-influxdb3-example.jar \
  --region us-east-1
```

Upload only the application `-all.jar`. Do not upload the connector JAR from
`integrations/flink_connector/flink-connector-influxdb/target/` as the
application code.

## 5. Create The Managed Flink Application

Use these application settings:

| Setting | Value |
| --- | --- |
| Runtime | Apache Flink 2.3.0 |
| Application code | The S3 object uploaded above |
| Main class | `software.amazon.timestream.influxdb3.flink.examples.KinesisToInfluxDB3Application` |
| Parallelism | 1 for the example |
| Automatic scaling | Disabled for the example |

## 6. Configure Runtime Properties

Optionally add property group `Application`:

| Key | Value |
| --- | --- |
| `pipeline` | `datastream` (default), `sql`, or `datagen`; see [Load testing with generated data](#load-testing-with-generated-data) |

Add property group `KinesisSource`:

| Key | Value |
| --- | --- |
| `streamArn` | ARN returned in step 2 |
| `region` | `us-east-1` |
| `initialPosition` | `LATEST` |

Add property group `InfluxDB3`:

| Key | Value |
| --- | --- |
| `endpoint` | Cluster endpoint including port, without an API path |
| `database` | `sensors` |
| `secretId` | Cluster-generated Secrets Manager secret ARN |
| `secretJsonField` | `token` |
| `region` | Secret region; defaults to the Kinesis region |

Optionally add property group `Sink` to tune the connector without rebuilding
the application. Every key is optional; unset keys keep the connector
defaults. The keys match the SQL sink options and apply to both pipelines.

| Key | Connector default | Value |
| --- | --- | --- |
| `sink.batch.max-size` | 10,000 | Maximum lines in one write request |
| `sink.requests.max-buffered` | The larger of 20,000 and `sink.batch.max-size` + 1 | Maximum lines buffered per subtask; must exceed `sink.batch.max-size` |
| `sink.requests.max-inflight` | 8 | Concurrent write requests per subtask |
| `sink.flush-buffer.size` | 10,485,760 | Maximum uncompressed bytes in one write request |
| `sink.flush-buffer.timeout` | 5,000 | Maximum milliseconds a partial batch waits before it is sent |
| `sink.max-retries` | 8 | Retries after the initial write attempt |

Unknown keys and non-integer values fail application startup. Changing the
group with `aws kinesisanalyticsv2 update-application` restarts the
application with the new values; a later CDK deployment replaces the property
groups with the ones the stack defines.

`initialPosition` also accepts `TRIM_HORIZON` and `AT_TIMESTAMP`. When using
`AT_TIMESTAMP`, add `initialTimestamp` in
`yyyy-MM-dd'T'HH:mm:ss.SSSXXX` format.

For local execution, copy `application-properties.example.json`, replace its
placeholders, and set `APPLICATION_PROPERTIES_FILE` to its absolute path.

## 7. Grant Execution-Role Access

The Managed Flink execution role needs:

- `s3:GetObject` and `s3:GetObjectVersion` for the application JAR
- `kinesis:DescribeStream`, `kinesis:DescribeStreamSummary`,
  `kinesis:ListShards`, `kinesis:GetShardIterator`, and `kinesis:GetRecords`
  for the input stream
- `secretsmanager:GetSecretValue` for the exact token secret
- `kms:Decrypt` for the secret key when using a customer-managed KMS key
- the standard Managed Flink CloudWatch Logs permissions

If the InfluxDB endpoint is private, attach the application to reachable VPC
subnets and security groups. The application still needs routes to Kinesis,
S3, Secrets Manager, and CloudWatch through NAT or VPC endpoints.

## 8. Start And Send A Record

Start the Managed Flink application first because the default Kinesis position
is `LATEST`. Then send one line protocol record:

```bash
aws kinesis put-record \
  --stream-name influxdb3-example-input \
  --partition-key example-1 \
  --data 'flink_connector_example,source=managed_flink value=1i' \
  --cli-binary-format raw-in-base64-out \
  --region us-east-1
```

For the SQL pipeline, send one JSON reading instead:

```bash
aws kinesis put-record \
  --stream-name influxdb3-example-input \
  --partition-key web-1 \
  --data '{"host":"web-1","region":"us-east-1","usage":41.7,"ts":"2026-10-08T19:00:04.120Z"}' \
  --cli-binary-format raw-in-base64-out \
  --region us-east-1
```

The connector flushes a partial batch after its default five-second buffering
interval.

## 9. Verify

Check that:

1. The Managed Flink application remains in `RUNNING` state.
2. CloudWatch logs contain no authentication, networking, or write errors.
3. The sink metric `numRecordsWritten` increases. The application enables
   `setManagedFlinkMetricsEnabled(true)`, so the sink metrics appear in
   CloudWatch under `AWS/KinesisAnalytics` with the dimensions
   `Application=<application name>` and `Connector=TimestreamInfluxDB3`.
4. This query returns the example point:

```sql
SELECT *
FROM flink_connector_example
WHERE source = 'managed_flink'
ORDER BY time DESC
LIMIT 10;
```

For the SQL pipeline, query the `cpu` measurement:

```sql
SELECT *
FROM cpu
WHERE host = 'web-1'
ORDER BY time DESC
LIMIT 10;
```

## Automated CDK Deployment

The `infra/` project creates:

- An on-demand Kinesis stream.
- An S3 asset containing the supplied application fat JAR.
- The Managed Flink execution role and CloudWatch logging.
- The `AWS::KinesisAnalyticsV2::Application`.
- A custom resource that starts the application and waits for `RUNNING`.
- A CloudWatch dashboard and dropped-records alarm for the sink. See
  [Monitoring](#monitoring).

It does not create an InfluxDB cluster, database, secret, VPC, subnet, security
group, NAT gateway, or VPC endpoint.

The infrastructure requires Node.js 20, AWS credentials, a bootstrapped CDK
environment, an existing InfluxDB 3 database and authentication secret, and a
previously built or downloaded application fat JAR. Java and Maven are only
required when producing that JAR locally.

From `integrations/flink_connector/flink-connector-influxdb/`, install and test
the infrastructure:

```bash
cd example/managed-flink/infra
npm ci
npm test
npm run synth -- \
  -c applicationJarPath=../app/target/managed-flink-influxdb3-example-0.1.0-SNAPSHOT-all.jar
```

Supply these CloudFormation parameters when deploying:

| Parameter | Required value |
| --- | --- |
| `InfluxDbEndpoint` | Base endpoint including scheme and port, without an API path |
| `InfluxDbSecretArn` | Secrets Manager ARN containing the token |

These CDK context values control deployment:

| Context key | Default | Purpose |
| --- | --- | --- |
| `applicationJarPath` | Required | Local application fat JAR |
| `applicationName` | `influxdb3-flink-example` | Managed Flink application name |
| `databaseName` | `sensors` | Existing InfluxDB 3 database |
| `flinkRuntime` | `FLINK-2_3` | Managed Flink runtime |
| `pipeline` | `datastream` | Application pipeline: `datastream` or `sql` |
| `influxDbTokenField` | `token` | Token field in the secret JSON |
| `influxDbSecretKmsKeyArn` | none | Customer-managed KMS key for the secret |
| `influxDbClusterName` | none | Timestream for InfluxDB 3 cluster name for the dashboard's cluster panel |
| `pipeline` | `datastream` | `datastream`, `sql`, or `datagen`; `datagen` creates no Kinesis stream |
| `parallelism` | `1` | Application parallelism |
| `parallelismPerKpu` | `1` | Parallel tasks per KPU, from 1 to 8 |
| `checkpointingEnabled` | `false` | Enables checkpointing |
| `checkpointIntervalMillis` | `60000` | Checkpoint interval when checkpointing is enabled |
| `dataGenProperties` | none | JSON object of `DataGen` runtime properties; `datagen` pipeline only |
| `sinkProperties` | none | JSON object of `Sink` runtime properties |
| `flinkSubnetIds` | none | Comma-separated existing subnet IDs |
| `flinkSecurityGroupIds` | none | Comma-separated existing security-group IDs |

For a publicly reachable endpoint:

```bash
export AWS_REGION=us-east-1
export CDK_DEFAULT_REGION="$AWS_REGION"

npx cdk bootstrap \
  -c applicationJarPath=../app/target/managed-flink-influxdb3-example-0.1.0-SNAPSHOT-all.jar

npm run deploy -- \
  -c applicationJarPath=../app/target/managed-flink-influxdb3-example-0.1.0-SNAPSHOT-all.jar \
  --parameters ManagedFlinkInfluxDB3ExampleStack:InfluxDbEndpoint=https://YOUR_ENDPOINT:8086 \
  --parameters ManagedFlinkInfluxDB3ExampleStack:InfluxDbSecretArn=arn:aws:secretsmanager:us-east-1:123456789012:secret:YOUR_SECRET
```

For a private endpoint, also supply reachable subnets and security groups:

```bash
npm run deploy -- \
  -c applicationJarPath=../app/target/managed-flink-influxdb3-example-0.1.0-SNAPSHOT-all.jar \
  -c flinkSubnetIds=subnet-11111111,subnet-22222222 \
  -c flinkSecurityGroupIds=sg-11111111 \
  --parameters ManagedFlinkInfluxDB3ExampleStack:InfluxDbEndpoint=https://YOUR_PRIVATE_ENDPOINT:8086 \
  --parameters ManagedFlinkInfluxDB3ExampleStack:InfluxDbSecretArn=arn:aws:secretsmanager:us-east-1:123456789012:secret:YOUR_SECRET
```

To run the SQL pipeline, add `-c pipeline=sql` to either deploy command:

```bash
npm run deploy -- \
  -c applicationJarPath=../app/target/managed-flink-influxdb3-example-0.1.0-SNAPSHOT-all.jar \
  -c pipeline=sql \
  --parameters ManagedFlinkInfluxDB3ExampleStack:InfluxDbEndpoint=https://YOUR_ENDPOINT:8086 \
  --parameters ManagedFlinkInfluxDB3ExampleStack:InfluxDbSecretArn=arn:aws:secretsmanager:us-east-1:123456789012:secret:YOUR_SECRET
```

Both networking context values must be supplied together. The existing network
must provide DNS and a route to the InfluxDB endpoint, endpoint ingress from
the Flink security group, and access to S3, Kinesis, Secrets Manager, and
CloudWatch Logs through NAT or VPC endpoints.

When the secret uses a customer-managed KMS key, also pass:

```bash
-c influxDbSecretKmsKeyArn=arn:aws:kms:REGION:ACCOUNT:key/KEY_ID
```

Use the `KinesisStreamName` stack output to send a test record after deployment
reaches `CREATE_COMPLETE`:

```bash
aws kinesis put-record \
  --stream-name YOUR_STREAM_NAME \
  --partition-key example-1 \
  --data 'flink_connector_example,source=cdk value=1i' \
  --cli-binary-format raw-in-base64-out \
  --region "$AWS_REGION"
```

For the SQL pipeline, send a JSON reading such as
`{"host":"web-1","region":"us-east-1","usage":41.7,"ts":"2026-10-08T19:00:04.120Z"}`.

Destroy the environment while the supplied JAR is still available:

```bash
npm run destroy -- \
  -c applicationJarPath=../app/target/managed-flink-influxdb3-example-0.1.0-SNAPSHOT-all.jar
```

## Monitoring

The CDK stack creates a CloudWatch dashboard named
`<applicationName>-influxdb3-sink`, output as `SinkDashboardName`, with these
panels:

| Panel | Metrics |
| --- | --- |
| Throughput | `numRecordsWrittenPerSecond`; application `numRecordsInPerSecond` |
| Write latency (ms) | `writeLatencyP50Millis`, `writeLatencyP95Millis`, `writeLatencyP99Millis` |
| Dropped records and failures | `numRecordsDroppedPerSecond`, `numRecordsSendErrorsPerSecond`, `numBatchesFailedPerSecond` |
| Throttling and retries | `numThrottledRequests`, `numRetries`, `numWriteTimeouts` running totals; `numRetryDelayMillis` total and `maxRetryAfterMillis` |
| Buffered bytes and in-flight requests | `numBufferedBytes`, `numInFlightWriteRequests` |
| Job health | `heapMemoryUtilization`, `cpuUtilization`, busiest task `busyTimeMsPerSecond` as a percentage, `numRestarts`, `numberOfFailedCheckpoints` |
| Bytes written | `numBytesWrittenPerSecond`; `numBytesWritten` and `numBytesOut` totals |
| Batch size | `batchSizeLines`, `batchSizeBytes` |
| Batch outcomes | `numBatchesSuccess`, `numBatchesRetried`, `numBatchesCompletedWithDrops`, `numBatchesFailed` totals |
| Write errors by type | `numServerErrors`, `numClientErrors`, `numThrottledRequests`, `numConnectionErrors`, `numWriteTimeouts` totals |
| JVM memory (%) | `heapMemoryUtilization` highest and lowest (after garbage collection), `metaspaceMemoryUtilization` |
| Threads and file descriptors | `threadsCount`, `taskmanagerFileDescriptorsOpen` |
| Old-generation GC and checkpoints (ms) | `oldGenerationGCTime`, `lastCheckpointDuration` |
| InfluxDB cluster | Cluster node `CPUUtilization` and `MemoryUtilization`; only when `influxDbClusterName` is set |

Each subtask publishes its own data point about every 15 seconds. The rate
and counter panels therefore divide the `Sum` over each minute by the
`SampleCount` of the job-level `uptime` metric, which gives job-wide values at
any parallelism. Counters are running totals that reset when the job restarts.

It also creates an alarm, output as `DroppedRecordsAlarmName`, that enters
`ALARM` when `numRecordsDroppedPerSecond` is above zero for one minute. Missing
data, such as while the application is stopped, is not treated as breaching.
The alarm has no actions; add an SNS topic or other action to be notified.

The Kinesis source and the sink run as one chained Flink task, so a slow sink
or InfluxDB endpoint shows as rising task busy time rather than as
backpressure, which stays at zero for this job.

The dashboard and alarm match the `APPLICATION` metrics level set on the
application. If you change `FLINK_METRICS_LEVEL` in
`infra/lib/sink-monitoring.ts`, Managed Flink adds dimensions and the
dashboard and alarm no longer match the published metrics.

The first three CloudWatch dashboards in an account are free; see
[CloudWatch pricing](https://aws.amazon.com/cloudwatch/pricing/) for
dashboard and custom metric costs.

## Load testing with generated data

The `datagen` pipeline generates line protocol inside the job with Flink's
`DataGeneratorSource` and writes it through the same sink, without Kinesis.
Use it to measure the connector and the InfluxDB endpoint without a producer or
a stream limiting the result.

Configure it with the optional `DataGen` property group:

| Key | Default | Value |
| --- | --- | --- |
| `ratePerSecond` | `0` | Rows per second across all subtasks; `0` is unlimited |
| `totalRecords` | Unbounded | Rows to generate before the job finishes |
| `seriesCardinality` | `1000` | Distinct `host` tag values |
| `fieldsPerLine` | `4` | Float fields per row, from 1 to 100 |
| `measurement` | `loadtest` | Measurement name |
| `runId` | `default` | `run` tag value, to tell runs apart in queries |

Rows look like:

```text
loadtest,run=r1,host=host-42 f0=13.02,f1=92.21,f2=71.40,f3=50.59 1760000000042000000
```

Fields and timestamps derive from the row's sequence number. Timestamps start
when the job is built and advance by one second divided by `ratePerSecond`, or
by 1 µs when unlimited, so every row is unique.

```bash
npm run deploy -- \
  -c applicationJarPath=../app/target/managed-flink-influxdb3-example-0.1.0-SNAPSHOT-all.jar \
  -c pipeline=datagen \
  -c parallelism=4 \
  -c checkpointingEnabled=true \
  -c dataGenProperties='{"ratePerSecond":"20000","runId":"p4-b10k"}' \
  -c sinkProperties='{"sink.batch.max-size":"10000"}' \
  --parameters ManagedFlinkInfluxDB3ExampleStack:InfluxDbEndpoint=https://YOUR_ENDPOINT:8181 \
  --parameters ManagedFlinkInfluxDB3ExampleStack:InfluxDbSecretArn=arn:aws:secretsmanager:us-east-1:123456789012:secret:YOUR_SECRET
```

- Enable checkpointing. Without it, a job restart regenerates rows from the
  first sequence number; they overwrite themselves, but `numRecordsWritten`
  counts them again.
- Updating, stopping, or starting the application restarts generation with new
  timestamps, adding new points. Use a new `runId` for each such deployment.
- Switching an existing stack to `pipeline=datagen` deletes its Kinesis stream,
  including unread records.
- Use a dedicated database; generated rows go to the configured one.
