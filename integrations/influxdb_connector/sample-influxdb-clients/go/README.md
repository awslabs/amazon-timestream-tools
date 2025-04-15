# InfluxDB Timestream Connector Sample Client

## Overview

This directory provides a sample Go client for use with the [InfluxDB Timestream Connector](../../influxdb-timestream-connector/README.md).

By default, the client reads data from `../data/bird-migration.line` and sends [line protocol data](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/) to the InfluxDB Timestream Connector. The client authenticates all requests with [AWS Signature Version 4](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-authenticating-requests.html) (SigV4) authentication. Its code provides an example of how SigV4 authentication can be implemented, since, when deployed as part of a CloudFormation stack, the InfluxDB Timestream Connector requires all requests use SigV4 authentication.

## Configuration

### Prerequisites

1. [Install Go](https://go.dev/doc/install).
2. [Configure credentials to be used for SigV4 authentication](https://docs.aws.amazon.com/sdkref/latest/guide/creds-config-files.html).
3. [Deploy the InfluxDB Timestream Connector](../../influxdb-timestream-connector/README.md#deployment-options).

### Command-Line Options

The following command-line options are available:

- `dataset`: The path to the line protocol dataset being ingested. Defaults to `../data/bird-migration.line`.
- `endpoint`: Endpoint for InfluxDB Timestream Connector. Defaults to `http://127.0.0.1:9000`.
- `precision`: Precision for line protocol: nanoseconds=`ns`, milliseconds=`ms`, microseconds=`us`, seconds=`s`. Defaults to `ns`.
- `region`: AWS region for InfluxDB Timestream Connector. Defaults to `us-east-1`.
- `service`: Service value for SigV4 header. Defaults to `lambda`.

## Example

### With Connector Deployed in a CloudFormation Stack

When the connector is [deployed as a Lambda function within a CloudFormation stack](../../influxdb-timestream-connector/README.md#aws-cloudformation-deployment), run the following, replacing `<region>` with the AWS region you deployed your stack in and `<endpoint>` with the endpoint of your deployed REST API Gateway:

```shell
go run line-protocol-client-demo.go --region <region> --service execute-api --endpoint <endpoint>
```

### With the Connector Deployed Locally

When the connector is [deployed locally](../../influxdb-timestream-connector/README.md#local-deployment), the default command-line option values suffice:

```shell
go run line-protocol-client-demo.go
```
