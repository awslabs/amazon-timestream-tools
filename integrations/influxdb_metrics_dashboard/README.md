# InfluxDB Metrics Dashboard

## Overview

The InfluxDB Metrics Dashboard creates a Grafana dashboard to visualize existing Timestream for InfluxDB instance performance metrics. The application deploys an EC2 instance running Telegraf to scrape the `/metrics` endpoint of one or more Timestream for InfluxDB instances in a VPC, and ingests the scraped metrics to CloudWatch. After the CloudFormation stack has been deployed, a Lambda function creates a Grafana workspace, and uploads the performance metrics dashboard. The Lambda function is run only once during CDK app initialization and uploads the JSON configuration for your Grafana dashboard.

<img src="./images/architecture.png" alt="drawing" width="800"/>

## Demo

![Demo video](./images/demo.gif)

## Configuration

### Prerequisites

1. If not already installed, install the AWS CDK CLI using the [Getting started with the AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) guide.
2. If you don't already have a database instance, create a new Timestream for InfluxDB instance with the [Getting started with Timestream for InfluxDB](https://docs.aws.amazon.com/timestream/latest/developerguide/timestream-for-influx-getting-started.html) guide.

### Context options

The following context options are required when deploying the CDK application:

1. **InfluxDBIds**: The comma separated list of Id(s) for Timestream for InfluxDB instances.

The following context options are optional when deploying the CDK application:

1. **GrafanaWorkspaceName**: The name used for the Grafana workspace. The context defaults to `InfluxDBMetricDashboardWorkspace`.
2. **CloudWatchDatasourceName**: The name of the data source used for populating the dashboard. This context option defaults to `Amazon CloudWatch Data Source`.
3. **DashboardName**: The name used for the Grafana dashboard. The context defaults to `InfluxDB Performance Dashboard`.
4. **TelegrafSshCidr**: The CIDR IP address used in a rule to allow SSH access to the EC2 instance running Telegraf. If the context is not used when deploying the application, no SSH rule will be added to the EC2 instance security group.
5. **EnableHighResolutionMetrics**: Set to `true` to ingest metrics to CloudWatch with an interval of every 10 seconds. The default ingestion rate is set to 1 minute when this context is not enabled.
6. **TelegrafEc2Tags**: List of tags to apply to the EC2 instance running Telegraf in the format `"key1:val1,key2:val2"`.
7. **GrafanaWorkspaceTags**: List of tags to apply to the Grafana workspace in the format `"key1:val1,key2:val2"`.

## Getting started

To deploy the InfluxDB Metrics Dashboard application, use the following CDK commands, and populate all required context options in the deploy command:

**note**: To deploy the stack in another region other than the default configured in your `~/.aws/credentials` file, set the environment variable `AWS_REGION` to the target deployment region.

1. Provision AWS environment with the following command:
```shell
cdk bootstrap --context InfluxDBIds="{influxdb_ids}"
```
2. Deploy the application with the following command:
```shell
cdk deploy --context InfluxDBIds="{influxdb_ids}"
```
## Viewing the dashboard

### Creating a new IAM Identity user

If you do not have an AWS IAM Identity user, complete the following steps to create a new user:

1. Go to the [IAM Identity Center console](https://console.aws.amazon.com/singlesignon/home).
2. In the navigation pane, choose **Users**.
3. Choose **Add user**.
4. Input user details.
5. Choose **Next**.
6. Add the user to a group if you wish.
7. Choose **Next**.
8. Choose **Add user**.

### Adding IAM Identity User to Workspace

Now that the dashboard has been deployed, you will need to add your AWS IAM Identity for the Grafana workspace. Complete the following steps in the AWS console to add your user:

1. Go to the [Amazon Managed Grafana console](https://console.aws.amazon.com/grafana/home).
2. In the navigation pane, choose **All workspaces**.
3. From the list of workspaces, choose the workspace named `InfluxDBMetricDashboardWorkspace`. If you altered the context for the workspace name during deployment, choose the altered workspace name.
4. in the **Authentication** tab, under **AWS IAM Identity Center (successor to AWS SSO)** choose **Assign new user or group**.
5. From the list of users, select the user you want to give access to the dashboard, and choose **Assign users and groups**.
6. Choose up one level in the navigation hierarchy to return to the dashboard workspace.
7. Choose the **Grafana workspace URL**.
8. Sign in with the **AWS IAM Identity Center** user you assigned as a viewer for the dashboard.
9. Choose **Dashboards** in the left navigation pane.
10. Choose the dashboard named **InfluxDB Performance Dashboard**, or alternative name if you altered the context when deploying the application.
11. You should now be able to view a dashboard showcasing the metrics for your Timestream for InfluxDB instance.

## Cleanup

To cleanup AWS resources created by the application during deployment, execute the following command:

```shell
cdk destroy --context InfluxDBIds="{influxdb_ids}"
```
