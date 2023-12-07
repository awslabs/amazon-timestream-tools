# Amazon Timestream - Kafka Sink Connector

The Timestream Kafka Sink Connector is designed to work with Kafka Connect and to be deployed to a Kafka Connect cluster.
The deployed connector polls for events from a source Kafka topic, and ingests them as records to a target Timestream table.

[Amazon Timestream](https://aws.amazon.com/timestream/) is a fast, scalable, and serverless time series database service that makes it straightforward to store and analyze trillions 
of events per day for use cases like monitoring hundreds of millions of Internet of Things (IoT) devices, industrial equipment, 
gaming sessions, streaming video sessions, and more.

You can securely scale your streaming data platform with hundreds and thousands of Kafka clusters using [Amazon Managed Streaming for Apache Kafka](https://aws.amazon.com/msk/) (Amazon MSK), 
a fully managed service to build and run applications to process streaming data, which simplifies the setup, scaling, 
and management of clusters running Kafka. And [Amazon MSK Connect](https://aws.amazon.com/msk/features/msk-connect/) enables you to deploy, monitor, and automatically scale connectors that move data between your MSK or Kafka clusters and external systems.

### Solution Overview
Here is the overview of how the connector can be deployed in an Amazon MSK connect that can be configured to receive messages from a Kafka topic from an Amazon MSK cluster.

![Data Flow - Timestream Sink Connector.png](resources%2FData%20Flow%20-%20Timestream%20Sink%20Connector.png)

At startup, the connector loads the target Timestream table's schema definition that is used for validating the incoming messages from the source Kafka topic before inserting to the Timestream table.
It supports loading the definition as JSON file from a configured [Amazon Simple Storage Service (Amazon S3)](http://aws.amazon.com/s3) bucket. See [sample schema definition ](#sample-schema-definition) section for schema definition file format.

_Note:_ While creating a table in Timestream, you do not need to define the schema up front as Timestream automatically detects the schema based on the data points being sent. But the connector needs an upfront schema definition for validation purpose. 

Once the connector is deployed, the data flow starts with a Kafka producer client that publishes messages to the source Kafka topic. 
As data arrives, an instance of the Timestream Sink Connector for Apache Kafka validates the incoming messages basis the schema definition and writes them as records in the target Timestream table, as shown in the above diagram.

### Connector Configuration parameters

The following table lists the complete set of the Timestream Kafka Sink Connector configuration properties

| #  | Key                                    | <div style="width:350px">Description</div>                                                                                    | <div style="width:350px">Remarks</div>                                                                                                                                                                           | Required | Default |
|----|----------------------------------------|-------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------|---------|
| 1  | connector.class                        | Specifies the name of the connector class                                                                                     | Must be mentioned as "software.amazon.timestream.TimestreamSinkConnector"                                                                                                                                        | Yes      | NONE    |
| 2  | tasks.max                              | The maximum number of active tasks for a sink connector                                                                       | Non negative number                                                                                                                                                                                              | Yes      | NONE    |
| 3  | aws.region                             | The region in which the AWS service resources are provisioned                                                                 | Example: "us-east-1"; see [here](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.RegionsAndAvailabilityZones.html) for the list of regions                                                       | Yes      | NONE    |
| 4  | topics                                 | Name of the Kafka topic which needs to be polled for messages                                                                 |                                                                                                                                                                                                                  | Yes      | NONE    |
| 5  | timestream.schema.s3.bucket.name       | Name of the Amazon S3 bucket in which the target Timestream table's schema definition is present                              |                                                                                                                                                                                                                  | Yes      | NONE    |
| 6  | timestream.schema.s3.key               | S3 object key of the targeted Timestream table schema                                                                         |                                                                                                                                                                                                                  | Yes      | NONE    |
| 7  | timestream.database.name               | Name of the Timestream database where the table exists                                                                        | See [Create a database](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.db.using-console) for details                                                    | Yes      | NONE    |
| 8  | timestream.table.name                  | Name of the Timestream table where the events will be ingested as records                                                     | See [Create a table](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.table.using-console) for details                                                    | Yes      | NONE    |
| 9  | timestream.ingestion.endpoint          | Ingestion endpoint for Timestream, in URI format                                                                              | Example: https://ingest-cell1.timestream.ap-southeast-2.amazonaws.com; see [here](https://docs.aws.amazon.com/timestream/latest/developerguide/VPCEndpoints.html) for details                                    | Yes      | NONE    |
| 10 | timestream.connections.max             | The maximum number of allowed concurrently opened HTTP connections to the Timestream service.                                 | See [Write SDK client](https://docs.aws.amazon.com/timestream/latest/developerguide/code-samples.write-client.html) for further details                                                                          | No       | 5000    |
| 11 | timestream.connections.timeoutseconds  | The time in seconds the AWS SDK will wait for a query request before timing out. Non-positive value disables request timeout. | See [Write SDK client](https://docs.aws.amazon.com/timestream/latest/developerguide/code-samples.write-client.html) for recommended values                                                                       | No       | 20      |
| 12 | timestream.connections.retries         | The maximum number of retry attempts for retryable errors with 5XX error codes in the SDK. The value must be non-negative.    | See [Write SDK client](https://docs.aws.amazon.com/timestream/latest/developerguide/code-samples.write-client.html) for recommended values                                                                       | No       | 10      |
| 13 | timestream.record.batch.size           | The maximum number of records in a WriteRecords API request.                                                                  |                                                                                                                                                                                                                  | No       | 100     |
| 14 | timestream.record.versioning.auto      | Enable if upserts are required. By default the version is set to 1                                                            | See [WriteRecords](https://docs.aws.amazon.com/timestream/latest/developerguide/API_WriteRecords.html) for further details                                                                                       | No       | false   |
| 15 | timestream.record.dimension.skip.empty | When a dimension value is not present/ empty, only that dimension would be skipped by default.                                | If disabled, it would be logged as error and the whole record would be skipped. See [Amazon Timestream concepts](https://docs.aws.amazon.com/timestream/latest/developerguide/concepts.html) for further details | No       | true    |
| 16 | timestream.record.measure.skip.empty   | When a measure value is not present/ empty, only that measure would be skipped by default.                                    | If disabled, it would be logged as error and the whole record would be skipped. See [Amazon Timestream concepts](https://docs.aws.amazon.com/timestream/latest/developerguide/concepts.html) for further details | No       | true    |

#### Sample: Connector Configuration

```properties
aws.region=ap-southeast-2
connector.class=software.amazon.timestream.TimestreamSinkConnector
tasks.max=2
topics=purchase-history
timestream.schema.s3.bucket.name=msk-timestream-ap-southeast-2-plugins-bucket
timestream.schema.s3.key=purchase_history.json
timestream.database.name=kafkastream
timestream.ingestion.endpoint=https://ingest-cell1.timestream.ap-southeast-2.amazonaws.com
timestream.table.name=purchase-history
```

### Worker Configuration parameters

A worker is a Java virtual machine (JVM) process that runs the connector logic. See [Workers](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-workers.html) for additional details.
The connector supports [JsonConverter](https://github.com/a0x8o/kafka/blob/master/connect/json/src/main/java/org/apache/kafka/connect/json/JsonConverter.java) for both key and values which is to be configured in the worker configuration as shown below.

```properties
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```

#### Sample Schema Definition

The connector supports [multi-measure](https://docs.aws.amazon.com/timestream/latest/developerguide/writes.html#writes.writing-data-multi-measure) schema mapping - the preferred approach, which stores each measure value in a dedicated column.
The table below shows mapping of multi-measure records for the sample dataset [purchase_history.csv](https://aws-blogs-artifacts-public.s3.amazonaws.com/DBBLOG-3618/purchase_history.csv), 
that has the following headings to map to a target column in a Timestream table.

| #  | Source Column | Target Column Name | Timestream Attribute Type | Data Type |
|----|---------------|--------------------|---------------------------|-----------|
| 1  | current_time  | current_time       | TIMESTAMP                 | TIMESTAMP |
| 2  | user_id       | user_id            | DIMENSION                 | VARCHAR   |
| 3  | product       | product            | DIMENSION                 | VARCHAR   |
| 4  | ip_address    | ip_address         | MULTI                     | VARCHAR   |
| 5  | session_id    | session_id         | MULTI                     | VARCHAR   |
| 6  | event         | event              | MULTI                     | VARCHAR   |
| 7  | user_group    | user_group         | MULTI                     | VARCHAR   |
| 8  | query         | query              | MULTI                     | VARCHAR   |
| 9  | product_id    | product_id         | MULTI                     | VARCHAR   |
| 10 | quantity      | quantity           | MULTI                     | BIGINT    |
| 11 | channel       | channel            | MEASURE_NAME              | -         |

Refer [purchase_history.json](https://aws-blogs-artifacts-public.s3.amazonaws.com/DBBLOG-3618/purchase_history.json) for representing the schema model in JSON file format; see [Data model mappings](https://docs.aws.amazon.com/timestream/latest/developerguide/batch-load-data-model-mappings.html#batch-load-data-model-mappings-example-multi) for additional details.

## Connector - Build from source
**Prerequisite**
* JDK >= 1.8
* Maven
  Execute the below script to get the github repo cloned, and to build the Timestream Kafka Sink connector

```shell
git clone https://github.com/awslabs/amazon-timestream-tools.git
cd ./amazon-timestream-tools/integrations/kafka_connector
mvn clean package
```
Check out for the built jar within target folder from the current directory

## Security: 
### Identity and Access Management
Amazon MSK uses an [AWS Identity and Access Management (IAM) role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) for all permissions that the connector needs like reading te schema definition from the S3 bucket, ingesting to Timestream table and more.
A service role is an IAM role that a service, in this case the MSK Connect can assume to perform actions on your behalf. 
In this section, we discuss the steps to create an IAM service role for the connector with the IAM policies attached to it.

#### IAM Policy with Permissions

1. On the IAM console, choose **Policies** in the left navigation menu and then choose **Create policy**
2. Choose **JSON** tab, and paste the below IAM policy JSON template in the **Policy editor** section

<details>
  <summary>Expand: Policy JSON Template</summary>

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "timestream0",
      "Effect": "Allow",
      "Action": [
        "timestream:*"
      ],
      "Resource": [
        "arn:aws:timestream:REGION_NAME:ACCOUNT_NUMBER:database/DATABASE_NAME",
        "arn:aws:timestream:REGION_NAME:ACCOUNT_NUMBER:database/DATABASE_NAME/*"
      ]
    },
    {
      "Sid": "timestream1",
      "Effect": "Allow",
      "Action": [
        "timestream:DescribeEndpoints"
      ],
      "Resource": "*"
    },
    {
      "Sid": "s30",
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::BUCKET_NAME",
        "arn:aws:s3:::BUCKET_NAME/*"
      ]
    },
    {
      "Sid": "msk0",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:DescribeGroup",
        "kafka-cluster:AlterGroup",
        "kafka-cluster:Connect",
        "kafka-cluster:DescribeCluster",
        "kafka-cluster:*Topic*",
        "kafka-cluster:WriteData",
        "kafka-cluster:WriteDataIdempotently",
        "kafka-cluster:ReadData",
        "kafka-cluster:DescribeClusterDynamicConfiguration"
      ],
      "Resource": [
        "arn:aws:kafka:REGION_NAME:ACCOUNT_NUMBER:cluster/MSK_CLUSTER_NAME/*",
        "arn:aws:kafka:REGION_NAME:ACCOUNT_NUMBER:topic/MSK_CLUSTER_NAME/*/*",
        "arn:aws:kafka:REGION_NAME:ACCOUNT_NUMBER:group/MSK_CLUSTER_NAME/*/*"
      ]
    },
    {
      "Sid": "cloudwatch0",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:REGION_NAME:ACCOUNT_NUMBER:/LOG_GROUP_NAME/*"
      ]
    }
  ]
}
```
</details>

3. Replace REGION_NAME, ACCOUNT_NUMBER, BUCKET_NAME, MSK_CLUSTER_NAME, DATABASE_NAME, and LOG_GROUP_NAME with the respective values from your AWS environment.
4. Choose **Next**, give a name for the **Policy name** and then choose **Create policy**

#### IAM Service Role

1. On the IAM console, choose **Roles** from the left navigation pane 
2. Choose the **Custom trust policy** role type.
3. In the **Custom trust policy** section, paste the below JSON content

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": "kafkaconnect.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        }
    ]
}
```
5. Choose **Next** 
6. In the **Permissions policies** section, select the policy that is created in the previous section and choose **Next**
6. Give a role in the **Role Name** and choose **Create role**.

### VPC Endpoints

The connector is recommended to be deployed in private subnet(s), hence it may not have access to internet.
It means that the connector does not have access to services like S3 and Timestream that are outside your MSK Connect VPC.
In order to have the connectivity, you can establish a private connection between your MSK Connect VPC and the services like Amazon S3 and Amazon Timestream using VPC Endpoints powered by [AWS PrivateLink](https://docs.aws.amazon.com/vpc/latest/privatelink/what-is-privatelink.html).
In this section, we create create the VPC endpoints that are required for the connector to have private connectivity to the services.

#### VPC endpoint from MSK Connect VPC to Amazon S3

1. On the Amazon VPC console, choose **Endpoints** from the let navigation menu.
2. Choose **Create endpoint**
3. Under **Service Name** choose the **com.amazonaws.REGION_NAME.s3** service (replace the REGION_NAME with your elected region) and the **Gateway** type.
4. Choose the VPC where your MSK cluster is created and then select the box to the left of the route table that is associated with the cluster's subnets.
5. Choose **Create endpoint**

#### VPC endpoint from MSK Connect VPC to Timestream
Follow the steps described in [Creating an interface VPC endpoint for Timestream](https://docs.aws.amazon.com/timestream/latest/developerguide/VPCEndpoints.vpc-endpoint-create.html) to create a private connection between MSK Connect and Timestream.