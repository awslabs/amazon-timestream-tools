# Amazon Timestream - Kafka Sink Connector

The Timestream Kafka Sink Connector is designed to work with Kafka Connect and to be deployed to a Kafka Connect cluster.
The deployed connector polls for events from a source Kafka topic, and ingests them as records to a target Timestream table.

[Amazon Timestream](https://aws.amazon.com/timestream/) is a fast, scalable, and serverless time series database service that makes it straightforward to store and analyze trillions 
of events per day for use cases like monitoring hundreds of millions of Internet of Things (IoT) devices, industrial equipment, 
gaming sessions, streaming video sessions, and more.

You can securely scale your streaming data platform with hundreds and thousands of Kafka clusters using [Amazon Managed Streaming for Apache Kafka](https://aws.amazon.com/msk/) (Amazon MSK), 
a fully managed service to build and run applications to process streaming data, which simplifies the setup, scaling, 
and management of clusters running Kafka. And [Amazon MSK Connect](https://aws.amazon.com/msk/features/msk-connect/) enables you to deploy, monitor, and automatically scale connectors that move data between your MSK or Kafka clusters and external systems.

### Schema Definition and Mapping

While creating a table in Timestream, you do not need to define the schema up front as Timestream automatically detects the schema based on the data points being sent. 
At the same time, the connector needs an upfront schema definition which is used for validating the incoming messages from the source Kafka topic and then, to ingest to the target Timestream table as records.
The connector supports loading the definition from a configured [Amazon Simple Storage Service (Amazon S3)](http://aws.amazon.com/s3) bucket.

#### Sample Schema Definition

The connector supports [multi-measure](https://docs.aws.amazon.com/timestream/latest/developerguide/writes.html#writes.writing-data-multi-measure) schema mapping - the preferred approach, which store each measure value in a dedicated column.
The example here demonstrates mapping to multi-measure records, You can refer the sample CSV [purchase_history.csv](resources%2Fpurchase_history.csv) file, that has the following headings to map to a target column in a Timestream table.

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

Refer [purchase_history.json](resources%2Fpurchase_history.json) file on how to represent the schema model in JSON format; see [Data model mappings](https://docs.aws.amazon.com/timestream/latest/developerguide/batch-load-data-model-mappings.html#batch-load-data-model-mappings-example-multi) for additional details.

### Connector Configuration parameters

The following table lists the complete set of the Timestream Kafka Sink Connector configuration properties

| #  | Key                                    | Description                                                                                                                   | Remarks                                                                                                                                                                                                          | Required | Default |
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

## Solution Overview
In this section, we discuss a solution architecture of an end-to-end data pipeline using the connector. 
As data arrives to the configured source Kafka topic, an instance of the connector ingests the data to a Timestream table, as shown in the following diagram.

![Data Flow - Timestream Sink Connector.png](resources%2FData%20Flow%20-%20Timestream%20Sink%20Connector.png)

The connector uses VPC endpoints (powered by [AWS PrivateLink](https://aws.amazon.com/privatelink/)) to securely connect to Timestream and S3 so that the traffic between your VPC and the connected services doesn’t leave the Amazon network, as shown in the following solution architecture diagram.

![Solution Architecture - Timestream Sink Connector.png](resources%2FSolution%20Architecture%20-%20Timestream%20Sink%20Connector.png)

Now, let us discuss the steps to deploy the Timestream Sink Kafka connector per the solution.
_Note:_ You can deploy it in a self-managed Apache Kafka Connect cluster as well.

### Prerequisites
Before deploying the connector, make sure you have the below steps completed.
1. A Timestream database and a table are created. Follow the steps - [Create a database](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.db.using-console) and [Create a table](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.table.using-console), if you do not have them created before.
2. A Timestream ingestion vpc endpoint is created. Follow the steps - [Creating an interface VPC endpoint for Timestream](#vpc-endpoint-from-msk-connect-vpc-to-timestream), if you do not have it created before.
3. A Kafka/ Amazon MSK Cluster is created in private subnets, and it has IAM authentication enabled. Make sure you have teh cluster in the same region as that of Timestream database/table. Follow the steps - [Create an Amazon MSK cluster](https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster.html), if you do not have it created before.
4. An Amazon EC2 machine to connect with the MSK cluster - to create a Kafka topic and more. Follow the steps - [Create an IAM role for the client machine](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-iam-role.html) for accessing the MSK cluster created in teh previous step and [Create a client machine](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html), if you do not have them created before 
5. A Kafka topic is created. Follow the steps - [Create a topic](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html) if you do not have it before. 
6. An S3 bucket in the same region as that of MSK Cluster and the Timestream database/table. Follow the steps - [Create your first S3 bucket](https://docs.aws.amazon.com/AmazonS3/latest/userguide/creating-bucket.html), if you do not have it created before. 
7. An Amazon S3 Gateway endpoint is created. Follow the steps - [Creating a gateway VPC endpoint for S3](#vpc-endpoint-from-msk-connect-vpc-to-amazon-s3), if you do not have it created before. 
8. Timestream schema json is uploaded to an S3 bucket. Follow the steps - [Uploading objects](https://docs.aws.amazon.com/AmazonS3/latest/userguide/upload-objects.html), if you do not have it uploaded before. 
9. An [Amazon CloudWatch](https://aws.amazon.com/cloudwatch/) log group. Follow the steps detailed in [create a log group in CloudWatch Logs](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/Working-with-log-groups-and-streams.html#Create-Log-Group), if you do not have it created before.
10. An IAM service role is created with the required permissions. Follow the steps detailed in [Security: Identity and Access Management](#identity-and-access-management) section to create a role and attach required permissions, if you do not have an IAM role created/ do not have the required permissions attached to it.

### I. Upload the Connector plugin to an S3 bucket
1. Download the plugin jar: [kafka-connector-timestream-1.0-SNAPSHOT-jar-with-dependencies.jar](resources%2Fkafka-connector-timestream-1.0-SNAPSHOT-jar-with-dependencies.jar)
2. On the Amazon S3 console, choose **Buckets** from the left navigation pane,
3. In the Buckets list, choose the name of the bucket that you want to upload the jar file. 
4. Choose **Upload**. 
5. In the Upload window, Choose **Add file**, and then choose the downloaded plugin jar file, and choose **Open**. 
6. At the bottom of the page, choose **Upload** and When the upload is finished, you see a success message on the **Upload: status** page 
7. Click on the name of the jar file under **Files and folders** section, that opens the S3 object details page 
8. Choose **Copy S3 URI**, preserve it as it would be used in the following step.

### II. Configure the Amazon MSK Custom Plugin

1. On the Amazon MSK console, choose **Custom plugins** (_Note:_ It is an AWS resource that contains the code that defines the connector logic)  in the navigation pane. 
2. Choose **Create custom plugin**. 
3. For **S3 URI**, enter the connector file’s S3 URI that is copied in the previous section. 
4. Give it a name and choose **Create custom plugin**.

### III. Configure the Amazon MSK worker configuration
_Note:_ A worker is a Java virtual machine (JVM) process that runs the connector logic. Next, create a custom worker configuration, to use JSON based convertor instead of String based convertor that is supported in default configuration.
1. On the Amazon MSK console, choose **Worker configurations** in the navigation pane. 
2. Choose **Create worker configuration**. 
3. In the **Worker configuration**, paste the below provided configuration.
```properties
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
```
4. Give it a name and choose **Create worker configuration**.

### IV. Configure the Timestream Sink Connector
Now it’s time to configure the connector using the custom plugin.
1. On the Amazon MSK console, choose **Connectors** in the navigation pane. 
2. Choose **Create connector**. 
3. Choose the plugin you created before and choose **Next**. 
4. Choose the MSK cluster where the connector has to poll from a source topic 
5. Enter your connector configuration after replacing the values with your respective configuration as follows.

```properties
#Kafka connect configurations
connector.class=software.amazon.timestream.TimestreamSinkConnector
tasks.max=2
topics=<Kafka Topic from which the messages need to be polled>
aws.region=<AWS Region>
timestream.ingestion.endpoint=<Timestream Ingestion Vpc Endpoint in UR format>
timestream.schema.s3.bucket.name=<S3 Bucket Name where the Timestream table schema definition is uploaded>
timestream.schema.s3.key=<S3 object key of the Timestream table schema definition in JSON format>
timestream.database.name=<Timestream Database Name>
timestream.table.name= <Timestream Table Name>
```
For complete set of supported configuration properties, see [Connector Configuration parameters](#connector-configuration-parameters)

6. Under **Worker configuration**, select **Use a custom configuration**, select the custom worker configuration that you have created in the previous step.
7. Under **Access permissions**, choose the IAM role you created as part of the [prerequisites steps](#prerequisites) 
8. Choose **Next**. 
9. In the **Logs** section, select **Deliver to Amazon CloudWatch Logs** and update **Log Group Arn** with the arn of teh CloudWatch log group that you have created as part of the [prerequisites steps](#prerequisites).
10. Choose **Create connector**.

The connector creation takes 5–10 minutes to complete. When its status changes to Active, the pipeline is ready.

### Testing
Now it's time to test the end-to-end pipeline by publishing messages to the Kafka topic and checking if Timestream table gets ingested with the records.
1. SSH into your Kafka client machine that you have launched as part of the [prerequisites](#prerequisites) steps.
2. Follow the steps detailed in [Produce and consume data ](https://docs.aws.amazon.com/msk/latest/developerguide/produce-consume.html) to publish messages to the source Kafka topic.
3. Follow the steps detailed in [Run a query](https://docs.aws.amazon.com/timestream/latest/developerguide/console_timestream.html#console_timestream.queries.using-console) to check if the records have been ingested to the target Timestream table.

### Troubleshooting
1. Check CloudWatch Logs that you have associated to deliver the logs while creating the connector, for any configuration validation errors or any runtime exceptions thrown. 
    For example, you might receive error message(s) with details as mentioned below:
    * **Unable to convert the sink record, check if you have configured the JsonConvertor in the worker configuration**: You get this error when the connector uses the default worker configuration with StringConverter. Create a custom worker configuration with JsonConverter as mentioned in the previous section.
    * **invalid.timestream.ingestion.endpoint: Supplied Timestream ingestion endpoint is not a valid URI**: You get this error when the ingestion endpoint is not a valid URI and more
2. Checkout the below troubleshooting guides for further details.
    * [How do I troubleshoot errors when I'm trying to create a connector using Amazon MSK Connect?](https://repost.aws/knowledge-center/msk-connector-connect-errors)
    * [Troubleshooting Amazon MSK Connect](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-troubleshooting.html)

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

## Connector download or build from source
#### Download uber jar
You can download the ready-to-use uber jar from [resources](resources%2Fkafka-connector-timestream-1.0-SNAPSHOT-jar-with-dependencies.jar)
#### Build uber jar
**Prerequisite**
    * JDK >= 1.11
    * Maven
Execute the below script to get the github repo cloned, and to build the Timestream Kafka Sink connector

```shell
git clone https://github.com/awslabs/amazon-timestream-tools.git
cd ./amazon-timestream-tools/integrations/kafka_connector
mvn clean package
```
Check out for the built jar within target folder from the current directory