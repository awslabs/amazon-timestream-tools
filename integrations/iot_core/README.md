# Timestream - Ingest data via an AWS IoT topic rule

You can ingest time series data from your IoT devices connected to AWS IoT Core into Amazon Timestream. This repository contains everything needed to setup and test data ingestion via an AWS IoT topic rule into Amazon Timestream:

1. a Python script which generates sample data: **sensordata.py**
2. a CloudFormation template which sets up the necessary resources in your AWS account: **cfn-iot-rule-to-timestream.json**

The CloudFormation template creates:

1. a Timestream database and table.
2. an IAM role. Part of a rule definition is an IAM role that grants permission to access resources specified in the rule's action.
3. IoT topic rule that writes sensor data into Timestream database.

## Setup

### AWS Region
**Note** The instructions in this repository will use the US East (N. Virginia) Region (us-east-1). If you want to choose another region replace the region in the instructions with the region of your choice. You need also replace the region then in the data generating script `sensordata.py`.

### Sample data generation script
The data generation script generates sample data that represent fictitious sensors in buildings which measure humidity, barometric pressure and temperature. 

Apart from the measurements the sensors will also report the building name and the room number where they are located.

Here is an example of what the IoT message payload will look like:

	{
	  "temperature": 18.05670413634157,
	  "pressure": 45.681328608970425,
	  "humidity": 919.8060681910588,
	  "device_id": "sensor_01",
	  "building": "Day 1",
	  "room": "2.01"
	}


The script is implemented in Python 3 and uses the [publish API](https://docs.aws.amazon.com/iot/latest/apireference/API_iotdata_Publish.html) of the AWS IoT Data Plane from the `boto3` module in order to ingest data into AWS IoT. In order to setup the environment for the data generating script you need to ensure the following:

1. Install Python 3
2. Install `boto3`
3. Setup authentication credentials and associate the permissions to publish to IoT (`"iot:Publish"`) and to describe the IoT endpoint (`"iot:DescribeEndpoint"`) with those credentials
4. Launch the CloudFormation stack in your account

**Note**: For more information on the installation of the `boto3` library and setting up of authentication credentials for use in your Python scripts please see the [Quick Start](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html) and [Credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) sections of the `boto3` documentation.

### CloudFormation stack
Launch the CloudFormation stack in the region `us-east-1`.

Go to the [AWS CloudFormation console](https://console.aws.amazon.com/cloudformation/home?region=us-east-1#)

1. Create stack
2. With new resources (standard)
3. Template is ready
4. Upload a template file
5. Choose file
6. Select `cfn-iot-rule-to-timestream.json`
7. Next
8. Enter an arbitrary stack name, e.g. `IoTTimestreamIntegration`
9. Next
10. Next
11. Check `I acknowledge that AWS CloudFormation might create IAM resources.`
12. Create stack
13. Wait until the status changes from `CREATE_IN_PROGRESS` to `CREATE_COMPLETE`. It should take some minutes for the stack to be launched.

Start the data generating script: `./sensordata.py`

## Sample queries
Do some queries in Timestream for example. Replace `DATABASE_NAME` and `TABLE_NAME` with the data base name and table name that the CloudFormation stack created.

You can use the [Timestream Query editor](https://console.aws.amazon.com/timestream/home?region=us-east-1#query-editor:) to run your queries.

* Number of rows: `select count(*) from "DATABASE_NAME"."TABLE_NAME"`
* Get some rows: `select * from "DATABASE_NAME"."TABLE_NAME" limit 20`
* Retrieve building names: `select distinct(building) from "DATABASE_NAME"."TABLE_NAME"`
* Get temperatures in `Day 1` building
```
select * from "DATABASE_NAME"."TABLE_NAME" 
where building = 'Day 1' and measure_name = 'temperature'
limit 20
```

## Troubleshooting
To monitor AWS IoT activity you should [enable logging](https://docs.aws.amazon.com/iot/latest/developerguide/configure-logging.html).

In case something doesn't work as expected try to use [Amazon CloudWatch insights](https://console.aws.amazon.com/cloudwatch/home?region=us-east-1#logsV2:logs-insights):

* Log group: `AWSIoTLogsV2`
* Filter

		fields @timestamp, @message
		| sort @timestamp desc
		| limit 20
		| filter ruleName = "ToTimestreamRule"
