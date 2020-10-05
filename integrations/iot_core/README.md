# Timestream - Ingest data via an AWS IoT topic rule

You can ingest time series data from your IoT devices connected to AWS IoT Core into Amazon Timestream. This repository contains everything needed to setup and test data ingestion via an AWS IoT topic rule into Amazon Timestream:

1. a Python script which generates sample data
2. instructions to create Amazon Timestream Database and Table and an IAM role which grants permission to AWS IoT Core to ingest data into Timestream
3. a JSON object containing the AWS IoT topic rule definition: **to-timestream-rule-template.json**

The CloudFormation template creates:

1. a Timestream database and table.
2. an IAM role. Part of a rule definition is an IAM role that grants permission to access resources specified in the rule's action.

The Timestream database name, the table name and the IAM role's ARN will be used to create the IoT topic rule. The values can be found in the outputs tab of your CloudFormation stack once that has successfully completed execution.

## Setup

### AWS Region
**Note** The instructions in this repository will use the US East (N. Virginia) Region (us-east-1). If you want to choose another region replace the region in the commands with the region of your choice. You need also replace the region then in the data generating script `sensordata.py`.

### AWS Command Line Interface
You must use an AWS Command Line Interface (AWS CLI) version which supports timestream and the timestream action for an iot rule. This applies as of version **2.0.54** for the [AWS CLI version 2](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html#cli-chap-install-v2) and as of version **1.18.150** for the former version of the AWS CLI.

### Sample data generation script
The data generation script generates sample data that represent fictitious sensors in buildings which measure humidity, barometric pressure and temperature. 

Apart from the measurements the sensors will also report the building name and the room number where they are located.

Here is an example of what the IoT message payload will look like:

	{
	  "temperature": 26.68671473343757,
	  "pressure": 43.18554985015594,
	  "humidity": 1036.1939942299964,
	  "device_id": "sensor_02",
	  "building": "Day 1",
	  "room": "10.01"
	}


The script is implemented in Python 3 and uses the [publish API](https://docs.aws.amazon.com/iot/latest/apireference/API_iotdata_Publish.html) of the AWS IoT Data Plane from the `boto3` module in order to ingest data into AWS IoT. In order to setup the environment for the data generating script you need to ensure the following:

1. Install Python 3
2. Install `boto3`
3. Setup authentication credentials and associate the permission to publish to IoT (`"iot:Publish"`) with those credentials
4. Launch the CloudFormation stack in your account

**Note**: For more information on the installation of the `boto3` library and setting up of authentication credentials for use in your Python scripts please see the [Quick Start](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html) and [Credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html) sections of the `boto3` documentation.

### Create Timestream data base and table
The example assume that you are using the names given below for `DATABASE_NAME` and `TABLE_NAME`. In case you change these names you must modify the files `inline-policy.json` and `to-timestream-rule-template.json` accordingly.

Set some shell variable for ease of use.
```bash
DATABASE_NAME=iotdemo
TABLE_NAME=sensordata
IAM_ROLE_NAME=IoTDataToTimesreamRole
IAM_POLICY_NAME=IoTDataToTimesreamPolicy
```

Create a data base in Amazon Timestream
```bash
aws timestream-write create-database --database-name $DATABASE_NAME --region us-east-1
```

The output of the command should look similar to:
```bash
{
    "Database": {
        "Arn": "arn:aws:timestream:us-east-1:your_aws_account_id:database/iotdemo",
        "DatabaseName": "iotdemo",
        "TableCount": 0,
        "KmsKeyId": "arn:aws:kms:us-east-1:your_aws_account_id:key/d69bfebe-68d3-41b8-ac6c-64b7768eeb70",
        "CreationTime": "2020-10-03T09:56:21.070000+00:00",
        "LastUpdatedTime": "2020-10-03T09:56:21.070000+00:00"
    }
}
```

Create a table within the data base you just created.
```bash
aws timestream-write create-table --database-name $DATABASE_NAME \
	--table-name $TABLE_NAME \
	--retention-properties MemoryStoreRetentionPeriodInHours=24,MagneticStoreRetentionPeriodInDays=5 \
	--region us-east-1
```

The output of the command should look similar to:
```bash
{
    "Table": {
        "Arn": "arn:aws:timestream:us-east-1:your_aws_account_id:database/iotdemo/table/sensordata",
        "TableName": "sensordata",
        "DatabaseName": "iotdemo",
        "TableStatus": "ACTIVE",
        "RetentionProperties": {
            "MemoryStoreRetentionPeriodInHours": 24,
            "MagneticStoreRetentionPeriodInDays": 5
        },
        "CreationTime": "2020-10-03T09:59:46.857000+00:00",
        "LastUpdatedTime": "2020-10-03T09:59:46.857000+00:00"
    }
}
```

Verify that the table has been created:
```bash
aws timestream-write describe-table --database-name $DATABASE_NAME \
	--table-name $TABLE_NAME --region us-east-1

```

### Create an IAM role


```bash
aws iam create-role --role-name $IAM_ROLE_NAME --assume-role-policy-document file://./iot-trust-policy.json
```

The output of the command should look similar to:
```bash
{
    "Role": {
        "Path": "/",
        "RoleName": "IoTDataToTimesreamRole",
        "RoleId": "AAAA5X43AFYUUHRK4OEPH",
        "Arn": "arn:aws:iam::your_aws_account_id:role/IoTDataToTimesreamRole",
        "CreateDate": "2020-10-03T10:09:36+00:00",
        "AssumeRolePolicyDocument": {
            "Version": "2012-10-17",
            "Statement": {
                "Effect": "Allow",
                "Principal": {
                    "Service": "iot.amazonaws.com"
                },
                "Action": "sts:AssumeRole"
            }
        }
    }
}
```
Take a note of the role arn. You will need the arn later to create an iot topic rule.

Repalce `YOUR_AWS_ACCOUNT_ID` with your aws account id in the file `inline-policy.json`.

Attach an inline policy to the role:
```bash
aws iam put-role-policy --role-name $IAM_ROLE_NAME \
	--policy-name $IAM_POLICY_NAME \
	--policy-document file://./inline-policy.json
```

Verify that role and policy have been created:
```bash
aws iam get-role --role-name $IAM_ROLE_NAME
```

```bash
aws iam get-role-policy --role-name $IAM_ROLE_NAME \
	--policy-name $IAM_POLICY_NAME
```


### IoT rule
Create an iot topic rule to ingest data incoming into AWS IoT Core into Amazon Timestream.


* In **to-timestream-rule-template.json** replace `REPLACE_WITH_YOUR_ROLE_ARN` with the role arn of the IAM role you created earlier.
* Create the topic rule

		aws iot create-topic-rule \
			--rule-name ToTimestreamRule \
			--topic-rule-payload file://./to-timestream-rule-template.json \
			--region us-east-1
* Verify that the rule has been created

		aws iot get-topic-rule \
			--rule-name ToTimestreamRule \
			--region us-east-1
		
* The output should look similar to:

		{
		    "ruleArn": "arn:aws:iot:us-east-1:your_aws_account_id:rule/ToTimestreamRule",
		    "rule": {
		        "ruleName": "ToTimestreamRule",
		        "sql": "SELECT humidity, pressure, temperature FROM 'dt/sensor/#'",
		        "createdAt": 1600448082.0,
		        "actions": [
		            {
		                "timestream": {
		                    "roleArn": "arn:aws:iam::your_aws_account_id:role/IoTDataToTimesreamRole",
		                    "databaseName": "iotdemo",
		                    "tableName": "sensordata",
		                    "dimensions": [
		                        {
		                            "name": "device_id",
		                            "value": "${device_id}"
		                        },
		                        {
		                            "name": "building",
		                            "value": "${building}"
		                        },
		                        {
		                            "name": "room",
		                            "value": "${room}"
		                        }
		                    ]
		                }
		            }
		        ],
		        "ruleDisabled": false,
		        "awsIotSqlVersion": "2016-03-23"
		    }
		}


Make the data generation script executable and start it: 

```bash
chmod +x ./sensordata.py
./sensordata.py
```

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


