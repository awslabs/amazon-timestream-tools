# Timestream for InfluxDB CloudFormation Bastion Host Sample

## Overview

This sample application deploys:
- A private [Timestream for InfluxDB instance](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-timestream-influxdbinstance.html).
- An [EC2 instance](https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-ec2-instance.html) with access to the Timestream for InfluxDB instance.
- Resources required for the Timestream for InfluxDB instance and EC2 instance.

The EC2 instance deployed by this sample is referred to as a "bastion host," meaning that it is a publicly-accessible server used to access a private network.

## Deployment

### Parameters

The SAM template provides a number of parameters, the following are the most important:
- `Bucket`: The name of the initial InfluxDB bucket. All InfluxDB data is stored in a bucket. A bucket combines the concept of a database and a retention period (the duration of time that each data point persists). A bucket belongs to an organization. Update required replacement. Defaults to `bucket`.

- `DbInstanceName`: The name that uniquely identifies the DB instance when interacting with the Amazon Timestream for InfluxDB API and CLI commands. This name will also be a prefix included in the endpoint. DB instance names must be unique per customer and per region. Update requires replacement. Defaults to `test-db-instance`.

- `Password`: The password of the initial admin user created in InfluxDB. This password will allow you to access the InfluxDB UI to perform various administrative tasks and also use the InfluxDB CLI to create an operator token. These attributes will be stored in a Secret created in Amazon SecretManager in your account. Update requires replacement.

- `Username`: The username of the initial admin user created in InfluxDB. Must start with a letter and can't end with a hyphen or contain two consecutive hyphens. For example, my-user1. This username will allow you to access the InfluxDB UI to perform various administrative tasks and also use the InfluxDB CLI to create an operator token. These attributes will be stored in a Secret created in Amazon Secrets Manager in your account. Update requires replacement. Defaults to `admin`.

- `ClientIp`: The IPv4 address to be granted access to the EC2 bastion host.

- `Ec2KeyName`: The name of the key pair to use for SSH access to the EC2 instance.

- `Ec2InstanceName`: The name to use for the EC2 instance. Defaults to `TimestreamInfluxDBBastionHost`.

### Steps

To deploy all resources defined in [`template.yml`](./template.yml):

1. Satisfy the [SAM CLI prerequisites](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/prerequisites.html).
2. [Download and install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html).
3. Run the following command to deploy and set parameter values with `--parameter-overrides`:
    ```shell
    sam deploy \
        -t template.yml \
        --parameter-overrides \
        ParameterKey=ExampleParameterKey,ParameterValue=ExampleParameterValue \
        ParameterKey=OtherExampleParameterKey,ParameterValue=OtherExampleParameterValue
    ```

The deployed stack, by default, is named `Ec2PrivateInfluxDb`.

When your resources have finished deploying, two values will be output:
- `TimestreamInfluxDBURL`: The full URL of your Timestream for InfluxDB instance, including its scheme and port.
- `EC2PublicIP`: The public IPv4 address if your EC2 instance.

### Deletion

To delete all resources defined in `template.yml`, run:
```
sam delete
```

## Accessing your Timestream for InfluxDB Instance

### Connecting to your EC2 Bastion Host

Once you have finished deploying the resources, you can access your deployed EC2 bastion host using the key you chose during deployment when you set the `Ec2KeyName` parameter. Use this key to SSH into your instance:

```shell
ssh -i <path to key> ec2-user@<EC2 public IP>
```

Replace `<path to key>` with the path to the key you chose to use with the instance and `<EC2 public IP>` with the public IP address if your EC2 instance.

### Interacting with your Timestream for InfluxDB Instance

#### Installing the Influx CLI

Once you have connected to your EC2 instance, download and install the Influx CLI:

```shell
wget https://dl.influxdata.com/influxdb/releases/influxdb2-client-2.7.5-linux-arm64.tar.gz && \
    echo "867c3cbabd63a34a9b1ac643fd5c5d268b694acc98e3b75fa5a78d63037097dd influxdb2-client-2.7.5-linux-arm64.tar.gz" | sha256sum -c - && \
    tar xvzf ./influxdb2-client-2.7.5-linux-arm64.tar.gz && \
    sudo cp ./influx /usr/local/bin/
```

#### Creating an Operator Token

To access your Timestream for InfluxDB instance, you will need an operator token. Create one with the following commands:

```shell
influx config create \
    --config-name CONFIG_NAME1 \
    --host-url <Timestream for InfluxDB URL> \
    --org <org name> \
    --username-password <username>:<password> \
    --active &&

influx auth create --org <org name> --operator
```

See Timestream's [Creating a new operator token for your InfluxDB instance guide](https://docs.aws.amazon.com/timestream/latest/developerguide/timestream-for-influx-getting-started-operator-token.html) for more information.

#### Writing Data to InfluxDB

The following is an example command for sending line protocol data to your Timestream for InfluxDB instance using the Influx CLI and your newly-created operator token:

```shell
influx write \
    --host <Timestream for InfluxDB URL> \
    -t <operator token> \
    --org <org name> \
    --bucket <bucket name> \
    --format lp \
'
m,host=host1 field1=1.2,field2=5i 1640995200000000000
m,host=host2 field1=2.4,field2=3i 1640995200000000000
'
```

#### Querying InfluxDB

Use the Influx CLI and the [Flux](https://docs.influxdata.com/flux/v0/) data scripting language to query your Timestream for InfluxDB bucket:

```shell
influx query \
    --host <Timestream for InfluxDB URL> \
    -t <operator token> \
    --org <org name> \
    'from(bucket: "<bucket name>") |> range(start: 0)'
```
