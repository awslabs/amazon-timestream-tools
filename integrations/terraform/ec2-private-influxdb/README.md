# Timestream for InfluxDB Terraform Bastion Host Sample

## Overview

This sample application deploys:
- A private [Timestream for InfluxDB instance](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/timestreaminfluxdb_db_instance).
- An [EC2 instance](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/instance) with access to the Timestream for InfluxDB instance.
- Resources required for the Timestream for InfluxDB instance and EC2 instance.

The EC2 instance deployed by this sample is referred to as a "bastion host," meaning that it is a publicly-accessible server used to access a private network.

## Deployment

### Variables

[`variables.tf`](./variables.tf) provide a number of configuration options. There are a few ways to set these options:

1. When applying changes, enter variable values when prompted:
    ```console
    $ terraform apply
    var.ec2_key_name
      Enter a value:
    ```

    Variables with default values defined in `variables.tf` will not require input.

2. Provide variable values to Terraform when applying changes:
    ```shell
    terraform apply -var="ec2_key_name=example-key-name" -var="other_var=other_val"
    ```

3. Edit `variables.tf`, setting default values, for example:
    ```terraform
    variable "ec2_key_name" {
      type = string
      default = "example-key-name" # Added
    }
    ```

### Tags

[Tagging AWS resources](https://aws.amazon.com/solutions/guidance/tagging-on-aws/) is useful. To tag all resources, define `default_tags` in an `aws` provider block, in [`main.tf`](./main.tf):

```terraform
provider "aws" {
  # ... other configuration ...
  default_tags {
    tags = {
      Environment = "Production"
      Owner       = "Ops"
    }
  }
}
# ... other configuration ...
```

Individual resources can be tagged using a resource's `tags` argument:
```terraform
resource "aws_vpc" "example" {
  # ... other configuration ...

  # This configuration by default will internally combine tags defined
  # within the provider configuration block and those defined here
  tags = {
    Name = "MyVPC"
  }
}
```

See [Terraform's guide on resource tagging with the AWS provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/guides/resource-tagging) for more information.

### Steps

To deploy all resources defined in `main.tf`:

1. [Download and install Terraform](https://developer.hashicorp.com/terraform/install).
2. [Configure AWS credentials for use with the AWS SDK for Go v2](https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html#specifying-credentials).
3. Run:
    ```shell
    terraform init
    ```
4. [Configure variables](#variables), when applying changes or before.
5. Run the following to see what changes will be applied:
    ```shell
    terraform plan
    ```
6. Run:
    ```shell
    terraform apply
    ```

When your resources have finished deploying, two values will be output:
- `instance_url`: The full URL of your Timestream for InfluxDB instance, including its scheme and port.
- `ec2_public_ip`: The public IPv4 address if your EC2 instance.

### Updates

To update your deployed resources, update either `main.tf` or `variables.tf` and run `terraform apply` again.

### Deletion

To delete all resources defined in `main.tf`, run:
```
terraform destroy
```

## Accessing your Timestream for InfluxDB Instance

### Connecting to your EC2 Bastion Host

Once you have finished deploying the resources defined in `main.tf`, you can access your deployed EC2 bastion host using the key you chose during deployment when you set the `ec2_key_name` variable. Use this key to SSH into your instance:

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
