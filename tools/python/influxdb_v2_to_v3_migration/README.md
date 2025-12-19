# Amazon Timestream for InfluxDB v2 to v3 Migration Script

## Overview

The [Amazon Timestream for InfluxDB](https://docs.aws.amazon.com/timestream/latest/developerguide/timestream-for-influxdb.html) [v2](https://docs.influxdata.com/influxdb/v2/) to [v3](https://docs.influxdata.com/influxdb3/enterprise/) migration script allows you to migrate your data from managed InfluxDB v2 to v3. The script uses the InfluxDB [v2](https://docs.influxdata.com/influxdb/v2/api/v2/) and [v3](https://docs.influxdata.com/influxdb3/enterprise/api/v3/) APIs, the [Influx CLI](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/), and the [InfluxDB v2 daemon](https://docs.influxdata.com/influxdb/v2/reference/cli/influxd/) to [backup](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/backup/) data, [translate backed-up data to line protocol](https://docs.influxdata.com/influxdb/v2/reference/cli/influxd/inspect/export-lp/), and [ingest the line protocol data to InfluxDB v3](https://docs.influxdata.com/influxdb3/enterprise/api/v3/#operation/PostWriteLP).

The script is available standalone or as part of an automated solution that deploys an EC2 instance with the script and all prerequisites installed. If you already have your backed-up data, a separate script that ingests to InfluxDB v3 is provided, [`influxdb_v3_ingestion.py`](./app/influxdb_v3_ingestion.py).

## Standalone Usage

### Steps

1. [Install minimum Python version 3.14.1](https://www.python.org/downloads/).
2. Navigate to the [`app`](./app/) directory:
   ```shell
   cd app
   ```
3. Create a Python virtual environment:
   ```shell
   python3.14 -m venv .env
   source .env/bin/activate
   ```
4. Install all Python dependencies:
   ```shell
   python3.14 -m pip install .
   ```
5. Create a secret in [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) containing your InfluxDB v2 and v3 tokens. For example, using the AWS CLI:
   ```shell
   aws secretsmanager create-secret \
       --region us-west-2 \
       --name influxdb_secret_name_example \
       --secret-string \
       '{"INFLUXDB_V2_TOKEN": "replace me", "INFLUXDB_V3_TOKEN": "replace me"}'
   ```
6. Make sure you have network connectivity to your Timestream for InfluxDB v2 and v3 instances.

   a. InfluxDB v2 connectivity can be checked with the [Influx CLI](https://docs.influxdata.com/influxdb/v2/tools/influx-cli/):
      ```shell
      influx ping --host <InfluxDB v2 host>
      ```
   b. InfluxDB v3 connectivity can be checked with [cURL](https://curl.se/):
      ```shell
      curl -X GET "<InfluxDB v3 host>/health" --header "Authorization: Bearer <InfluxDB v3 token>"
      ```
7. Make sure you have enough disk space to hold all of the data that you want to migrate, uncompressed. Data will be backed up to an `engine` directory, by default in `~`. Within this directory, backed up data is organized into `data/<bucket_id>/` directories. Each bucket's [line protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/) data file will be, by default, named `output.lp`, and will be in their respective bucket directories.
8. Run the script, providing:
    - Your InfluxDB v2 URL.
    - Your InfluxDB v3 URL.
    - The InfluxDB v2 buckets that you want to migrate and their organizations.
    - The name of the secret you created in AWS Secrets Manager that contains your InfluxDB v2 and v3 tokens.
    ```shell
    python3.14 influxdb_v2_to_v3_migration.py \
        --influxdb-v2-url "https://example.com:8086" \
        --influxdb-v3-url "https://example.com:8181" \
        --influxdb-v2-buckets-and-orgs "bucket-one:organization-one,bucket-two:organization-two" \
        --tokens-secret-name "influxdb_v2_to_v3_migration"
    ```

### Clean Up

Remove the backed-up data. By default, data is placed in `~/engine`:
```
rm -rf ~/engine
```

## Usage with Deployed EC2 Instance

### Steps

1. [Download and install Packer](https://developer.hashicorp.com/packer/install).
2. [Download and install Terraform](https://developer.hashicorp.com/terraform/install).
3. Update [`variables.tf`](./variables.tf), filling in all `"replace me"` placeholders:
   
   - `vpc_id`: The ID of an existing VPC.
   - `subnet_id`: The ID of an existing subnet in the above VPC.
   - `ssh_access_ip`: The IP to grant SSH access to the deployed EC2 instance. For example, `127.0.0.1/32`.
   - `tokens`: Your InfluxDB v2 and v3 tokens. These tokens will be placed in a secret in AWS Secrets Manager and redacted from all Terraform output.
   - `runner_ssh_key_name`: The name of an existing key you wish to use to SSH onto your deployed EC2 instance.
4. Within the [`app`](./app/) directory, initialize Packer and build the AMI, this will produce an AMI in your account with the name `influxdb-v2-to-v3-migration-runner-<timestamp>`:
   ```shell
   packer init
   packer build packer.pkr.hcl
   ```
5. In the [`influxdb_v2_to_v3_migration`](.) directory, initialize and apply Terraform changes:
   ```shell
   terraform init
   terraform apply
   ```
6. Review the proposed changes by Terraform and type `yes`.
7. Take a note of the output `runner_ip` value. This IP will need to be added as an ingress rule to your Timestream for InfluxDB v2 and v3 security groups. This can be accomplished with the AWS CLI:
   ```shell
   # InfluxDB v2.
   aws ec2 authorize-security-group-ingress \
       --group-id <InfluxDB v2 security group ID> \
       --protocol tcp \
       --port 8086 \
       --cidr <runner_ip>/32

   # InfluxDB v3.
      aws ec2 authorize-security-group-ingress \
       --group-id <InfluxDB v3 security group ID> \
       --protocol tcp \
       --port 8181 \
       --cidr <runner_ip>/32
   ```
8. Using the key pair you specified in `variables.tf`, SSH onto the instance, using the output `runner_ip`:
   ```shell
   ssh -i <path to key> ec2-user@<runner_ip>
   ```
   - If you don't want to use SSH, you can use AWS SSM instead:
      ```shell
      aws ssm start-session --target <instance ID>
      ```
9. Run the script, providing:
    - Your InfluxDB v2 URL.
    - Your InfluxDB v3 URL.
    - The InfluxDB v2 buckets that you want to migrate and their organizations.
    - The name of the secret you created in AWS Secrets Manager that contains your InfluxDB v2 and v3 tokens.
    ```shell
    python influxdb_v2_to_v3_migration.py \
        --influxdb-v2-url "https://example.com:8086" \
        --influxdb-v3-url "https://example.com:8181" \
        --influxdb-v2-buckets-and-orgs "bucket-one:organization-one,bucket-two:organization-two" \
        --tokens-secret-name "influxdb_v2_to_v3_migration"
    ```
    - **Note**: Packer builds Python 3.14 from source and installs it in the AMI simply as `python`.

### Clean Up

1. In the `influxdb_v2_to_v3_migration` directory, destroy all Terraform-deployed resources:
   ```shell
   terraform destroy
   ```
2. When prompted, type `yes`.
3. Find, [disable](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/disable-an-ami.html), and [deregister](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/deregister-ami.html) all `influxdb-v2-to-v3-migration-runner-<timestamp>` AMIs.
4. In AWS Systems Manager, delete the `/amis/influxdb-v2-to-v3-migration-runner/latest` parameter in the parameter store:
   ```shell
   aws ssm delete-paramter \
       --name "/amis/influxdb-v2-to-v3-migration-runner/latest"
   ```

## Testing

1. Before running tests, make sure the Docker daemon is running. On macOS, this means having Podman desktop or Docker desktop running.

2. Navigate to the [`app`](./app/) directory and install the optional test dependencies:
   ```shell
   python3.14 -m pip install -e '.[test]'
   ```

3. Navigate to [`tests/integration/`](./app/tests/integration/) and run all tests:
   ```shell
   python3.14 -m pytest .
   ```
   These tests will create a secret in AWS Secrets Manager, create Docker containers for InfluxDB v2 OSS and v3 Core, create a temporary directory for migrations, and perform a number of migrations. Tests should clean up all resources after they have finished. Errors during teardown, if any occur, may leave residual resources.
