# Amazon Timestream for InfluxDB v2 to v3 Migration Script

## Overview

The [Amazon Timestream for InfluxDB](https://docs.aws.amazon.com/timestream/latest/developerguide/timestream-for-influxdb.html) [v2](https://docs.influxdata.com/influxdb/v2/) to [v3](https://docs.influxdata.com/influxdb3/enterprise/) migration script allows you to migrate your data from managed InfluxDB v2 to v3. The script uses the InfluxDB [v2](https://docs.influxdata.com/influxdb/v2/api/v2/) and [v3](https://docs.influxdata.com/influxdb3/enterprise/api/v3/) APIs, the [Influx CLI](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/), and the [InfluxDB v2 daemon](https://docs.influxdata.com/influxdb/v2/reference/cli/influxd/) to [backup](https://docs.influxdata.com/influxdb/v2/reference/cli/influx/backup/) data, [translate backed-up data to line protocol](https://docs.influxdata.com/influxdb/v2/reference/cli/influxd/inspect/export-lp/), and [ingest the line protocol data to InfluxDB v3](https://docs.influxdata.com/influxdb3/enterprise/api/v3/#operation/PostWriteLP).

The script is available standalone or as part of an automated solution that deploys an EC2 instance with the script and all prerequisites installed. If you already have your backed-up data, a separate script that ingests to InfluxDB v3 is provided, `influxdb_v3_ingestion.py`. See the [**InfluxDB v3 Ingestion**](#influxdb-v3-ingestion) section below for more information.

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
5. [Retrieve an operator token from your InfluxDB v2 instance](https://docs.aws.amazon.com/timestream/latest/developerguide/timestream-for-influx-getting-started-operator-token.html). An operator token is necessary for the migration.
   - This can also be done by logging in to the InfluxDB v2 UI and cloning an existing operator token.
6. Retrieve a token from your InfluxDB v3 instance. In Timestream for InfluxDB v3, they are placed in a secret in AWS Secrets Manager that is associated with the instance. In the AWS console, the secret ARN is included in the instance's summary.
7. Create a secret in [AWS Secrets Manager](https://aws.amazon.com/secrets-manager/) containing your InfluxDB v2 and v3 tokens. For example, using the AWS CLI:
   ```shell
   aws secretsmanager create-secret \
       --region us-west-2 \
       --name influxdb_secret_name_example \
       --secret-string \
       '{"INFLUXDB_V2_TOKEN": "replace me", "INFLUXDB_V3_TOKEN": "replace me"}'
   ```
8. [Download and Install the InfluxDB v2 CLI](https://docs.influxdata.com/influxdb/v2/tools/influx-cli/).
9. [Download InfluxDB v2](https://docs.influxdata.com/influxdb/v2/install/). Once you have downloaded InfluxDB v2, make sure the [InfluxDB v2 daemon (`influxd`)](https://docs.influxdata.com/influxdb/v2/reference/cli/influxd/) has been added to your PATH. InfluxDB v2 does not need to be running, the daemon will be used in isolation.
10. Make sure you have network connectivity to your Timestream for InfluxDB v2 and v3 instances.

    a. InfluxDB v2 connectivity can be checked with the [Influx CLI](https://docs.influxdata.com/influxdb/v2/tools/influx-cli/):
       ```shell
       influx ping --host <InfluxDB v2 host>
       ```

    b. InfluxDB v3 connectivity can be checked with [cURL](https://curl.se/):
       ```shell
       curl -X GET "<InfluxDB v3 host>/health" --header "Authorization: Bearer <InfluxDB v3 token>"
       ```

11. Make sure you have enough disk space to hold all of the data that you want to migrate, uncompressed. Data will be backed up to an `engine` directory, by default in `~`. Within this directory, backed up data is organized into `data/<bucket_id>/` directories. Each bucket's [line protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/) data file will be `<bucket name>.lp`, and will be in their respective bucket directories.

12. Run the script, providing:
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

1. [Download and install Packer](https://developer.hashicorp.com/packer/install). Packer will be used to create an AMI with all necessary dependencies and scripts. This AMI will be used later to deploy an EC2 instance.
2. [Download and install Terraform](https://developer.hashicorp.com/terraform/install). Terraform will be used to deploy an EC2 instance and all other necessary resources for the EC2 instance to perform a migration.
3. [Create an EC2 key pair](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/create-key-pairs.html) to use to SSH onto your deployed EC2 instance if you don't already have an existing EC2 key pair.
4. Update [`variables.tf`](./variables.tf), filling in all `"replace me"` placeholders:
   
   - `vpc_id`: The ID of an existing VPC.
   - `subnet_id`: The ID of an existing subnet in the above VPC.
   - `ssh_access_ip`: The IP to grant SSH access to the deployed EC2 instance. For example, `127.0.0.1/32`.
   - `tokens`: Your InfluxDB v2 and v3 tokens. These tokens will be placed in a secret in AWS Secrets Manager and redacted from all Terraform output.
   - `runner_ssh_key_name`: The name of an existing EC2 key pair you wish to use to SSH onto your deployed EC2 instance.
5. Within the [`app`](./app/) directory, initialize Packer and build the AMI, this will produce an AMI in your account with the name `influxdb-v2-to-v3-migration-runner-<timestamp>`:
   ```shell
   packer init packer.pkr.hcl
   packer build packer.pkr.hcl
   ```
6. In the [`influxdb_v2_to_v3_migration`](.) directory, initialize and apply Terraform changes:
   ```shell
   terraform init
   terraform apply
   ```
7. Review the proposed changes by Terraform and type `yes`.
8. Take a note of the output `runner_ip` value. This IP will need to be added as an ingress rule to your Timestream for InfluxDB v2 and v3 security groups. This can be accomplished with the AWS CLI:
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
9. Using the key pair you specified in `variables.tf`, SSH onto the instance, using the output `runner_ip`:
   ```shell
   ssh -i <path to private key> ec2-user@<runner_ip>
   ```
   - If you don't want to use SSH, you can use AWS SSM instead:
      ```shell
      aws ssm start-session --target <instance ID>
      ```
10. Run the script, providing:
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

## Manual Verification

After completing a migration, you may want to verify that all points were migrated.

For verification it is important to note that during the migration process, buckets are mapped to databases, measurements are mapped to tables, and each bucket's data is output to a single line protocol file.

### Verifying InfluxDB v2 Count

You can query InfluxDB v2, returning all points in bucket:

```flux
from(bucket: "<bucket name>")
   |> range(start: 0)
   |> filter(fn: (r) => r["_measurement"] == "your measurement name")
   |> group()
   |> count()
```

If you are confident that line protocol data was exported correctly, you can get the number of points in a bucket by calculating the number of lines in each file:
```shell
wc -l ~/engine/data/0b6d5261bb527ac5/bucket_name.lp
```

### Verifying InfluxDB v3 Count

To check the number of records in a table within a database, use SQL:
```sql
SELECT COUNT(*) FROM my_table_name
```

To compare the total number of records in an InfluxDB v3 database to the known number of records in an InfluxDB v2 bucket, this query must be executed for all tables in a database, since databases correspond to buckets.

## InfluxDB v3 Ingestion

A script, `./app/influxdb_v3_ingestion.py`, is provided that ingests data to Timestream for InfluxDB v3. This script is useful if you have already backed up your InfluxDB v2 data. The ingestion script is tailored to be used by the end-to-end migration script, and expects data to be organized in a specific way. To organize data the way that the ingestion script expects, you can backup, extract InfluxDB v2 data, and convert InfluxDB v2 data to line protocol using the following commands:
```shell
# Required input values.
INFLUXDB_V2_HOST=<InfluxDB v2 host>
INFLUXDB_V2_ORG=<organization name>
INFLUXDB_V2_TOKEN=<InfluxDB v2 operator token>

# Create backup directory. Naming is important.
mkdir -p ~/engine/data

# Backup all buckets from an InfluxDB v2 instance to a local directory
# using the InfluxDB v2 CLI.
influx backup \
    --host $INFLUXDB_V2_HOST \
    --org $INFLUXDB_V2_ORG \
    --token $INFLUXDB_V2_TOKEN \
    --compression none \
    ~/engine/data

# Extract all .tar files.
for f in ~/engine/data/*.tar; do tar -xzf "$f" -C ~/engine/data; done

# Extract all data to line protocol using the InfluxDB v2 CLI, the InfluxDB v2 daemon,
# and jq.
for bucket_id in $(ls -1d ~/engine/data/*/ | xargs -n 1 basename); do
    BUCKET_NAME=$(influx bucket list \
        --host $INFLUXDB_V2_HOST \
        --token $INFLUXDB_V2_TOKEN \
        --org $INFLUXDB_V2_ORG \
        --json \
        -i $bucket_id | jq -r ".[0].name")

    influxd inspect export-lp \
        --bucket-id $bucket_id \
        --engine-path ~/engine \
        --output-path "$(cd ~/engine/data/${bucket_id} && pwd)/${BUCKET_NAME}.lp"
done
```

Once your data is available in a `engine/data/` directory, `influxdb_v3_ingestion.py` can be used to ingest your data to InfluxDB v3:
```shell
# The AWS Secrets Manager secret holding InfluxDB v2 and v3 tokens.
TOKENS_SECRET_NAME=<tokens secret name>
INFLUXDB_V3_HOST=<InfluxDB v3 host>

# Create list of bucket names and bucket IDs.
BUCKET_NAMES_AND_IDS=""
for bucket_id in $(ls -1d ~/engine/data/*/ | xargs -n 1 basename); do
    BUCKET_LP_FILE=$(ls -1d ~/engine/data/${bucket_id}/*.lp | xargs -n 1 basename)
    BUCKET_NAME="${BUCKET_LP_FILE::${#BUCKET_LP_FILE}-3}"
    BUCKET_NAMES_AND_IDS="${BUCKET_NAME}:${BUCKET_ID},${BUCKET_NAMES_AND_IDS}"
done
# Remove trailing comma.
BUCKET_NAMES_AND_IDS="${BUCKET_NAMES_AND_IDS%?}"

# Ingest data.
python3 influxdb_v3_ingestion.py \
    --influxdb-v3-url $INFLUXDB_V3_HOST \
    --influxdb-v2-bucket-names-and-ids $BUCKET_NAMES_AND_IDS \
    --tokens-secret-name $TOKENS_SECRET_NAME \
    --backup-path ~/engine/data
```

Run the following command to view the ingestion script's full options:
```shell
python3 influxdb_v3_ingestion.py --help
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

## FAQ

### What is the cutoff time for migrated points?
All points before the migration begins will be migrated. Points ingested after or during the migration will not. This is due to the behaviour of the InfluxDB v2 CLI.

### Do I need the InfluxDB v2 daemon to be running?
No, the daemon (`influxd`) simply needs to be in your PATH, available for the script to use.

### Will Python `3.12` work?
No, you must use Python minimum version `3.14.1`.

### Why do I need to install the InfluxDB v2 CLI?
The InfluxDB v2 CLI is capable of doing backups efficiently. This could be done instead entirely with InfluxDB v2's HTTP API, but doing a backup is not as simple as a few HTTP requests.

### Why do I need to have the InfluxDB v2 daemon in my PATH?
InfluxDB v2 and InfluxDB v3 share a common file format, line protocol. Unfortunately, the only way to extract line protocol data from InfluxDB v2 is to use the InfluxDB v2 daemon. This cannot be done across a network. The InfluxDB v2 CLI is used to back up bucket data across a network, that bucket data is extracted, and the InfluxDB v2 daemon is used to transform data to line protocol. This is possible due to the fact that a backup is basically a file copy of an InfluxDB v2's `engine/data/` directory.
