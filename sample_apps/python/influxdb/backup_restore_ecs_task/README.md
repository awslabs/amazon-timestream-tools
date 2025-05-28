# Backing up and Restoring Timestream for InfluxDB with ECS

This guide explains how to set up an ECS-based solution for backing up and restoring data contained within a Timestream for InfluxDB bucket according to a schedule.

## Solution Overview

This solution uses:

- **Amazon ECS** with Fargate to run containerized backup/restore tasks.
- **Amazon EFS** for temporary storage during backup and restore operations.
- **Amazon S3** for long-term storage of backed up data.
- **Amazon EventBridge** for scheduling regular backup and restore operations.
- **Amazon ECR** for storing the container image.

## Prerequisites

- [AWS CLI installed and configured](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html).
- [SAM CLI installed and configured](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html).
- [Docker installed locally](https://www.docker.com/get-started/).
- Access to one or more Timestream for InfluxDB instances.

## Setup Instructions

### Deployment Script

Instead of following the steps below, a deployment script for macOS and Linux has been provided, `deploy.sh`. This script prompts for parameters and deploys the stack.

### 1. Build the Docker Image

```bash
docker buildx build --platform linux/arm64 -t influxdb-backup-restore:latest .
```

### 2. Create a Secret Containing Timestream for InfluxDB Operator Tokens

Backing up and restoring requires operator tokens from both the source and destination instances. To provide the tokens safely to the ECS task, AWS Secrets Manager will be used.

Run the following command to create the secret, replacing `<backup token>` with an operator token from your source instance, `<restore token>` with an operator token from your destination instance, and `<tokens secret name>` with the name that you want to use to name the secret:

```bash
SECRET_STRING="{\"BACKUP_TOKEN\":\"<backup token>\",\"RESTORE_TOKEN\":\"<restore token>\"}"

aws secretsmanager create-secret \
    --name "<tokens secret name>" \
    --description "Tokens for backing up and restoring Timestream for InfluxDB data" \
    --secret-string "$SECRET_STRING"
```

### 3. Deploy the CloudFormation Stack

Use the following command to deploy the stack. Replace the following:
- `<stack name>` with your desired stack name.
- `<backup URL>` with the URL for your source Timestream for InfluxDB instance, including its scheme and port. For example, `https://example.com:8086`.
- `<tokens secret name>` with the name of the secret you created in step 2.
- `<backup org>` with the name of the organization your backup bucket belongs to in your source instance.
- `<backup port>` with the port that the instance to backup from listens on. This defaults to `8086` if not provided.
- `<restore URL>` with the URL of your destination Timestream for InfluxDB instance, including its scheme and port.
- `<restore org>` with the name of the organization you want to restore your bucket to in the destination instance.
- `<restore port>` with the port that the instance to restore to listens on. This defaults to `8086` if not provided.
- `<bucket name>` with the name of the bucket you want to back up in the source instance.
- `<s3 backup bucket name>` with the name that you want to use for a newly-created S3 bucket that will be used to hold bucket data.
- `<backup schedule>` with a [cron expression](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-scheduled-rule-pattern.html) describing how often you want backups to occur. For example, `cron(0 12 ? * FRI *)` does a backup every Friday at noon, UTC.
- `<restore schedule>` with a [cron expression](https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-scheduled-rule-pattern.html) describing how often you want restores to occur.
- `<EFS file system KMS key ID>` with the ID of a KMS key that you want to use to encrypt the temporary EFS storage the ECS task uses during execution. If not provided, then the default KMS key for EFS, `/aws/elasticfilesystem`, will be used.
- `<ECR repository name>` with the name that you want to use for a newly-created ECR repository.

```bash
sam deploy \
  --template-file template.yaml \
  --stack-name <stack name> \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    BackupUrl=<backup URL> \
    TokensSecretName=<tokens secret name> \
    BackupOrg=<backup org> \
    BackupPort=<backup port> \
    RestoreUrl=<restore URL> \
    RestoreOrg=<restore org> \
    RestorePort=<restore port> \
    BucketName=<bucket name> \
    S3BackupBucketName=<s3 backup bucket name> \
    BackupSchedule="\"<backup schedule>\"" \
    RestoreSchedule="\"<restore schedule>\"" \
    EFSFileSystemKMSKeyID=<EFS file system KMS key ID> \
    ECRRepositoryName=<ECR repository name>
```

Scheduled restores will replace any buckets with the same name as `<bucket name>` in the destination instance.

### 4. Tag and Upload the Docker Image

Now that your stack has successfully deployed, use the following command to push your image to your ECR repository, replacing `<ECR repository name>` with the name of your ECR repository:

```bash
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=$(aws configure get region)

aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

docker tag influxdb-backup-restore:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/<ECR repository name>:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/<ECR repository name>:latest
```

## Manual Execution

On macOS and Linux, you can always manually trigger backup or restore operations regardless of the schedule using `run-task.sh`:

```bash
./run-task.sh backup --stack-name <stack name>
./run-task.sh restore --force-replace --stack-name <stack name>
```

## Flexible Bucket Restore Options

The solution now supports flexible options for handling bucket restoration:

### Restore Options

One of the following options must be set:

1. **Force Replace**: Deletes the existing bucket before restoring.
   ```bash
   ./run-task.sh restore --force-replace
   ```

2. **Unique Restore Name**: Creates a new bucket with a timestamped name (e.g., `my_bucket_20250404_123045`).
   ```bash
   ./run-task.sh restore --unique-restore-name
   ```

## Stack Deletion

On macOS and Linux, the `delete.sh` has been provided to empty your S3 bucket and delete your stack.
