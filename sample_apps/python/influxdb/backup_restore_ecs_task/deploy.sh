#!/bin/bash

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# Script to build and deploy the InfluxDB backup/restore ECS solution using SAM

set -e

DEFAULT_STACK_NAME="influxdb-backup-restore"

usage() {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  --stack-name <n>    CloudFormation stack name (default: $DEFAULT_STACK_NAME)"
    echo "  --help                 Display this help message"
    exit 1
}

STACK_NAME=$DEFAULT_STACK_NAME

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --stack-name)
            STACK_NAME="$2"
            shift # past argument
            shift # past value
            ;;
        --help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

if ! command -v aws &> /dev/null; then
    echo "AWS CLI is not installed. Please install it first."
    exit 1
fi

if ! command -v docker &> /dev/null; then
    echo "Docker is not installed. Please install it first."
    exit 1
fi

if ! command -v sam &> /dev/null; then
    echo "AWS SAM CLI is not installed. Please install it first."
    echo "Run: pip install aws-sam-cli"
    exit 1
fi

AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AWS_REGION=$(aws configure get region)
ECR_REPO_NAME=$STACK_NAME

echo "Using AWS Account: $AWS_ACCOUNT_ID"
echo "Using AWS Region: $AWS_REGION"
echo "Using stack name: $STACK_NAME"

echo "Building Docker image ${ECR_REPO_NAME}:latest ..."
docker buildx build --platform linux/arm64 -t $ECR_REPO_NAME:latest .

if [[ -z "$TOKENS_SECRET_NAME" ]]; then
    read -p "Enter the name of the AWS Secrets Manager secret to store tokens [${STACK_NAME}-secret]: " TOKENS_SECRET_NAME
    TOKENS_SECRET_NAME=${TOKENS_SECRET_NAME:-"${STACK_NAME}-secret"}
fi

if [[ -z "$BACKUP_URL" ]]; then
    read -p "Enter Timestream for InfluxDB backup endpoint (e.g., https://example.com:8086): " BACKUP_URL
fi

if [[ -z "$BACKUP_PORT" ]]; then
    if [[ $BACKUP_URL =~ :[0-9]+$ ]]; then
        BACKUP_PORT=$(echo $BACKUP_URL | sed -e 's,^.*:,:,g' -e 's,.*:\([0-9]*\).*,\1,g' -e 's,[^0-9],,g')
    else
        echo "${BACKUP_URL} is missing a port. URL must use the format scheme://influxdb_endpoint:port. For example, https://example.com:8086"
        exit 1
    fi
fi

if [[ ! "$BACKUP_PORT" =~ ^[0-9]+$ ]]; then
    echo "Port ${BACKUP_PORT} was incorrectly parsed from ${BACKUP_URL}"
    exit 1
fi

if [[ -z "$BACKUP_ORG" ]]; then
    read -p "Enter Timestream for InfluxDB backup organization: " BACKUP_ORG
fi

if [[ -z "$RESTORE_URL" ]]; then
    read -p "Enter Timestream for InfluxDB restore endpoint (e.g., https://influxdb-endpoint:8086): " RESTORE_URL
fi

if [[ -z "$RESTORE_PORT" ]]; then
    if [[ $RESTORE_URL =~ :[0-9]+$ ]]; then
        RESTORE_PORT=$(echo $RESTORE_URL | sed -e 's,^.*:,:,g' -e 's,.*:\([0-9]*\).*,\1,g' -e 's,[^0-9],,g')
    else
        echo "${RESTORE_URL} is missing a port. URL must use the format scheme://influxdb_endpoint:port. For example, https://example.com:8086"
        exit 1
    fi
fi

if [[ ! "$RESTORE_PORT" =~ ^[0-9]+$ ]]; then
    echo "Port ${RESTORE_PORT} was incorrectly parsed from ${RESTORE_URL}"
    exit 1
fi

if [[ -z "$RESTORE_ORG" ]]; then
    read -p "Enter Timestream for InfluxDB restore organization: " RESTORE_ORG
fi

if [[ -z "$RESTORE_PORT" ]]; then
    read -p "Enter the port that the Timestream for InfluxDB restore instance uses (8086): " RESTORE_PORT
    RESTORE_PORT=${RESTORE_PORT:8086}
fi

if [[ -z "$BUCKET_NAME" ]]; then
    read -p "Enter bucket name to backup and restore: " BUCKET_NAME
fi

if [[ -z "$S3_BACKUP_BUCKET_NAME" ]]; then
    read -p "Enter S3 bucket name for storing backups (will be created if it doesn't exist): " S3_BACKUP_BUCKET_NAME
fi

if [[ -z "$DELETE_S3_BACKUP_BUCKET" ]]; then
    read -p "Delete backup S3 bucket after a configurable amount of time? true/false (false): " DELETE_S3_BACKUP_BUCKET
    DELETE_S3_BACKUP_BUCKET=${DELETE_S3_BACKUP_BUCKET:-"false"}
fi

if [[ "$DELETE_S3_BACKUP_BUCKET" != "true" && "$DELETE_S3_BACKUP_BUCKET" != "false" ]]; then
    echo "Invalid value ${DELETE_S3_BACKUP_BUCKET}: must be true or false"
    exit 1
fi

if [[ -z "$S3_BACKUP_BUCKET_RETENTION_DAYS" ]]; then
    read -p "Enter the number of days to retain the backup S3 bucket before deleting, if deletion is enabled (365): " S3_BACKUP_BUCKET_RETENTION_DAYS
    S3_BACKUP_BUCKET_RETENTION_DAYS=${S3_BACKUP_BUCKET_RETENTION_DAYS:-"365"}
fi

S3_BACKUP_BUCKET_RETENTION_DAYS_REGEX="^[0-9]+$"

if [[ ! "$S3_BACKUP_BUCKET_RETENTION_DAYS" =~ $S3_BACKUP_BUCKET_RETENTION_DAYS_REGEX ]]; then
    echo "Invalid value ${S3_BACKUP_BUCKET_RETENTION_DAYS}: must be a positive integer"
    exit 1
fi

if [[ -z "$BACKUP_SCHEDULE" ]]; then
    read -p "Enter backup schedule cron expression [cron(0 12 ? * FRI *)] (every Friday at noon): " BACKUP_SCHEDULE
    BACKUP_SCHEDULE=${BACKUP_SCHEDULE:-"cron(0 12 ? * FRI *)"}
fi

if [[ -z "$RESTORE_SCHEDULE" ]]; then
    read -p "Enter restore schedule cron expression [cron(0 12 ? * MON *)] (every Monday at noon): " RESTORE_SCHEDULE
    RESTORE_SCHEDULE=${RESTORE_SCHEDULE:-"cron(0 12 ? * MON *)"}
fi

if [[ -z "$BACKUP_TOKEN" ]]; then
    read -sp "Enter Timestream for InfluxDB backup token: " BACKUP_TOKEN
    echo
fi

if [[ -z "$RESTORE_TOKEN" ]]; then
    read -sp "Enter Timestream for InfluxDB restore token: " RESTORE_TOKEN
    echo
fi

if [[ -z "$EFS_FILE_SYSTEM_KMS_KEY_ID" ]]; then
    read -sp "Enter KMS key ID for EFS file system (no value): " EFS_FILE_SYSTEM_KMS_KEY_ID
    # /aws/elasticfilesystem is the default KMS key ID used when an EFS file system is encrypted but no
    # customer-defined KMS key ID is provided.
    EFS_FILE_SYSTEM_KMS_KEY_ID=${EFS_FILE_SYSTEM_KMS_KEY_ID:-"\"\""}
fi

echo "Creating/updating tokens in AWS Secrets Manager..."
SECRET_STRING="{\"BACKUP_TOKEN\":\"$BACKUP_TOKEN\",\"RESTORE_TOKEN\":\"$RESTORE_TOKEN\"}"

SECRET_EXISTS=$(aws secretsmanager describe-secret --secret-id "$TOKENS_SECRET_NAME" 2> /dev/null || echo "false")

if [[ "$SECRET_EXISTS" == "false" ]]; then
    echo "Creating new secret $TOKENS_SECRET_NAME ..."
    aws secretsmanager create-secret \
        --name "$TOKENS_SECRET_NAME" \
        --description "Tokens for backing up and restoring Timestream for InfluxDB data" \
        --secret-string "$SECRET_STRING" \
        --output text > /dev/null
else
    echo "Updating existing secret $TOKENS_SECRET_NAME ..."
    aws secretsmanager update-secret \
        --secret-id "$TOKENS_SECRET_NAME" \
        --secret-string "$SECRET_STRING" \
        --output text > /dev/null
fi

echo "Deploying SAM template..."
sam deploy \
  --template-file template.yaml \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    BackupUrl=$BACKUP_URL \
    TokensSecretName=$TOKENS_SECRET_NAME \
    BackupOrg=$BACKUP_ORG \
    BackupPort=$BACKUP_PORT \
    RestoreUrl=$RESTORE_URL \
    RestoreOrg=$RESTORE_ORG \
    RestorePort=$RESTORE_PORT \
    BucketName=$BUCKET_NAME \
    S3BackupBucketName=$S3_BACKUP_BUCKET_NAME \
    BackupSchedule="\"$BACKUP_SCHEDULE\"" \
    RestoreSchedule="\"$RESTORE_SCHEDULE\"" \
    DeleteS3BackupBucket=$DELETE_S3_BACKUP_BUCKET \
    S3BackupBucketRetentionDays=$S3_BACKUP_BUCKET_RETENTION_DAYS \
    EFSFileSystemKMSKeyID=$EFS_FILE_SYSTEM_KMS_KEY_ID \
    ECRRepositoryName=$ECR_REPO_NAME

echo "SAM deployment complete. Now pushing Docker image to the created ECR repository..."

echo "Logging in to ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com

echo "Tagging and pushing image to ECR..."
docker tag $ECR_REPO_NAME:latest $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_NAME:latest
docker push $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com/$ECR_REPO_NAME:latest

printf "Deployment complete\n\n"
printf "To run a manual backup:\n\t./run-task.sh backup --stack-name $STACK_NAME\n"
printf "To run a manual restore, replacing any existing buckets with the same name:\n\t./run-task.sh restore --stack-name $STACK_NAME --force-replace\n"
printf "To run a manual restore, creating a new bucket with a unique name:\n\t./run-task.sh restore --stack-name $STACK_NAME --unique-restore-name\n"
