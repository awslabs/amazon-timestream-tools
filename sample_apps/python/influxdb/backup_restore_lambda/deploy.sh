#!/bin/bash
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

if [[ -z "$BACKUP_ORG" ]]; then
    read -p "Enter Timestream for InfluxDB backup organization: " BACKUP_ORG
fi

if [[ -z "$RESTORE_URL" ]]; then
    read -p "Enter Timestream for InfluxDB restore endpoint (e.g., https://influxdb-endpoint:8086): " RESTORE_URL
fi

if [[ -z "$RESTORE_ORG" ]]; then
    read -p "Enter Timestream for InfluxDB restore organization: " RESTORE_ORG
fi

if [[ -z "$BUCKET_NAME" ]]; then
    read -p "Enter bucket name to backup and restore: " BUCKET_NAME
fi

if [[ -z "$S3_BACKUP_BUCKET_NAME" ]]; then
    read -p "Enter S3 bucket name for storing backups (will be created if it doesn't exist): " S3_BACKUP_BUCKET_NAME
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
    RestoreUrl=$RESTORE_URL \
    RestoreOrg=$RESTORE_ORG \
    BucketName=$BUCKET_NAME \
    S3BackupBucketName=$S3_BACKUP_BUCKET_NAME \
    BackupSchedule="\"$BACKUP_SCHEDULE\"" \
    RestoreSchedule="\"$RESTORE_SCHEDULE\"" \
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
