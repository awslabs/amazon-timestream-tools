#!/bin/bash
# Script to delete the InfluxDB backup/restore stack

set -e

# Default stack name
DEFAULT_STACK_NAME="influxdb-backup-restore"

# Parse command line arguments
STACK_NAME=$DEFAULT_STACK_NAME
FORCE=false

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --stack-name)
            STACK_NAME="$2"
            shift # past argument
            shift # past value
            ;;
        --force)
            FORCE=true
            shift # past argument
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--stack-name <name>] [--force]"
            exit 1
            ;;
    esac
done

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "AWS CLI is not installed. Please install it first."
    exit 1
fi

# Get the S3 backup bucket name from the stack outputs
S3_BACKUP_BUCKET_NAME=$(aws cloudformation describe-stacks --stack-name $STACK_NAME \
    --query "Stacks[0].Outputs[?OutputKey=='S3BackupBucket'].OutputValue" --output text)

if [[ ! -z "$S3_BACKUP_BUCKET_NAME" && "$S3_BACKUP_BUCKET_NAME" != "None" ]]; then
    if [ "$FORCE" = true ]; then
        echo "Emptying S3 bucket $S3_BACKUP_BUCKET_NAME before deletion..."
        aws s3 rm s3://$S3_BACKUP_BUCKET_NAME --recursive
    else
        read -p "Do you want to empty the S3 bucket $S3_BACKUP_BUCKET_NAME? This will delete all backup data. (y/n) " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "Emptying S3 bucket $S3_BACKUP_BUCKET_NAME before deletion..."
            aws s3 rm s3://$S3_BACKUP_BUCKET_NAME --recursive
        else
            echo "Bucket will not be emptied. Stack deletion may fail."
        fi
    fi
fi

# Delete the stack
echo "Deleting stack $STACK_NAME..."
aws cloudformation delete-stack --stack-name $STACK_NAME

echo "Waiting for stack deletion to complete..."
aws cloudformation wait stack-delete-complete --stack-name $STACK_NAME

echo "Stack deletion complete!"
