#!/bin/bash
# Script to manually run the InfluxDB backup/restore ECS task

set -e

# Default stack name
DEFAULT_STACK_NAME="influxdb-backup-restore"

# Function to display usage information
usage() {
    echo "Usage: $0 [backup|restore] [options]"
    echo "Options:"
    echo "  --unique-restore-name When restoring, create a new uniquely named bucket, retaining the original bucket"
    echo "  --force-replace       Delete and recreate the bucket in Timestream for InfluxDB when restoring"
    echo "  --stack-name <name>   CloudFormation stack name (default: $DEFAULT_STACK_NAME)"
    echo "  --help                Display this help message"
    exit 1
}

# Check if operation is provided
if [[ $# -lt 1 ]]; then
    usage
fi

OPERATION=$1
shift

if [[ "$OPERATION" != "backup" && "$OPERATION" != "restore" ]]; then
    echo "Error: First argument must be either 'backup' or 'restore'"
    usage
fi

# Parse command line arguments
STACK_NAME=$DEFAULT_STACK_NAME

while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --stack-name)
            STACK_NAME="$2"
            shift # past argument
            shift # past value
            ;;
        --unique-restore-name)
            UNIQUE_RESTORE_NAME=true
            shift
            ;;
        --force-replace)
            FORCE_REPLACE=true
            shift
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

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "AWS CLI is not installed. Please install it first."
    exit 1
fi

if [[ $OPERATION = "restore" && ! -z "$UNIQUE_RESTORE_NAME" && ! -z "$FORCE_REPLACE" ]]; then
    echo "Use either --force-replace or --unique-restore-name, not both"
    exit 1
fi

if [[ $OPERATION = "restore" && -z "$UNIQUE_RESTORE_NAME" && -z "$FORCE_REPLACE" ]]; then
    echo "--force-replace or --unique-restore-name must be set"
    exit 1
fi

# Build command array for container overrides
COMMAND_ARRAY="[\"--operation\",\"$OPERATION\""

# Add optional parameters if set
if [[ ! -z "$UNIQUE_RESTORE_NAME" ]]; then
    COMMAND_ARRAY+=",\"--unique-restore-name\""
fi

if [[ ! -z "$FORCE_REPLACE" ]]; then
    COMMAND_ARRAY+=",\"--force-replace\""
fi

COMMAND_ARRAY+="]"

# Get required resources from CloudFormation stack
echo "Getting resources from CloudFormation stack $STACK_NAME..."
CLUSTER_NAME=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query "Stacks[0].Outputs[?OutputKey=='ClusterName'].OutputValue" --output text)
TASK_DEFINITION=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query "Stacks[0].Outputs[?OutputKey=='TaskDefinition'].OutputValue" --output text)
SUBNET_1=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query "Stacks[0].Outputs[?OutputKey=='PublicSubnet1'].OutputValue" --output text)
SUBNET_2=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query "Stacks[0].Outputs[?OutputKey=='PublicSubnet2'].OutputValue" --output text)
SECURITY_GROUP=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query "Stacks[0].Outputs[?OutputKey=='TaskSecurityGroup'].OutputValue" --output text)
CONTAINER_NAME=$(aws cloudformation describe-stacks --stack-name $STACK_NAME --query "Stacks[0].Outputs[?OutputKey=='ContainerName'].OutputValue" --output text)

if [[ -z "$CLUSTER_NAME" || -z "$TASK_DEFINITION" || -z "$SUBNET_1" || -z "$SUBNET_2" || -z "$SECURITY_GROUP" ]]; then
    echo "Error: Could not retrieve all required resources from CloudFormation stack."
    exit 1
fi

# Run the ECS task
echo "Running $OPERATION task..."
TASK_ARN=$(aws ecs run-task \
    --cluster $CLUSTER_NAME \
    --task-definition $TASK_DEFINITION \
    --launch-type FARGATE \
    --network-configuration "awsvpcConfiguration={subnets=[$SUBNET_1,$SUBNET_2],securityGroups=[$SECURITY_GROUP],assignPublicIp=ENABLED}" \
    --overrides "{\"containerOverrides\":[{\"name\":\"$CONTAINER_NAME\",\"command\":$COMMAND_ARRAY}]}" \
    --query "tasks[0].taskArn" \
    --output text)

if [[ -z "$TASK_ARN" ]]; then
    echo "Error: Failed to start ECS task."
    exit 1
fi

echo "Task started successfully: $TASK_ARN"
echo "You can monitor the task status with:"
printf "\taws ecs describe-tasks --cluster $CLUSTER_NAME --tasks $TASK_ARN --query \"tasks[0].containers[0]\" --no-cli-pager\n"
echo "And view logs in CloudWatch Logs."

