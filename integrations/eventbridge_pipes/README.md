# Amazon Timestream integration with AWS EventBridge Pipes

This sample demostrates the process of ingesting data into Amazon Timestream from Kinesis Streams via EventBridge Pipes.

Learn more about this integration and steps to map Kinesis record to Timestream data model from [this blog post](https://aws.amazon.com/blogs/database/build-time-series-applications-faster-with-amazon-eventbridge-pipes-and-timestream-for-liveanalytics/)

## Requirements

* [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) installed and configured
* [Git Installed](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* [AWS Serverless Application Model](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) (AWS SAM) installed

## Deployment Instructions

1. Create a new directory, navigate to that directory in a terminal and clone the GitHub repository:
    ``` 
    git clone https://github.com/awslabs/amazon-timestream-tools.git
    ```
1. Change directory:
    ```
    cd integrations/eventbridge_pipes/
    ```
1. From the command line, use AWS SAM to deploy the AWS resources for the pattern as specified in the template.yml file:
    ```
    sam deploy --guided
    ```
1. During the prompts:
    * Enter a stack name
    * Enter the desired AWS Region
    * Allow SAM CLI to create IAM roles with the required permissions.

## How it works

The template will create a Timestream table, Kinesis Stream, SQS Queue, CW log group and an IAM Role, and connect it together with a new EventBridge Pipe.

When records are sent to a Kinesis stream, the Pipe will convert into a valid Timestream record and ingest it to the Timestream table.

Please follow [this documentation](https://docs.aws.amazon.com/timestream/latest/developerguide/Kinesis.html#Kinesis-via-pipes) for more information.

## Testing

```
sh command.sh <Kinesis Stream Name> <Region>
```

## Cleanup
 
1. Delete the stack
    ```bash
    aws cloudformation delete-stack --stack-name STACK_NAME
    ```