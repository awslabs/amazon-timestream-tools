from boto3 import session
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from http import HTTPStatus
from requests.exceptions import HTTPError
import json
import os
import requests
import time
import zipfile

MAX_WAIT_SECONDS = 900 # 15 minutes

def create_lambda(session: session, lambda_name: str, database_name: str, table_name: str, role_name: str,
                  partition_key_enforcement='OPTIONAL', dimension_partition_key=None,
                  mem_store_retention_period_in_hours=12, mag_store_retention_period_in_days=3653, batch_size=100) -> str:
    """
    Creates a Lambda function that will accept time series data and ingest the data into Timestream for LiveAnalytics.

    :param session: session: The AWS session to use for clients.
    :param lambda_name: str: What to name the Lambda function.
    :param database_name: str: The Timestream for LiveAnalytics database to ingest into. Will be created if it doesn't already exist.
    :param table_name: str: The Timestream for LiveAnalytics table to ingest into. Will be created if it doesn't already exist.
    :param role_name: str: The name to use for the Lambda's IAM role.
    :param partition_key_enforcement: str: Whether to require that all records contain the partition key. Options
        are 'OPTIONAL' or 'REQUIRED'. (Default = 'OPTIONAL')
    :param dimension_partition_key: str: The name of the dimension to use for the partition key. If not provided,
        the default partition key for the new table is 'MEASURE'. (Default = None)
    :param mem_store_retention_period_in_hours: If the table is created, the number of hours Timestream for LiveAnalytics will keep data in memory. (Default value = 12)
    :param mag_store_retention_period_in_days: If the table is created, the number of days Timestream for LiveAnalytics will keep data in magnetic storage. (Default value = 3653)
    :param batch_size: The number of records to write at a time to Timestream for LiveAnalytics. 100 is the maximum. (Default value = 100)
    :return: The Lambda function's URL.
    """

    role_arn = create_lambda_role(session, lambda_name, database_name, table_name, role_name)
    lambda_client = session.client('lambda')

    lambda_function_filename = "lambda_function.py"
    local_path = os.path.dirname(os.path.abspath(__file__))
    lambda_function_file_path = f"{local_path}/{lambda_function_filename}"

    # Create a deployment package (zip file)
    lambda_zip = "lambda_function.zip"
    with zipfile.ZipFile(lambda_zip, 'w') as zipf:
        zipf.write(filename=lambda_function_file_path, arcname=lambda_function_filename)

    total_wait_seconds = 0
    wait_seconds = 2
    while total_wait_seconds < MAX_WAIT_SECONDS:
        try:
            with open(lambda_zip, 'rb') as f:
                lambda_client.create_function(
                    FunctionName=lambda_name,
                    Runtime='python3.12',
                    Role=role_arn,
                    Handler='lambda_function.lambda_handler',
                    Architectures=['arm64'],
                    Code={'ZipFile': f.read()},
                    Environment={
                        'Variables': {
                            'REGION_NAME': lambda_client.meta.region_name,
                            'DATABASE_NAME': database_name,
                            'TABLE_NAME': table_name,
                            'MEM_STORE_RETENTION_PERIOD_IN_HOURS': str(mem_store_retention_period_in_hours),
                            'MAG_STORE_RETENTION_PERIOD_IN_DAYS': str(mag_store_retention_period_in_days),
                            'BATCH_SIZE': str(batch_size),
                            'PARTITION_KEY_ENFORCEMENT': partition_key_enforcement,
                            'DIMENSION_PARTITION_KEY': dimension_partition_key
                        }
                    },
                    Timeout=30,
                    MemorySize=128
                )
                print(f"Lambda function {lambda_name} created successfully.")
                break
        except lambda_client.exceptions.InvalidParameterValueException as e:
            print(e)
            print("Retrying creation of new Lambda function")
            time.sleep(wait_seconds)
            total_wait_seconds += wait_seconds
            wait_seconds *= 2
        except lambda_client.exceptions.ResourceConflictException:
            print(f"Lambda function {lambda_name} already exists.")
            break
        except Exception as e:
            raise

    os.remove(lambda_zip)

    # Add a resource policy to allow invocation via the function URL

    try:
        lambda_client.add_permission(
            FunctionName=lambda_name,
            StatementId='FunctionURLAllowInvoke',
            Action='lambda:InvokeFunctionUrl',
            Principal=role_arn,
            FunctionUrlAuthType='AWS_IAM'
        )
        print(f"Added resource policy to allow function URL invocation for {lambda_name}.")
    except lambda_client.exceptions.ResourceConflictException:
        print(f"Resource policy for {lambda_name} already exists.")

    # Create or get the Lambda Function URL
    try:
        response = lambda_client.create_function_url_config(
            FunctionName=lambda_name,
            AuthType='AWS_IAM'
        )
        function_url = response['FunctionUrl']
        print(f"Lambda Function URL: {function_url}")
    except lambda_client.exceptions.ResourceConflictException:
        # If the URL configuration already exists, retrieve it
        response = lambda_client.get_function_url_config(FunctionName=lambda_name)
        function_url = response['FunctionUrl']
        print(f"Lambda Function URL (existing): {function_url}")
    return function_url

def create_lambda_role(session: session, lambda_name: str, database_name: str, table_name: str, role_name: str) -> str:
    """
    Creates an IAM role to be used by a Lambda function to write time series data to Timestream for LiveAnalytics.

    :param session: session: The AWS session to use for clients.
    :param lambda_name: str: The name of the Lambda function.
    :param database_name: str: The Timestream for LiveAnalytics database.
    :param table_name: str: The Timestream for LiveAnalytics table.
    :param role_name: str: The name to use for the role.
    :returns: The IAM role ARN.
    """
    lambda_client = session.client('lambda')
    iam_client = session.client('iam')
    sts_client = session.client('sts')

    account_id = account_id = sts_client.get_caller_identity()['Account']

    # Create IAM Role for Lambda
    role_name = "TimestreamLambdaRole"
    assume_role_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole"
            }
        ]
    }

    role_arn = ""

    try:
        create_role_response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy),
            Description="Role for Lambda to write to Timestream"
        )
        print(f"Created IAM Role: {role_name}")
        role_arn = create_role_response['Role']['Arn']
    except iam_client.exceptions.EntityAlreadyExistsException:
        print(f"IAM Role {role_name} already exists")
        try:
            role_arn = iam_client.get_role(RoleName=role_name)['Role']['Arn']
        except iam_client.exceptions.NoSuchEntityException:
            print("IAM Role could not be found")
            raise

    # CloudWatch logs policy to be added to the role
    cloudwatch_logs_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents"
                ],
                "Resource": f"arn:aws:logs:{lambda_client.meta.region_name}:{account_id}:log-group:/aws/lambda/{lambda_name}*"
            }
        ]
    }

    # Add the CloudWatch logs policy to the role
    try:
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName='CloudWatchLogsPolicy',
            PolicyDocument=json.dumps(cloudwatch_logs_policy)
        )
        print(f"Attached CloudWatch logs policy to role: {role_name}")
    except Exception as e:
        print(f"Error attaching CloudWatch logs policy: {e}")

    timestream_write_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "timestream:WriteRecords",
                    "timestream:Select",
                    "timestream:DescribeTable",
                    "timestream:CreateTable"
                ],
                "Resource": f"arn:aws:timestream:{lambda_client.meta.region_name}:{account_id}:database/{database_name}/table/{table_name}"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "timestream:DescribeEndpoints"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "timestream:DescribeDatabase",
                    "timestream:CreateDatabase"
                ],
                "Resource": f"arn:aws:timestream:{lambda_client.meta.region_name}:{account_id}:database/{database_name}"
            }
        ]
    }

    # Add the Timestream write policy to the role
    try:
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName='TimestreamSampleWritePolicy',
            PolicyDocument=json.dumps(timestream_write_policy)
        )
        print(f"Attached TimestreamSampleWritePolicy policy to role: {role_name}")
    except Exception as e:
        print(f"Error attaching TimestreamSampleWritePolicy: {e}")

    print(f"Attached TimestreamSampleWritePolicy policy to {role_name}")
    return role_arn

def send_data_to_lambda(session: session, data: dict, function_url: str, precision="MILLISECONDS"):
    """
    Sends generated data to the Lambda function in chunks, in order to not exceed AWS Lambda's quota for the size of each request.

    :param session: session: The AWS session to use for clients.
    :param data: dict: The time series data to send to the Lambda function.
    :param function_url: str: The Lambda function's URL.
    :param precision: The Unix timestream precision for the data. (Default value = "MILLISECONDS")
    """
    MAX_REQUEST_SIZE = 6 * 1024 * 1024  # 6 MB in bytes
    method = "POST"

    # Calculate the size of the entire data payload
    data_payload = json.dumps({'records': data})
    total_size = len(data_payload.encode('utf-8'))

    # Check if the total size exceeds the maximum request size
    if total_size <= MAX_REQUEST_SIZE:
        send_request(session, method, function_url, data_payload, precision)
    else:
        # Chunk the data if it's too large
        chunk_size = MAX_REQUEST_SIZE - len(b'{"records":[]}')  # Reserve space for the JSON structure
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
        for chunk in chunks:
            chunk_payload = json.dumps({'records': chunk})
            send_request(session, method, function_url, chunk_payload, precision)

def send_request(session: session, method: str, function_url: str, payload: dict, precision="MILLISECONDS"):
    """
    Sends a single request to the Lambda function.

    :param session: session: The AWS session to use for clients.
    :param method: str: The HTTP method to use in the request.
    :param function_url: str: The Lambda function's URL.
    :param payload: dict: The time series data payload.
    :param precision: The Unix timestream precision for the data. (Default value = "MILLISECONDS")
    """
    request = AWSRequest(
        method=method,
        url=function_url,
        params={'precision': precision},
        headers={'Content-Type': 'application/json'},
        data=payload
    )

    SigV4Auth(session.get_credentials(), 'lambda', session.region_name).add_auth(request)

    MAX_WAIT_SECONDS = 900 # 15 minutes
    total_wait_seconds = 0
    wait_seconds = 2
    while total_wait_seconds < MAX_WAIT_SECONDS:
        try:
            response = requests.request(method, function_url, params={"precision": precision}, headers=dict(request.headers), data=payload, timeout=30)
            response.raise_for_status()
            print(f'Response Status: {response.status_code}')
            print(f'Response Body: {response.content.decode("utf-8")}')
            break
        except HTTPError as e:
            if e.response.status_code != HTTPStatus.GATEWAY_TIMEOUT:
                raise
            print(e)
            print("Retrying sending data")
            time.sleep(wait_seconds)
            total_wait_seconds += wait_seconds
            wait_seconds *= 2
