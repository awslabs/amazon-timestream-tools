
import boto3
from logger_utils import create_logger
import botocore
import json

class S3Utility:
    def __init__(self, region):
        botocore_config = botocore.config.Config(
            max_pool_connections=5000, retries={'max_attempts': 10})
        self.s3_client = boto3.client(
            's3', region_name=region, config=botocore_config)
        self.logger = create_logger("s3_logger")
        self.region = region


    # create s3 bucket and return S3 bucket URI
    def create_s3_bucket(self, bucket_name):
        try:
            if self.region == 'us-east-1':
                response_s3 = self.s3_client.create_bucket(
                    Bucket=bucket_name
                )
            else:
                response_s3 = self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region
                    }
                )

            bucket_s3_uri = 's3://' + bucket_name
            self.logger.info(f'S3 Bucket created successfully {bucket_s3_uri}')
        except self.s3_client.exceptions.BucketAlreadyOwnedByYou:
            self.logger.warning(f"Bucket {bucket_name} is already created and owned by you")
            bucket_s3_uri = 's3://' + bucket_name
        except Exception as err:
            self.logger.error("Creating bucket {bucket_name} failed :", err)
            raise
        
        return bucket_s3_uri
    
    def fetch_json_from_s3(self, bucket, key):
        """Fetch JSON file from S3 and parse it."""
        response = self.s3_client.get_object(Bucket=bucket, Key=key)
        json_content = response["Body"].read().decode("utf-8")
        return json.loads(json_content)
