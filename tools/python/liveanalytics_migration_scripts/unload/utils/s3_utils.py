import boto3
from logger_utils import create_logger
import botocore
import json


class S3Utility:
    def __init__(self, region=None, log_file=None):
        botocore_config = botocore.config.Config(
            max_pool_connections=5000, retries={"max_attempts": 10}
        )
        self.s3_client = boto3.client("s3", region_name=region, config=botocore_config)
        self.logger = create_logger("s3_logger", log_file=log_file)
        self.region = region

    def s3_bucket_exists(self, bucket_name: str) -> bool:
        """
        Checks whether an S3 bucket exists.

        Args:
            bucket_name (str): The name of the bucket to check.

        Returns:
            bool: Whether the bucket exists.
        """
        waiter = self.s3_client.get_waiter("bucket_exists")
        try:
            waiter.wait(Bucket=bucket_name, WaiterConfig={"Delay": 5, "MaxAttempts": 5})
            return True
        except Exception:
            return False

    def check_bucket_access(self, s3_path: str) -> dict:
        """
        Checks if a bucket exists and is accessible for the current user.
        Args:
            s3_path (str): The S3 path to check (can be bucket only or bucket with prefix).
        Returns:
            dict: A dictionary with 'exists' (bool), 'accessible' (bool), 'message' (str),
                  'bucket' (str), and 'prefix' (str) keys.
        """
        self.logger.info(f"Checking for S3 Bucket access")
        result = {
            'exists': False,
            'accessible': False,
            'message': '',
            'bucket': '',
            'prefix': ''
        }
        # Remove s3:// prefix if present
        if s3_path.lower().startswith('s3://'):
            s3_path = s3_path[5:]
        # Extract bucket name and prefix
        parts = s3_path.split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        result['bucket'] = bucket_name
        result['prefix'] = prefix
        # Check if bucket exists
        if not self.s3_bucket_exists(bucket_name):
            result['message'] = f"Bucket {bucket_name} does not exist"
            return result
        result['exists'] = True
        # Check if we can list objects (read access)
        try:
            # If prefix is provided, check if we can list objects with that prefix
            if prefix:
                self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix, MaxKeys=1)
                result['accessible'] = True
                result['message'] = f"Bucket {bucket_name} with prefix '{prefix}' exists and is accessible"
            else:
                self.s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
                result['accessible'] = True
                result['message'] = f"Bucket {bucket_name} exists and is accessible"
        except Exception as e:
            result['message'] = f"Bucket {bucket_name}{' with prefix '+prefix if prefix else ''} exists but is not accessible: {str(e)}"
        return result

    def create_s3_bucket(self, bucket_name: str) -> str:
        """
        Creates an S3 bucket and returns the S3 bucket URI.

        Args:
            bucket_name (str): The name of the bucket to create.

        Returns:
            str: The newly-created bucket URI.
        """
        try:
            if self.region == "us-east-1":
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={"LocationConstraint": self.region},
                )

            bucket_s3_uri = "s3://" + bucket_name
            self.logger.info(f"S3 Bucket created successfully {bucket_s3_uri}")
        except self.s3_client.exceptions.BucketAlreadyOwnedByYou:
            self.logger.warning(
                f"Bucket {bucket_name} is already created and owned by you"
            )
            bucket_s3_uri = "s3://" + bucket_name
        except Exception as err:
            self.logger.error("Creating bucket {bucket_name} failed :", err)
            raise

        return bucket_s3_uri

    def fetch_json_from_s3(self, bucket_name: str, key: str) -> dict:
        """
        Fetches a JSON file from an S3 bucket and parses it to a dict.

        Args:
            bucket_name (str): The name of the S3 bucket to fetch JSON from.
            key (str): The key in which the JSON file is located.

        Returns:
            dict: The JSON file as a dict.
        """
        response = self.s3_client.get_object(Bucket=bucket_name, Key=key)
        json_content = response["Body"].read().decode("utf-8")
        return json.loads(json_content)

    def get_latest_unload_path(
        self,
        bucket_name: str,
        timestream_database_name: str,
        timestream_table_name: str,
    ):
        """
        Gets the latest unload path within an S3 bucket. Using the unload script, paths for unloaded data
        is expected to be in
        s3://<s3 bucket name>/<timestream database name>/<timestream table name>/unload-<%Y-%m-%d-%H:%M:%S>/results
        """
        prefix = f"{timestream_database_name}/{timestream_table_name}/"
        list_response = self.s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, Delimiter="/"
        )

        unload_dirs = []
        if "CommonPrefixes" in list_response:
            for obj in list_response["CommonPrefixes"]:
                dir_prefix = obj["Prefix"]
                if "unload-" in dir_prefix:
                    unload_dirs.append(dir_prefix)
        if not unload_dirs:
            raise RuntimeError(
                f"No unload paths found for s3://{bucket_name}/{timestream_database_name}/{timestream_table_name}"
            )
        unload_dirs.sort()
        latest_dir = unload_dirs[-1]
        parts = latest_dir.rstrip("/").split("/")
        return parts[-1]
