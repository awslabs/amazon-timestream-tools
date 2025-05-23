import random
import boto3
from logger_utils import create_logger
import botocore
import json
import time


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

    def s3_bucket_path_exists(
        self, bucket_name: str, prefix: str, delimiter="/"
    ) -> bool:
        try:
            # A multipart upload may be in progress, causing the path to be
            # inaccessible.
            self.wait_for_multipart_uploads(
                bucket_name=bucket_name, prefix=prefix, delimeter=delimiter
            )
            list_response = self.s3_client.list_objects_v2(
                Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter, MaxKeys=1
            )
            return "Contents" in list_response or (
                "CommonPrefixes" in list_response and list_response["CommonPrefixes"]
            )
        except Exception as e:
            self.logger.error(f"Checking bucket path failed: {e}")
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
        self.logger.info("Checking for S3 bucket access")
        result = {
            "exists": False,
            "accessible": False,
            "message": "",
            "bucket": "",
            "prefix": "",
        }
        # Remove s3:// prefix if present
        if s3_path.lower().startswith("s3://"):
            s3_path = s3_path[5:]
        # Extract bucket name and prefix
        parts = s3_path.split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""
        result["bucket"] = bucket_name
        result["prefix"] = prefix
        # Check if bucket exists
        if not self.s3_bucket_exists(bucket_name):
            result["message"] = f"Bucket {bucket_name} does not exist"
            return result
        result["exists"] = True
        # Check if we can list objects (read access)
        try:
            # If prefix is provided, check if we can list objects with that prefix
            if prefix:
                self.wait_for_multipart_uploads(bucket_name=bucket_name, prefix=prefix)
                self.s3_client.list_objects_v2(
                    Bucket=bucket_name, Prefix=prefix, MaxKeys=1
                )
                result["accessible"] = True
                result["message"] = (
                    f"Bucket {bucket_name} with prefix '{prefix}' exists and is accessible"
                )
            else:
                self.s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
                result["accessible"] = True
                result["message"] = f"Bucket {bucket_name} exists and is accessible"
        except Exception as e:
            result["message"] = (
                f"Bucket {bucket_name}{' with prefix ' + prefix if prefix else ''} exists but is not accessible: {str(e)}"
            )
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

        # For general-purpose buckets, list_objects_v2 will not return
        # prefixes while that prefix is related to an in-progress
        # multipart upload.
        self.wait_for_multipart_uploads(bucket_name=bucket_name, prefix=prefix)

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

    def get_first_of_type(self, bucket_path: str, file_type="parquet") -> str:
        """
        Return the full S3 URI to the first *.file_type* file that exists under the
        supplied bucket_path prefix.

        Args:
            bucket_path (str): An S3 URI ie. `s3://my-bucket/benchmark22/cpu/
                unload-2025-05-23-18:58:58/results`

        Returns:
            str: Full S3 URI to the first file type (ie. parquet) object discovered.

        Raises:
            ValueError: If *bucket_path* is not an S3 URI.
            FileNotFoundError: If no *.file_type* files are found beneath the prefix.
        """
        if not bucket_path.lower().startswith("s3://"):
            raise ValueError(
                "bucket_path must be an S3 URI of the form s3://<bucket>/<prefix>"
            )

        bucket_and_prefix = bucket_path[5:]
        bucket_name, prefix = bucket_and_prefix.split("/", 1)

        # Ensure the prefix ends with a slash so we stay inside bucket_path
        if not prefix.endswith("/"):
            prefix += "/"

        self.wait_for_multipart_uploads(bucket_name=bucket_name, prefix=prefix)

        # Paginate through keys until we encounter the first *.file_type
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(f".{file_type}"):
                    return f"s3://{bucket_name}/{key}"

        raise FileNotFoundError(f"No .{file_type} objects found under {bucket_path}")

    def wait_for_multipart_uploads(
        self, bucket_name, prefix, delimeter="/", max_attempts=20, base_delay=1
    ):
        """
        Waits for all multipart uploads related to a prefix within an S3 bucket
        to complete or abort.

        Args:
            bucket_name (str): The name of the S3 bucket.
            prefix (str): The prefix to check. For example, "benchmark22/cpu/".
            delimeter (str): The delimiter that separates paths within the S3
                bucket. Defaults to "/".
            max_attempts (int): The maximum attempts to check whether any
                multipart uploads are in progress. Defaults to 20.
            base_delay (int): The base delay in seconds to use for
                backoffs with exponential retries and jitter.

        Returns:
            None
        """
        for attempt in range(max_attempts):
            multipart_uploads = self.s3_client.list_multipart_uploads(
                Bucket=bucket_name, Delimiter=delimeter, Prefix=prefix
            ).get("Uploads", [])

            if not multipart_uploads:
                return

            # Max 1 minute delay.
            delay = min(base_delay * (2**attempt), 60)
            # 10% jitter.
            jitter = random.uniform(0, delay * 0.1)
            sleep_time = delay + jitter
            self.logger.info(
                f"Multipart uploads are still in progress. Retrying in {sleep_time:.2f}s (attempt {attempt + 1})/{max_attempts})"
            )
            time.sleep(sleep_time)
        raise TimeoutError(
            f"Multipart uploads did not complete after {max_attempts} attempts"
        )
