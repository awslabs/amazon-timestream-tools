# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import os
import random
import sys
import time

import boto3
import botocore

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from unload.utils.logger_utils import create_logger


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
            bucket_path (str): An S3 URI including bucket name and prefix i.e.,
                `s3://my-bucket/benchmark22/cpu/unload-2025-05-23-18:58:58/results`

        Returns:
            str: Full S3 URI to the first file type (i.e., parquet) object discovered.

        Raises:
            ValueError: If *bucket_path* does not include both bucket and prefix.
            FileNotFoundError: If no *.file_type* files are found beneath the prefix.
        """
        if bucket_path.lower().startswith("s3://"):
            bucket_path = bucket_path[5:]

        try:
            bucket_name, prefix = bucket_path.split("/", 1)
        except ValueError:
            raise ValueError("bucket_path must include both bucket and prefix.")

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

    def sync_line_protocol_to_storage(
        self,
        s3_bucket_path: str,
        directory: str,
        timestream_database_name="",
        timestream_table_name="",
    ):
        """
        Downloads line protocol data from an S3 bucket path to a directory.
        If provided simply the bucket path and the Timestream database and
        table name, the latest unload directory will be searched for. If the
        S3 bucket path is a path within an S3 bucket, such as s3://my-bucket/my-path,
        then all objects will be downloaded from this path.

        Args:
            s3_bucket_path (str): The path of the S3 bucket to download objects from,
                for example, s3://my-bucket, or, s3://my-bucket/my-path.
            directory (str): The path to the local directory to download objects to.
            timestream_database_name (str): The name of the Timestream for LiveAnalytics
                database used in the unload process.
            timestream_table_name (str): The name of the Timestream for LiveAnalytics
                table used in the unload process.
        Returns:
            None
        """
        if s3_bucket_path.lower().startswith("s3://"):
            s3_bucket_path = s3_bucket_path[5:]
        s3_bucket_parts = s3_bucket_path.split("/")
        if not s3_bucket_parts:
            raise RuntimeError("S3 bucket path was empty")
        s3_bucket_name = s3_bucket_parts[0]
        if not self.s3_bucket_exists(s3_bucket_name):
            raise RuntimeError(f"S3 bucket {s3_bucket_name} does not exist")

        os.makedirs(directory, exist_ok=True)

        if len(s3_bucket_parts) > 1:
            prefix = "/".join(s3_bucket_parts[1:])
            if not self.s3_bucket_path_exists(
                bucket_name=s3_bucket_name, prefix=prefix
            ):
                raise RuntimeError(
                    f"The S3 bucket path {s3_bucket_path} does not exist"
                )
            line_protocol_path = s3_bucket_path
        else:
            if not timestream_database_name or not timestream_table_name:
                raise RuntimeError(
                    "Timestream database and table name are required when syncing using only an S3 bucket name"
                )
            latest_unload = self.get_latest_unload_path(
                bucket_name=s3_bucket_name,
                timestream_database_name=timestream_database_name,
                timestream_table_name=timestream_table_name,
            )
            line_protocol_path = f"{s3_bucket_name}/{timestream_database_name}/{timestream_table_name}/{latest_unload}/line-protocol-output"
            prefix_parts = line_protocol_path.split("/")[1:]
            prefix = "/".join(prefix_parts)

        s3_keys = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=s3_bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                s3_key = obj["Key"]
                if s3_key == prefix:
                    continue
                s3_keys.append(s3_key)
                rel_path = s3_key[len(prefix) :]
                # A leading "/" will cause os.path.join to believe the path is
                # the root directory.
                rel_path = rel_path.lstrip("/")
                local_file_path = os.path.join(directory, rel_path)
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                self.s3_client.download_file(s3_bucket_name, s3_key, local_file_path)

    def sync_csv_to_storage(
        self,
        s3_bucket_path: str,
        directory: str,
        timestream_database_name="",
        timestream_table_name="",
    ):
        """
        Downloads CSV data from an S3 bucket path to a directory.
        If provided simply the bucket path and the Timestream database and
        table name, the latest unload directory will be searched for. If the
        S3 bucket path is a path within an S3 bucket, such as s3://my-bucket/my-path,
        then all objects will be downloaded from this path.

        Args:
            s3_bucket_path (str): The path of the S3 bucket to download objects from,
                for example, s3://my-bucket, or, s3://my-bucket/my-path.
            directory (str): The path to the local directory to download objects to.
            timestream_database_name (str): The name of the Timestream for LiveAnalytics
                database used in the unload process.
            timestream_table_name (str): The name of the Timestream for LiveAnalytics
                table used in the unload process.
        Returns:
            None
        """
        if s3_bucket_path.lower().startswith("s3://"):
            s3_bucket_path = s3_bucket_path[5:]
        s3_bucket_parts = s3_bucket_path.split("/")
        if not s3_bucket_parts:
            raise RuntimeError("S3 bucket path was empty")
        s3_bucket_name = s3_bucket_parts[0]
        if not self.s3_bucket_exists(s3_bucket_name):
            raise RuntimeError(f"S3 bucket {s3_bucket_name} does not exist")

        os.makedirs(directory, exist_ok=True)

        if len(s3_bucket_parts) > 1:
            prefix = "/".join(s3_bucket_parts[1:])
            if not self.s3_bucket_path_exists(
                bucket_name=s3_bucket_name, prefix=prefix
            ):
                raise RuntimeError(
                    f"The S3 bucket path {s3_bucket_path} does not exist"
                )
            csv_path = s3_bucket_path
        else:
            if not timestream_database_name or not timestream_table_name:
                raise RuntimeError(
                    "Timestream database and table name are required when syncing using only an S3 bucket name"
                )
            latest_unload = self.get_latest_unload_path(
                bucket_name=s3_bucket_name,
                timestream_database_name=timestream_database_name,
                timestream_table_name=timestream_table_name,
            )
            csv_path = f"{s3_bucket_name}/{timestream_database_name}/{timestream_table_name}/{latest_unload}/"
            prefix_parts = csv_path.split("/")[1:]
            prefix = "/".join(prefix_parts)

        s3_keys = []
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=s3_bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                s3_key = obj["Key"]
                if s3_key == prefix:
                    continue
                s3_keys.append(s3_key)
                rel_path = s3_key[len(prefix) :]
                # A leading "/" will cause os.path.join to believe the path is
                # the root directory.
                rel_path = rel_path.lstrip("/")
                local_file_path = os.path.join(directory, rel_path)
                os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
                self.s3_client.download_file(s3_bucket_name, s3_key, local_file_path)

    def delete_bucket_and_contents(self, bucket_name: str, batch_size: int = 1000):
        """
        Deletes every object (and its versions if versioning is enabled) inside the
        bucket referenced by *bucket_name* and then deletes the bucket itself.

        Args:
            bucket_name (str): The S3 bucket name.
            batch_size (int): The number of objects to delete per ``delete_objects`` call.
                              The AWS API supports a maximum of 1000 per request.

        Raises:
            RuntimeError: If the bucket does not exist or deletion fails.
        """
        if not self.s3_bucket_exists(bucket_name):
            raise RuntimeError(f"Bucket {bucket_name} does not exist")

        self.logger.info(f"Deleting all objects from bucket '{bucket_name}'")

        # Delete object versions (handles versioned buckets) as well as
        # any delete markers.
        paginator = self.s3_client.get_paginator("list_object_versions")
        delete_batch = []
        for page in paginator.paginate(Bucket=bucket_name):
            for version in page.get("Versions", []) + page.get("DeleteMarkers", []):
                delete_batch.append(
                    {"Key": version["Key"], "VersionId": version["VersionId"]}
                )
                if len(delete_batch) == batch_size:
                    self.s3_client.delete_objects(
                        Bucket=bucket_name, Delete={"Objects": delete_batch}
                    )
                    delete_batch.clear()

            # Flush remaining items in batch at end of page
            if delete_batch:
                self.s3_client.delete_objects(
                    Bucket=bucket_name, Delete={"Objects": delete_batch}
                )
                delete_batch.clear()

        # For non-versioned buckets or if version listing was empty, ensure
        # current objects are also removed.
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name):
            objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
            if objects:
                # Batch in chunks of batch_size
                for i in range(0, len(objects), batch_size):
                    self.s3_client.delete_objects(
                        Bucket=bucket_name,
                        Delete={"Objects": objects[i : i + batch_size]},
                    )

        self.logger.info(f"Deleting bucket '{bucket_name}'")
        self.s3_client.delete_bucket(Bucket=bucket_name)
        self.logger.info(
            f"Bucket '{bucket_name}' and all of its contents were deleted successfully"
        )
