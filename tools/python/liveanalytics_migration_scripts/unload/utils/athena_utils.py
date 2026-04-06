# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import boto3
import time
import re
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from unload.utils.logger_utils import create_logger


# 24 hours
MAX_WAIT_SECONDS = 86400


class AthenaUtility:
    def __init__(self, region=None):
        """
        Initialize the AthenaUtility class.

        Args:
            region (str): The AWS region.
        """
        self.athena_client = boto3.client("athena", region_name=region)
        self.glue_client = boto3.client("glue", region_name=region)
        self.logger = create_logger("athena_logger")

    def create_glue_table_from_parquet(
        self,
        database_name: str,
        table_name: str,
        columns: list[dict],
        s3_bucket_path: str,
    ):
        """
        Creates a new Glue table using Parquet data.

        Args:
            database_name (str): The Glue database name to use.
            table_name (str): The name of the Glue table to create.
            columns (list[dict]): A list of columns to use for the schema. Each column must have "Name" and "Type" keys.
            s3_bucket_path (str): The S3 bucket path where Parquet data is stored. For example, s3://my-bucket/my-path.

        Returns:
            None
        """
        self.glue_client.create_table(
            DatabaseName=database_name,
            TableInput={
                "Name": table_name,
                "StorageDescriptor": {
                    "Columns": columns,
                    "Location": s3_bucket_path,
                    "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                    "SerdeInfo": {
                        "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                        "Parameters": {},
                    },
                },
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {"classification": "parquet"},
            },
        )

    def start_query_execution(
        self, query_string: str, output_location: str, database_name="default"
    ):
        return self.athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={"Database": database_name},
            ResultConfiguration={
                "OutputLocation": output_location,
            },
        )

    def wait_for_athena_query(
        self, query_execution_id: str, max_wait_seconds=MAX_WAIT_SECONDS
    ):
        elapsed_seconds = 0
        time_wait_period = 15
        state = ""
        query_status = {}
        while elapsed_seconds < max_wait_seconds:
            query_status = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            state = query_status["QueryExecution"]["Status"]["State"]

            self.logger.info(f"State: {state}")

            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(time_wait_period)
            elapsed_seconds += time_wait_period

        if elapsed_seconds >= max_wait_seconds:
            raise RuntimeError(
                f"Timed out waiting for query after {elapsed_seconds} seconds"
            )

        if state == "SUCCEEDED":
            self.logger.info("Query successful")
        else:
            failure_state = (
                query_status.get("QueryExecution", {})
                .get("Status", {})
                .get("State", "UNKNOWN")
            )
            error_message = (
                query_status.get("QueryExecution", {})
                .get("Status", {})
                .get("AthenaError", {})
                .get("ErrorMessage", "UNKNOWN")
            )
            failure_message = f"Athena query failed with state: {failure_state}, error: {error_message}"
            raise RuntimeError(failure_message)

    @staticmethod
    def is_valid_athena_table_name(athena_table_name: str) -> bool:
        """
        Validates an Athena table name.

        Args:
            athena_table_name (str): The Athena table name to validate.

        Returns:
            bool: Whether the Athena table name is valid.
        """
        VALID_ATHENA_NAME = re.compile(r"^[A-Za-z0-9._ ][A-Za-z0-9._]{1,255}$")
        return VALID_ATHENA_NAME.match(athena_table_name) is not None
