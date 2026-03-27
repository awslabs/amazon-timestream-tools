import json
import logging
import os
import random
import re
import string
import sys
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import pandas
import pytest
import requests
from botocore.exceptions import ClientError
from influxdb_client_3 import InfluxDBClient3
from testcontainers.core.container import DockerContainer, ExecResult
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

logger = logging.getLogger()

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import liveanalytics_influxdb3_migration_client


class MigrationTestCase(unittest.TestCase):
    """
    Tests for the liveanalytics_influxdb3_migration_client.py script.
    This test suite verifies migrations end to end, from LiveAnalytics data stored in S3 to InfluxDB v3.
    """

    influxdb_v3_container: DockerContainer
    influxdb_token: str
    influx_host: str = "http://localhost:8183"
    influx_database: str

    session: boto3.Session
    s3_client: Any
    s3_bucket_name: str
    timestream_write_client: Any
    timestream_query_client: Any
    la_database_name: str
    la_table_name: str

    @classmethod
    def setUpClass(cls):
        """
        Overrides unittest.TestCase.setUpClass, called once before any
        tests in the class.
        """
        plugin_dir: Path = Path("../../../").resolve()

        # Mounting a host directory as the plugins directory causes
        # issues initializing .venv. We must do this ourselves as
        # part of the startup command.
        cls.influxdb_v3_container: DockerContainer = (
            DockerContainer(
                image="influxdb:3.8-core",
                name="influxdb3.8-core",
                ports=[8181],
                command=(
                    "influxdb3 serve "
                    "--node-id=my-node-0 "
                    "--object-store=file "
                    "--data-dir=/var/lib/influxdb3/data "
                    "--plugin-dir=/var/lib/influxdb3/plugins "
                    "--query-file-limit=5000"
                ),
                volumes=[(str(plugin_dir), "/var/lib/influxdb3/plugins", "rw")],
            )
            .with_env(
                "PATH",
                "/usr/lib/influxdb3/python/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            )
            .with_env("LOG_FILTER", "info")
            .with_kwargs(mem_limit="8g", memswap_limit="8g")
            .waiting_for(LogMessageWaitStrategy(re.compile(".*startup time.*")))
            .with_bind_ports(container="8181/tcp", host=8183)
            .start()
        )

        # Create token.
        token_creation_result: ExecResult = cls.influxdb_v3_container.exec(
            command=["influxdb3", "create", "token", "--admin"]
        )

        # Instead of simply outputting the token, tokens are included as part of a message
        # colored with ANSI escape codes. The output must be decoded from UTF-8, the
        # ANSI escape codes must be removed, and the token must be extracted from the message.
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

        decoded_token_creation_output = str(token_creation_result.output, "utf-8")
        clean_token_creation_output = ansi_escape.sub("", decoded_token_creation_output)
        token_regex_search = re.search(
            r"Token:\s*([^\s]+)", clean_token_creation_output
        )
        if not token_regex_search:
            raise RuntimeError(
                "Unable to extract InfluxDB v3 token from container logs"
            )
        cls.influx_token = token_regex_search.group(1)

        # Install Python packages that the plugin relies on.
        install_packages_result: ExecResult = cls.influxdb_v3_container.exec(
            command=[
                "influxdb3",
                "install",
                "package",
                "--token",
                cls.influx_token,
                "requests",
                "pyarrow",
                "pandas",
                "numpy",
            ]
        )

        if install_packages_result.exit_code != 0:
            raise RuntimeError(
                "Unable to install Python packages in InfluxDB v3 container"
            )

        cls.session = boto3.Session()
        cls.timestream_write_client = cls.session.client("timestream-write")
        cls.timestream_query_client = cls.session.client("timestream-query")
        cls.s3_client = cls.session.client("s3")

        cls.la_database_name = f"la-to-v3-plugin-database-{cls.get_random_string(7)}"
        cls.influx_database = cls.la_database_name
        cls.timestream_write_client.create_database(DatabaseName=cls.la_database_name)
        # Enforce the maximum of 1 create or delete action per second in Timestream for LiveAnalytics.
        time.sleep(1)
        cls.wait_for_database_creation(cls.la_database_name)

    def setUp(self):
        """
        Overrides unittest.TestCase.setUp, called before each test runs.
        """
        os.environ["INFLUXDB3_HOST_URL"] = self.influx_host
        os.environ["INFLUXDB3_AUTH_TOKEN"] = self.influx_token
        os.environ["INFLUXDB3_DATABASE_NAME"] = self.influx_database

        self.la_table_name = f"la-to-v3-plugin-table-{self.get_random_string(10)}"
        time.sleep(1)
        self.timestream_write_client.create_table(
            DatabaseName=self.la_database_name,
            TableName=self.la_table_name,
            RetentionProperties={
                "MemoryStoreRetentionPeriodInHours": 8766,
                "MagneticStoreRetentionPeriodInDays": 7305,
            },
        )
        self.wait_for_table_creation(
            database_name=self.la_database_name, table_name=self.la_table_name
        )

        self.s3_bucket_name = f"la-to-v3-plugin-{self.get_random_string(7)}"
        if self.session.region_name == "us-east-1":
            self.s3_client.create_bucket(
                Bucket=self.s3_bucket_name, ObjectLockEnabledForBucket=True
            )
        else:
            self.s3_client.create_bucket(
                Bucket=self.s3_bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": self.session.region_name
                },
                ObjectLockEnabledForBucket=True,
            )
        self.s3_client.put_bucket_versioning(
            Bucket=self.s3_bucket_name,
            VersioningConfiguration={"MFADelete": "Disabled", "Status": "Enabled"},
        )
        s3_bucket_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "DenyInsecureTransport",
                    "Effect": "Deny",
                    "Principal": "*",
                    "Action": "s3:*",
                    "Resource": [
                        f"arn:aws:s3:::{self.s3_bucket_name}",
                        f"arn:aws:s3:::{self.s3_bucket_name}/*",
                    ],
                    "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                }
            ],
        }
        self.s3_client.put_bucket_policy(
            Bucket=self.s3_bucket_name, Policy=json.dumps(s3_bucket_policy)
        )

    @classmethod
    def wait_for_database_creation(
        cls, database_name: str, max_attempts: int = 20, delay_seconds: int = 5
    ) -> None:
        attempts = 0
        while attempts < max_attempts:
            try:
                cls.timestream_write_client.describe_database(
                    DatabaseName=database_name
                )
                return True
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code != "ResourceNotFoundException":
                    raise
            attempts += 1
            time.sleep(delay_seconds)

    def wait_for_table_creation(
        self, database_name: str, table_name: str, max_attempts=20, delay_seconds=5
    ):
        attempts = 0
        while attempts < max_attempts:
            try:
                response = self.timestream_write_client.describe_table(
                    DatabaseName=database_name, TableName=table_name
                )
                status = response.get("Table", {}).get("TableStatus")
                if status == "ACTIVE":
                    return
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code")
                if error_code != "ResourceNotFoundException":
                    raise
            attempts += 1
            time.sleep(delay_seconds)

    @classmethod
    def tearDownClass(cls):
        """
        Overrides unittest.TestCase.tearDownClass, called after all tests have finished.
        """
        try:
            cls.influxdb_v3_container.stop(force=True, delete_volume=True)
        except Exception as e:
            logging.warning(
                f"tearDownClass: Failed to delete InfluxDB v3 container: {e}"
            )
        try:
            # Enforce the maximum of 1 create or delete action per second in Timestream for LiveAnalytics.
            time.sleep(1)
            cls.delete_database(database_name=cls.la_database_name)
        except Exception as e:
            logging.warning(f"tearDownClass: Failed to delete Timestream database: {e}")

    def tearDown(self):
        """
        Overrides unittest.TestCase.tearDown, called after each test runs.
        """
        # Tests may purposely fail, causing resource to not be created.
        # To handle this, each resource needs its own try except block
        # with logging in case of failure.
        try:
            deletion_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            deletion_response = requests.delete(
                url=f"{self.influx_host}/api/v3/configure/database",
                headers={"Authorization": f"Bearer {self.influx_token}"},
                params={
                    "db": self.influx_database,
                    "hard_delete_at": deletion_date,
                },
            )
            _ = deletion_response.raise_for_status()
        except Exception as e:
            logging.warning(f"tearDown: Failed to delete InfluxDB v3 database: {e}")

        try:
            # Enforce the maximum of 1 create or delete action per second in Timestream for LiveAnalytics.
            time.sleep(1)
            self.timestream_write_client.delete_table(
                DatabaseName=self.la_database_name, TableName=self.la_table_name
            )
        except Exception as e:
            logging.warning(f"tearDown: Failed to delete Timestream table: {e}")

        try:
            self.delete_s3_bucket(bucket_name=self.s3_bucket_name)
        except Exception as e:
            logging.warning(f"tearDown: Failed to delete S3 bucket: {e}")

    @classmethod
    def delete_database(cls, database_name):
        list_tables_response = cls.timestream_write_client.list_tables(
            DatabaseName=database_name
        )
        table_names = list_tables_response.get("Tables", [])
        if not table_names and "NextToken" in list_tables_response:
            next_token = list_tables_response["NextToken"]
            while next_token is not None:
                time.sleep(5)
                list_tables_response = cls.timestream_write_client.list_tables(
                    DatabaseName=database_name
                )
                next_token = list_tables_response.get("NextToken", None)
                table_names.extend(list_tables_response.get("Tables", []))

        for table in table_names:
            cls.timestream_write_client.delete_table(
                DatabaseName=database_name, TableName=table["TableName"]
            )
        cls.timestream_write_client.delete_database(DatabaseName=database_name)

    def delete_s3_bucket(self, bucket_name: str):
        paginator = self.s3_client.get_paginator("list_object_versions")
        pages = paginator.paginate(Bucket=bucket_name)

        for page in pages:
            for version in page.get("Versions", []):
                # Disable object lock. Only Parquet files have object locks.
                if version["Key"].endswith(".parquet"):
                    try:
                        object_lock_response = self.s3_client.get_object_legal_hold(
                            Bucket=bucket_name,
                            Key=version["Key"],
                            VersionId=version["VersionId"],
                        )
                        if object_lock_response["LegalHold"]["Status"] == "ON":
                            self.s3_client.put_object_legal_hold(
                                Bucket=bucket_name,
                                Key=version["Key"],
                                VersionId=version["VersionId"],
                                LegalHold={"Status": "OFF"},
                            )
                    except Exception as e:
                        logging.warning(
                            f"Failed to remove object lock from {version['Key']} with version ID {version['VersionId']}: {e}"
                        )

                # Delete object.
                try:
                    self.s3_client.delete_object(
                        Bucket=bucket_name,
                        Key=version["Key"],
                        VersionId=version["VersionId"],
                        BypassGovernanceRetention=True,
                    )
                except Exception as e:
                    logging.error(
                        f"Failed to delete object {version['Key']} with version ID {version['VersionId']}: {e}"
                    )

            for marker in page.get("DeleteMarkers", []):
                try:
                    self.s3_client.delete_object(
                        Bucket=bucket_name,
                        Key=marker["Key"],
                        VersionId=marker["VersionId"],
                    )
                except Exception:
                    logging.error(
                        f"Failed to delete deletion marker for object {marker['Key']} with version ID {marker['VersionId']}"
                    )
        self.s3_client.delete_bucket(Bucket=bucket_name)

    def put_records(self, records: list):
        batches = [records[i : i + 100] for i in range(0, len(records), 100)]

        with ThreadPoolExecutor(max_workers=8) as executor:
            executor.map(self.post_records, batches)

    def post_records(self, batch):
        try:
            self.timestream_write_client.write_records(
                DatabaseName=self.la_database_name,
                TableName=self.la_table_name,
                Records=batch,
            )
        except Exception as e:
            print(str(e))
            raise

    @staticmethod
    def get_random_string(length: int):
        """
        Gets a random string.
        Args:
            length (int): The length of the random string to get.
        Returns:
            str: A random string.
        """
        return "".join(
            random.SystemRandom().choice(string.ascii_lowercase + string.digits)
            for _ in range(length)
        )

    def check_live_analytics_table_count(
        self, database_name: str, table_name: str
    ) -> int:
        query_str: str = f'SELECT COUNT(*) FROM "{database_name}"."{table_name}"'
        response = self.timestream_query_client.query(QueryString=query_str)

        # When a query takes a long time to run, pagination must be used as a
        # waiting mechanism.
        while not response["Rows"] and response["NextToken"]:
            response = self.timestream_query_client.query(
                QueryString=query_str, NextToken=response["NextToken"]
            )
        return int(response["Rows"][0]["Data"][0]["ScalarValue"])

    def check_influxdb_v3_table_count(self, database_name: str, table_name: str) -> int:
        """
        Queries the number of records in an InfluxDB v3 table.
        Args:
            database_name (str): The name of the database in which the table resides.
            table_name (str): The table name to query.
        Returns:
            int: The number of records in the table.
        """
        with InfluxDBClient3(
            host=self.influx_host,
            token=self.influx_token,
            database=database_name,
        ) as influxdb_v3_client:
            query_str = f'SELECT COUNT(*) AS row_count FROM "{table_name}"'
            print("Executing InfluxDB v3 validation query")
            results = influxdb_v3_client.query(query_str)
            return results.column("row_count")[0].as_py()

    def test_migration_basic(self):
        """
        Tests basic migration of a single table.
        """
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - pandas.Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + pandas.Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        record = {
            "Dimensions": dimensions,
            "MeasureName": "cpu_utilization",
            "MeasureValue": "13.5",
            "MeasureValueType": "DOUBLE",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
        }
        self.put_records([record])

        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                self.la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
            ]
        )

        self.assertEqual(return_code, 0)
        la_table_count = self.check_live_analytics_table_count(
            database_name=self.la_database_name, table_name=self.la_table_name
        )
        influxdb_v3_table_count = self.check_influxdb_v3_table_count(
            database_name=self.influx_database, table_name=self.la_table_name
        )
        print(f"Table counts: {la_table_count}, {influxdb_v3_table_count}")
        self.assertEqual(
            la_table_count,
            influxdb_v3_table_count,
        )

    def test_migration_basic_timestamps(self):
        """
        Tests basic migration of a single table where the table's only
        record uses a timestamp as its measure value.
        """
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - pandas.Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + pandas.Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        # Only multi-measure records can use TIMESTAMP as a measure type.
        record = {
            "Dimensions": dimensions,
            "MeasureName": "cpu_utilization",
            "MeasureValueType": "MULTI",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
            "MeasureValues": [
                {
                    "Name": "request_time",
                    "Value": str(start_time.value),
                    "Type": "TIMESTAMP",
                }
            ],
        }

        self.put_records([record])

        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                self.la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
            ]
        )

        self.assertEqual(return_code, 0)
        la_table_count = self.check_live_analytics_table_count(
            database_name=self.la_database_name, table_name=self.la_table_name
        )
        influxdb_v3_table_count = self.check_influxdb_v3_table_count(
            database_name=self.influx_database, table_name=self.la_table_name
        )
        print(f"Table counts: {la_table_count}, {influxdb_v3_table_count}")
        self.assertEqual(
            la_table_count,
            influxdb_v3_table_count,
        )

    def test_migration_nanoseconds(self):
        """
        Tests basic migration of a single table that contains two records
        that are identical apart from a 1 nanosecond difference. If nanoseconds
        aren't properly retained, both records will be merged in InfluxDB.
        """
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - pandas.Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + pandas.Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        second_record_time = start_time + pandas.Timedelta(1, unit="ns")

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        records = [
            {
                "Dimensions": dimensions,
                "MeasureName": "cpu_utilization",
                "MeasureValue": "13.5",
                "MeasureValueType": "DOUBLE",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
            },
            {
                "Dimensions": dimensions,
                "MeasureName": "cpu_utilization",
                "MeasureValue": "13.5",
                "MeasureValueType": "DOUBLE",
                "Time": str(second_record_time.value),
                "TimeUnit": "NANOSECONDS",
            },
        ]
        self.put_records(records)

        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                self.la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
            ]
        )

        self.assertEqual(return_code, 0)
        la_table_count = self.check_live_analytics_table_count(
            database_name=self.la_database_name, table_name=self.la_table_name
        )
        influxdb_v3_table_count = self.check_influxdb_v3_table_count(
            database_name=self.influx_database, table_name=self.la_table_name
        )
        print(f"Table counts: {la_table_count}, {influxdb_v3_table_count}")
        self.assertEqual(
            la_table_count,
            influxdb_v3_table_count,
        )

    def test_migration_basic_resume(self):
        """
        Tests resuming a migration of a basic migration.
        Resuming is accomplished by uploading Parquet files to S3 manually.
        """
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - pandas.Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + pandas.Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        record = {
            "Dimensions": dimensions,
            "MeasureName": "cpu_utilization",
            "MeasureValue": "13.5",
            "MeasureValueType": "DOUBLE",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
        }
        self.put_records([record])

        self.timestream_query_client.query(
            QueryString=f"""
                UNLOAD (SELECT *, DATE_FORMAT(time, '%y-%m-%d') as partition_date 
                       FROM \"{self.la_database_name}\".\"{self.la_table_name}\") 
                TO 's3://{self.s3_bucket_name}/{self.la_database_name}/{self.la_table_name}' 
                WITH (partitioned_by = ARRAY['partition_date'], 
                      format = 'PARQUET', 
                      max_file_size='2GB', 
                      compression = 'NONE')
                """
        )

        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                self.la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
                "--resume",
            ]
        )

        self.assertEqual(return_code, 0)
        la_table_count = self.check_live_analytics_table_count(
            database_name=self.la_database_name, table_name=self.la_table_name
        )
        influxdb_v3_table_count = self.check_influxdb_v3_table_count(
            database_name=self.influx_database, table_name=self.la_table_name
        )
        print(f"Table counts: {la_table_count}, {influxdb_v3_table_count}")
        self.assertEqual(
            la_table_count,
            influxdb_v3_table_count,
        )

    def test_migration_basic_resume_multiple_chunks(self):
        """
        Tests migrating multiple chunks with --resume.

        This test will generate 3 chunks, assuming each chunk fits 99 days.
        """
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - pandas.Timedelta(days=200)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = current_time
        assert isinstance(end_time, pandas.Timestamp)

        current_record_time = start_time

        print("Generating data")
        records = []
        dimensions = [
            {
                "Name": "hostname",
                "Value": self.get_random_string(12),
                "DimensionValueType": "VARCHAR",
            },
            {
                "Name": "region",
                "Value": self.get_random_string(18),
                "DimensionValueType": "VARCHAR",
            },
        ]
        for _ in range(200):
            record = {
                "Dimensions": dimensions,
                "MeasureName": "cpu_utilization",
                "MeasureValue": self.get_random_string(25),
                "MeasureValueType": "VARCHAR",
                "Time": str(current_record_time.value),
                "TimeUnit": "NANOSECONDS",
            }
            records.append(record)
            current_record_time = current_record_time + pandas.Timedelta(days=1)
        self.put_records(records)

        print("Migrating")
        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                self.la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
                "--max-parquet-files",
                "1",
            ]
        )
        self.assertEqual(return_code, 0)

        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                self.la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
                "--resume",
            ]
        )
        self.assertEqual(return_code, 0)

        la_table_count = self.check_live_analytics_table_count(
            database_name=self.la_database_name, table_name=self.la_table_name
        )
        influxdb_v3_table_count = self.check_influxdb_v3_table_count(
            database_name=self.influx_database, table_name=self.la_table_name
        )
        print(f"Table counts: {la_table_count}, {influxdb_v3_table_count}")
        self.assertEqual(
            la_table_count,
            influxdb_v3_table_count,
        )

    def test_edge_case_migration(self):
        """
        Migrates all data from the test-data directory.

        The test data in ./test-data/ was generated long in the past. Ordinarily,
        this would be a problem for InfluxDB v3 core, as it has a time limit of 72 hours
        in the past for queries. However, this limit is based on the number of files
        in InfluxDB v3. Therefore, if only the test data is ingested, all data can
        be queried.
        """
        # The test-data directory assumes that the LA database is named "EdgeCaseDB".
        self.influx_database = "EdgeCaseDB"
        os.environ["INFLUXDB3_DATABASE_NAME"] = self.influx_database
        # We use a local variable here since this database doesn't exist in LiveAnalytics
        # and doesn't need to be cleaned up.
        la_database_name = self.influx_database

        # Upload all data in ./test-data/ to the test case's S3 bucket.
        for root, _, files in os.walk("./test-data/"):
            for file in files:
                key = f"{root}/{file}".removeprefix("./test-data/")
                print(f"Putting {key}")
                self.s3_client.upload_file(
                    Filename=os.path.join(root, file),
                    Bucket=self.s3_bucket_name,
                    Key=key,
                )

        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
                "--resume",
            ]
        )

        self.assertEqual(return_code, 0)
        record_count: int = 0

        record_count += self.check_influxdb_v3_table_count(
            database_name=self.influx_database,
            table_name="EdgeCaseBulkMultiMeasureTable",
        )

        record_count += self.check_influxdb_v3_table_count(
            database_name=self.influx_database, table_name="EdgeCaseMultiMeasureTable"
        )

        record_count += self.check_influxdb_v3_table_count(
            database_name=self.influx_database, table_name="EdgeCaseSingleMeasureTable"
        )

        print(f"InfluxDB v3 table count: {record_count}")
        self.assertEqual(record_count, 19_811)

        return

    @pytest.mark.slow
    def test_migration_massive(self):
        """
        Tests migrating 1,000,000 records.
        """
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - pandas.Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + pandas.Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        current_record_time = start_time

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        print("Generating data")
        records = []
        for _ in range(1_000_000):
            record = {
                "Dimensions": dimensions,
                "MeasureName": "cpu_utilization",
                "MeasureValue": self.get_random_string(25),
                "MeasureValueType": "VARCHAR",
                "Time": str(current_record_time.value),
                "TimeUnit": "NANOSECONDS",
            }
            records.append(record)
            current_record_time = current_record_time + pandas.Timedelta(1, unit="ns")
        self.put_records(records)

        print("Migrating")
        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                self.la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
            ]
        )

        self.assertEqual(return_code, 0)
        la_table_count = self.check_live_analytics_table_count(
            database_name=self.la_database_name, table_name=self.la_table_name
        )
        influxdb_v3_table_count = self.check_influxdb_v3_table_count(
            database_name=self.influx_database, table_name=self.la_table_name
        )
        print(f"Table counts: {la_table_count}, {influxdb_v3_table_count}")
        self.assertEqual(
            la_table_count,
            influxdb_v3_table_count,
        )

    @pytest.mark.slow
    def test_migration_massive_resume(self):
        """
        Tests migrating 2,000,000 records with --resume.

        This tests assumes that migrating 2,000,000 records results in
        two Parquet files. The test migrates the first with --max-parquet-files
        equal to 1, then does a resume, migrating the second Parquet file.
        """
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - pandas.Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + pandas.Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        current_record_time = start_time

        print("Generating data")
        records = []
        for _ in range(2_000_000):
            dimensions = [
                {
                    "Name": "hostname",
                    "Value": self.get_random_string(12),
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "region",
                    "Value": self.get_random_string(18),
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "statat",
                    "Value": self.get_random_string(2),
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "mono",
                    "Value": self.get_random_string(25),
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "som",
                    "Value": self.get_random_string(10),
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "cher",
                    "Value": self.get_random_string(4),
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "no",
                    "Value": self.get_random_string(15),
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "sine",
                    "Value": self.get_random_string(26),
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "reno",
                    "Value": self.get_random_string(11),
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "sing",
                    "Value": self.get_random_string(6),
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "bus",
                    "Value": self.get_random_string(18),
                    "DimensionValueType": "VARCHAR",
                },
            ]
            record = {
                "Dimensions": dimensions,
                "MeasureName": "cpu_utilization",
                "MeasureValue": self.get_random_string(25),
                "MeasureValueType": "VARCHAR",
                "Time": str(current_record_time.value),
                "TimeUnit": "NANOSECONDS",
            }
            records.append(record)
            current_record_time = current_record_time + pandas.Timedelta(1, unit="ns")
        self.put_records(records)

        print("Migrating")
        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                self.la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
                "--max-parquet-files",
                "1",
            ]
        )
        self.assertEqual(return_code, 0)

        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                self.la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
                "--resume",
            ]
        )
        self.assertEqual(return_code, 0)

        la_table_count = self.check_live_analytics_table_count(
            database_name=self.la_database_name, table_name=self.la_table_name
        )
        influxdb_v3_table_count = self.check_influxdb_v3_table_count(
            database_name=self.influx_database, table_name=self.la_table_name
        )
        print(f"Table counts: {la_table_count}, {influxdb_v3_table_count}")
        self.assertEqual(
            la_table_count,
            influxdb_v3_table_count,
        )

    def test_migration_different_database_names(self):
        """
        Tests basic migration of a single table where the Timestream for LiveAnalytics
        database and InfluxDB v3 database have different names.
        """
        self.influx_database = "test-influxdb-v3-database"
        os.environ["INFLUXDB3_DATABASE_NAME"] = self.influx_database

        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - pandas.Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + pandas.Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        record = {
            "Dimensions": dimensions,
            "MeasureName": "cpu_utilization",
            "MeasureValue": "13.5",
            "MeasureValueType": "DOUBLE",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
        }
        self.put_records([record])

        return_code = liveanalytics_influxdb3_migration_client.main(
            [
                "--live-analytics-database-name",
                self.la_database_name,
                "--s3-bucket-name",
                self.s3_bucket_name,
            ]
        )

        self.assertEqual(return_code, 0)
        la_table_count = self.check_live_analytics_table_count(
            database_name=self.la_database_name, table_name=self.la_table_name
        )
        influxdb_v3_table_count = self.check_influxdb_v3_table_count(
            database_name=self.influx_database, table_name=self.la_table_name
        )
        print(f"Table counts: {la_table_count}, {influxdb_v3_table_count}")
        self.assertEqual(
            la_table_count,
            influxdb_v3_table_count,
        )

    def test_migration_expired_presigned_urls(self):
        """
        Tests migrating a single record where all presigned URLs are expired.
        """
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - pandas.Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + pandas.Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        record = {
            "Dimensions": dimensions,
            "MeasureName": "cpu_utilization",
            "MeasureValue": "13.5",
            "MeasureValueType": "DOUBLE",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
        }
        self.put_records([record])

        with self.assertLogs(
            "liveanalytics_influxdb3_migration_client", level="INFO"
        ) as captured_logs:
            return_code = liveanalytics_influxdb3_migration_client.main(
                [
                    "--live-analytics-database-name",
                    self.la_database_name,
                    "--s3-bucket-name",
                    self.s3_bucket_name,
                    "--presigned-url-expiry-seconds",
                    "0",
                ]
            )

            self.assertEqual(return_code, 1)

            expected_error = "403 Client Error: Forbidden for url: *****"
            self.assertTrue(
                any(expected_error in log for log in captured_logs.output),
                f"Expected error not found in logs: {expected_error}",
            )


if __name__ == "__main__":
    unittest.main()
