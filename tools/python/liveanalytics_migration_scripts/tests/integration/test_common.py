# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import io
import logging
import os
import random
import re
import string
import sys
import time
import unittest
from contextlib import redirect_stdout

import pandas
from boto3 import Session
from botocore.client import BaseClient
from botocore.exceptions import ClientError
from pandas import Timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import unload
from cardinality import cardinality
from unload.utils.s3_utils import S3Utility

# Format expected by unload.py.
UNLOAD_TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
# Format expected by the cardinality and validation scripts.
ISO_8601_TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


class BaseIntegrationTestCase(unittest.TestCase):
    """
    Base TestCase for all integration test cases.
    This class provides a number of fields and methods common to
    all integration tests.

    Child classes should add new fields specific to their test case, add tests,
    and, to manage resources, should override:
        - setUpClass, run before all tests.
        - setUp, run before each test.
        - tearDownClass, run after all tests have finished.
        - tearDown, run after each test has finished.
    """

    session: Session
    timestream_write_client: BaseClient
    timestream_query_client: BaseClient
    s3_client: BaseClient
    dynamodb_client: BaseClient
    s3_utility: S3Utility

    # Prefixes stand for
    # "LiveAnalytics InfluxDB Integration Test".
    database_name_prefix = "la-idb-it-db-"
    database_name: str

    table_name_prefix = "la-idb-it-table-"
    table_name: str

    s3_bucket_name_prefix = "la-idb-it-bucket-"
    s3_bucket_name: str

    # Whether to silence warnings logs during the cleanup process.
    # Some tests end early or purposely raise exceptions, causing
    # the cleanup process to encounter deletion failures.
    silence_cleanup_logging = False

    @classmethod
    def setUpClass(cls):
        """
        Overrides unittest.TestCase.setUpClass, called once before any
        tests in the class.
        """
        cls.session = Session()
        cls.timestream_write_client = cls.session.client("timestream-write")
        cls.s3_client = cls.session.client("s3")

        cls.s3_utility = S3Utility()

        cls.database_name = cls.database_name_prefix + cls.get_random_string(10)
        cls.timestream_write_client.create_database(DatabaseName=cls.database_name)
        # Enforce the maximum of 1 create or delete action per second in Timestream for LiveAnalytics.
        time.sleep(1)
        cls.wait_for_database_creation(cls.database_name)

    def setUp(self):
        """
        Overrides unittest.TestCase.setUp, called before each test runs.
        """
        self.table_name = self.table_name_prefix + self.get_random_string(10)
        time.sleep(1)
        self.timestream_write_client.create_table(
            DatabaseName=self.database_name,
            TableName=self.table_name,
            RetentionProperties={
                "MemoryStoreRetentionPeriodInHours": 8766,
                "MagneticStoreRetentionPeriodInDays": 7305,
            },
        )
        self.wait_for_table_creation(
            database_name=self.database_name, table_name=self.table_name
        )

        self.s3_bucket_name = self.s3_bucket_name_prefix + self.get_random_string(10)
        if self.session.region_name == "us-east-1":
            self.s3_client.create_bucket(Bucket=self.s3_bucket_name)
        else:
            self.s3_client.create_bucket(
                Bucket=self.s3_bucket_name,
                CreateBucketConfiguration={
                    "LocationConstraint": self.session.region_name
                },
            )

        self.silence_cleanup_logging = False

    @classmethod
    def tearDownClass(cls):
        """
        Overrides unittest.TestCase.tearDownClass, called after all tests have finished.
        """
        try:
            # Enforce the maximum of 1 create or delete action per second in Timestream for LiveAnalytics.
            time.sleep(1)
            cls.delete_database(database_name=cls.database_name)
        except Exception as e:
            if not cls.silence_cleanup_logging:
                logging.warning(
                    f"tearDownClass: Failed to delete Timestream database: {e}"
                )

    def tearDown(self):
        """
        Overrides unittest.TestCase.tearDown, called after each test runs.
        """
        # Tests may purposely fail, causing resource to not be created.
        # To handle this, each resource needs its own try except block
        # with logging in case of failure.
        try:
            # Enforce the maximum of 1 create or delete action per second in Timestream for LiveAnalytics.
            time.sleep(1)
            self.timestream_write_client.delete_table(
                DatabaseName=self.database_name, TableName=self.table_name
            )
        except Exception as e:
            if not self.silence_cleanup_logging:
                logging.warning(f"tearDown: Failed to delete Timestream table: {e}")

        try:
            self.delete_s3_bucket(bucket_name=self.s3_bucket_name)
        except Exception as e:
            if not self.silence_cleanup_logging:
                logging.warning(f"tearDown: Failed to delete S3 bucket: {e}")

    @classmethod
    def wait_for_database_creation(
        cls, database_name: str, max_attempts=20, delay_seconds=5
    ):
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

    @staticmethod
    def get_random_string(length: int):
        return "".join(
            random.SystemRandom().choice(string.ascii_lowercase + string.digits)
            for _ in range(length)
        )

    def put_records(self, records: list) -> dict:
        return self.timestream_write_client.write_records(
            DatabaseName=self.database_name,
            TableName=self.table_name,
            Records=records,
        )

    def set_table_name(self, table_name):
        self.table_name = table_name

    def set_database_name(self, database_name):
        self.database_name = database_name

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
        object_response_paginator = self.s3_client.get_paginator("list_objects_v2")
        for object_response in object_response_paginator.paginate(Bucket=bucket_name):
            if "Contents" in object_response:
                objects_to_delete = [
                    {"Key": obj["Key"]} for obj in object_response.get("Contents", [])
                ]
                if objects_to_delete:
                    self.s3_client.delete_objects(
                        Bucket=bucket_name, Delete={"Objects": objects_to_delete}
                    )
        self.s3_client.delete_bucket(Bucket=bucket_name)


class UnloadTestCase(BaseIntegrationTestCase):
    """
    Tests unload.py, which unloads data from Timestream for LiveAnalytics
    into an S3 bucket.
    """

    def s3_bucket_has_contents(
        self, bucket_name: str, prefix: str, delimiter="/"
    ) -> bool:
        """
        Recursively searches for any object within an S3 bucket.

        Args:
            bucket_name (str): The name of the S3 bucket to search.
            prefix (str): The prefix to start the search from.
            delimiter (str): The delimiter between prefixes.
        """
        self.s3_utility.wait_for_multipart_uploads(
            bucket_name=bucket_name, prefix=prefix
        )
        resp = self.s3_client.list_objects_v2(
            Bucket=bucket_name, Prefix=prefix, Delimiter=delimiter
        )
        if "CommonPrefixes" in resp and len(resp["CommonPrefixes"]) > 0:
            prefix = resp["CommonPrefixes"][0]["Prefix"]
            return self.s3_bucket_has_contents(bucket_name=bucket_name, prefix=prefix)
        return "Contents" in resp

    def test_single_measure_export_table(self):
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        self.assertFalse(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

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
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
            ]
        )
        self.assertTrue(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

    def test_single_measure_export_table_gzip(self):
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        self.assertFalse(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

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
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
                "--compression",
                "GZIP",
            ]
        )
        self.assertTrue(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

    def test_single_measure_export_table_csv(self):
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        self.assertFalse(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

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
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
                "--export-format",
                "CSV",
            ]
        )
        self.assertTrue(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

    def test_single_measure_export_table_partition_hour(self):
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        self.assertFalse(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

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
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
                "--partition",
                "hour",
            ]
        )
        self.assertTrue(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

    def test_single_measure_export_table_partition_day(self):
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        self.assertFalse(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

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
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
                "--partition",
                "day",
            ]
        )
        self.assertTrue(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

    def test_single_measure_export_table_partition_month(self):
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        self.assertFalse(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

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
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
                "--partition",
                "month",
            ]
        )
        self.assertTrue(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

    def test_single_measure_export_table_partition_year(self):
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        self.assertFalse(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

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
        unload.main(
            [
                "--database",
                self.database_name,
                "--table",
                self.table_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-table",
                "--partition",
                "year",
            ]
        )
        self.assertTrue(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

    def test_single_measure_export_database(self):
        current_time: pandas.Timestamp = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        self.assertFalse(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

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
        unload.main(
            [
                "--database",
                self.database_name,
                "--s3-uri",
                f"s3://{self.s3_bucket_name}",
                "--start-time",
                start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--end-time",
                end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                "--export-database",
            ]
        )
        self.assertTrue(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )

    def test_single_measure_start_time_before_end_time(self):
        self.silence_cleanup_logging = True
        with self.assertRaises(Exception):
            current_time = pandas.Timestamp.now()
            end_time = current_time - Timedelta(days=30)
            assert isinstance(end_time, pandas.Timestamp)
            start_time = end_time + Timedelta(days=1)
            assert isinstance(start_time, pandas.Timestamp)

            dimensions = [
                {
                    "Name": "hostname",
                    "Value": "hostname1",
                    "DimensionValueType": "VARCHAR",
                },
                {
                    "Name": "region",
                    "Value": "us-west-2",
                    "DimensionValueType": "VARCHAR",
                },
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
            unload.main(
                [
                    "--database",
                    self.database_name,
                    "--table",
                    self.table_name,
                    "--s3-uri",
                    f"s3://{self.s3_bucket_name}",
                    "--start-time",
                    start_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                    "--end-time",
                    end_time.strftime(UNLOAD_TIMESTAMP_FORMAT),
                    "--export-table",
                ]
            )
        self.assertFalse(
            self.s3_bucket_has_contents(
                bucket_name=self.s3_bucket_name, prefix=self.database_name
            )
        )


class CardinalityTestCase(BaseIntegrationTestCase):
    """
    Tests cardinality.py, which calculates the cardinality (as defined by
    InfluxData) of a Timestream for LiveAnalytics table.
    """

    def test_one_record_one_cardinality(self):
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
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

        expected_cardinality = 1

        output = io.StringIO()
        with redirect_stdout(output):
            cardinality.main(
                [
                    "--database-name",
                    self.database_name,
                    "--table-name",
                    self.table_name,
                    "--start-time",
                    start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                    "--end-time",
                    end_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                ]
            )
        str_output = output.getvalue()
        cardinality_match = re.search(
            r'Cardinality of "[^"]+"\."[^"]+":\s+(\d+)', str_output
        )
        self.assertIsNotNone(cardinality_match)
        if cardinality_match is not None:
            cardinality_value = int(cardinality_match.group(1))
            self.assertEqual(cardinality_value, expected_cardinality)

    def test_two_records_two_cardinality(self):
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
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
                "MeasureName": "memory_utilization",
                "MeasureValue": "13.5",
                "MeasureValueType": "DOUBLE",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
            },
        ]
        self.put_records(records)

        expected_cardinality = 2

        output = io.StringIO()
        with redirect_stdout(output):
            cardinality.main(
                [
                    "--database-name",
                    self.database_name,
                    "--table-name",
                    self.table_name,
                    "--start-time",
                    start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                    "--end-time",
                    end_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                ]
            )
        str_output = output.getvalue()
        cardinality_match = re.search(
            r'Cardinality of "[^"]+"\."[^"]+":\s+(\d+)', str_output
        )
        self.assertIsNotNone(cardinality_match)
        if cardinality_match is not None:
            cardinality_value = int(cardinality_match.group(1))
            self.assertEqual(cardinality_value, expected_cardinality)

    def test_sixty_four_records_one_cardinality(self):
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        records = []

        for _ in range(0, 64):
            dimensions = [
                {
                    "Name": "test_dimension_name",
                    "Value": "test_dimension_value",
                    "DimensionValueType": "VARCHAR",
                },
            ]

            record = {
                "Dimensions": dimensions,
                "MeasureName": "test_measure_name",
                "MeasureValue": "test_measure_value",
                "MeasureValueType": "VARCHAR",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
            }

            records.append(record)

        self.put_records(records)

        expected_cardinality = 1

        output = io.StringIO()
        with redirect_stdout(output):
            cardinality.main(
                [
                    "--database-name",
                    self.database_name,
                    "--table-name",
                    self.table_name,
                    "--start-time",
                    start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                    "--end-time",
                    end_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                ]
            )
        str_output = output.getvalue()
        cardinality_match = re.search(
            r'Cardinality of "[^"]+"\."[^"]+":\s+(\d+)', str_output
        )
        self.assertIsNotNone(cardinality_match)
        if cardinality_match is not None:
            cardinality_value = int(cardinality_match.group(1))
            self.assertEqual(cardinality_value, expected_cardinality)

    def test_sixty_four_records_sixty_four_cardinality(self):
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        records = []

        for i in range(0, 64):
            dimensions = [
                {
                    "Name": "test_dimension_name",
                    "Value": f"test_dimension_value_{i}",
                    "DimensionValueType": "VARCHAR",
                },
            ]

            record = {
                "Dimensions": dimensions,
                "MeasureName": "test_measure_name",
                "MeasureValue": "test_measure_value",
                "MeasureValueType": "VARCHAR",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
            }

            records.append(record)

        self.put_records(records)

        expected_cardinality = 64

        output = io.StringIO()
        with redirect_stdout(output):
            cardinality.main(
                [
                    "--database-name",
                    self.database_name,
                    "--table-name",
                    self.table_name,
                    "--start-time",
                    start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                    "--end-time",
                    end_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                ]
            )
        str_output = output.getvalue()
        cardinality_match = re.search(
            r'Cardinality of "[^"]+"\."[^"]+":\s+(\d+)', str_output
        )
        self.assertIsNotNone(cardinality_match)
        if cardinality_match is not None:
            cardinality_value = int(cardinality_match.group(1))
            self.assertEqual(cardinality_value, expected_cardinality)


if __name__ == "__main__":
    unittest.main()
