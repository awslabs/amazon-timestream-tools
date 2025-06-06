import os
import unittest
import shutil
import sys
import logging

import pandas
from pandas import Timedelta
import pytest
from influxdb_client.client.influxdb_client import InfluxDBClient
from testcontainers.influxdb2 import InfluxDb2Container

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from test_common import (
    UNLOAD_TIMESTAMP_FORMAT,
    ISO_8601_TIMESTAMP_FORMAT,
    BaseIntegrationTestCase,
)
import unload
from targets.timestream_for_influxdb.transform import transform
from targets.timestream_for_influxdb.ingestion import influxdb_ingestion
from targets.timestream_for_influxdb.validation import validator


class InfluxDbV2TargetTestCase(BaseIntegrationTestCase):
    """
    Tests end-to-end migrations from Timestream for LiveAnalytics
    to InfluxDB 2.x.
    """

    influxdb_container: InfluxDb2Container
    influxdb_client: InfluxDBClient

    influxdb_bucket_name_prefix = "la-idb-it-lp-influxdb-bucket-"
    influxdb_bucket_name: str

    # Directory for all tests to unload lp data into.
    lp_base_directory = "la-idb-it-lp-output-base"
    # Unique subdirectory for a tests's lp output.
    lp_directory_prefix = "la-idb-it-lp-output-"
    lp_directory: str

    athena_database_name = "default"
    athena_table_name: str
    athena_lp_table_name: str

    @classmethod
    def setUpClass(cls):
        """
        Overrides unittest.TestCase.setUpClass, called once before any
        tests in the class.
        """
        super().setUpClass()

        # InfluxDB setup.
        influxdb_host_port = int(os.environ.get("TEST_INFLUXDB_HOST_PORT", 8087))
        influxdb_internal_port = 8086
        influxdb_host = "http://localhost"
        influxdb_url = f"{influxdb_host}:{influxdb_host_port}"

        # The ingestion script requires these environment variables.
        os.environ["INFLUXDB_V2_URL"] = influxdb_url
        os.environ["INFLUXDB_V2_ORG"] = "test-org"
        os.environ["INFLUXDB_V2_TOKEN"] = "test-token"

        cls.influxdb_container = InfluxDb2Container(
            "influxdb:2.7",
            container_port=influxdb_internal_port,
            host_port=influxdb_host_port,
            init_mode="setup",
            username="root",
            password="test-password",
            org_name=os.environ["INFLUXDB_V2_ORG"],
            bucket="test-bucket",
            admin_token=os.environ["INFLUXDB_V2_TOKEN"],
        ).start()

        cls.influxdb_client = InfluxDBClient.from_env_properties()
        health_check = cls.influxdb_client.ping()
        if not health_check:
            raise ConnectionError(
                f"setUpClass: Failed to connect to InfluxDB v2 at {influxdb_url}"
            )

        # For interacting with Athena.
        cls.glue_client = cls.session.client("glue")

        # Create directory to hold nested directories of line protocol data.
        os.makedirs(cls.lp_base_directory, exist_ok=True)

    def setUp(self):
        """
        Overrides unittest.TestCase.setUp, called before each test runs.
        """
        super().setUp()

        # Keep a copy of the names of Athena tables that transform will create.
        # This should be overridden if --athena-table-name is used.
        self.athena_table_name = (
            self.database_name.replace("-", "_")
            + "_"
            + self.table_name.replace("-", "_")
        )
        self.athena_lp_table_name = f"lp_{self.athena_table_name}"

        self.lp_directory = f"{self.lp_base_directory}/{self.lp_directory_prefix + self.get_random_string(10)}"
        os.makedirs(self.lp_directory, exist_ok=True)

        self.influxdb_bucket_name = (
            self.influxdb_bucket_name_prefix + self.get_random_string(10)
        )
        self.influxdb_client.buckets_api().create_bucket(
            bucket_name=self.influxdb_bucket_name
        )

    def delete_athena_tables(self, athena_database_name: str, athena_table_names: list):
        for athena_table_name in athena_table_names:
            self.glue_client.delete_table(
                DatabaseName=athena_database_name, Name=athena_table_name
            )

    @classmethod
    def tearDownClass(cls):
        """
        Overrides unittest.TestCase.tearDownClass, called after all tests have finished.
        """
        super().tearDownClass()

        try:
            if os.path.exists(cls.lp_base_directory):
                shutil.rmtree(cls.lp_base_directory)
        except Exception as e:
            if not cls.silence_cleanup_logging:
                logging.warning(
                    f"tearDownClass: Failed to delete local line protocol base directory: {e}"
                )

        cls.influxdb_client.close()

        try:
            cls.influxdb_container.stop(force=True, delete_volume=True)
        except Exception as e:
            if not cls.silence_cleanup_logging:
                logging.warning(
                    f"tearDownClass: Failed to delete InfluxDB v2 container: {e}"
                )

    def tearDown(self):
        """
        Overrides unittest.TestCase.tearDown, called after each test runs.
        """
        super().tearDown()

        # Tests may purposely fail, causing resource to not be created.
        # To handle this, each resource needs its own try except block
        # with logging in case of failure.
        try:
            self.delete_athena_tables(
                athena_database_name=self.athena_database_name,
                athena_table_names=[self.athena_table_name],
            )
        except Exception as e:
            if not self.silence_cleanup_logging:
                logging.warning(f"tearDown: Failed to delete Athena unload table: {e}")

        try:
            self.delete_athena_tables(
                athena_database_name=self.athena_database_name,
                athena_table_names=[self.athena_lp_table_name],
            )
        except Exception as e:
            if not self.silence_cleanup_logging:
                logging.warning(
                    f"tearDown: Failed to delete Athena line protocol table: {e}"
                )

        try:
            if os.path.exists(self.lp_directory):
                shutil.rmtree(self.lp_directory)
        except Exception as e:
            if not self.silence_cleanup_logging:
                logging.warning(
                    f"tearDown: Failed to delete local line protocol directory: {e}"
                )

        try:
            influxdb_bucket = self.influxdb_client.buckets_api().find_bucket_by_name(
                self.influxdb_bucket_name
            )
            self.influxdb_client.buckets_api().delete_bucket(influxdb_bucket)
        except Exception as e:
            if not self.silence_cleanup_logging:
                logging.warning(f"tearDown: Failed to delete InfluxDB bucket: {e}")

    @staticmethod
    def get_quoted_tags(dimensions: list) -> str:
        """
        Produces line protocol tags in a format that the validation script
        expects for its --schema-tags argument. To do this, this function
        builds a string comprised of comma-separated dimension names, adding
        quotes to any dimension name that includes commas.

        Args:
            dimensions (list[dict]): A list of dimensions where each dimension
                is a dict with the key "Name".

        Returns:
            str
        """
        # measure_name is assumed to always be present as a tag.
        quoted_tags = ["measure_name"]
        for dimension in dimensions:
            tag = dimension["Name"]
            if "," in tag:
                quoted_tags.append(f'"{tag}"')
            else:
                quoted_tags.append(tag)
        return ",".join(quoted_tags)

    def test_single_measure_basic(self):
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
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

        schema_tags = self.get_quoted_tags(dimensions)

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
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "athena",
                "--athena-output",
                "s3://" + self.s3_bucket_name,
                "--athena-database-name",
                self.athena_database_name,
                "--athena-table-name",
                self.athena_table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--schema-tags",
                schema_tags,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    def test_single_measure_boolean(self):
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        record = {
            "Dimensions": dimensions,
            "MeasureName": "is_success",
            "MeasureValue": "TRUE",
            "MeasureValueType": "BOOLEAN",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
        }

        schema_tags = self.get_quoted_tags(dimensions)

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
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "athena",
                "--athena-output",
                "s3://" + self.s3_bucket_name,
                "--athena-database-name",
                self.athena_database_name,
                "--athena-table-name",
                self.athena_table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--schema-tags",
                schema_tags,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    def test_single_measure_nanosecond_timestamp_sequential(self):
        """
        Tests migrating a dataset that is comprised of two records where the
        two records have the same dimensions and measure names but different
        measure values and are one nanosecond apart.

        In InfluxDB, if these data points had the same timestamp, possibly
        due to Timestreamp precision loss, one would override the other,
        causing only one data point to exist in InfluxDB.
        """
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

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
                "MeasureValue": "44.0",
                "MeasureValueType": "DOUBLE",
                "Time": str((start_time + Timedelta(nanoseconds=1)).value),
                "TimeUnit": "NANOSECONDS",
            },
        ]
        self.put_records(records)

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
                "--append-timestamps",
                "true",
            ]
        )
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "timestream",
                "--timestream-database-name",
                self.database_name,
                "--timestream-table-name",
                self.table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    def test_multi_measure_timestamp_measure(self):
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        # Only multi-measure records can use TIMESTAMP as a measure type.
        record = {
            "Dimensions": dimensions,
            "MeasureName": "metrics",
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

        schema_tags = self.get_quoted_tags(dimensions)

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
                "--append-timestamps",
                "true",
            ]
        )
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "athena",
                "--athena-output",
                "s3://" + self.s3_bucket_name,
                "--athena-database-name",
                self.athena_database_name,
                "--athena-table-name",
                self.athena_table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--schema-tags",
                schema_tags,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    def test_multi_measure_basic(self):
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {"Name": "hostname", "Value": "hostname1", "DimensionValueType": "VARCHAR"},
            {"Name": "region", "Value": "us-west-2", "DimensionValueType": "VARCHAR"},
        ]

        record = {
            "Dimensions": dimensions,
            "MeasureName": "metrics",
            "MeasureValueType": "MULTI",
            "Time": str(start_time.value),
            "TimeUnit": "NANOSECONDS",
            "MeasureValues": [
                {"Name": "cpu_utilization", "Value": "12.3", "Type": "DOUBLE"},
                {"Name": "memory_utilization", "Value": "33.8", "Type": "DOUBLE"},
            ],
        }

        schema_tags = self.get_quoted_tags(dimensions)

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
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "athena",
                "--athena-output",
                "s3://" + self.s3_bucket_name,
                "--athena-database-name",
                self.athena_database_name,
                "--athena-table-name",
                self.athena_table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--schema-tags",
                schema_tags,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    def test_multi_measure_special_characters(self):
        """
        Tests the migration of data with special characters, characters
        that need to be escaped in line protocol such as spaces, commas, and equals
        signs, in dimension names, dimension values, measure_name, measure_value names,
        and measure values.
        """
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {
                "Name": "spaces in dimension name",
                "Value": "spaces in dimension value",
                "DimensionValueType": "VARCHAR",
            },
            {
                "Name": "commas,in,dimension,name",
                "Value": "commas,in,dimension,value",
                "DimensionValueType": "VARCHAR",
            },
            {
                "Name": "equals=in=dimension=name",
                "Value": "equals=in=dimension=value",
                "DimensionValueType": "VARCHAR",
            },
        ]

        records = [
            {
                "Dimensions": dimensions,
                "MeasureName": "spaces in measure name",
                "MeasureValueType": "MULTI",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
                "MeasureValues": [
                    {
                        "Name": "spaces in measure value name",
                        "Value": "spaces in measure value",
                        "Type": "VARCHAR",
                    },
                ],
            },
            {
                "Dimensions": dimensions,
                "MeasureName": "commas,in,measure,name",
                "MeasureValueType": "MULTI",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
                "MeasureValues": [
                    {
                        "Name": "commas,in,measure,value,name",
                        "Value": "commas,in,measure,value",
                        "Type": "VARCHAR",
                    },
                ],
            },
            {
                "Dimensions": dimensions,
                "MeasureName": "equals=in=measure=name",
                "MeasureValueType": "MULTI",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
                "MeasureValues": [
                    {
                        "Name": "equals=in=measure=value=name",
                        "Value": "equals=in=measure=value",
                        "Type": "VARCHAR",
                    },
                ],
            },
        ]

        schema_tags = self.get_quoted_tags(dimensions)

        self.put_records(records)
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
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "athena",
                "--athena-output",
                "s3://" + self.s3_bucket_name,
                "--athena-database-name",
                self.athena_database_name,
                "--athena-table-name",
                self.athena_table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--schema-tags",
                schema_tags,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )

    def test_single_measure_special_characters(self):
        """
        Tests the migration of data with special characters, characters
        that need to be escaped in line protocol such as spaces, commas, and equals
        signs, in dimension names, dimension values, measure_name, measure_value names,
        and measure values.
        """
        current_time = pandas.Timestamp.now()

        start_time = current_time - Timedelta(days=30)
        assert isinstance(start_time, pandas.Timestamp)
        end_time = start_time + Timedelta(days=1)
        assert isinstance(end_time, pandas.Timestamp)

        dimensions = [
            {
                "Name": "spaces in dimension name",
                "Value": "spaces in dimension value",
                "DimensionValueType": "VARCHAR",
            },
            {
                "Name": "equals=in=dimension=name",
                "Value": "equals=in=dimension=value",
                "DimensionValueType": "VARCHAR",
            },
        ]

        records = [
            {
                "Dimensions": dimensions,
                "MeasureName": "spaces in measure name",
                "MeasureValue": "spaces in measure value",
                "MeasureValueType": "VARCHAR",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
            },
            {
                "Dimensions": dimensions,
                "MeasureName": "commas,in,measure,name",
                "MeasureValue": "commas,in,measure,value",
                "MeasureValueType": "VARCHAR",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
            },
            {
                "Dimensions": dimensions,
                "MeasureName": "equals=in=measure=name",
                "MeasureValue": "equals=in=measure=value",
                "MeasureValueType": "VARCHAR",
                "Time": str(start_time.value),
                "TimeUnit": "NANOSECONDS",
            },
        ]

        schema_tags = self.get_quoted_tags(dimensions)

        self.put_records(records)
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
        transform.main(
            [
                "--database-name",
                self.database_name,
                "--tables",
                self.table_name,
                "--s3-bucket-path",
                self.s3_bucket_name,
                "--add-validation-field",
                "true",
            ]
        )
        self.s3_utility.sync_line_protocol_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.lp_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        influxdb_ingestion.main(
            [
                "-w",
                "5",
                "-l",
                "5000",
                "-m",
                "5",
                self.influxdb_bucket_name,
                self.lp_directory,
            ]
        )
        validator.main(
            [
                "--source-engine",
                "athena",
                "--athena-output",
                "s3://" + self.s3_bucket_name,
                "--athena-database-name",
                self.athena_database_name,
                "--athena-table-name",
                self.athena_table_name,
                "--influxdb-v2-url",
                os.environ["INFLUXDB_V2_URL"],
                "--influxdb-v2-token",
                os.environ["INFLUXDB_V2_TOKEN"],
                "--influxdb-v2-org",
                os.environ["INFLUXDB_V2_ORG"],
                "--influxdb-v2-bucket",
                self.influxdb_bucket_name,
                "--influxdb-v2-measurement",
                self.table_name,
                "--schema-tags",
                schema_tags,
                "--start-time",
                start_time.strftime(ISO_8601_TIMESTAMP_FORMAT),
                "--skip-wal-check",
            ]
        )


if __name__ == "__main__":
    unittest.main()
