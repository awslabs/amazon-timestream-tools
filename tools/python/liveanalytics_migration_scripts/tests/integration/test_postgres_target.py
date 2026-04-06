# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import logging
import os
import shutil
import sys
import unittest
from typing import Any

import pandas
import sqlalchemy
from pandas import Timedelta
from testcontainers.postgres import PostgresContainer

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import unload
from targets.rds_for_postgresql import postgres_ingestion
from test_common import (
    UNLOAD_TIMESTAMP_FORMAT,
    BaseIntegrationTestCase,
)


class PostgresTargetTestCase(BaseIntegrationTestCase):
    """
    Tests end-to-end migrations from Timestream for LiveAnalytics
    to PostgreSQL.
    """

    postgres_container: PostgresContainer

    postgres_database_prefix = "la_idb_it_lp_postgres_database-"
    postgres_database_name: str
    postgres_table_prefix = "la_idb_it_lp_postgres_table"
    postgres_table_name: str
    postgres_username: str = "postgres"
    postgres_password: str = "helloworld"
    postgres_port: int
    postgres_host: str
    postgres_url: str

    # Directory for all tests to unload CSV data into.
    csv_base_directory = "la-idb-it-lp-output-base"
    # Unique subdirectory for a tests's CSV output.
    csv_directory_prefix = "la-idb-it-lp-output-"
    csv_directory: str

    secrets_client: Any
    secret_name_prefix: str = "la-idb-it-postgres-secret-"
    secret_name: str
    secret_arn: str

    @classmethod
    def setUpClass(cls):
        """
        Overrides unittest.TestCase.setUpClass, called once before any
        tests in the class.
        """
        super().setUpClass()

        # PostgreSQL setup.
        cls.postgres_container = PostgresContainer(
            image="postgres:16",
            username=cls.postgres_username,
            password=cls.postgres_password,
            driver="pg8000",
        ).start()
        cls.postgres_host = cls.postgres_container.get_container_host_ip()
        cls.postgres_port = int(cls.postgres_container.get_exposed_port(5432))
        cls.postgres_url = f"{cls.postgres_host}:{cls.postgres_port}"

        cls.secrets_client = cls.session.client("secretsmanager")
        cls.secret_name = cls.secret_name_prefix + cls.get_random_string(5)
        create_secret_response = cls.secrets_client.create_secret(
            Name=cls.secret_name,
            SecretString=json.dumps(
                {
                    "password": cls.postgres_password,
                }
            ),
        )
        cls.secret_arn = create_secret_response["ARN"]

        # Create directory to hold nested directories of CSV data.
        os.makedirs(cls.csv_base_directory, exist_ok=True)

    def setUp(self):
        """
        Overrides unittest.TestCase.setUp, called before each test runs.
        """
        super().setUp()

        self.csv_directory = f"{self.csv_base_directory}/{self.csv_directory_prefix + self.get_random_string(10)}"
        os.makedirs(self.csv_directory, exist_ok=True)

        self.postgres_database_name = (
            self.postgres_database_prefix + self.get_random_string(10)
        )
        self.postgres_table_name = self.postgres_table_prefix + self.get_random_string(
            10
        )
        engine = sqlalchemy.create_engine(self.postgres_container.get_connection_url())
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            connection.execute(
                sqlalchemy.text(f'CREATE DATABASE "{self.postgres_database_name}"')
            )
        engine.dispose()

    @classmethod
    def tearDownClass(cls):
        """
        Overrides unittest.TestCase.tearDownClass, called after all tests have finished.
        """
        super().tearDownClass()

        try:
            if os.path.exists(cls.csv_base_directory):
                shutil.rmtree(cls.csv_base_directory)
        except Exception as e:
            if not cls.silence_cleanup_logging:
                logging.warning(
                    f"tearDownClass: Failed to delete local CSV base directory: {e}"
                )

        try:
            cls.postgres_container.stop(force=True, delete_volume=True)
        except Exception as e:
            if not cls.silence_cleanup_logging:
                logging.warning(
                    f"tearDownClass: Failed to delete PostgreSQL container: {e}"
                )

        # Delete PostgreSQL secret in Secrets Manager.
        try:
            cls.secrets_client.delete_secret(
                SecretId=cls.secret_name, ForceDeleteWithoutRecovery=True
            )
        except Exception as e:
            if not cls.silence_cleanup_logging:
                logging.warning(
                    f"tearDownClass: Failed to delete {cls.secret_name} secret: {e}"
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
            if os.path.exists(self.csv_directory):
                shutil.rmtree(self.csv_directory)
        except Exception as e:
            if not self.silence_cleanup_logging:
                logging.warning(f"tearDown: Failed to delete local CSV directory: {e}")

        try:
            engine = sqlalchemy.create_engine(
                self.postgres_container.get_connection_url()
            )
            with engine.connect().execution_options(
                isolation_level="AUTOCOMMIT"
            ) as connection:
                connection.execute(
                    sqlalchemy.text(f'DROP DATABASE "{self.postgres_database_name}"')
                )
            engine.dispose()
        except Exception as e:
            if not self.silence_cleanup_logging:
                logging.warning(f"tearDown: Failed to delete PostgreSQL database: {e}")

    def table_row_counts_match(self):
        # Check Timestream for LiveAnalytics.
        timestream_query_client = self.session.client("timestream-query")
        la_response = timestream_query_client.query(
            QueryString=f'SELECT COUNT(*) FROM "{self.database_name}"."{self.table_name}"'
        )
        la_count = int(la_response["Rows"][0]["Data"][0]["ScalarValue"])

        # Check PostgreSQL.
        base_url = sqlalchemy.engine.make_url(
            self.postgres_container.get_connection_url()
        )
        db_url = base_url.set(database=self.postgres_database_name)
        engine = sqlalchemy.create_engine(db_url)
        pg_row = None
        pg_count = 0
        with engine.connect() as connection:
            pg_row = connection.execute(
                sqlalchemy.text(f'SELECT COUNT(*) FROM "{self.postgres_table_name}"')
            ).fetchone()
            pg_count = int(pg_row[0])
        engine.dispose()

        print(
            f"PostgreSQL record count: {pg_count}, Timestream record count: {la_count}"
        )
        return pg_count == la_count

    def create_postgres_table_with_schema(self, schema_dict: dict[str, str]):
        schema = ", ".join(
            f'"{column_name}" {data_type}'
            for column_name, data_type in schema_dict.items()
        )
        base_url = sqlalchemy.make_url(self.postgres_container.get_connection_url())
        db_url = base_url.set(database=self.postgres_database_name)
        engine = sqlalchemy.create_engine(db_url)
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            connection.execute(
                sqlalchemy.text(
                    f"CREATE TABLE IF NOT EXISTS {self.postgres_table_name}({schema})"
                )
            )
        engine.dispose()

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

        self.put_records([record])

        self.create_postgres_table_with_schema(
            {
                "hostname": "VARCHAR(256)",
                "region": "VARCHAR(256)",
                "measure_name": "VARCHAR(256)",
                "measure_value::double": "DECIMAL",
                "time": "VARCHAR(100)",
                "time_ns": "BIGINT",
            }
        )

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
        self.s3_utility.sync_csv_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.csv_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        postgres_ingestion.main(
            [
                "--database",
                self.postgres_database_name,
                "--table",
                self.postgres_table_name,
                "--user",
                self.postgres_username,
                "--input-files",
                f"./{self.csv_directory}/results/*/*.csv",
                "--host",
                self.postgres_host,
                "--port",
                str(self.postgres_port),
                "--secret-arn",
                self.secret_arn,
            ]
        )

        self.assertTrue(
            self.table_row_counts_match(),
            msg="Timestream for LiveAnalytics and PostgreSQL table row counts do not match",
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

        self.put_records([record])

        self.create_postgres_table_with_schema(
            {
                "hostname": "VARCHAR(256)",
                "region": "VARCHAR(256)",
                "measure_name": "VARCHAR(256)",
                "measure_value::boolean": "BOOL",
                "time": "VARCHAR(100)",
                "time_ns": "BIGINT",
            }
        )

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
        self.s3_utility.sync_csv_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.csv_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        postgres_ingestion.main(
            [
                "--database",
                self.postgres_database_name,
                "--table",
                self.postgres_table_name,
                "--user",
                self.postgres_username,
                "--input-files",
                f"./{self.csv_directory}/results/*/*.csv",
                "--host",
                self.postgres_host,
                "--port",
                str(self.postgres_port),
                "--secret-arn",
                self.secret_arn,
            ]
        )

        self.assertTrue(
            self.table_row_counts_match(),
            msg="Timestream for LiveAnalytics and PostgreSQL table row counts do not match",
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

        self.create_postgres_table_with_schema(
            {
                "hostname": "VARCHAR(256)",
                "region": "VARCHAR(256)",
                "measure_name": "VARCHAR(256)",
                "measure_value::double": "DECIMAL",
                "time": "VARCHAR(100)",
                "time_ns": "BIGINT",
            }
        )

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
                "--export-format",
                "CSV",
            ]
        )
        self.s3_utility.sync_csv_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.csv_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        postgres_ingestion.main(
            [
                "--database",
                self.postgres_database_name,
                "--table",
                self.postgres_table_name,
                "--user",
                self.postgres_username,
                "--input-files",
                f"./{self.csv_directory}/results/*/*.csv",
                "--host",
                self.postgres_host,
                "--port",
                str(self.postgres_port),
                "--secret-arn",
                self.secret_arn,
            ]
        )

        self.assertTrue(
            self.table_row_counts_match(),
            msg="Timestream for LiveAnalytics and PostgreSQL table row counts do not match",
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

        self.put_records([record])

        self.create_postgres_table_with_schema(
            {
                "hostname": "VARCHAR(256)",
                "region": "VARCHAR(256)",
                "measure_name": "VARCHAR(256)",
                "request_time": "VARCHAR(100)",
                "request_time_ns": "BIGINT",
                "time": "VARCHAR(100)",
                "time_ns": "BIGINT",
            }
        )

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
                "--export-format",
                "CSV",
            ]
        )
        self.s3_utility.sync_csv_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.csv_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        postgres_ingestion.main(
            [
                "--database",
                self.postgres_database_name,
                "--table",
                self.postgres_table_name,
                "--user",
                self.postgres_username,
                "--input-files",
                f"./{self.csv_directory}/results/*/*.csv",
                "--host",
                self.postgres_host,
                "--port",
                str(self.postgres_port),
                "--secret-arn",
                self.secret_arn,
            ]
        )

        self.assertTrue(
            self.table_row_counts_match(),
            msg="Timestream for LiveAnalytics and PostgreSQL table row counts do not match",
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

        self.put_records([record])

        self.create_postgres_table_with_schema(
            {
                "hostname": "VARCHAR(256)",
                "region": "VARCHAR(256)",
                "measure_name": "VARCHAR(256)",
                "cpu_utilization": "DECIMAL",
                "memory_utilization": "DECIMAL",
                "time": "VARCHAR(100)",
                "time_ns": "BIGINT",
            }
        )

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
        self.s3_utility.sync_csv_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.csv_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        postgres_ingestion.main(
            [
                "--database",
                self.postgres_database_name,
                "--table",
                self.postgres_table_name,
                "--user",
                self.postgres_username,
                "--input-files",
                f"./{self.csv_directory}/results/*/*.csv",
                "--host",
                self.postgres_host,
                "--port",
                str(self.postgres_port),
                "--secret-arn",
                self.secret_arn,
            ]
        )

        self.assertTrue(
            self.table_row_counts_match(),
            msg="Timestream for LiveAnalytics and PostgreSQL table row counts do not match",
        )

    def test_multi_measure_object(self):
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
                {
                    "Name": "value_uno",
                    "Value": '{"dog_walk_double + run addition": 0.1, "dogsneakinteraction_smell_double": 0.63}',
                    "Type": "VARCHAR",
                },
                {
                    "Name": "value_dos",
                    "Value": "{'dog_walk_single + run addition': 0.1, 'dogsneakinteraction_smell_single': 0.63}",
                    "Type": "VARCHAR",
                },
                {"Name": "memory_utilization", "Value": "33.8", "Type": "DOUBLE"},
            ],
        }

        self.put_records([record])

        self.create_postgres_table_with_schema(
            {
                "hostname": "VARCHAR(256)",
                "region": "VARCHAR(256)",
                "measure_name": "VARCHAR(256)",
                "value_uno": "VARCHAR(256)",
                "value_dos": "VARCHAR(256)",
                "memory_utilization": "DECIMAL",
                "time": "VARCHAR(100)",
                "time_ns": "BIGINT",
            }
        )

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
        self.s3_utility.sync_csv_to_storage(
            s3_bucket_path=self.s3_bucket_name,
            directory=self.csv_directory,
            timestream_database_name=self.database_name,
            timestream_table_name=self.table_name,
        )
        postgres_ingestion.main(
            [
                "--database",
                self.postgres_database_name,
                "--table",
                self.postgres_table_name,
                "--user",
                self.postgres_username,
                "--input-files",
                f"./{self.csv_directory}/results/*/*.csv",
                "--host",
                self.postgres_host,
                "--port",
                str(self.postgres_port),
                "--secret-arn",
                self.secret_arn,
            ]
        )

        self.assertTrue(
            self.table_row_counts_match(),
            msg="Timestream for LiveAnalytics and PostgreSQL table row counts do not match",
        )


if __name__ == "__main__":
    unittest.main()
