# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import os
import unittest
import random
import string
import sys
import tempfile
import time
import logging

from influxdb_client.client.influxdb_client import InfluxDBClient
from testcontainers.influxdb2 import InfluxDb2Container

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from targets.timestream_for_influxdb.ingestion import influxdb_ingestion


class IngestionTestCase(unittest.TestCase):
    """
    Tests for the influxdb_ingestion.py script.

    This test suite verifies ingesting gz files into InfluxDB buckets with the influxdb_ingestion.py script.
    """

    influxdb_container: InfluxDb2Container
    influxdb_client: InfluxDBClient

    influxdb_bucket_name_prefix = "la-idb-it-lp-influxdb-bucket-"
    influxdb_bucket_name: str

    test_data_dir = os.path.join(os.path.dirname(__file__), "test-data")
    valid_data_dir = os.path.join(test_data_dir, "valid")
    invalid_data_dir = os.path.join(test_data_dir, "invalid")
    tmp_path: tempfile.TemporaryDirectory

    silence_cleanup_logging = False

    @classmethod
    def setUpClass(cls):
        """
        Overrides unittest.TestCase.setUpClass, called once before any
        tests in the class.
        """
        # InfluxDB setup.
        influxdb_host_port = int(os.environ.get("TEST_INFLUXDB_HOST_PORT", 8087))
        influxdb_internal_port = 8086
        influxdb_host = "http://localhost"
        influxdb_url = f"{influxdb_host}:{influxdb_host_port}"

        # The ingestion script requires these environment variables.
        os.environ["INFLUXDB_V2_URL"] = influxdb_url
        os.environ["INFLUXDB_V2_ORG"] = "test-org"
        os.environ["INFLUXDB_V2_TOKEN"] = "test-token"
        os.environ["INFLUXDB_V2_TIMEOUT"] = "30000"

        cls.influxdb_container = InfluxDb2Container(
            "influxdb:2.7",
            container_port=influxdb_internal_port,
            host_port=influxdb_host_port,
            init_mode="setup",
            username="root",
            password="test-password",
            org_name=os.environ["INFLUXDB_V2_ORG"],
            bucket="testbucket",
            admin_token=os.environ["INFLUXDB_V2_TOKEN"],
        ).start()

        cls.influxdb_client = InfluxDBClient.from_env_properties()
        health_check = cls.influxdb_client.ping()
        if not health_check:
            raise ConnectionError(
                f"setUpClass: Failed to connect to InfluxDB v2 at {influxdb_url}"
            )

    def setUp(self):
        """
        Overrides unittest.TestCase.setUp, called before each test runs.
        """
        self.tmp_path = tempfile.TemporaryDirectory(suffix=self.get_random_string(10))
        self.influxdb_bucket_name = (
            self.influxdb_bucket_name_prefix + self.get_random_string(10)
        )
        self.create_bucket(self.influxdb_bucket_name)

    @classmethod
    def tearDownClass(cls):
        """
        Overrides unittest.TestCase.tearDownClass, called after all tests have finished.
        """

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
        self.tmp_path.cleanup()
        try:
            influxdb_bucket = self.influxdb_client.buckets_api().find_bucket_by_name(
                self.influxdb_bucket_name
            )
            self.influxdb_client.buckets_api().delete_bucket(influxdb_bucket)
        except Exception as e:
            if not self.silence_cleanup_logging:
                logging.warning(f"tearDown: Failed to delete InfluxDB bucket: {e}")

    @staticmethod
    def get_random_string(length: int):
        return "".join(
            random.SystemRandom().choice(string.ascii_lowercase + string.digits)
            for _ in range(length)
        )

    def create_bucket(self, bucket_name):
        """
        Create a bucket with the given name.

        Args:
            client: The InfluxDB client
            bucket_name: Name of the bucket to create

        Returns:
            None
        """
        buckets_api = self.influxdb_client.buckets_api()
        org = os.environ.get("INFLUXDB_V2_ORG")

        try:
            buckets_api.create_bucket(bucket_name=bucket_name, org=org)
            print(f"Created bucket: {bucket_name}")
        except Exception as e:
            if "already exists" in str(e):
                print(f"Bucket {bucket_name} already exists")
            else:
                raise

    def query_bucket_count(self, bucket_name) -> int:
        """
        Query the count of points in a bucket for the la_unload field.

        Args:
            client: The InfluxDB client
            bucket_name: Name of the bucket to query
            field: The field to count (default: "la_unload")

        Returns:
            int: The count of points
        """
        query_api = self.influxdb_client.query_api()
        query = f"""
        from(bucket: "{bucket_name}")
          |> range(start: 0)
          |> filter(fn: (r) => r["_field"] == "la_unload")
          |> group()
          |> count()
        """

        print("Executing validation query")
        query_result = query_api.query(query)

        assert len(query_result) > 0, "Query returned no results"

        # Extract the count value from the result
        count_value = None
        for table in query_result:
            for record in table.records:
                count_value = record.get_value()
                break
            if count_value is not None:
                break

        if count_value is None:
            raise RuntimeError(
                f"Failed to get query bucket count for bucket {bucket_name}"
            )
        return count_value

    def test_valid_dataset_ingestion(self):
        """
        Test ingestion of the full valid dataset using 11 workers.

        This test:
        1. Creates a 'valid-01' bucket.
        2. Ingests all valid test data using 11 workers.
        3. Validates ingestion.
        """
        logging.info(
            f"Starting ingestion of valid dataset to {self.influxdb_bucket_name}"
        )
        start_time = time.time()
        influxdb_ingestion.main(
            [
                self.influxdb_bucket_name,
                self.valid_data_dir,
                "-w",
                "11",
                "-r",
                "5",
            ]
        )

        end_time = time.time()
        duration = end_time - start_time

        logging.info(f"Ingestion completed in {duration:.2f} seconds")

        count_value = self.query_bucket_count(self.influxdb_bucket_name)

        expected_count = 50000250
        self.assertEqual(
            count_value,
            expected_count,
            f"Expected count {expected_count}, got {count_value}",
        )
        logging.info(
            f"Validation successful: count matches expected value of {expected_count}"
        )

    def test_invalid_dataset_ingestion_error(self):
        """
        Test ingestion failure with a malformed dataset.

        This test:
        1. Creates an 'invalid-01' bucket
        2. Attempts to ingest data
        3. Verifies the script fails early
        """
        with self.assertRaises(SystemExit) as e:
            influxdb_ingestion.main(
                [
                    self.influxdb_bucket_name,
                    self.invalid_data_dir,
                    "-w",
                    "5",
                    "-r",
                    "3",
                ]
            )
        self.assertEqual(e.exception.code, 1)
        logging.info(
            "Successfully stopped ingestion when reaching error and not using continue-on-error flag"
        )

    def test_invalid_dataset_ingestion_continue_on_error(self):
        """
        Test ingestion with a malformed dataset and using continue-on-error flag.

        This test:
        1. Creates an 'invalid-02' bucket
        2. Attempts to ingest malformed data with continue-on-error flag set
        3. Verifies all non-malformed data is ingested
        """
        logging.info(
            f"Starting ingestion with invalid dataset to {self.influxdb_bucket_name} using 5 workers"
        )
        start_time = time.time()

        influxdb_ingestion.main(
            [
                self.influxdb_bucket_name,
                self.invalid_data_dir,
                "-w",
                "5",
                "-r",
                "3",
                "--continue-on-error",
            ]
        )

        end_time = time.time()
        duration = end_time - start_time

        logging.info(f"Ingestion completed in {duration:.2f} seconds")

        count_value = self.query_bucket_count(self.influxdb_bucket_name)

        complete_dataset_count = 50000250
        # File test-data/invalid/influxdb_data_05.gz is malformed and has a line count of 5000000.
        malformed_file_count = 5000000
        self.assertIsNotNone(count_value, f"Unexpected count {count_value}")
        self.assertLess(
            count_value, complete_dataset_count, f"Unexpected count {count_value}"
        )
        self.assertGreater(
            count_value,
            (complete_dataset_count - malformed_file_count),
            f"Unexpected count {count_value}",
        )

        logging.info(
            "Successfully ingested all non-malformed data using continue-on-error flag"
        )

    def test_resume_functionality(self):
        """
        Test the resume functionality of the ingestion script.

        This test:
        1. Creates a 'resume-test' bucket
        2. Attempts to ingest invalid data which should fail
        3. Then runs the script again with valid data and the resume flag
        4. Verifies that all points are successfully ingested
        """
        logs_dir = os.path.join(self.tmp_path.name, "resume-logs")
        os.makedirs(logs_dir, exist_ok=True)

        logging.info(
            f"Starting first ingestion attempt with invalid data to {self.influxdb_bucket_name}"
        )

        # First run with invalid data - using continue-on-error to process all valid files
        influxdb_ingestion.main(
            [
                self.influxdb_bucket_name,
                self.invalid_data_dir,
                "-w",
                "5",
                "-r",
                "3",
                "--logs-dir",
                logs_dir,
                "--continue-on-error",
            ]
        )

        logging.info("First run completed successfully with continue-on-error flag")

        # Find the tracking directory created by the first run
        tracking_dirs = [d for d in os.listdir(logs_dir) if d.startswith("tracking_")]
        self.assertGreater(
            len(tracking_dirs), 0, "No tracking directory found from first run"
        )
        tracking_dir = os.path.join(logs_dir, tracking_dirs[0])

        logging.info(f"Found tracking directory: {tracking_dir}")
        self.assertEqual(
            open(tracking_dir + "/failed_files.txt").read(), "influxdb_data_05.gz\n"
        )
        logging.info("Tracking directory has expected failed file influxdb_data_05.gz")

        # Second run with valid data and resume flag
        logging.info(
            "Starting second ingestion attempt with valid data and resume flag"
        )
        influxdb_ingestion.main(
            [
                self.influxdb_bucket_name,
                self.valid_data_dir,
                "-w",
                "5",
                "--logs-dir",
                logs_dir,
                "--resume-from",
                tracking_dir,
            ]
        )

        logging.info("Second run completed successfully with resume flag")

        count_value = self.query_bucket_count(self.influxdb_bucket_name)

        expected_count = 50000250
        self.assertEqual(
            count_value,
            expected_count,
            f"Expected count {expected_count}, got {count_value}",
        )

        logging.info(
            f"Validation successful: count matches expected value of {expected_count}"
        )

    def test_check_bucket_exists(self):
        """Test that the bucket existence check works correctly."""
        self.assertTrue(influxdb_ingestion.check_bucket_exists("testbucket"))
        self.assertFalse(influxdb_ingestion.check_bucket_exists("nonexistent_bucket"))

    def test_decompress_gzip_file(self):
        """Test that gzip files can be decompressed correctly."""
        # Copy a test file to a temporary directory
        test_file = os.path.join(self.valid_data_dir, "influxdb_data_11.gz")
        test_file_copy = os.path.join(self.tmp_path.name, "test_file.gz")

        with open(test_file, "rb") as src, open(test_file_copy, "wb") as dst:
            dst.write(src.read())
        extracted_path = influxdb_ingestion.decompress_gzip_file(test_file_copy)

        self.assertTrue(os.path.exists(extracted_path))

        with open(extracted_path, "r") as f:
            content = f.read()
            self.assertGreater(len(content), 0)


if __name__ == "__main__":
    unittest.main()
