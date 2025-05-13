"""
Tests for the influxdb_ingestion.py script.

This test suite verifies ingesting gz files into InfluxDB buckets with the influxdb_ingestion.py script.
"""

import os
import sys
import time
import subprocess

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import influxdb_ingestion

def create_bucket(client, bucket_name):
    """
    Create a bucket with the given name.

    Args:
        client: The InfluxDB client
        bucket_name: Name of the bucket to create

    Returns:
        None
    """
    buckets_api = client.buckets_api()
    org = os.environ.get("INFLUXDB_V2_ORG")

    try:
        buckets_api.create_bucket(bucket_name=bucket_name, org=org)
        print(f"Created bucket: {bucket_name}")
    except Exception as e:
        if "already exists" in str(e):
            print(f"Bucket {bucket_name} already exists")
        else:
            raise


def query_bucket_count(client, bucket_name):
    """
    Query the count of points in a bucket for the la_unload field.

    Args:
        client: The InfluxDB client
        bucket_name: Name of the bucket to query
        field: The field to count (default: "la_unload")

    Returns:
        int: The count of points
    """
    query_api = client.query_api()
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

    print(f"Query returned count: {count_value}")
    return count_value


class TestInfluxDBIngestion:
    """Test suite for InfluxDB ingestion script."""

    def test_valid_dataset_ingestion(self, influxdb_setup, valid_data_dir):
        """
        Test ingestion of the full valid dataset using 11 workers.

        This test:
        1. Creates a 'valid-01' bucket
        2. Ingests all valid test data using 11 workers
        3. Validates ingestion
        """
        client = influxdb_setup
        bucket_name = "valid-01"

        create_bucket(client, bucket_name)

        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "influxdb_ingestion.py",
        )

        print(f"Starting ingestion of valid dataset to {bucket_name}")
        start_time = time.time()

        result = subprocess.run(
            [
                sys.executable,
                script_path,
                bucket_name,
                valid_data_dir,
                "-w",
                "11",
                "-r",
                "5",
            ],
            capture_output=True,
            text=True,
        )

        end_time = time.time()
        duration = end_time - start_time

        print(f"Ingestion completed in {duration:.2f} seconds")

        assert result.returncode == 0, f"Script failed with output: {result.stderr}"

        count_value = query_bucket_count(client, bucket_name)

        expected_count = 50000250
        assert count_value == expected_count, (
            f"Expected count {expected_count}, got {count_value}"
        )

        print(
            f"Validation successful: count matches expected value of {expected_count}"
        )

    def test_invalid_dataset_ingestion_error(self, influxdb_setup, invalid_data_dir):
        """
        Test ingestion failure with a malformed dataset.

        This test:
        1. Creates an 'invalid-01' bucket
        2. Attempts to ingest data
        3. Verifies the script fails early
        """
        client = influxdb_setup
        bucket_name = "invalid-01"

        create_bucket(client, bucket_name)

        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "influxdb_ingestion.py",
        )
        result = subprocess.run(
            [
                sys.executable,
                script_path,
                bucket_name,
                invalid_data_dir,
                "-w",
                "5",
                "-r",
                "3",
            ],
            capture_output=True,
            text=True,
        )

        # should return non-zero if encountering an error and continue-on-error is not set
        assert result.returncode != 0, f"Script failed with output: {result.stderr}"
        print(
            "Successfully stopped ingestion when reaching error and not using continue-on-error flag"
        )

    def test_invalid_dataset_ingestion_continue_on_error(
        self, influxdb_setup, invalid_data_dir
    ):
        """
        Test ingestion with a malformed dataset and using continue-on-error flag.

        This test:
        1. Creates an 'invalid-02' bucket
        2. Attempts to ingest malformed data with continue-on-error flag set
        3. Verifies all non-malformed data is ingested
        """
        client = influxdb_setup
        bucket_name = "invalid-02"

        create_bucket(client, bucket_name)

        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "influxdb_ingestion.py",
        )

        print(
            f"Starting ingestion with invalid dataset to {bucket_name} using 5 workers"
        )
        start_time = time.time()

        result = subprocess.run(
            [
                sys.executable,
                script_path,
                bucket_name,
                invalid_data_dir,
                "-w",
                "5",
                "-r",
                "3",
                "--continue-on-error",
            ],
            capture_output=True,
            text=True,
        )

        end_time = time.time()
        duration = end_time - start_time

        print(f"Ingestion completed in {duration:.2f} seconds")

        count_value = query_bucket_count(client, "invalid-01")

        # Should return zero with continue-on-error
        assert result.returncode == 0, f"Script failed with output: {result.stderr}"

        complete_dataset_count = 50000250
        # File test-data/invalid/influxdb_data_05.gz is malformed and has a line count of 5000000
        malformed_file_count = 5000000
        assert (
            count_value is not None
            and count_value < complete_dataset_count
            and count_value > (complete_dataset_count - malformed_file_count)
        ), f"Unexpected count {count_value}"

        print(
            "Successfully ingested all non-malformed data using continue-on-error flag"
        )

    def test_resume_functionality(self, influxdb_setup, invalid_data_dir, valid_data_dir, tmp_path):
        """
        Test the resume functionality of the ingestion script.

        This test:
        1. Creates a 'resume-test' bucket
        2. Attempts to ingest invalid data which should fail
        3. Then runs the script again with valid data and the resume flag
        4. Verifies that all points are successfully ingested
        """
        client = influxdb_setup
        bucket_name = "resume-test"

        create_bucket(client, bucket_name)

        script_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "influxdb_ingestion.py",
        )

        logs_dir = os.path.join(tmp_path, "resume-logs")
        os.makedirs(logs_dir, exist_ok=True)

        print(f"Starting first ingestion attempt with invalid data to {bucket_name}")

        # First run with invalid data - using continue-on-error to process all valid files
        first_run = subprocess.run(
            [
                sys.executable,
                script_path,
                bucket_name,
                invalid_data_dir,
                "-w", "5",
                "-r", "3",
                "--logs-dir", logs_dir,
                "--continue-on-error"
            ],
            capture_output=True,
            text=True,
        )

        # Should return zero when using continue-on-error
        assert first_run.returncode == 0, "Expected first run to succeed with continue-on-error flag"
        print("First run completed successfully with continue-on-error flag")

        # Find the tracking directory created by the first run
        tracking_dirs = [d for d in os.listdir(logs_dir) if d.startswith("tracking_")]
        assert len(tracking_dirs) > 0, "No tracking directory found from first run"
        tracking_dir = os.path.join(logs_dir, tracking_dirs[0])

        print(f"Found tracking directory: {tracking_dir}")
        assert(open(tracking_dir + "/failed_files.txt").read() == "influxdb_data_05.gz\n")
        print("Tracking directory has expected failed file influxdb_data_05.gz")

        # Second run with valid data and resume flag
        print(f"Starting second ingestion attempt with valid data and resume flag")
        second_run = subprocess.run(
            [
                sys.executable,
                script_path,
                bucket_name,
                valid_data_dir,
                "-w", "5",
                "--logs-dir", logs_dir,
                "--resume-from", tracking_dir,
            ],
            capture_output=True,
            text=True,
        )

        assert second_run.returncode == 0, f"Second run failed with output: {second_run.stderr}"
        print("Second run completed successfully with resume flag")

        count_value = query_bucket_count(client, bucket_name)

        expected_count = 50000250
        assert count_value == expected_count, (
            f"Expected count {expected_count}, got {count_value}"
        )

        print(
            f"Validation successful: count matches expected value of {expected_count}"
        )

    def test_check_bucket_exists(self, influxdb_setup):
        """Test that the bucket existence check works correctly."""
        assert influxdb_ingestion.check_bucket_exists("testbucket") is True

        assert (
            influxdb_ingestion.check_bucket_exists("nonexistent_bucket") is False
        )

    def test_decompress_gzip_file(self, valid_data_dir, tmp_path):
        """Test that gzip files can be decompressed correctly."""
        # Copy a test file to a temporary directory
        test_file = os.path.join(
            valid_data_dir, "influxdb_data_11.gz"
        )
        test_file_copy = os.path.join(tmp_path, "test_file.gz")

        with open(test_file, "rb") as src, open(test_file_copy, "wb") as dst:
            dst.write(src.read())
        extracted_path = influxdb_ingestion.decompress_gzip_file(test_file_copy)

        assert os.path.exists(extracted_path)

        with open(extracted_path, "r") as f:
            content = f.read()
            assert len(content) > 0


