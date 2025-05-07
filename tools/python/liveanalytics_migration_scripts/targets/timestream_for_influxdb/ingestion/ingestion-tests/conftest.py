"""
Pytest configuration for InfluxDB ingestion tests.
"""

import os
import time
import pytest
import subprocess
from influxdb_client import InfluxDBClient


@pytest.fixture(scope="session")
def docker_compose_file(pytestconfig):
    """Path to the docker-compose.yml file."""
    return os.path.join(os.path.dirname(__file__), "docker-compose.yml")


@pytest.fixture(scope="session")
def influxdb_setup(docker_compose_file, request):
    """
    Set up InfluxDB container using docker-compose.

    This fixture:
    1. Starts the InfluxDB container
    2. Waits for it to be healthy
    3. Creates test buckets
    4. Yields the client for tests to use
    5. Tears down the container after tests
    """
    subprocess.run(
        ["docker", "compose", "-f", docker_compose_file, "up", "-d"], check=True
    )

    print("Waiting for InfluxDB container to start...")

    max_retries = 60
    retry_interval = 3

    url = "http://localhost:8086"
    token = "testtokenstring"
    org = "testorg"

    # Try connection
    client = None
    for i in range(max_retries):
        try:
            client = InfluxDBClient(url=url, token=token, org=org)
            health = client.ping()
            if health:
                print(f"InfluxDB is healthy after {i * retry_interval} seconds")
                # Ensure the service is fully ready
                time.sleep(5)
                break
            else:
                print(
                    f"InfluxDB ping returned false, retrying... ({i + 1}/{max_retries})"
                )
                if client:
                    client.close()
                    client = None
                time.sleep(retry_interval)
        except Exception as e:
            print(
                f"Waiting for InfluxDB to be ready... ({i + 1}/{max_retries}): {str(e)}"
            )
            if client:
                client.close()
                client = None
            time.sleep(retry_interval)
    else:
        subprocess.run(
            ["docker", "compose", "-f", docker_compose_file, "down", "-v"], check=True
        )
        pytest.fail("InfluxDB did not become healthy in time")

    if client is None:
        pytest.fail("InfluxDB client failed to be created")
    else:
        buckets_api = client.buckets_api()

    existing_buckets = [b.name for b in buckets_api.find_buckets().buckets]
    print(f"Existing buckets: {existing_buckets}")

    # Set environment variables for the script to use
    os.environ["INFLUXDB_V2_URL"] = url
    os.environ["INFLUXDB_V2_ORG"] = org
    os.environ["INFLUXDB_V2_TOKEN"] = token

    yield client

    client.close()
    cmd = ["docker", "compose", "-f", docker_compose_file, "down", "-v"]

    # Append extra flags if all tests passed
    if request.session.testsfailed == 0:
        cmd += ["--rmi", "all"]
    else:
        print("Tests failed.")

    # Run the composed command
    subprocess.run(cmd, check=True)


@pytest.fixture(scope="session")
def test_data_dir():
    """Path to the test data directory."""
    return os.path.join(os.path.dirname(__file__), "test-data")


@pytest.fixture(scope="session")
def valid_data_dir(test_data_dir):
    """Path to the valid test data directory."""
    return os.path.join(test_data_dir, "valid")


@pytest.fixture(scope="session")
def invalid_data_dir(test_data_dir):
    """Path to the invalid test data directory."""
    return os.path.join(test_data_dir, "invalid")
