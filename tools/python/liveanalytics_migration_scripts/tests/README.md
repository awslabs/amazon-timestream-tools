# Tests

## Integration Tests

The directory `integration` contains integration tests.

### Prerequisites

1. AWS CLI configured with appropriate permissions.
2. Docker.
3. Python 3.12+.
4. Required Python packages (see [test-requirements.txt](test-requirements.txt)).

## Installation

Create a virtual environment using `venv` and install required dependencies:

```shell
python3 -m venv .env && \
source .env/bin/activate && \
python3 -m pip install -r test-requirements.txt
```

### Common Tests

`test_common.py` contains integration tests for scripts that are independent of the migration target, such as `unload.py`. These tests mainly integrate with Timestream for LiveAnalytics.

### InfluxDB V2 End-to-End Tests

`test_influxdb_v2_target.py` contains end-to-end integration tests for migrating from Timestream for LiveAnalytics to InfluxDB.

This test case will create an InfluxDB v2 Docker container on http://localhost:8087, configure it, and delete it once all tests have finished. Make sure Docker is running and ports are available

If you want to use a different port for the InfluxDB v2 Docker container, the environment variable `TEST_INFLUXDB_HOST_PORT` can be used to set the port that the container uses. For example:

```shell
export TEST_INFLUXDB_HOST_PORT=8085
```

### InfluxDB V2 Ingestion Tests

`test_ingestion.py` tests the `influxdb_ingestion.py` script, which ingests `.gz` line protocol files to InfluxDB.

The ingestion tests, like the InfluxDB v2 target tests, use an InfluxDB v2 Docker container.

### Running All Tests

Assuming you have satisfied the prerequisites for all integration tests, such as having installed all packages from `../requirements.txt`, all tests can be run with the following command:

```
python3 -m pytest .
```

### Running a Specific Test

A specific test can be run with the following command, replacing `<test file name>` with the name of the test file that you want to run, `<test case name>` with the name of the test case name that you want to run (such as `InfluxDBV2TestCase`), and `<test name>` with the name of the test that you want to run:

```
python3 -m pytest <test file name>.py::<test case name>::<test name>
```

### Reducing Test Verbosity

A `pytest.ini` file is provided with some default configurations. By default, tests show `INFO` logs and output from print statements. To disable this, use the following command, replacing `<test file name>` with the name of the test file that you want to run:

```
python3 -m pytest <test file name>.py \
    --override-ini="log_cli=false" \
    --override-ini="addopts="
```
