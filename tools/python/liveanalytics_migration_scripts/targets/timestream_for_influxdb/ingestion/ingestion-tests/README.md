# InfluxDB Ingestion Tests

Test suite for the `influxdb_ingestion.py` script, which ingests `.gz` line protocol files to InfluxDB.

## Test Structure

- `docker-compose.yml`: Defines the InfluxDB V2 container for testing
- `conftest.py`: Contains pytest fixtures for setting up and tearing down test environment
- `ingestion_test.py`: Contains the test cases
- `test-data/`: Contains test data files
  - `valid/`: Valid line protocol files for testing successful ingestion
  - `invalid/`: Invalid line protocol files for testing error handling
    - `test-data/invalid/influxdb_data_05.gz` is malformed in the last line and contains 5000000 lines in total

## Running Tests

### Prerequisites

- Docker and Docker Compose
- Python 3.7+
- Required Python packages (install with `pip install -r requirements.txt`)

### Installation Notes

```bash
pip install --no-build-isolation -r requirements.txt
```

### Running All Tests

```bash
cd ingestion-tests
pytest -v
```

### Running Specific Tests

```bash
# Run a specific test
pytest -v ingestion_test.py::TestInfluxDBIngestion::test_valid_dataset_ingestion
```

## Test Environment

The tests use Docker Compose to create an InfluxDB V2 container with the following configuration:

- URL: http://localhost:8086
- Organization: testorg
- Username: testuser
- Password: testpassword
- Token: testtokenstring
- Default bucket: testbucket
- Test buckets: source_bucket, destination_bucket, valid-01

## Troubleshooting

- If tests fail with connection errors, make sure Docker is running and ports are available
- Check Docker logs with `docker-compose logs influxdb_test`
