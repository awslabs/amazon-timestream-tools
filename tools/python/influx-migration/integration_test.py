from datetime import datetime
import glob
import os
import shutil
import time
import unittest

import influx_migration
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.util.date_utils_pandas import PandasDateTimeHelper
import influxdb_client.client.util.date_utils as date_utils
import pandas
from pandas import Timedelta

date_utils.date_helper = PandasDateTimeHelper()

class BaseTestCases:

    class BaseTestCase(unittest.TestCase):
        # Invalid/incorrect values
        invalid_token = "invalid_token"
        invalid_host = "invalid_host"
        incorrect_port = "8081"
        incorrect_org = "incorrect-org"

        # Valid values
        src_bucket = "test-bucket"
        dest_bucket = "dest-bucket"
        scheme = "http"
        domain = "127.0.0.1"
        port = "8086"
        test_dir_prefix = "test-influxdb-backup-"
        test_dir_name = ""
        client = None

        def setUp(self):
            """
            Overrides unittest.TestCase.setUp, called before each test runs
            """
            MILLISECOND_TIMEOUT = 10000
            self.client = InfluxDBClient(url=f'{self.scheme}://{self.domain}:{self.port}',
                token="test-token", org="test-org", timeout=MILLISECOND_TIMEOUT)
            os.environ['INFLUX_DEST_TOKEN'] = self.client.token
            os.environ['INFLUX_SRC_TOKEN'] = self.client.token
            self.test_dir_name = self.test_dir_prefix + str(int(time.time() * 1000))
            self.delete_bucket(self.dest_bucket)

        def delete_bucket(self, bucket_name):
            try:
                buckets_api = self.client.buckets_api()
                bucket = buckets_api.find_bucket_by_name(bucket_name)
                if bucket is not None:
                    buckets_api.delete_bucket(bucket)
            except:
                return

        def delete_backup_dir(self):
            """
            Deletes the uniquely-named backup directory used for testing
            """
            try:
                if os.path.exists(self.test_dir_name) and os.path.isdir(self.test_dir_name):
                    shutil.rmtree(path=self.test_dir_name, ignore_errors=True)
            except OSError as error:
                print(str(error))

        def validate_results(self):
            """
            Validates that the inserted data in the destination bucket matches the source bucket using raw queries.
            Not performant for large datasets due to raw query and simple comparison.
            """
            query_api = self.client.query_api()
            dest_data = query_api.query_raw(f'from(bucket: "{self.dest_bucket}")'
                '|> range(start: 1678-01-01T00:00:00Z, stop: 2800-01-01T00:00:00Z)'
                '|> group()'
                '|> sort(columns: ["_time", "_measurement"])')
            src_data = query_api.query_raw(f'from(bucket: "{self.src_bucket}")'
                '|> range(start: 1678-01-01T00:00:00Z, stop: 2800-01-01T00:00:00Z)'
                '|> group()'
                '|> sort(columns: ["_time", "_measurement"])')
            self.assertEqual(src_data.data, dest_data.data)

        def tearDown(self):
            """
            Overrides unittest.TestCase.tearDown, called after each test runs
            """
            self.delete_bucket(self.dest_bucket)
            self.client.close()
            self.delete_backup_dir()

class InvalidAuthTestCase(BaseTestCases.BaseTestCase):
    def test_invalid_src_host(self):
        with self.assertRaises(SystemExit) as error:
            influx_migration.main([
                "--src-host", self.invalid_host,
                "--src-bucket", self.src_bucket,
                "--dest-host", self.client.url,
                "--dest-bucket", self.dest_bucket,
                "--src-org", self.client.org,
                "--dir-name", self.test_dir_name
                ])
        self.assertEqual(error.exception.code, 1)

    def test_invalid_dest_host(self):
        with self.assertRaises(SystemExit) as error:
            influx_migration.main([
                "--src-host", self.client.url,
                "--src-bucket", self.src_bucket,
                "--dest-host", self.invalid_host,
                "--dest-bucket", self.dest_bucket,
                "--src-org", self.client.org,
                "--dir-name", self.test_dir_name
                ])
        self.assertEqual(error.exception.code, 1)

    def test_invalid_src_token(self):
        os.environ['INFLUX_SRC_TOKEN'] = self.invalid_token
        with self.assertRaises(SystemExit) as error:
            influx_migration.main([
                "--src-host", self.client.url,
                "--src-bucket", self.src_bucket,
                "--dest-host", self.client.url,
                "--dest-bucket", self.dest_bucket,
                "--src-org", self.client.org,
                "--dir-name", self.test_dir_name
                ])
        self.assertEqual(error.exception.code, 1)

    def test_invalid_dest_token(self):
        os.environ['INFLUX_DEST_TOKEN'] = self.invalid_token
        with self.assertRaises(SystemExit) as error:
            influx_migration.main([
                "--src-host", self.client.url,
                "--src-bucket", self.src_bucket,
                "--dest-host", self.client.url,
                "--dest-bucket", self.dest_bucket,
                "--src-org", self.client.org,
                "--dir-name", self.test_dir_name
                ])
        self.assertEqual(error.exception.code, 1)

    def test_incorrect_src_port(self):
        with self.assertRaises(SystemExit) as error:
            influx_migration.main([
                "--src-host", f"{self.scheme}://{self.domain}:{self.incorrect_port}",
                "--src-bucket", self.src_bucket,
                "--dest-host", self.client.url,
                "--dest-bucket", self.dest_bucket,
                "--src-org", self.client.org,
                "--dir-name", self.test_dir_name
                ])
        self.assertEqual(error.exception.code, 1)

    def test_incorrect_dest_port(self):
        with self.assertRaises(SystemExit) as error:
            influx_migration.main([
                "--src-host", self.client.url,
                "--src-bucket", self.src_bucket,
                "--dest-host", f"{self.scheme}://{self.domain}:{self.incorrect_port}",
                "--dest-bucket", self.dest_bucket,
                "--src-org", self.client.org,
                "--dir-name", self.test_dir_name
                ])
        self.assertEqual(error.exception.code, 1)

    def test_missing_src_token(self):
        os.environ.pop('INFLUX_SRC_TOKEN')
        with self.assertRaises(SystemExit) as error:
            influx_migration.main([
                "--src-host", self.client.url,
                "--src-bucket", self.src_bucket,
                "--dest-host", self.client.url,
                "--dest-bucket", self.dest_bucket,
                "--src-org", self.client.org,
                "--dir-name", self.test_dir_name
                ])
        self.assertEqual(error.exception.code, 1)

    def test_missing_dest_token(self):
        os.environ.pop('INFLUX_DEST_TOKEN')
        with self.assertRaises(SystemExit) as error:
            influx_migration.main([
                "--src-host", self.client.url,
                "--src-bucket", self.src_bucket,
                "--dest-host", self.client.url,
                "--dest-bucket", self.dest_bucket,
                "--src-org", self.client.org,
                "--dir-name", self.test_dir_name
                ])
        self.assertEqual(error.exception.code, 1)

    def test_incorrect_src_org(self):
        with self.assertRaises(SystemExit) as error:
            influx_migration.main([
                "--src-host", self.client.url,
                "--src-bucket", self.src_bucket,
                "--dest-host", self.client.url,
                "--dest-bucket", self.dest_bucket,
                "--src-org", self.incorrect_org,
                "--dir-name", self.test_dir_name
                ])
        self.assertEqual(error.exception.code, 1)

    def test_incorrect_dest_org(self):
        with self.assertRaises(SystemExit) as error:
            influx_migration.main([
                "--src-host", self.client.url,
                "--src-bucket", self.src_bucket,
                "--dest-host", self.client.url,
                "--dest-bucket", self.dest_bucket,
                "--src-org", self.client.org,
                "--dest-org", self.incorrect_org,
                "--dir-name", self.test_dir_name
                ])
        self.assertEqual(error.exception.code, 1)

class ValidAuthTestCase(BaseTestCases.BaseTestCase):
    def test_valid_auth(self):
        # If this fails it will throw an exception and fail the test
        influx_migration.main([
            "--src-host", self.client.url,
            "--src-bucket", self.src_bucket,
            "--dest-host", self.client.url,
            "--dest-bucket", self.dest_bucket,
            "--src-org", self.client.org,
            "--dir-name", self.test_dir_name
            ])
        self.validate_results()

    def test_valid_auth_csv(self):
        # If this fails it will throw an exception and fail the test
        influx_migration.main([
            "--src-host", self.client.url,
            "--src-bucket", self.src_bucket,
            "--dest-host", self.client.url,
            "--dest-bucket", self.dest_bucket,
            "--src-org", self.client.org,
            "--dir-name", self.test_dir_name,
            "--csv"
            ])
        self.validate_results()

    def test_csv_with_gap_ns(self):
        """
        Tests an ingested dataset where one table has all of its
        data in the 1990s and another table has nearly half of its
        data in the 1990s and the rest in 2023, a large time gap.
        The time between points for this test is one nanosecond.
        """
        self.src_bucket = "gap-test"
        self.delete_bucket(self.src_bucket)
        buckets_api = self.client.buckets_api()
        buckets_api.create_bucket(bucket_name=self.src_bucket, org=self.client.org)
        writer = self.client.write_api()
        early_start = pandas.Timestamp("1990-01-01T00:00:00Z")
        late_start = pandas.Timestamp("2020-01-01T00:00:00Z")

        # Table with dates all in the 90s
        for i in range(100):
            point = Point.from_dict({
                "measurement": "pressure",
                "tags": {},
                "fields": {
                    "old-table-field": i
                },
                "time": early_start + Timedelta(i, 'ns')
            }, WritePrecision.NS)
            writer.write(bucket=self.src_bucket, record=point.to_line_protocol(), org=self.client.org)

        # Table with mixed dates
        for i in range(49):
            point = Point.from_dict({
                "measurement": "pressure",
                "tags": {},
                "fields": {
                    "mixed-table-field": i
                },
                "time": early_start + Timedelta(i, 'ns')
            }, WritePrecision.NS)
            writer.write(bucket=self.src_bucket, record=point.to_line_protocol(), org=self.client.org)
        for i in range(51):
            point = Point.from_dict({
                "measurement": "pressure",
                "tags": {},
                "fields": {
                    "mixed-table-field": i
                },
                "time": late_start + Timedelta(i, 'ns')
            }, WritePrecision.NS)
            writer.write(bucket=self.src_bucket, record=point.to_line_protocol(), org=self.client.org)
        writer.close()

        influx_migration.main([
            "--src-host", self.client.url,
            "--src-bucket", self.src_bucket,
            "--dest-host", self.client.url,
            "--dest-bucket", self.dest_bucket,
            "--src-org", self.client.org,
            "--dir-name", self.test_dir_name,
            "--csv"
            ])
        self.validate_results()
        self.delete_bucket(self.src_bucket)

    def test_csv_with_gap_m(self):
        """
        Tests an ingested dataset where one table has all of its
        data in the 1990s and another table has nearly half of its
        data in the 1990s and the rest in 2023, a large time gap.
        The time between points for this test is one minute.
        """
        self.src_bucket = "gap-test"
        self.delete_bucket(self.src_bucket)
        self.client.buckets_api().create_bucket(bucket_name=self.src_bucket)
        writer = self.client.write_api()
        early_start = pandas.Timestamp("1990-01-01T00:00:00Z")
        late_start = pandas.Timestamp("2020-01-01T00:00:00Z")

        # Table with dates all in the 90s
        for i in range(100):
            point = Point.from_dict({
                "measurement": "pressure",
                "tags": {},
                "fields": {
                    "old-table-field": i
                },
                "time": early_start + Timedelta(i, 'm')
            }, WritePrecision.NS)
            writer.write(bucket=self.src_bucket, record=point)

        # Table with mixed dates
        for i in range(49):
            point = Point.from_dict({
                "measurement": "pressure",
                "tags": {},
                "fields": {
                    "mixed-table-field": i
                },
                "time": early_start + Timedelta(i, 'm')
            }, WritePrecision.NS)
            writer.write(bucket=self.src_bucket, record=point)
        for i in range(51):
            point = Point.from_dict({
                "measurement": "pressure",
                "tags": {},
                "fields": {
                    "mixed-table-field": i
                },
                "time": late_start + Timedelta(i, 'm')
            }, WritePrecision.NS)
            writer.write(bucket=self.src_bucket, record=point)
        writer.close()

        influx_migration.main([
            "--src-host", self.client.url,
            "--src-bucket", self.src_bucket,
            "--dest-host", self.client.url,
            "--dest-bucket", self.dest_bucket,
            "--src-org", self.client.org,
            "--dir-name", self.test_dir_name,
            "--csv"
            ])
        self.validate_results()
        self.delete_bucket(self.src_bucket)

    def test_csv_multiple_fields(self):
        """
        Tests csv migration using a dataset with multiple fields.
        """
        self.src_bucket = "multiple-field-test"
        self.delete_bucket(self.src_bucket)
        self.client.buckets_api().create_bucket(bucket_name=self.src_bucket)
        writer = self.client.write_api()
        start_timestamp = pandas.Timestamp("2021-11-09T14:03:20Z")

        # Four fields means a total of 400 records will be inserted
        for i in range(100):
            point = Point.from_dict({
                "measurement": "pressure",
                "tags": {},
                "fields": {
                    "field1": i,
                    "field2": i * 2,
                    "field3": i * 3,
                    "field4": i * 4
                },
                "time": start_timestamp + Timedelta(i, 'm')
            }, WritePrecision.NS)
            writer.write(bucket=self.src_bucket, record=point)
        writer.close()

        influx_migration.main([
            "--src-host", self.client.url,
            "--src-bucket", self.src_bucket,
            "--dest-host", self.client.url,
            "--dest-bucket", self.dest_bucket,
            "--src-org", self.client.org,
            "--dir-name", self.test_dir_name,
            "--csv"
            ])
        self.validate_results()
        self.delete_bucket(self.src_bucket)

if __name__ == "__main__":
    unittest.main()
