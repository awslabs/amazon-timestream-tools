import pytest
from datetime import datetime, timezone
import json
import os
import re
import time
import unittest
import random
import string
import sys
import tempfile
import logging

import boto3
import httpx
from influxdb_client.client.organizations_api import OrganizationsApi
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client_3 import InfluxDBClient3
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.influxdb2 import InfluxDb2Container
from testcontainers.core.container import DockerContainer, ExecResult

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import influxdb_v2_to_v3_migration

INFLUXDB_V2_DEFAULT_ORG_NAME: str = "test-org"
INFLUXDB_V2_SECONDARY_ORG_NAME: str = "test-org-two"
DEFAULT_RECORD_NUMBER: int = 10000
DEFAULT_MEASUREMENT_NAME: str = "testMeasurement"


class MigrationTestCase(unittest.TestCase):
    """
    Tests for the influxdb_v2_to_v3_migration.py script.

    This test suite verifies migrations end to end, from InfluxDB v2 to v3.
    """

    influxdb_v2_container: InfluxDb2Container
    influxdb_v2_token: str = "test-token"
    influxdb_v2_url: str = "http://localhost:8087"

    influxdb_v2_bucket_name_prefix = "v2-to-v3-bucket-"

    influxdb_v3_container: DockerContainer
    influxdb_v3_token: str
    influxdb_v3_url: str = "http://localhost:8183"

    backup_path: tempfile.TemporaryDirectory

    tokens_secret_name: str = "v2-to-v3-integration-test-secret"

    session: boto3.Session
    secrets_manager_client

    suppress_teardown_warnings: bool

    @classmethod
    def setUpClass(cls):
        """
        Overrides unittest.TestCase.setUpClass, called once before any
        tests in the class.
        """
        # InfluxDB v2 setup.
        cls.influxdb_v2_container: InfluxDb2Container = (
            InfluxDb2Container(
                "influxdb:2.7",
                container_port=8086,
                host_port=8087,
                init_mode="setup",
                username="root",
                password="test-password",
                org_name=INFLUXDB_V2_DEFAULT_ORG_NAME,
                bucket="test-bucket",
                admin_token=cls.influxdb_v2_token,
            )
            .waiting_for(LogMessageWaitStrategy(re.compile(r".*msg=Listening.*")))
            .start()
        )

        with InfluxDBClient(
            url="http://localhost:8087",
            token=cls.influxdb_v2_token,
            org=INFLUXDB_V2_DEFAULT_ORG_NAME,
        ) as influxdb_v2_client:
            influxdb_v2_health_check: bool = influxdb_v2_client.ping()
            if not influxdb_v2_health_check:
                raise ConnectionError(
                    f"setUpClass: Failed to connect to InfluxDB v2 at {cls.influxdb_v2_url}"
                )

            orgs_api: OrganizationsApi = influxdb_v2_client.organizations_api()
            _ = orgs_api.create_organization(name=INFLUXDB_V2_SECONDARY_ORG_NAME)

        # InfluxDB v3 core setup.
        cls.influxdb_v3_container: DockerContainer = (
            DockerContainer(
                image="influxdb:3-core",
                ports=[8181],
                command="influxdb3 serve --node-id=my-node-0 --object-store=file --data-dir=/var/lib/influxdb3/data",
            )
            .waiting_for(LogMessageWaitStrategy(re.compile(".*startup time.*")))
            .with_bind_ports(container="8181/tcp", host=8183)
            .start()
        )

        token_creation_result: ExecResult = cls.influxdb_v3_container.exec(
            ["influxdb3", "create", "token", "--admin"]
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
        cls.influxdb_v3_token = token_regex_search.group(1)

        database_creation_result = cls.influxdb_v3_container.exec(
            [
                "influxdb3",
                "create",
                "database",
                "--token",
                cls.influxdb_v3_token,
                "test-database",
            ]
        )
        if database_creation_result.exit_code != 0:
            raise ConnectionError(
                f"setUpClass: Failed to create InfluxDB v3 at {cls.influxdb_v3_url}"
            )

        with InfluxDBClient3(
            host=cls.influxdb_v3_url,
            token=cls.influxdb_v3_token,
            database="test-database",
        ) as influxdb_v3_client:
            influxdb_v3_health_check: str = influxdb_v3_client.get_server_version()
            if not influxdb_v3_health_check:
                raise ConnectionError(
                    f"setUpClass: Failed to connect to InfluxDB v3 at {cls.influxdb_v3_url}"
                )

        cls.session = boto3.Session()
        cls.secrets_manager_client = cls.session.client("secretsmanager")
        cls.secrets_manager_client.create_secret(
            Name=cls.tokens_secret_name,
            SecretString=json.dumps(
                {
                    "INFLUXDB_V2_TOKEN": cls.influxdb_v2_token,
                    "INFLUXDB_V3_TOKEN": cls.influxdb_v3_token,
                }
            ),
        )

    def setUp(self):
        """
        Overrides unittest.TestCase.setUp, called before each test runs.
        """
        self.backup_path = tempfile.TemporaryDirectory(
            suffix=self.get_random_string(10)
        )
        influxdb_v2_bucket_name = (
            self.influxdb_v2_bucket_name_prefix + self.get_random_string(10)
        )
        self.create_and_fill_bucket(influxdb_v2_bucket_name)
        self.bucket_org_pairs: list[tuple[str, str]] = list()
        self.bucket_org_pairs.append(
            (influxdb_v2_bucket_name, INFLUXDB_V2_DEFAULT_ORG_NAME)
        )
        self.suppress_teardown_warnings = False

    @classmethod
    def tearDownClass(cls):
        """
        Overrides unittest.TestCase.tearDownClass, called after all tests have finished.
        """
        try:
            cls.influxdb_v2_container.stop(force=True, delete_volume=True)
        except Exception as e:
            if not cls.suppress_teardown_warnings:
                logging.warning(
                    f"tearDownClass: Failed to delete InfluxDB v2 container: {e}"
                )

        try:
            cls.influxdb_v3_container.stop(force=True, delete_volume=True)
        except Exception as e:
            if not cls.suppress_teardown_warnings:
                logging.warning(
                    f"tearDownClass: Failed to delete InfluxDB v3 container: {e}"
                )

        try:
            cls.secrets_manager_client.delete_secret(
                SecretId=cls.tokens_secret_name, ForceDeleteWithoutRecovery=True
            )
        except Exception as e:
            if not cls.suppress_teardown_warnings:
                logging.warning(
                    f"tearDownClass: Failed to delete {cls.tokens_secret_name} secret: {e}"
                )

    def tearDown(self):
        """
        Overrides unittest.TestCase.tearDown, called after each test runs.
        """
        self.backup_path.cleanup()
        for bucket_name, org_name in self.bucket_org_pairs:
            try:
                with InfluxDBClient(
                    url=self.influxdb_v2_url, token=self.influxdb_v2_token, org=org_name
                ) as influxdb_v2_client:
                    influxdb_bucket = (
                        influxdb_v2_client.buckets_api().find_bucket_by_name(
                            bucket_name
                        )
                    )
                    if not influxdb_bucket:
                        raise RuntimeError(
                            f"tearDown: Failed to find bucket {bucket_name}"
                        )
                    influxdb_v2_client.buckets_api().delete_bucket(influxdb_bucket)
            except Exception as e:
                if not self.suppress_teardown_warnings:
                    logging.warning(
                        f"tearDown: Failed to delete InfluxDB v2 bucket: {e}"
                    )

            try:
                deletion_date = datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                deletion_response = httpx.delete(
                    url=f"{self.influxdb_v3_url}/api/v3/configure/database",
                    headers={"Authorization": f"Bearer {self.influxdb_v3_token}"},
                    params={
                        "db": bucket_name.replace("_", "-"),
                        "hard_delete_at": deletion_date,
                    },
                )
                _ = deletion_response.raise_for_status()
            except Exception as e:
                if not self.suppress_teardown_warnings:
                    logging.warning(
                        f"tearDown: Failed to delete InfluxDB v3 database: {e}"
                    )

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

    def create_and_fill_bucket(
        self, bucket_name: str, org_name: str = INFLUXDB_V2_DEFAULT_ORG_NAME
    ):
        """
        Creates an InfluxDB v2 bucket with the given name and fills it with test data.

        Args:
            bucket_name (str): The name of the bucket to create.
            org_name (str): The organization name in which to create the bucket.

        Returns:
            None
        """
        with InfluxDBClient(
            url=self.influxdb_v2_url, token=self.influxdb_v2_token, org=org_name
        ) as influxdb_v2_client:
            buckets_api = influxdb_v2_client.buckets_api()

            try:
                buckets_api.create_bucket(bucket_name=bucket_name, org=org_name)
                print(f"Created bucket: {bucket_name} in org {org_name}")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"Bucket {bucket_name} already exists")
                else:
                    raise

            write_api = influxdb_v2_client.write_api()
            print("Loading test data")
            for _ in range(DEFAULT_RECORD_NUMBER):
                record = f"{DEFAULT_MEASUREMENT_NAME},tag1={self.get_random_string(9)} field1={random.randint(0, 300)}i {time.time_ns()}\n"
                write_api.write(record=record, bucket=bucket_name, org=org_name)

    def check_inflxudb_v2_bucket_count(
        self, bucket_name: str, org_name: str = INFLUXDB_V2_DEFAULT_ORG_NAME
    ) -> int:
        """
        Queries the count of points in a bucket.

        Args:
            bucket_name (str): Name of the bucket to query.
            org_name (str): The name of the organization in which the bucket belongs.

        Returns:
            int: The count of points.
        """
        with InfluxDBClient(
            url=self.influxdb_v2_url, token=self.influxdb_v2_token, org=org_name
        ) as client:
            query_api = client.query_api()
            query = f"""
            from(bucket: "{bucket_name}")
              |> range(start: 0)
            """

            print("Executing InfluxDB v2 validation query")
            query_result = query_api.query(query)
            assert len(query_result) > 0, "Query returned no results"
            return len(query_result.to_values())

    def check_influxdb_v3_table_count(
        self, database_name: str, table_name: str = DEFAULT_MEASUREMENT_NAME
    ) -> int:
        """
        Queries the number of records in an InfluxDB v3 table.

        Args:
            database_name (str): The name of the database in which the table resides.
            table_name (str): The table name to query.

        Returns:
            int: The number of records in the table.
        """
        with InfluxDBClient3(
            host=self.influxdb_v3_url,
            token=self.influxdb_v3_token,
            database=database_name,
        ) as influxdb_v3_client:
            query_str = f'SELECT COUNT(*) AS row_count FROM "{table_name}"'
            print("Executing InfluxDB v3 validation query")
            results = influxdb_v3_client.query(query_str)
            return results.column("row_count")[0].as_py()

    def test_migration_basic(self):
        """
        Tests basic migration of a single bucket.
        """
        return_code = influxdb_v2_to_v3_migration.main(
            [
                "--influxdb-v2-url",
                self.influxdb_v2_url,
                "--influxdb-v3-url",
                self.influxdb_v3_url,
                "--influxdb-v2-buckets-and-orgs",
                f"{self.bucket_org_pairs[0][0]}:{INFLUXDB_V2_DEFAULT_ORG_NAME}",
                "--tokens-secret-name",
                self.tokens_secret_name,
                "--backup-path-root",
                self.backup_path.name,
            ]
        )

        database_name = self.bucket_org_pairs[0][0].replace("_", "-")

        self.assertEqual(return_code, 0)
        self.assertEqual(
            self.check_inflxudb_v2_bucket_count(
                bucket_name=self.bucket_org_pairs[0][0]
            ),
            self.check_influxdb_v3_table_count(database_name=database_name),
        )

    def test_migration_two_buckets_same_org(self):
        """
        Tests migrating two buckets from the same organization.
        """
        secondary_bucket_name: str = (
            self.influxdb_v2_bucket_name_prefix + self.get_random_string(10)
        )
        self.create_and_fill_bucket(secondary_bucket_name)
        self.bucket_org_pairs.append(
            (secondary_bucket_name, INFLUXDB_V2_DEFAULT_ORG_NAME)
        )

        return_code = influxdb_v2_to_v3_migration.main(
            [
                "--influxdb-v2-url",
                self.influxdb_v2_url,
                "--influxdb-v3-url",
                self.influxdb_v3_url,
                "--influxdb-v2-buckets-and-orgs",
                f"{self.bucket_org_pairs[0][0]}:{INFLUXDB_V2_DEFAULT_ORG_NAME},{self.bucket_org_pairs[1][0]}:{INFLUXDB_V2_DEFAULT_ORG_NAME}",
                "--tokens-secret-name",
                self.tokens_secret_name,
                "--backup-path-root",
                self.backup_path.name,
            ]
        )

        self.assertEqual(return_code, 0)
        for bucket_name, org_name in self.bucket_org_pairs:
            database_name = bucket_name.replace("_", "-")
            self.assertEqual(
                self.check_inflxudb_v2_bucket_count(bucket_name, org_name),
                self.check_influxdb_v3_table_count(database_name),
            )

    def test_migration_two_buckets_different_orgs(self):
        """
        Tests migrating two buckets from different organizations.
        """
        secondary_bucket_name: str = (
            self.influxdb_v2_bucket_name_prefix + self.get_random_string(10)
        )
        self.create_and_fill_bucket(
            secondary_bucket_name, INFLUXDB_V2_SECONDARY_ORG_NAME
        )
        self.bucket_org_pairs.append(
            (secondary_bucket_name, INFLUXDB_V2_SECONDARY_ORG_NAME)
        )

        return_code = influxdb_v2_to_v3_migration.main(
            [
                "--influxdb-v2-url",
                self.influxdb_v2_url,
                "--influxdb-v3-url",
                self.influxdb_v3_url,
                "--influxdb-v2-buckets-and-orgs",
                f"{self.bucket_org_pairs[0][0]}:{INFLUXDB_V2_DEFAULT_ORG_NAME},{self.bucket_org_pairs[1][0]}:{INFLUXDB_V2_SECONDARY_ORG_NAME}",
                "--tokens-secret-name",
                self.tokens_secret_name,
                "--backup-path-root",
                self.backup_path.name,
            ]
        )

        self.assertEqual(return_code, 0)
        for bucket_name, org_name in self.bucket_org_pairs:
            database_name = bucket_name.replace("_", "-")
            self.assertEqual(
                self.check_inflxudb_v2_bucket_count(bucket_name, org_name),
                self.check_influxdb_v3_table_count(database_name),
            )

    def test_migration_two_orgs(self):
        """
        Tests migrating two buckets from different organizations using
        the --influxdb-v2-orgs option.
        """
        secondary_bucket_name: str = (
            self.influxdb_v2_bucket_name_prefix + self.get_random_string(10)
        )
        self.create_and_fill_bucket(
            secondary_bucket_name, INFLUXDB_V2_SECONDARY_ORG_NAME
        )
        self.bucket_org_pairs.append(
            (secondary_bucket_name, INFLUXDB_V2_SECONDARY_ORG_NAME)
        )

        return_code = influxdb_v2_to_v3_migration.main(
            [
                "--influxdb-v2-url",
                self.influxdb_v2_url,
                "--influxdb-v3-url",
                self.influxdb_v3_url,
                "--influxdb-v2-orgs",
                f"{INFLUXDB_V2_DEFAULT_ORG_NAME},{INFLUXDB_V2_SECONDARY_ORG_NAME}",
                "--tokens-secret-name",
                self.tokens_secret_name,
                "--backup-path-root",
                self.backup_path.name,
            ]
        )

        self.assertEqual(return_code, 0)
        for bucket_name, org_name in self.bucket_org_pairs:
            database_name = bucket_name.replace("_", "-")
            self.assertEqual(
                self.check_inflxudb_v2_bucket_count(bucket_name, org_name),
                self.check_influxdb_v3_table_count(database_name),
            )

    def test_migration_mutually_exclusive_arguments(self):
        """
        Tests setting arguments that are mutually exclusive.
        """
        secondary_bucket_name: str = (
            self.influxdb_v2_bucket_name_prefix + self.get_random_string(10)
        )
        self.create_and_fill_bucket(
            secondary_bucket_name, INFLUXDB_V2_SECONDARY_ORG_NAME
        )
        self.bucket_org_pairs.append(
            (secondary_bucket_name, INFLUXDB_V2_SECONDARY_ORG_NAME)
        )

        # --influxdb-v2-orgs and --influxdb-v2-buckets-and-orgs are mutually exclusive.
        with pytest.raises(SystemExit) as ex:
            influxdb_v2_to_v3_migration.main(
                [
                    "--influxdb-v2-url",
                    self.influxdb_v2_url,
                    "--influxdb-v3-url",
                    self.influxdb_v3_url,
                    "--influxdb-v2-orgs",
                    f"{INFLUXDB_V2_DEFAULT_ORG_NAME},{INFLUXDB_V2_SECONDARY_ORG_NAME}",
                    "--influxdb-v2-buckets-and-orgs",
                    f"{self.bucket_org_pairs[0][0]}:{INFLUXDB_V2_DEFAULT_ORG_NAME},{self.bucket_org_pairs[1][0]}:{INFLUXDB_V2_SECONDARY_ORG_NAME}",
                    "--tokens-secret-name",
                    self.tokens_secret_name,
                    "--backup-path-root",
                    self.backup_path.name,
                ]
            )

        self.assertTrue(ex.errisinstance(SystemExit))
        self.suppress_teardown_warnings = True

    def test_migration_custom_separators(self):
        """
        Tests migrating with custom separators.
        """
        secondary_bucket_name: str = (
            self.influxdb_v2_bucket_name_prefix + self.get_random_string(10)
        )
        self.create_and_fill_bucket(secondary_bucket_name)
        self.bucket_org_pairs.append(
            (secondary_bucket_name, INFLUXDB_V2_DEFAULT_ORG_NAME)
        )

        return_code = influxdb_v2_to_v3_migration.main(
            [
                "--influxdb-v2-url",
                self.influxdb_v2_url,
                "--influxdb-v3-url",
                self.influxdb_v3_url,
                "--bucket-org-separator",
                "?",
                "--bucket-separator",
                ":",
                "--influxdb-v2-buckets-and-orgs",
                f"{self.bucket_org_pairs[0][0]}?{INFLUXDB_V2_DEFAULT_ORG_NAME}:{self.bucket_org_pairs[1][0]}?{INFLUXDB_V2_DEFAULT_ORG_NAME}",
                "--tokens-secret-name",
                self.tokens_secret_name,
                "--backup-path-root",
                self.backup_path.name,
            ]
        )

        self.assertEqual(return_code, 0)
        for bucket_name, org_name in self.bucket_org_pairs:
            database_name = bucket_name.replace("_", "-")
            self.assertEqual(
                self.check_inflxudb_v2_bucket_count(bucket_name, org_name),
                self.check_influxdb_v3_table_count(database_name),
            )


if __name__ == "__main__":
    unittest.main()
