# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from typing import Any
import pytest
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
from influxdb_client.client.organizations_api import OrganizationsApi
from influxdb_client.client.influxdb_client import InfluxDBClient
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from testcontainers.influxdb2 import InfluxDb2Container

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

import influxdb_v2_to_v3_migration

INFLUXDB_V2_DEFAULT_ORG_NAME: str = "test-org"
INFLUXDB_V2_SECONDARY_ORG_NAME: str = "test-org-two"
DEFAULT_RECORD_NUMBER: int = 10000
DEFAULT_MEASUREMENT_NAME: str = "testMeasurement"


class V2toV2MigrationTestCase(unittest.TestCase):
    """
    Tests for the influxdb_v2_to_v3_migration.py script.

    This test suite verifies migrations end to end, from InfluxDB v2 to v2:.
    """

    source_container: InfluxDb2Container
    source_token: str = "test-token"
    source_url: str = "http://localhost:8087"

    influxdb_v2_bucket_name_prefix = "v2-to-v2-bucket-"

    destination_container: InfluxDb2Container
    destination_token: str = "test-token"
    destination_url: str = "http://localhost:8088"
    destination_org_name: str = "test-organization"

    backup_path: tempfile.TemporaryDirectory

    tokens_secret_name: str = "v2-to-v2-migration-automation-integration-test-secret"

    session: boto3.Session
    secrets_manager_client: Any

    suppress_teardown_warnings: bool

    @classmethod
    def setUpClass(cls):
        """
        Overrides unittest.TestCase.setUpClass, called once before any
        tests in the class.
        """
        # Source setup.
        cls.source_container: InfluxDb2Container = (
            InfluxDb2Container(
                "influxdb:2.7",
                container_port=8086,
                host_port=8087,
                init_mode="setup",
                username="root",
                password="test-password",
                org_name=INFLUXDB_V2_DEFAULT_ORG_NAME,
                bucket="test-bucket",
                admin_token=cls.source_token,
            )
            .waiting_for(LogMessageWaitStrategy(re.compile(r".*msg=Listening.*")))
            .start()
        )

        with InfluxDBClient(
            url="http://localhost:8087",
            token=cls.source_token,
            org=INFLUXDB_V2_DEFAULT_ORG_NAME,
        ) as influxdb_v2_client:
            if not influxdb_v2_client.ping():
                raise ConnectionError(
                    f"setUpClass: Failed to connect to InfluxDB v2 at {cls.source_url}"
                )

            orgs_api: OrganizationsApi = influxdb_v2_client.organizations_api()
            _ = orgs_api.create_organization(name=INFLUXDB_V2_SECONDARY_ORG_NAME)

        cls.destination_container: InfluxDb2Container = (
            InfluxDb2Container(
                "influxdb:2.7",
                container_port=8086,
                host_port=8088,
                init_mode="setup",
                username="root",
                password="test-password",
                org_name=cls.destination_org_name,
                bucket="test-bucket",
                admin_token=cls.destination_token,
            )
            .waiting_for(LogMessageWaitStrategy(re.compile(r".*msg=Listening.*")))
            .start()
        )

        with InfluxDBClient(
            url="http://localhost:8088",
            token=cls.destination_token,
            org=cls.destination_org_name,
        ) as influxdb_v2_client:
            if not influxdb_v2_client.ping():
                raise ConnectionError(
                    f"setUpClass: Failed to connect to InfluxDB v2 at {cls.destination_url}"
                )

        cls.session = boto3.Session()
        cls.secrets_manager_client = cls.session.client("secretsmanager")
        cls.secrets_manager_client.create_secret(
            Name=cls.tokens_secret_name,
            SecretString=json.dumps(
                {
                    "SOURCE_TOKEN": cls.source_token,
                    "DESTINATION_TOKEN": cls.destination_token,
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
            cls.source_container.stop(force=True, delete_volume=True)
        except Exception as e:
            if not cls.suppress_teardown_warnings:
                logging.warning(
                    f"tearDownClass: Failed to delete InfluxDB v2 container: {e}"
                )

        try:
            cls.destination_container.stop(force=True, delete_volume=True)
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
                    url=self.source_url, token=self.source_token, org=org_name
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

        for bucket_name, _ in self.bucket_org_pairs:
            try:
                with InfluxDBClient(
                    url=self.destination_url,
                    token=self.destination_token,
                    org=self.destination_org_name,
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
            url=self.source_url, token=self.source_token, org=org_name
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
        self,
        url: str,
        token: str,
        bucket_name: str,
        org_name: str = INFLUXDB_V2_DEFAULT_ORG_NAME,
    ) -> int:
        """
        Queries the count of points in a bucket.

        Args:
            url (str): The URL to use for queries, including scheme and port.
            token (str): The InfluxDB v2 or v3 token to use for the query.
            bucket_name (str): Name of the bucket to query.
            org_name (str): The name of the organization in which the bucket belongs.

        Returns:
            int: The count of points.
        """
        with InfluxDBClient(url=url, token=token, org=org_name) as client:
            query_api = client.query_api()
            query = f"""
            from(bucket: "{bucket_name}")
              |> range(start: 0)
            """

            print("Executing InfluxDB v2 validation query")
            query_result = query_api.query(query)
            return len(query_result.to_values())

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
                "--source-url",
                self.source_url,
                "--destination-url",
                self.destination_url,
                "--destination-org",
                self.destination_org_name,
                "--source-buckets-and-orgs",
                f"{self.bucket_org_pairs[0][0]}:{INFLUXDB_V2_DEFAULT_ORG_NAME},{self.bucket_org_pairs[1][0]}:{INFLUXDB_V2_DEFAULT_ORG_NAME}",
                "--tokens-secret-name",
                self.tokens_secret_name,
                "--backup-path-root",
                self.backup_path.name,
            ]
        )

        self.assertEqual(return_code, 0)
        for bucket_name, org_name in self.bucket_org_pairs:
            self.assertEqual(
                self.check_inflxudb_v2_bucket_count(
                    url=self.source_url,
                    token=self.source_token,
                    bucket_name=bucket_name,
                    org_name=org_name,
                ),
                self.check_inflxudb_v2_bucket_count(
                    url=self.destination_url,
                    token=self.destination_token,
                    bucket_name=bucket_name,
                    org_name=self.destination_org_name,
                ),
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
                "--source-url",
                self.source_url,
                "--destination-url",
                self.destination_url,
                "--destination-org",
                self.destination_org_name,
                "--source-buckets-and-orgs",
                f"{self.bucket_org_pairs[0][0]}:{INFLUXDB_V2_DEFAULT_ORG_NAME},{self.bucket_org_pairs[1][0]}:{INFLUXDB_V2_SECONDARY_ORG_NAME}",
                "--tokens-secret-name",
                self.tokens_secret_name,
                "--backup-path-root",
                self.backup_path.name,
            ]
        )

        self.assertEqual(return_code, 0)
        for bucket_name, org_name in self.bucket_org_pairs:
            self.assertEqual(
                self.check_inflxudb_v2_bucket_count(
                    url=self.source_url,
                    token=self.source_token,
                    bucket_name=bucket_name,
                    org_name=org_name,
                ),
                self.check_inflxudb_v2_bucket_count(
                    url=self.destination_url,
                    token=self.destination_token,
                    bucket_name=bucket_name,
                    org_name=self.destination_org_name,
                ),
            )

    def test_migration_two_orgs(self):
        """
        Tests migrating two buckets from different organizations using
        the --source-orgs option.
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
                "--source-url",
                self.source_url,
                "--destination-url",
                self.destination_url,
                "--destination-org",
                self.destination_org_name,
                "--source-orgs",
                f"{INFLUXDB_V2_DEFAULT_ORG_NAME},{INFLUXDB_V2_SECONDARY_ORG_NAME}",
                "--tokens-secret-name",
                self.tokens_secret_name,
                "--backup-path-root",
                self.backup_path.name,
            ]
        )

        self.assertEqual(return_code, 0)
        for bucket_name, org_name in self.bucket_org_pairs:
            self.assertEqual(
                self.check_inflxudb_v2_bucket_count(
                    url=self.source_url,
                    token=self.source_token,
                    bucket_name=bucket_name,
                    org_name=org_name,
                ),
                self.check_inflxudb_v2_bucket_count(
                    url=self.destination_url,
                    token=self.destination_token,
                    bucket_name=bucket_name,
                    org_name=self.destination_org_name,
                ),
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

        # --source-orgs and --source-buckets-and-orgs are mutually exclusive.
        with pytest.raises(SystemExit) as ex:
            influxdb_v2_to_v3_migration.main(
                [
                    "--source-url",
                    self.source_url,
                    "--destination-url",
                    self.destination_url,
                    "--destination-org",
                    self.destination_org_name,
                    "--source-orgs",
                    f"{INFLUXDB_V2_DEFAULT_ORG_NAME},{INFLUXDB_V2_SECONDARY_ORG_NAME}",
                    "--source-buckets-and-orgs",
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
                "--source-url",
                self.source_url,
                "--destination-url",
                self.destination_url,
                "--destination-org",
                self.destination_org_name,
                "--bucket-org-separator",
                "?",
                "--bucket-separator",
                ":",
                "--source-buckets-and-orgs",
                f"{self.bucket_org_pairs[0][0]}?{INFLUXDB_V2_DEFAULT_ORG_NAME}:{self.bucket_org_pairs[1][0]}?{INFLUXDB_V2_DEFAULT_ORG_NAME}",
                "--tokens-secret-name",
                self.tokens_secret_name,
                "--backup-path-root",
                self.backup_path.name,
            ]
        )

        self.assertEqual(return_code, 0)
        for bucket_name, org_name in self.bucket_org_pairs:
            self.assertEqual(
                self.check_inflxudb_v2_bucket_count(
                    url=self.source_url,
                    token=self.source_token,
                    bucket_name=bucket_name,
                    org_name=org_name,
                ),
                self.check_inflxudb_v2_bucket_count(
                    url=self.destination_url,
                    token=self.destination_token,
                    bucket_name=bucket_name,
                    org_name=self.destination_org_name,
                ),
            )


if __name__ == "__main__":
    unittest.main()
