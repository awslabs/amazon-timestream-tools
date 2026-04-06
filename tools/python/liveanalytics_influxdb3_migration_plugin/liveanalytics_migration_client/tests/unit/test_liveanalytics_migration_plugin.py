# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import unittest
import os
import sys
import unittest

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
)

from liveanalytics_migration_plugin.liveanalytics_migration_plugin import (
    HttpStatus,
    create_http_response,
)


class PluginTestCase(unittest.TestCase):
    def test_create_http_response_with_token(self):
        """
        Tests creating an HTTP response where an InfluxDB v3 API token is included in the message.
        """
        token = "apiv3_SomeTokenTextThatIsSensitive"
        message = f"The request was successful. This is your token: {token}"
        http_response = create_http_response(HttpStatus.OK, message)
        self.assertEqual(
            http_response["message"],
            "The request was successful. This is your token: *****",
        )

    def test_create_http_response_with_presigned_https_url(self):
        """
        Tests creating an HTTP response where a presigned HTTPS URL is included in the message.
        """
        presigned_url = (
            "https://example.com/someDB/someTable/"
            "results/partition_date%3D23-04-18/data.parquet/done.ack?"
            "AWSAccessKeyId=test_access_key_id"
            "&Signature=test_signature"
            "&x-amz-security-token=test_security_token"
            "&Expires=1770931001"
        )
        message = (
            "2026-02-05T21:17:04.215743Z ERROR "
            "influxdb3_py_api::system_py: processing engine: "
            "Error putting done file for someDB/someTable/results"
            "/partition_date=23-04-18/data.parquet: "
            "403 Client Error: Forbidden for url:"
        )
        http_response = create_http_response(
            HttpStatus.INTERNAL_ERROR, f"{message} {presigned_url}"
        )
        self.assertEqual(http_response["message"], f"{message} *****")

    def test_create_http_response_with_presigned_http_url(self):
        """
        Tests creating an HTTP response where a presigned HTTP URL is included in the message.
        """
        presigned_url = (
            "http://example.com/someDB/someTable/"
            "results/partition_date%3D23-04-18/data.parquet/done.ack?"
            "AWSAccessKeyId=test_access_key_id"
            "&Signature=test_signature"
            "&x-amz-security-token=test_security_token"
            "&Expires=1770931001"
        )
        message = (
            "2026-02-05T21:17:04.215743Z ERROR "
            "influxdb3_py_api::system_py: processing engine: "
            "Error putting done file for someDB/someTable/results"
            "/partition_date=23-04-18/data.parquet: "
            "403 Client Error: Forbidden for url:"
        )
        http_response = create_http_response(
            HttpStatus.INTERNAL_ERROR, f"{message} {presigned_url}"
        )
        self.assertEqual(http_response["message"], f"{message} *****")
