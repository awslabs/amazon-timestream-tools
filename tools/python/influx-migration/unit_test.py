import logging
import os
from parameterized import parameterized
from sys import platform
import unittest
from unittest import mock

import influx_migration

src_bucket = "test-bucket"
dest_bucket = "dest-bucket"
test_dir_name = "test-dir"
org = "org"
host = "http://some_address.com:8086"

class BaseTestCases:

    class BaseTestCase(unittest.TestCase):
        mock_env = None

        def setUp(self):
            """
            Overrides unittest.TestCase.setUp, called before each test runs
            """
            self.mock_env = mock.patch.dict(os.environ, {"INFLUX_SRC_TOKEN": "src_token", "INFLUX_DEST_TOKEN": "dest_token"}, clear=True)
            self.mock_env.start()

        def tearDown(self):
            """
            Overrides unittest.TestCase.tearDown, called after each test runs
            """
            if self.mock_env is not None:
                self.mock_env.stop()

class ExpectFailureTestCase(BaseTestCases.BaseTestCase):
    @parameterized.expand([
        ("no args", [], SystemExit, 2),
    ])
    def test_parse_args(self, name, input, exception_type, error_code):
        with self.assertRaises(exception_type) as error:
            influx_migration.parse_args(input)
        self.assertEqual(error.exception.code, error_code)

    @parameterized.expand([
        (
            "not full no src bucket", 
            [
                "--src-host", host,
                "--dest-host", host
            ], 
            ValueError,
            "ValueError('Both --src-bucket and --full have been left empty, at least one or the "
            "other is required.')"
        ),
        (
            "dest org no src org",
            [
                "--src-host", host,
                "--dest-host", host,
                "--src-bucket", src_bucket,
                "--dest-org", org,
            ],
            ValueError,
            "ValueError('Remote migration with --dest-org requires --src-org to work')"
        ),
    ])
    def test_verify_args(self, name, args_list, exception_type, exception_str):
        args = influx_migration.parse_args(args_list)
        with self.assertRaises(exception_type) as error:
            influx_migration.verify_args(args)
        self.assertEqual(repr(error.exception), exception_str)

    @parameterized.expand([
        (
            "s3 bucket name only", 
            [
                "--src-host", host,
                "--dest-host", host,
                "--src-bucket", src_bucket,
                "--s3-bucket", "s3-bucket"
            ], 
            ValueError,
            {
                "darwin": "ValueError('S3 bucket was s3-bucket but expected value on darwin is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')",
                "win32": "ValueError('S3 bucket was s3-bucket but expected value on win32 is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')"
             }
        ),
        (
            "colon end", 
            [
                "--src-host", host,
                "--dest-host", host,
                "--src-bucket", src_bucket,
                "--s3-bucket", "s3-remote:"
            ], 
            ValueError,
            {
                "darwin": "ValueError('S3 bucket was s3-remote: but expected value on darwin is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')",
                "win32": "ValueError('S3 bucket was s3-remote: but expected value on win32 is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')"
             }
        ),
        (
            "colon start", 
            [
                "--src-host", host,
                "--dest-host", host,
                "--src-bucket", src_bucket,
                "--s3-bucket", ":s3-bucket"
            ], 
            ValueError,
            {
                "darwin": "ValueError('S3 bucket was :s3-bucket but expected value on darwin is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')",
                "win32": "ValueError('S3 bucket was :s3-bucket but expected value on win32 is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')"
             }
        ),
    ])
    def test_verify_args_with_s3_bucket(self, name, args_list, exception_type, exception_str_dict):
        if platform == "darwin" or platform == "win32":
            args = influx_migration.parse_args(args_list)
            with self.assertRaises(exception_type) as error:
                influx_migration.verify_args(args)
            self.assertEqual(repr(error.exception), exception_str_dict[platform])

    @parameterized.expand([
        (
            "s3 bucket name only", 
            "s3-bucket", 
            ValueError,
            {
                "darwin": "ValueError('S3 bucket was s3-bucket but expected value on darwin is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')",
                "win32": "ValueError('S3 bucket was s3-bucket but expected value on win32 is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')"
             }
        ),
        (
            "colon end", 
            "s3-remote:", 
            ValueError,
            {
                "darwin": "ValueError('S3 bucket was s3-remote: but expected value on darwin is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')",
                "win32": "ValueError('S3 bucket was s3-remote: but expected value on win32 is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')"
             }
        ),
        (
            "colon start", 
            ":s3-bucket", 
            ValueError,
            {
                "darwin": "ValueError('S3 bucket was :s3-bucket but expected value on darwin is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')",
                "win32": "ValueError('S3 bucket was :s3-bucket but expected value on win32 is an rclone "
                    "configured remote and bucket name, i.e., remote-name:s3-bucket-name')"
             }
        ),
    ])
    def test_parse_bucket_name(self, name, arg, exception_type, exception_str_dict):
        if platform == "darwin" or platform == "win32":
            with self.assertRaises(exception_type) as error:
                influx_migration.parse_bucket_name(arg)
            self.assertEqual(repr(error.exception), exception_str_dict[platform])

    def test_verify_environment_tokens_no_tokens(self):
        os.environ.pop("INFLUX_SRC_TOKEN")
        os.environ.pop("INFLUX_DEST_TOKEN")
        with self.assertRaises(OSError) as error:
            influx_migration.verify_environment_tokens(False)
        self.assertEqual(repr(error.exception), "OSError('INFLUX_SRC_TOKEN environment variable has not been set')")

    def test_verify_environment_tokens_no_src_token(self):
        os.environ.pop("INFLUX_SRC_TOKEN")
        with self.assertRaises(OSError) as error:
            influx_migration.verify_environment_tokens(False)
        self.assertEqual(repr(error.exception), "OSError('INFLUX_SRC_TOKEN environment variable has not been set')")

    def test_verify_environment_tokens_no_dest_token(self):
        os.environ.pop("INFLUX_DEST_TOKEN")
        with self.assertRaises(OSError) as error:
            influx_migration.verify_environment_tokens(False)
        self.assertEqual(repr(error.exception), "OSError('INFLUX_DEST_TOKEN environment variable has not been set')")

    def test_set_logging_level_failure(self):
        influx_migration.set_logging("invalid")
        # Defaults to logging.INFO
        self.assertLogs(logging.getLogger('influx_migration'), level=logging.INFO)

class ExpectSuccessTestCase(BaseTestCases.BaseTestCase):
    def test_verify_args_not_full_with_src_bucket(self):
        args = influx_migration.parse_args([
                "--src-bucket", src_bucket,
                "--src-host", host,
                "--dest-host", host,
                ])
        self.assertEqual(influx_migration.verify_args(args), None)

    def test_verify_args_full_without_src_bucket(self):
        args = influx_migration.parse_args([
                "--src-host", host,
                "--dest-host", host,
                "--full",
                "--confirm-full"
                ])
        self.assertEqual(influx_migration.verify_args(args), None)

    def test_verify_args_dest_org_and_src_org(self):
        args = influx_migration.parse_args([
                "--src-bucket", src_bucket,
                "--src-host", host,
                "--dest-host", host,
                "--src-org", org,
                "--dest-org", org
                ])
        self.assertEqual(influx_migration.verify_args(args), None)

    def test_verify_environment_tokens(self):
        influx_migration.verify_environment_tokens(False)

    def test_verify_environment_tokens_skip_backup(self):
        influx_migration.verify_environment_tokens(True)

    def test_parse_bucket_name(self):
        if platform == "darwin" or platform == "win32":
            self.assertEqual(influx_migration.parse_bucket_name("s3-remote:test-s3-bucket"), "test-s3-bucket")
            self.assertEqual(influx_migration.parse_bucket_name("s3-remote:t"), "t")
            self.assertEqual(influx_migration.parse_bucket_name("e:test-s3-bucket"), "test-s3-bucket")
            self.assertEqual(influx_migration.parse_bucket_name("e:t"), "t")
        elif platform == "linux":
            self.assertEqual(influx_migration.parse_bucket_name("test-s3-bucket"), "test-s3-bucket")

    def test_verify_args_with_s3_bucket(self):
        args = None
        if platform == "darwin" or platform == "win32":
            args = influx_migration.parse_args([
                    "--src-bucket", src_bucket,
                    "--src-host", host,
                    "--dest-host", host,
                    "--s3-bucket", "s3-remote:test-s3-bucket"
                    ])
            self.assertEqual(influx_migration.verify_args(args), None)
        elif platform == "linux":
            args = influx_migration.parse_args([
                    "--src-bucket", src_bucket,
                    "--src-host", host,
                    "--dest-host", host,
                    "--s3-bucket", "test-s3-bucket"
                    ])
            self.assertEqual(influx_migration.verify_args(args), None)

    def test_set_logging_level_debug(self):
        influx_migration.set_logging("debug")
        self.assertLogs(logging.getLogger('influx_migration'), level=logging.DEBUG)
        influx_migration.set_logging("DEBUG")
        self.assertLogs(logging.getLogger('influx_migration'), level=logging.DEBUG)

    def test_set_logging_level_info(self):
        influx_migration.set_logging("info")
        self.assertLogs(logging.getLogger('influx_migration'), level=logging.INFO)
        influx_migration.set_logging("INFO")
        self.assertLogs(logging.getLogger('influx_migration'), level=logging.INFO)

    def test_set_logging_level_error(self):
        influx_migration.set_logging("error")
        self.assertLogs(logging.getLogger('influx_migration'), level=logging.ERROR)
        influx_migration.set_logging("ERROR")
        self.assertLogs(logging.getLogger('influx_migration'), level=logging.ERROR)

if __name__ == "__main__":
    unittest.main()
