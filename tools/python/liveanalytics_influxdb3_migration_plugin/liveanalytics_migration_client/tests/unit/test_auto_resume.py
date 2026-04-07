import unittest
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
)

from liveanalytics_influxdb3_migration_client import (
    InfluxDBMigrationWrapper,
    ExpiredPresignedUrlError,
)


def make_wrapper():
    """Creates a wrapper with all external dependencies mocked out."""
    with patch("liveanalytics_influxdb3_migration_client.boto3"), \
         patch("liveanalytics_influxdb3_migration_client.InfluxDBClient3"):
        os.environ["INFLUXDB3_HOST_URL"] = "https://localhost:8181"
        os.environ["INFLUXDB3_AUTH_TOKEN"] = "test-token"
        os.environ["INFLUXDB3_DATABASE_NAME"] = "test-db"
        wrapper = InfluxDBMigrationWrapper(
            liveanalytics_database="test-db",
            s3_bucket_name="test-bucket",
        )
    return wrapper


class AutoResumeTestCase(unittest.TestCase):

    def test_auto_resume_triggers_on_expired_url(self):
        """
        Tests that when bulk_invoke_http_trigger raises ExpiredPresignedUrlError,
        the loop retries with fresh credentials and resume_migration set to True.
        """
        wrapper = make_wrapper()

        wrapper.verify_bucket = MagicMock(return_value=True)
        wrapper.unload_db = MagicMock()
        wrapper.get_s3_objects_list = MagicMock(return_value=["db/table/file1.parquet", "db/table/file2.parquet"])
        wrapper.generate_metadata = MagicMock(return_value={
            "db/table/file1.parquet": {"presigned_get_url": "https://s3/file1?Expires=9999999999", "presigned_done_url": "https://s3/file1/done.ack"},
            "db/table/file2.parquet": {"presigned_get_url": "https://s3/file2?Expires=9999999999", "presigned_done_url": "https://s3/file2/done.ack"},
        })
        wrapper.setup_influxdb_metadata = MagicMock(return_value=True)
        wrapper.write_metadata_to_influxdb = MagicMock()
        wrapper.create_processing_engine_trigger = MagicMock()
        wrapper.verify_final_row_counts = MagicMock()
        wrapper.delete_unloaded_data = MagicMock()
        wrapper.setup_clients = MagicMock()

        # First call raises ExpiredPresignedUrlError, second succeeds.
        wrapper.bulk_invoke_http_trigger = MagicMock(
            side_effect=[ExpiredPresignedUrlError("expired"), None]
        )

        wrapper.run()

        self.assertEqual(wrapper.bulk_invoke_http_trigger.call_count, 2)
        wrapper.setup_clients.assert_called_once()
        self.assertTrue(wrapper.resume_migration)

    def test_auto_resume_fails_if_no_progress(self):
        """
        Tests that the loop aborts if credentials expire again without any files
        being completed (no progress between two consecutive auto-resumes).
        """
        wrapper = make_wrapper()

        wrapper.verify_bucket = MagicMock(return_value=True)
        wrapper.unload_db = MagicMock()
        # Same file count on both attempts — no progress.
        wrapper.get_s3_objects_list = MagicMock(return_value=["db/table/file1.parquet"])
        wrapper.generate_metadata = MagicMock(return_value={
            "db/table/file1.parquet": {"presigned_get_url": "https://s3/file1?Expires=9999999999", "presigned_done_url": "https://s3/file1/done.ack"},
        })
        wrapper.setup_influxdb_metadata = MagicMock(return_value=True)
        wrapper.write_metadata_to_influxdb = MagicMock()
        wrapper.create_processing_engine_trigger = MagicMock()
        wrapper.setup_clients = MagicMock()
        wrapper.bulk_invoke_http_trigger = MagicMock(
            side_effect=ExpiredPresignedUrlError("expired")
        )

        with self.assertRaises(RuntimeError) as ctx:
            wrapper.run()

        self.assertIn("no progress", str(ctx.exception).lower())

    def test_no_auto_resume_on_other_errors(self):
        """
        Tests that non-expiry errors propagate immediately without triggering
        the auto-resume loop.
        """
        wrapper = make_wrapper()

        wrapper.verify_bucket = MagicMock(return_value=True)
        wrapper.unload_db = MagicMock()
        wrapper.get_s3_objects_list = MagicMock(return_value=["db/table/file1.parquet"])
        wrapper.generate_metadata = MagicMock(return_value={
            "db/table/file1.parquet": {"presigned_get_url": "https://s3/file1?Expires=9999999999", "presigned_done_url": "https://s3/file1/done.ack"},
        })
        wrapper.setup_influxdb_metadata = MagicMock(return_value=True)
        wrapper.write_metadata_to_influxdb = MagicMock()
        wrapper.create_processing_engine_trigger = MagicMock()
        wrapper.setup_clients = MagicMock()
        wrapper.bulk_invoke_http_trigger = MagicMock(
            side_effect=RuntimeError("plugin error")
        )

        with self.assertRaises(RuntimeError) as ctx:
            wrapper.run()

        self.assertIn("plugin error", str(ctx.exception))
        # setup_clients should never be called — no auto-resume attempted.
        wrapper.setup_clients.assert_not_called()
        self.assertEqual(wrapper.bulk_invoke_http_trigger.call_count, 1)


if __name__ == "__main__":
    unittest.main()
