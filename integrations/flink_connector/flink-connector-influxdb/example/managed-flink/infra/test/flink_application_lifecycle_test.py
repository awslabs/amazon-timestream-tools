# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import importlib.util
import pathlib
import sys
import types
import unittest


class _ResourceNotFoundException(Exception):
    pass


class _ResourceInUseException(Exception):
    pass


class _FakeKinesisAnalyticsClient:
    exceptions = types.SimpleNamespace(
        ResourceNotFoundException=_ResourceNotFoundException,
        ResourceInUseException=_ResourceInUseException,
    )

    def __init__(
        self,
        status,
        operation_status="IN_PROGRESS",
        operation_error=None,
        start_resource_in_use_count=0,
        application_operations=None,
    ):
        self.status = status
        self.operation_status = operation_status
        self.operation_error = operation_error
        self.start_resource_in_use_count = start_resource_in_use_count
        self.application_operations = application_operations or []
        self.start_calls = []
        self.stop_calls = []
        self.list_operation_calls = []
        self.describe_operation_calls = []

    def describe_application(self, **_kwargs):
        if self.status is None:
            raise _ResourceNotFoundException()
        return {
            "ApplicationDetail": {
                "ApplicationStatus": self.status,
            }
        }

    def start_application(self, **kwargs):
        self.start_calls.append(kwargs)
        if self.start_resource_in_use_count > 0:
            self.start_resource_in_use_count -= 1
            raise _ResourceInUseException()
        return {"OperationId": "start-operation-id"}

    def stop_application(self, **kwargs):
        self.stop_calls.append(kwargs)
        return {"OperationId": "stop-operation-id"}

    def list_application_operations(self, **kwargs):
        self.list_operation_calls.append(kwargs)
        return {
            "ApplicationOperationInfoList": self.application_operations,
        }

    def describe_application_operation(self, **kwargs):
        self.describe_operation_calls.append(kwargs)
        details = {
            "Operation": kwargs.get("OperationId"),
            "OperationStatus": self.operation_status,
        }
        if self.operation_error is not None:
            details["OperationFailureDetails"] = {
                "ErrorInfo": {
                    "ErrorString": self.operation_error,
                }
            }
        return {"ApplicationOperationInfoDetails": details}


def _load_handler(
    status,
    operation_status="IN_PROGRESS",
    operation_error=None,
    start_resource_in_use_count=0,
    application_operations=None,
):
    fake_client = _FakeKinesisAnalyticsClient(
        status,
        operation_status,
        operation_error,
        start_resource_in_use_count,
        application_operations,
    )
    fake_boto3 = types.SimpleNamespace(client=lambda _service_name: fake_client)
    sys.modules["boto3"] = fake_boto3

    handler_path = (
        pathlib.Path(__file__).parents[1]
        / "lambda"
        / "flink-application-lifecycle"
        / "handler.py"
    )
    spec = importlib.util.spec_from_file_location(
        "flink_application_lifecycle_handler",
        handler_path,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module, fake_client


def _event(
    request_type="Create",
    physical_resource_id=None,
    operation_id=None,
    operation_name=None,
):
    event = {
        "RequestType": request_type,
        "ResourceProperties": {
            "ApplicationName": "influxdb3-flink-example",
        },
    }
    if physical_resource_id is not None:
        event["PhysicalResourceId"] = physical_resource_id
    if operation_id is not None:
        event["LifecycleOperationId"] = operation_id
    if operation_name is not None:
        event["LifecycleOperation"] = operation_name
    return event


class FlinkApplicationLifecycleTest(unittest.TestCase):
    def test_create_requests_start_from_ready(self):
        handler, client = _load_handler("READY")

        result = handler.on_event(_event(), None)

        self.assertEqual(
            result["PhysicalResourceId"],
            "influxdb3-flink-example",
        )
        self.assertEqual(
            result["LifecycleOperationId"],
            "start-operation-id",
        )
        self.assertEqual(result["LifecycleOperation"], "StartApplication")
        self.assertEqual(len(client.start_calls), 1)
        self.assertEqual(
            client.start_calls[0]["RunConfiguration"][
                "ApplicationRestoreConfiguration"
            ]["ApplicationRestoreType"],
            "SKIP_RESTORE_FROM_SNAPSHOT",
        )

    def test_create_tracks_an_existing_start(self):
        handler, client = _load_handler(
            "STARTING",
            application_operations=[
                {
                    "Operation": "StartApplication",
                    "OperationId": "existing-start-operation-id",
                    "OperationStatus": "IN_PROGRESS",
                }
            ],
        )

        result = handler.on_event(_event(), None)

        self.assertEqual(
            result["LifecycleOperationId"],
            "existing-start-operation-id",
        )
        self.assertEqual(client.start_calls, [])

    def test_create_rejects_an_unrelated_transitional_state(self):
        handler, client = _load_handler("UPDATING")

        with self.assertRaisesRegex(
            RuntimeError,
            "from status 'UPDATING'",
        ):
            handler.on_event(_event(), None)

        self.assertEqual(client.start_calls, [])

    def test_resource_in_use_recovers_and_tracks_in_progress_start(self):
        handler, client = _load_handler(
            "READY",
            start_resource_in_use_count=1,
            application_operations=[
                {
                    "Operation": "StartApplication",
                    "OperationId": "existing-start-operation-id",
                    "OperationStatus": "IN_PROGRESS",
                }
            ],
        )

        initial_result = handler.on_event(_event(), None)

        self.assertEqual(
            initial_result["LifecycleOperationId"],
            "existing-start-operation-id",
        )
        self.assertEqual(initial_result["LifecycleOperation"], "StartApplication")
        self.assertEqual(
            client.list_operation_calls,
            [
                {
                    "ApplicationName": "influxdb3-flink-example",
                    "Limit": 1,
                    "Operation": "StartApplication",
                    "OperationStatus": "IN_PROGRESS",
                }
            ],
        )

        client.operation_status = "FAILED"
        client.operation_error = "endpoint must contain a host"
        completion_event = _event()
        completion_event.update(initial_result)

        with self.assertRaisesRegex(
            RuntimeError,
            "endpoint must contain a host",
        ):
            handler.is_complete(completion_event, None)

    def test_resource_in_use_without_observable_start_fails(self):
        handler, _client = _load_handler(
            "READY",
            start_resource_in_use_count=1,
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "no in-progress start operation could be found",
        ):
            handler.on_event(_event(), None)

    def test_delete_preserves_the_cloudformation_physical_resource_id(self):
        handler, client = _load_handler("RUNNING")
        event = _event(
            "Delete",
            physical_resource_id="cloudformation-generated-id",
        )

        result = handler.on_event(event, None)

        self.assertEqual(
            result["PhysicalResourceId"],
            "cloudformation-generated-id",
        )
        self.assertEqual(
            result["LifecycleOperationId"],
            "stop-operation-id",
        )
        self.assertEqual(result["LifecycleOperation"], "StopApplication")
        self.assertEqual(
            client.stop_calls,
            [{"ApplicationName": "influxdb3-flink-example"}],
        )

    def test_delete_preserves_the_physical_id_when_application_is_missing(self):
        handler, client = _load_handler(None)
        event = _event(
            "Delete",
            physical_resource_id="cloudformation-generated-id",
        )

        result = handler.on_event(event, None)

        self.assertEqual(
            result["PhysicalResourceId"],
            "cloudformation-generated-id",
        )
        self.assertNotIn("LifecycleOperationId", result)
        self.assertEqual(client.stop_calls, [])

    def test_running_application_completes_start(self):
        handler, _client = _load_handler("RUNNING")

        self.assertEqual(
            handler.is_complete(_event(), None),
            {"IsComplete": True},
        )

    def test_failed_start_operation_fails_without_waiting_for_timeout(self):
        handler, client = _load_handler(
            "READY",
            operation_status="FAILED",
            operation_error="endpoint must contain a host",
        )
        event = _event(
            operation_id="start-operation-id",
            operation_name="StartApplication",
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "endpoint must contain a host",
        ):
            handler.is_complete(event, None)

        self.assertEqual(
            client.describe_operation_calls,
            [
                {
                    "ApplicationName": "influxdb3-flink-example",
                    "OperationId": "start-operation-id",
                }
            ],
        )
        self.assertEqual(client.start_calls, [])

    def test_failed_operation_error_is_bounded_and_keeps_the_root_cause(self):
        operation_error = "outer failure " + ("x" * 4000) + " root cause"
        handler, _client = _load_handler(
            "READY",
            operation_status="FAILED",
            operation_error=operation_error,
        )
        event = _event(
            operation_id="start-operation-id",
            operation_name="StartApplication",
        )

        with self.assertRaises(RuntimeError) as raised:
            handler.is_complete(event, None)

        message = str(raised.exception)
        self.assertLess(len(message), 1500)
        self.assertIn("outer failure", message)
        self.assertIn("root cause", message)

    def test_in_progress_start_operation_keeps_waiting(self):
        handler, _client = _load_handler("READY")
        event = _event(
            operation_id="start-operation-id",
            operation_name="StartApplication",
        )

        self.assertEqual(
            handler.is_complete(event, None),
            {"IsComplete": False},
        )

    def test_successful_start_that_returns_to_ready_fails(self):
        handler, _client = _load_handler(
            "READY",
            operation_status="SUCCESSFUL",
        )
        event = _event(
            operation_id="start-operation-id",
            operation_name="StartApplication",
        )

        with self.assertRaisesRegex(
            RuntimeError,
            "returned to READY",
        ):
            handler.is_complete(event, None)

    def test_delete_stops_running_application(self):
        handler, client = _load_handler("RUNNING")

        result = handler.is_complete(_event("Delete"), None)

        self.assertEqual(result, {"IsComplete": False})
        self.assertEqual(
            client.stop_calls,
            [{"ApplicationName": "influxdb3-flink-example"}],
        )

    def test_delete_is_complete_when_application_is_ready(self):
        handler, client = _load_handler("READY")

        self.assertEqual(
            handler.is_complete(_event("Delete"), None),
            {"IsComplete": True},
        )
        self.assertEqual(client.stop_calls, [])

    def test_failed_or_cancelled_stop_operation_does_not_block_delete(self):
        for operation_status, operation_error in (
            ("FAILED", "stop failed"),
            ("CANCELLED", "stop cancelled"),
        ):
            with self.subTest(operation_status=operation_status):
                handler, client = _load_handler(
                    "RUNNING",
                    operation_status=operation_status,
                    operation_error=operation_error,
                )
                event = _event(
                    "Delete",
                    physical_resource_id="cloudformation-generated-id",
                    operation_id="stop-operation-id",
                    operation_name="StopApplication",
                )

                with self.assertLogs(handler._LOGGER, level="WARNING") as logs:
                    result = handler.is_complete(event, None)

                self.assertEqual(result, {"IsComplete": True})
                self.assertIn(operation_status.lower(), logs.output[0])
                self.assertIn(operation_error, logs.output[0])
                self.assertEqual(client.stop_calls, [])

    def test_delete_is_complete_from_terminal_non_running_state(self):
        handler, client = _load_handler("ROLLED_BACK")

        self.assertEqual(
            handler.is_complete(_event("Delete"), None),
            {"IsComplete": True},
        )
        self.assertEqual(client.stop_calls, [])


if __name__ == "__main__":
    unittest.main()
