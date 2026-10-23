# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

import logging

import boto3


_LOGGER = logging.getLogger(__name__)
_CLIENT = boto3.client("kinesisanalyticsv2")
_TRANSITIONAL_STATES = {
    "AUTOSCALING",
    "FORCE_STOPPING",
    "MAINTENANCE",
    "ROLLING_BACK",
    "STARTING",
    "STOPPING",
    "UPDATING",
}
_FAILED_OPERATION_STATES = {"CANCELLED", "FAILED"}
_MAX_OPERATION_ERROR_LENGTH = 1200


def _application_status(application_name):
    try:
        response = _CLIENT.describe_application(
            ApplicationName=application_name,
            IncludeAdditionalDetails=False,
        )
    except _CLIENT.exceptions.ResourceNotFoundException:
        return None
    return response["ApplicationDetail"]["ApplicationStatus"]


def _in_progress_operation_id(application_name, operation_name):
    response = _CLIENT.list_application_operations(
        ApplicationName=application_name,
        Limit=1,
        Operation=operation_name,
        OperationStatus="IN_PROGRESS",
    )
    operations = response.get("ApplicationOperationInfoList") or []
    if not operations:
        return None
    return operations[0].get("OperationId")


def _request_start(application_name, status):
    if status == "RUNNING":
        return None
    if status == "STARTING":
        operation_id = _in_progress_operation_id(
            application_name,
            "StartApplication",
        )
        if operation_id:
            return operation_id
        raise RuntimeError(
            f"Managed Flink application '{application_name}' is STARTING, "
            "but no in-progress start operation could be found"
        )
    if status != "READY":
        raise RuntimeError(
            f"Cannot start Managed Flink application '{application_name}' "
            f"from status '{status}'"
        )
    try:
        response = _CLIENT.start_application(
            ApplicationName=application_name,
            RunConfiguration={
                "ApplicationRestoreConfiguration": {
                    "ApplicationRestoreType": "SKIP_RESTORE_FROM_SNAPSHOT"
                },
                "FlinkRunConfiguration": {
                    "AllowNonRestoredState": True,
                },
            },
        )
    except _CLIENT.exceptions.ResourceInUseException as error:
        operation_id = _in_progress_operation_id(
            application_name,
            "StartApplication",
        )
        if operation_id:
            return operation_id
        raise RuntimeError(
            f"Managed Flink application '{application_name}' rejected "
            "StartApplication as already in use, but no in-progress start "
            "operation could be found"
        ) from error
    operation_id = response.get("OperationId")
    if not operation_id:
        raise RuntimeError(
            f"StartApplication for Managed Flink application "
            f"'{application_name}' did not return an operation ID"
        )
    return operation_id


def _request_stop(application_name, status):
    if status != "RUNNING":
        return None
    try:
        response = _CLIENT.stop_application(ApplicationName=application_name)
    except _CLIENT.exceptions.ResourceInUseException:
        return None
    operation_id = response.get("OperationId")
    if not operation_id:
        raise RuntimeError(
            f"StopApplication for Managed Flink application "
            f"'{application_name}' did not return an operation ID"
        )
    return operation_id


def _operation_details(application_name, operation_id):
    try:
        response = _CLIENT.describe_application_operation(
            ApplicationName=application_name,
            OperationId=operation_id,
        )
    except _CLIENT.exceptions.ResourceNotFoundException:
        return None
    return response["ApplicationOperationInfoDetails"]


def _summarize_operation_error(error):
    normalized = " ".join((error or "No failure details were returned").split())
    if len(normalized) <= _MAX_OPERATION_ERROR_LENGTH:
        return normalized

    separator = " ... "
    head_length = (_MAX_OPERATION_ERROR_LENGTH - len(separator)) // 2
    tail_length = _MAX_OPERATION_ERROR_LENGTH - len(separator) - head_length
    return normalized[:head_length] + separator + normalized[-tail_length:]


def _operation_status(
    application_name,
    operation_id,
    operation_name,
    raise_on_failure=True,
):
    details = _operation_details(application_name, operation_id)
    if details is None:
        return None

    status = details["OperationStatus"]
    if status in _FAILED_OPERATION_STATES:
        failure_details = details.get("OperationFailureDetails") or {}
        error_info = failure_details.get("ErrorInfo") or {}
        error = _summarize_operation_error(error_info.get("ErrorString"))
        message = (
            f"{operation_name} operation '{operation_id}' {status.lower()} "
            f"for Managed Flink application '{application_name}': {error}"
        )
        if raise_on_failure:
            raise RuntimeError(message)
        _LOGGER.warning("%s; continuing CloudFormation deletion", message)
    return status


def on_event(event, _context):
    application_name = event["ResourceProperties"]["ApplicationName"]
    request_type = event["RequestType"]
    status = _application_status(application_name)
    operation_id = None
    operation_name = None

    if request_type == "Delete":
        operation_id = _request_stop(application_name, status)
        operation_name = "StopApplication"
    else:
        if status is None:
            raise RuntimeError(
                f"Managed Flink application '{application_name}' was not found"
            )
        operation_id = _request_start(application_name, status)
        operation_name = "StartApplication"

    response = {
        "PhysicalResourceId": event.get("PhysicalResourceId", application_name),
        "Data": {
            "ObservedStatus": status or "NOT_FOUND",
        },
    }
    if operation_id:
        response["LifecycleOperationId"] = operation_id
        response["LifecycleOperation"] = operation_name
    return response


def is_complete(event, _context):
    application_name = event["ResourceProperties"]["ApplicationName"]
    request_type = event["RequestType"]
    status = _application_status(application_name)
    operation_id = event.get("LifecycleOperationId")
    operation_name = event.get("LifecycleOperation", "Lifecycle")

    if request_type == "Delete":
        if status is None or status == "READY":
            return {"IsComplete": True}
        operation_status = None
        if operation_id:
            operation_status = _operation_status(
                application_name,
                operation_id,
                operation_name,
                raise_on_failure=False,
            )
        if operation_status in _FAILED_OPERATION_STATES:
            # Application deletion also halts execution, so a failed best-effort
            # stop must not strand the stack in DELETE_FAILED.
            return {"IsComplete": True}
        if status in _TRANSITIONAL_STATES or status == "RUNNING":
            if status == "RUNNING" and operation_status is None:
                _request_stop(application_name, status)
            return {"IsComplete": False}
        return {"IsComplete": True}

    if status == "RUNNING":
        return {"IsComplete": True}
    if status is None:
        raise RuntimeError(
            f"Managed Flink application '{application_name}' was deleted "
            "while waiting for RUNNING"
        )

    operation_status = None
    if operation_id:
        operation_status = _operation_status(
            application_name,
            operation_id,
            operation_name,
        )
    if status in _TRANSITIONAL_STATES:
        return {"IsComplete": False}
    if status == "READY":
        if operation_id and operation_status in {None, "IN_PROGRESS"}:
            return {"IsComplete": False}
        # Failed operations were raised above. A successful start that leaves
        # the application READY is no longer progressing toward RUNNING.
        raise RuntimeError(
            f"Managed Flink application '{application_name}' returned to READY "
            "before reaching RUNNING"
        )
    raise RuntimeError(
        f"Cannot start Managed Flink application '{application_name}' "
        f"from status '{status}'"
    )
