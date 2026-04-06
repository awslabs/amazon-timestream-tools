#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
A simple application providing a basic example of how to use the InfluxDB APIs.
This application reads a sample JSON dataset and creates InfluxDB line protocol points to ingest into InfluxDB.
"""

import json
import os
import sys
from datetime import datetime
from typing import Any, Dict, List

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

# Constants
MAX_BATCH_SIZE = 5000  # InfluxDB has an optimal batch size of 5000 points


def create_influxdb_client() -> InfluxDBClient:
    """
    Create and configure an InfluxDB client.

    Returns:
        InfluxDBClient: Configured InfluxDB client
    """
    return InfluxDBClient.from_env_properties()


def create_bucket_if_nonexistent(
    client: InfluxDBClient, bucket_name: str, retention_hours: int
) -> None:
    """
    Create an InfluxDB bucket if it doesn't already exist.

    Args:
        client (InfluxDBClient): InfluxDB client
        bucket_name (str): Name of the bucket to create
        retention_hours (int): Data retention period in hours (0 for infinite retention)
    """
    from influxdb_client.domain.bucket_retention_rules import BucketRetentionRules

    buckets_api = client.buckets_api()
    bucket = buckets_api.find_bucket_by_name(bucket_name)

    if bucket is None:
        print(f"Creating bucket: {bucket_name}")

        retention_rules = None
        if retention_hours > 0:
            retention_rules = BucketRetentionRules(
                type="expire", every_seconds=retention_hours * 3600
            )
            print(f"Setting retention policy: {retention_hours} hours")

        buckets_api.create_bucket(
            bucket_name=bucket_name, org=client.org, retention_rules=retention_rules
        )
    else:
        print(f"Bucket {bucket_name} already exists")


def create_point(measurement: str) -> Point:
    """
    Create an empty InfluxDB point.

    Args:
        measurement (str): The measurement name

    Returns:
        Point: An empty InfluxDB point
    """
    return Point(measurement)


def set_point_tags(point: Point, tags: Dict[str, str]) -> Point:
    """
    Set the tags for an InfluxDB point.

    Args:
        point (Point): The point to modify
        tags (Dict[str, str]): Dictionary of tag names and values

    Returns:
        Point: The modified point
    """
    for name, value in tags.items():
        point = point.tag(name, value)

    return point


def set_point_fields(point: Point, fields: Dict[str, Any]) -> Point:
    """
    Set the fields for an InfluxDB point.

    Args:
        point (Point): The point to modify
        fields (Dict[str, Any]): Dictionary of field names and values

    Returns:
        Point: The modified point
    """
    for name, value in fields.items():
        if isinstance(value, dict) and "value" in value:
            field_value = value["value"]
            point = point.field(name, field_value)
        else:
            point = point.field(name, value)

    return point


def set_point_timestamp(point: Point, timestamp: str) -> Point:
    """
    Set the timestamp for an InfluxDB point.

    Args:
        point (Point): The point to modify
        timestamp (str): The timestamp string in nanosecond precision

    Returns:
        Point: The modified point
    """
    # Parse the timestamp string with nanosecond precision
    # First, split the string into datetime part and nanosecond part
    datetime_part = timestamp[:26]  # Up to microseconds
    nanosecond_part = timestamp[26:] if len(timestamp) > 26 else "000"

    # Parse the datetime part
    dt = datetime.strptime(datetime_part, "%Y-%m-%d %H:%M:%S.%f")

    # Convert to nanosecond precision timestamp
    # First convert to seconds since epoch
    epoch_seconds = dt.timestamp()
    # Convert to nanoseconds and add the nanosecond part
    epoch_nanoseconds = int(epoch_seconds * 1_000_000_000) + int(nanosecond_part)

    point = point.time(epoch_nanoseconds, write_precision="ns")

    return point


def load_json_data(file_path: str) -> List[Dict[str, Any]]:
    """
    Load JSON data from a file.

    Args:
        file_path (str): Path to the JSON file

    Returns:
        List[Dict[str, Any]]: List of records from the JSON file
    """
    with open(file_path, "r") as f:
        return json.load(f)


def build_line_protocol(
    json_records: List[Dict[str, Any]], measurement: str
) -> List[Point]:
    """
    Convert JSON records to InfluxDB points.

    Args:
        json_records (List[Dict[str, Any]]): List of records from the JSON file
        measurement (str): The measurement name

    Returns:
        List[Point]: List of InfluxDB points
    """
    influxdb_points = []

    for json_record in json_records:
        # Create a new point
        point = create_point(measurement)

        # Set tags (dimensions in Timestream)
        point = set_point_tags(point, json_record["dimensions"])

        # Set fields (measures in Timestream)
        point = set_point_fields(point, json_record["measures"])

        # Set timestamp
        point = set_point_timestamp(point, json_record["timestamp"])

        influxdb_points.append(point)

    return influxdb_points


def write_line_protocol(
    client: InfluxDBClient, bucket: str, points: List[Point]
) -> int:
    """
    Write line protocol to InfluxDB in batches of max 5000 points.

    Args:
        client (InfluxDBClient): InfluxDB client
        bucket (str): Name of the bucket
        points (List[Point]): List of points to write

    Returns:
        int: Number of points written
    """
    total_points = len(points)
    points_written = 0

    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Process points in batches of MAX_BATCH_SIZE
    for i in range(0, total_points, MAX_BATCH_SIZE):
        batch = points[i : i + MAX_BATCH_SIZE]
        try:
            write_api.write(bucket=bucket, org=client.org, record=batch)
            points_written += len(batch)
            print(
                f"Successfully wrote {len(batch)} points. Total: {points_written}/{total_points}"
            )
        except Exception as e:
            print(f"Error writing points: {e}")

    return points_written


def check_required_env_vars():
    required_vars = ["INFLUXDB_V2_URL", "INFLUXDB_V2_TOKEN", "INFLUXDB_V2_ORG"]

    missing_vars = []
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)

    if missing_vars:
        print(
            f"Error: The following required environment variables are not set: {', '.join(missing_vars)}"
        )
        sys.exit(1)


def main():
    """
    Main function to run the application.
    """
    bucket_name = "iot_sample"
    measurement = "system_metrics"
    bucket_retention_hours = 27216

    check_required_env_vars()
    client = create_influxdb_client()
    create_bucket_if_nonexistent(client, bucket_name, bucket_retention_hours)

    json_data_path = "data/sample-multi.json"
    print(f"Loading data from {json_data_path}")
    json_records = load_json_data(json_data_path)
    print(f"Loaded {len(json_records)} records from JSON file")

    # Convert JSON records to InfluxDB line protocol
    influxdb_points = build_line_protocol(json_records, measurement)

    # Write points to InfluxDB
    points_written = write_line_protocol(client, bucket_name, influxdb_points)

    print("\nSummary:")
    print(f"Total records processed: {len(json_records)}")
    print(f"Points successfully written: {points_written}")

    client.close()


if __name__ == "__main__":
    main()
