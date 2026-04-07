#!/usr/bin/env python3

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
"""
Script for backing up and restoring Timestream for InfluxDB data using ECS.
This script is designed to run as an ECS task and handles both backup and restore operations.
"""

import boto3
from botocore.exceptions import ClientError
import datetime
import os
import shutil
import subprocess
import logging
import sys
import argparse
import json
from influxdb_client.client.influxdb_client import InfluxDBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("influxdb-backup-restore")


def get_secret(secret_name, region_name=None):
    """Retrieve a secret from AWS Secrets Manager"""
    if not region_name:
        region_name = os.environ.get("AWS_REGION", "us-east-1")

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
        if "SecretString" in response:
            return json.loads(response["SecretString"])
        else:
            logger.error(f"Secret {secret_name} does not contain a SecretString")
            return None
    except Exception as e:
        logger.error(f"Error retrieving secret {secret_name}: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Backup or restore Timestream for InfluxDB data"
    )
    parser.add_argument(
        "--operation",
        choices=["backup", "restore"],
        required=True,
        help="Operation to perform (backup or restore)",
    )
    parser.add_argument(
        "--unique-restore-name",
        action="store_true",
        help="Create a unique timestamped bucket name for restore",
    )
    parser.add_argument(
        "--force-replace",
        action="store_true",
        help="Delete existing bucket before restore (WARNING: data loss)",
    )
    args = parser.parse_args()

    # Get endpoints and organizations from environment variables
    backup_endpoint = os.environ.get("BACKUP_URL")
    backup_org = os.environ.get("BACKUP_ORG")
    restore_endpoint = os.environ.get("RESTORE_URL")
    restore_org = os.environ.get("RESTORE_ORG")
    bucket_name = os.environ.get("BUCKET_NAME")
    s3_backup_bucket_name = os.environ.get("S3_BACKUP_BUCKET_NAME")

    # Get secret name from environment variable
    tokens_secret_name = os.environ.get("TOKENS_SECRET_NAME")

    # Validate required environment variables
    required_vars = {
        "TOKENS_SECRET_NAME": tokens_secret_name,
        "BACKUP_URL": backup_endpoint,
        "BACKUP_ORG": backup_org,
        "RESTORE_URL": restore_endpoint,
        "RESTORE_ORG": restore_org,
        "BUCKET_NAME": bucket_name,
        "S3_BACKUP_BUCKET_NAME": s3_backup_bucket_name,
    }

    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        logger.error(
            f"Missing required environment variables: {', '.join(missing_vars)}"
        )
        sys.exit(1)

    try:
        tokens = get_secret(tokens_secret_name)
        if not tokens:
            logger.error(f"Failed to retrieve tokens from secret {tokens_secret_name}")
            sys.exit(1)

        backup_token = tokens.get("BACKUP_TOKEN")
        restore_token = tokens.get("RESTORE_TOKEN")

        if not backup_token:
            logger.error(f"Secret {tokens_secret_name} does not contain BACKUP_TOKEN")
            sys.exit(1)

        if not restore_token:
            logger.error(f"Secret {tokens_secret_name} does not contain RESTORE_TOKEN")
            sys.exit(1)

        logger.info(
            f"Successfully retrieved tokens from Secrets Manager: {tokens_secret_name}"
        )
    except Exception as e:
        logger.error(f"Failed to retrieve tokens from Secrets Manager: {str(e)}")
        sys.exit(1)

    logger.info(f"Using backup endpoint: {backup_endpoint}")
    logger.info(f"Using restore endpoint: {restore_endpoint}")

    session = boto3.session.Session()

    if args.operation == "backup":
        logger.info("Starting backup operation")
        result = backup(
            session,
            backup_endpoint,
            backup_token,
            s3_backup_bucket_name,
            bucket_name,
            backup_org,
        )
        logger.info(result)
    elif args.operation == "restore":
        if not args.force_replace and not args.unique_restore_name:
            raise RuntimeError("--force-replace or --unique-restore-name must be set")
        if args.force_replace and args.unique_restore_name:
            raise RuntimeError(
                "Either --force-replace or --unique-restore-name must be set, not both"
            )

        logger.info("Starting restore operation")
        result = restore(
            session,
            restore_endpoint,
            restore_token,
            s3_backup_bucket_name,
            bucket_name,
            restore_org,
            unique_restore_name=args.unique_restore_name,
            force_replace=args.force_replace,
        )
        logger.info(result)


def backup(
    session, backup_endpoint, backup_token, s3_backup_bucket_name, bucket_name, org_name
):
    """Backup InfluxDB data to S3."""
    s3_client = session.client("s3")

    backup_path = "/data/backup_directory"

    # Delete backup directory if it already exists and recreate it
    if os.path.exists(backup_path):
        logger.info(f"Removing existing backup directory: {backup_path}")
        shutil.rmtree(backup_path)
    os.makedirs(backup_path)
    logger.info(f"Created backup directory: {backup_path}")

    logger.info(f"Starting backup from {backup_endpoint} for bucket {bucket_name}")

    # Use environment variables for the subprocess to avoid tokens in process listings
    env = os.environ.copy()
    env["INFLUX_TOKEN"] = backup_token

    # A subprocess command is necessary for backing up, as influxdb_client does not
    # have a backup method
    bucket_backup_command = [
        "influx",
        "backup",
        "--host",
        backup_endpoint,
        "--org",
        org_name,
        "--bucket",
        bucket_name,
        backup_path,
    ]

    try:
        subprocess.run(
            bucket_backup_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
            env=env,
        )
        logger.info("Backup command completed successfully")
    except subprocess.CalledProcessError as e:
        safe_error = (
            str(e.stderr).replace(backup_token, "****")
            if backup_token
            else str(e.stderr)
        )
        logger.error(f"Backup failed: {safe_error}")
        raise RuntimeError("Backup failed")

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_prefix = f"{bucket_name}/{timestamp}/"

    logger.info(
        f"Uploading backup data to S3 bucket {s3_backup_bucket_name} with prefix {backup_prefix}"
    )
    file_count = 0
    total_size = 0

    for root, _, files in os.walk(backup_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            file_size = os.path.getsize(local_file_path)
            total_size += file_size

            relative_path = os.path.relpath(local_file_path, backup_path)
            s3_key = f"{backup_prefix}{relative_path}"

            try:
                s3_client.upload_file(local_file_path, s3_backup_bucket_name, s3_key)
                file_count += 1
                if file_count % 100 == 0:
                    logger.info(f"Uploaded {file_count} files so far...")
            except ClientError as e:
                logger.error(f"Failed to upload {local_file_path}: {e}")
                logger.info(f"Attempting to delete partial backup directory {backup_prefix}")
                try:
                    s3_resource = boto3.resource('s3')
                    bucket = s3_resource.Bucket(s3_backup_bucket_name)
                    bucket.objects.filter(Prefix=backup_prefix).delete()
                except Exception as deletion_error:
                    logger.error(f"Error while attempting to clean up partial backup: {deletion_error}")
                raise

    total_size_mb = total_size / (1024 * 1024)
    logger.info(
        f"Backup complete. Uploaded {file_count} files ({total_size_mb:.2f} MB) to S3"
    )

    # Remove data stored in EFS.
    # This data will persist if not removed.
    if os.path.exists(backup_path):
        try:
            shutil.rmtree(backup_path)
            logger.info(f"Removed {backup_path} directory")
        except Exception as e:
            logger.warning(f"Failed to remove {backup_path} directory: {str(e)}")

    return f"Successfully backed up {bucket_name} to {s3_backup_bucket_name}/{backup_prefix}"


def restore(
    session,
    restore_endpoint,
    restore_token,
    s3_backup_bucket_name,
    bucket_name,
    org_name,
    unique_restore_name=False,
    force_replace=False,
):
    """Restore InfluxDB data from S3.

    Args:
        session: boto3 session
        restore_endpoint: Timestream for InfluxDB endpoint URL
        restore_token: Timestream for InfluxDB operator token
        s3_backup_bucket_name: S3 bucket containing backups
        bucket_name: Original bucket name to restore
        org_name: Organization name
        unique_restore_name: If True, create a unique bucket name for restore
        force_replace: If True, delete existing bucket before restore
        keep_original: If True, keep original bucket (requires unique_restore_name=True)
    """
    s3_client = session.client("s3")
    s3_resource = session.resource("s3")
    s3_backup_bucket = s3_resource.Bucket(s3_backup_bucket_name)

    restore_path = "/data/restore_directory"

    # Delete restore directory if it already exists and recreate it
    if os.path.exists(restore_path):
        logger.info(f"Removing existing restore directory: {restore_path}")
        shutil.rmtree(restore_path)
    os.makedirs(restore_path)
    logger.info(f"Created restore directory: {restore_path}")

    # Find the latest backup for this bucket
    prefix = f"{bucket_name}/"
    response = s3_client.list_objects_v2(
        Bucket=s3_backup_bucket_name, Prefix=prefix, Delimiter="/"
    )

    if "CommonPrefixes" not in response or not response["CommonPrefixes"]:
        logger.error(
            f"No backups found for bucket {bucket_name} in S3 bucket {s3_backup_bucket_name}"
        )
        return f"No backups found for {bucket_name}"

    # Sort prefixes by name (which includes timestamp) to get the latest
    latest_prefix = sorted(response["CommonPrefixes"], key=lambda x: x["Prefix"])[-1][
        "Prefix"
    ]
    logger.info(f"Found latest backup at {latest_prefix}")

    # Download the backup files
    logger.info(
        f"Downloading backup data from S3 bucket {s3_backup_bucket_name}/{latest_prefix}"
    )
    file_count = 0

    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=s3_backup_bucket_name, Prefix=latest_prefix):
        if "Contents" not in page:
            continue

        for obj in page["Contents"]:
            s3_key = obj["Key"]
            # Remove the bucket name and timestamp prefix to get the relative path
            relative_path = s3_key[len(latest_prefix) :]
            local_file_path = os.path.join(restore_path, relative_path)

            # Create directory structure if it doesn't exist
            os.makedirs(os.path.dirname(local_file_path), exist_ok=True)

            # Download the file
            s3_backup_bucket.download_file(s3_key, local_file_path)
            file_count += 1

            if file_count % 100 == 0:
                logger.info(f"Downloaded {file_count} files so far...")

    logger.info(f"Downloaded {file_count} files from S3")

    target_bucket_name = bucket_name

    # Use environment variables for influxdb_client to avoid tokens in process listings
    env = os.environ.copy()
    env["INFLUX_TOKEN"] = restore_token

    # Handle existing bucket based on options
    if force_replace:
        # Delete the bucket if it already exists
        logger.info(f"Deleting {target_bucket_name} in {restore_endpoint}")

        try:
            client = InfluxDBClient(
                url=restore_endpoint, token=restore_token, org=org_name
            )
            # Returns None if the bucket does not exist
            bucket = client.buckets_api().find_bucket_by_name(target_bucket_name)
            if bucket:
                client.buckets_api().delete_bucket(bucket)
                logger.info(f"Deleted existing bucket {target_bucket_name}")
            else:
                logger.info(
                    f"Bucket {target_bucket_name} does not exist, no need to delete"
                )
        except Exception as e:
            logger.error(f"Error during bucket deletion: {e}")
            # Raise the exception, since a restore will fail if the target bucket
            # already exists
            raise
    elif unique_restore_name:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        target_bucket_name = f"{bucket_name}_{timestamp}"
        logger.info(f"Will restore to new bucket: {target_bucket_name}")
    else:
        raise RuntimeError(
            "Restore failed: Neither force_replace nor unique_restore_name were set"
        )

    logger.info(f"Restoring data to {restore_endpoint} bucket {target_bucket_name}")

    # A subprocess command is necessary for restoring, as influxdb_client does not
    # have a restore method
    restore_command = [
        "influx",
        "restore",
        "--host",
        restore_endpoint,
        "--org",
        org_name,
        "--bucket",
        bucket_name,
        "--new-bucket",
        target_bucket_name,
        restore_path,
    ]

    try:
        subprocess.run(
            restore_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
            env=env,
        )
        logger.info("Restore command completed successfully")
    except subprocess.CalledProcessError as e:
        safe_error = (
            str(e.stderr).replace(restore_token, "****")
            if restore_token
            else str(e.stderr)
        )
        logger.error(f"Restore failed: {safe_error}")
        raise RuntimeError("Restore failed")

    # Remove data stored in EFS.
    # This data will persist if not removed.
    if os.path.exists(restore_path):
        try:
            shutil.rmtree(restore_path)
            logger.info(f"Removed {restore_path} directory")
        except Exception as e:
            logger.warning(f"Failed to remove {restore_path} directory: {str(e)}")

    return f"Successfully restored {bucket_name} to {target_bucket_name}"


if __name__ == "__main__":
    main()
