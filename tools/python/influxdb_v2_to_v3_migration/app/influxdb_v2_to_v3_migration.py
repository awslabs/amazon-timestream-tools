#!/usr/bin/env python3
"""
Script for backing up and restoring Timestream for InfluxDB data using ECS.
This script is designed to run as an ECS task and handles both backup and restore operations.
"""

import argparse
from concurrent.futures import InterpreterPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import shutil
import os
import subprocess
import logging
import sys
from influxdb_client.client.influxdb_client import InfluxDBClient
from influxdb_client.client.bucket_api import BucketsApi
from influxdb_client.domain.bucket import Bucket

import influxdb_v3_ingestion
import utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger: logging.Logger = logging.getLogger("influxdb_v2_to_v3_migration")


def parse_bucket_and_org_names(
    bucket_and_org_names: str, bucket_separator: str, bucket_org_separator: str
) -> list[tuple[str, ...]]:
    """
    Parses a string of buckets and their organization names separated first by a bucket separator and then a bucket and org separator.
    """
    return [
        tuple(bucket_org_pair.split(bucket_org_separator))
        for bucket_org_pair in bucket_and_org_names.split(bucket_separator)
    ]


def verify_required_subprocess_tools() -> bool:
    """
    Verifies that required subprocess tools are installed
    and in the user's PATH.

    Args:
        None

    Returns:
        bool: Whether all required subprocess tools are installed
        and in the user's PATH.
    """
    if shutil.which("influx") is None:
        logger.error("influx could not be found")
        return False

    if shutil.which("influxd") is None:
        logger.error("influxd could not be found")
        return False

    return True


def verify_timestamps(start_time: str | None, end_time: str | None) -> bool:
    if start_time is not None and end_time is not None:
        try:
            start_time_timestamp = datetime.fromisoformat(
                start_time.replace("Z", "+00:00")
            )
            end_time_timestamp = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
            if end_time_timestamp <= start_time_timestamp:
                logger.error("End timestamp is less than or equal to start timestamp")
                return False
        except ValueError:
            logger.error(
                f"Start timestamp {start_time} or end timestamp {end_time} is invalid"
            )
            return False

    elif start_time is not None:
        try:
            _ = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
        except ValueError:
            logger.error(f"Start timestamp {start_time} is invalid")
            return False

    elif end_time is not None:
        try:
            _ = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
        except ValueError:
            logger.error(f"End timestamp {end_time} is invalid")
            return False

    return True


def backup_influxdb_v2_buckets(
    influxdb_v2_url: str,
    influxdb_v2_token: str,
    bucket_org_pairs: list[tuple[str, ...]],
    backup_path: Path,
    num_backup_workers: int = 5,
) -> bool:
    """
    Backup InfluxDB v2 data to a local directory.
    """
    backup_path.mkdir(parents=True, exist_ok=True)
    results: list[str] = list()
    failed_buckets: list[tuple[str, ...]] = list()

    with InterpreterPoolExecutor(max_workers=num_backup_workers) as executor:
        futures = {
            executor.submit(
                backup_influxdb_v2_bucket,
                influxdb_v2_url,
                influxdb_v2_token,
                bucket_org_pair,
                str(backup_path),
            ): bucket_org_pair
            for bucket_org_pair in bucket_org_pairs
        }

        for future in as_completed(futures):
            pair = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing {pair}: {e}")
                failed_buckets.append(pair)

    for result in results:
        logger.info(result)

    if failed_buckets:
        logger.error(
            "Backups failed for %d bucket(s): %s",
            len(failed_buckets),
            ", ".join(map(str, failed_buckets)),
        )
        return False

    return True


def backup_influxdb_v2_bucket(
    influxdb_v2_url: str,
    influxdb_v2_token: str,
    bucket_org_pair: tuple[str, ...],
    backup_path: str,
) -> str:
    bucket_name, org_name = bucket_org_pair
    logger.info(f"Backing up {bucket_name}")

    # Use environment variables for the subprocess to avoid tokens in process listings.
    env: dict[str, str] = os.environ.copy()
    env["INFLUX_TOKEN"] = influxdb_v2_token

    # A subprocess command is necessary for backing up, as influxdb_client does not
    # have a backup method.
    bucket_backup_command: list[str] = [
        "influx",
        "backup",
        "--host",
        influxdb_v2_url,
        "--compression",
        "none",
        "--org",
        org_name,
        "--bucket",
        bucket_name,
        backup_path,
    ]

    try:
        _ = subprocess.run(
            bucket_backup_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
            env=env,
        )
        logger.info("Backup command completed successfully")
    except subprocess.CalledProcessError as e:
        error_message: str = f"Backup failed for bucket {bucket_name}: {e}"
        logger.error(error_message)
        raise RuntimeError(error_message)

    return f"Backed up {bucket_name} in org {org_name} to {backup_path}"


def export_influxdb_v2_buckets_to_lp(
    influxdb_v2_url: str,
    influxdb_v2_token: str,
    bucket_org_pairs: list[tuple[str, ...]],
    backup_path: Path,
    start_time: str | None = None,
    end_time: str | None = None,
    num_export_lp_workers: int = 5,
    lp_filename: str = "output.lp",
) -> tuple[bool, list[tuple[str, int]]]:
    if not backup_path.exists():
        raise RuntimeError(f"Backup path {backup_path} does not exist")

    bucket_name_id_pairs: list[tuple[str, int]] = list()
    failed_buckets: list[tuple[str, ...]] = list()

    with InterpreterPoolExecutor(max_workers=num_export_lp_workers) as executor:
        futures = {
            executor.submit(
                export_influxdb_v2_bucket_to_lp,
                influxdb_v2_url,
                influxdb_v2_token,
                bucket_org_pair,
                backup_path,
                start_time,
                end_time,
                lp_filename,
            ): bucket_org_pair
            for bucket_org_pair in bucket_org_pairs
        }

        for future in as_completed(futures):
            pair = futures[future]
            try:
                result = future.result()
                bucket_name_id_pairs.append(result)
            except Exception as e:
                logger.error(f"Error processing {pair}: {e}")
                failed_buckets.append(pair)

    if failed_buckets:
        logger.error(
            "Exporting to line protocol failed for %d bucket(s): %s",
            len(failed_buckets),
            ", ".join(map(str, failed_buckets)),
        )
        return False, bucket_name_id_pairs

    return True, bucket_name_id_pairs


def export_influxdb_v2_bucket_to_lp(
    influxdb_v2_url: str,
    influxdb_v2_token: str,
    bucket_org_pair: tuple[str, ...],
    backup_path: Path,
    start_time: str | None = None,
    end_time: str | None = None,
    lp_filename: str = "output.lp",
) -> tuple[str, int]:
    bucket_name, org_name = bucket_org_pair
    client: InfluxDBClient = InfluxDBClient(
        url=influxdb_v2_url, org=org_name, token=influxdb_v2_token
    )
    bucket_api: BucketsApi = client.buckets_api()
    bucket: Bucket = bucket_api.find_bucket_by_name(bucket_name)
    bucket_id: int = bucket.id

    lp_output_path = backup_path / Path(str(bucket_id)) / Path(lp_filename)

    # Backup path should always end with engine/data. influxd will
    # want the path to the engine directory only.
    engine_path = backup_path.parent

    logger.info(f"Exporting {bucket_name} to line protocol")

    # A subprocess command is necessary for exporting to line protocol,
    # as only the daemon can export to line protocol.
    export_lp_command: list[str] = [
        "influxd",
        "inspect",
        "export-lp",
        "--bucket-id",
        str(bucket_id),
        "--engine-path",
        str(engine_path),
        "--output-path",
        str(lp_output_path),
    ]

    if start_time is not None:
        export_lp_command.extend(["--start", start_time])
    if end_time is not None:
        export_lp_command.extend(["--end", end_time])

    try:
        _ = subprocess.run(
            export_lp_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=True,
        )
        logger.info("Export LP command completed successfully")
    except subprocess.CalledProcessError as e:
        logger.error(f"Exporting to line protocol failed: {e}")
        raise RuntimeError("Exporting to line protocol failed")

    logger.info(f"Exported {bucket_name} to {str(lp_output_path)}")
    return bucket_name, bucket_id


def main(input_args: list[str]) -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        prog="influxdb_v2_to_v3_migration",
        description="A script that migrates data from InfluxDB v2 to InfluxDB v3",
    )
    _ = parser.add_argument(
        "--influxdb-v2-url",
        help="The InfluxDB v2 URL to migrate from. Example: https://example.com:8086",
    )
    _ = parser.add_argument(
        "--influxdb-v3-url",
        help="The InfluxDB v3 URL to migrate to. Example: https://example.com:8181",
    )
    _ = parser.add_argument(
        "--backup-path-root",
        default="/",
        help=(
            "The root of the backup path. The backup directory must have the following "
            "structure and naming: 'engine/data/'. By default, the 'engine' directory is "
            "assumed to be in the root directory (/engine). This option allows this to be "
            "changed, for example, to '~' or '/home/ec2-user/some-directory'. If the 'engine' "
            "and 'data' directories do not exist, they will be created."
        ),
    )
    _ = parser.add_argument(
        "--influxdb-v2-buckets-and-orgs",
        help=(
            "A list of bucket names paired with the organization each bucket resides in. "
            "Example: 'bucket-one:org-one,bucket-two:org-two'. The separators used in this "
            "list can be changed with the --bucket-separator and --bucket-org-separator arguments."
        ),
    )
    _ = parser.add_argument(
        "--bucket-separator",
        default=",",
        help="The character used to separate buckets in the --influxdb-v2-buckets-and-orgs argument. Defaults to ','",
    )
    _ = parser.add_argument(
        "--bucket-org-separator",
        default=":",
        help="The character used to separate a bucket and its organization in the --influxdb-v2-buckets-and-orgs argument. Defaults to ':'",
    )
    _ = parser.add_argument(
        "--num-backup-workers",
        default=5,
        help="The number of workers to use in parallel to backup buckets from InfluxDB v2.",
    )
    _ = parser.add_argument(
        "--num-export-lp-workers",
        default=5,
        help="The number of workers to use in parallel to transform data to line protocol before ingestion to InfluxDB v3.",
    )
    _ = parser.add_argument(
        "--num-ingestion-workers",
        default=5,
        help="The number of workers to use in parallel to ingest data to InfluxDB v3.",
    )
    _ = parser.add_argument(
        "--start-time",
        required=False,
        help="Inclusive lower time bound in RFC 3339 format (e.g., '2026-01-01T00:00:00Z' for UTC or '2026-01-01T00:00:00-08:00' for PST).",
    )
    _ = parser.add_argument(
        "--end-time",
        required=False,
        help="Exclusive upper time bound in RFC 3339 format (e.g., '2026-01-01T00:00:00Z' for UTC or '2026-01-01T00:00:00-08:00' for PST).",
    )
    _ = parser.add_argument(
        "--tokens-secret-name",
        default=os.getenv("TOKEN_SECRET_NAME"),
        required=False,
        help=(
            "The name of the AWS Secret Manager secret in which both the InfluxDB v2 and v3 tokens have been placed. "
            "Defaults to the value of the TOKEN_SECRET_NAME environment variable."
        ),
    )
    _ = parser.add_argument(
        "--line-protocol-filename",
        default="output.lp",
        required=False,
        help=(
            "The name that all line protocol files share. A line protocol file is associated "
            "with a bucket by residing in a directory that uses a bucket's ID as its name. "
            "By default, this is 'output.lp'."
        ),
    )
    _ = parser.add_argument(
        "--influxdb-v3-database-retention-period",
        required=False,
        help=(
            "The retention period to use for all new InfluxDB v3 databases. Retention periods can be updated later. "
            "For example, '1d', '30m'. By default, this is infinity."
        ),
    )

    args = parser.parse_args(input_args)

    tokens_secret_name: str = args.tokens_secret_name
    influxdb_v2_url: str = args.influxdb_v2_url
    influxdb_v3_url: str = args.influxdb_v3_url
    start_time: str | None = args.start_time
    end_time: str | None = args.end_time
    num_backup_workers: int = args.num_backup_workers
    num_export_lp_workers: int = args.num_export_lp_workers
    line_protocol_filename: str = args.line_protocol_filename
    backup_path_root = args.backup_path_root
    bucket_org_pairs: list[tuple[str, ...]] = parse_bucket_and_org_names(
        args.influxdb_v2_buckets_and_orgs,
        args.bucket_separator,
        args.bucket_org_separator,
    )

    backup_path = Path(backup_path_root).expanduser() / Path("engine/data")

    if not verify_required_subprocess_tools():
        logger.error("Could not verify all required subprocess tools are installed")
        return 1

    if not verify_timestamps(start_time, end_time):
        logger.error("Could not verify that timestamps are valid")
        return 1

    try:
        tokens: dict[str, str] = utils.get_secret(tokens_secret_name)
        influxdb_v2_token: str | None = tokens.get("INFLUXDB_V2_TOKEN")
        influxdb_v3_token: str | None = tokens.get("INFLUXDB_V3_TOKEN")
        assert influxdb_v2_token is not None
        assert influxdb_v3_token is not None

        logger.info(
            f"Successfully retrieved tokens from Secrets Manager: {tokens_secret_name}"
        )
    except Exception as e:
        logger.error(f"Failed to retrieve tokens from Secrets Manager: {str(e)}")
        return 1

    logger.info(f"Using InfluxDB v2 URL: {influxdb_v2_url}")
    logger.info(f"Using InfluxDB v3 URL: {influxdb_v3_url}")

    backup_result: bool = backup_influxdb_v2_buckets(
        influxdb_v2_url,
        influxdb_v2_token,
        bucket_org_pairs,
        backup_path,
        num_backup_workers,
    )
    if not backup_result:
        logger.error("Backup failed")
        return 1

    utils.extract_all_tar_files_in_path(backup_path)

    export_lp_result: tuple[bool, list[tuple[str, int]]] = (
        export_influxdb_v2_buckets_to_lp(
            influxdb_v2_url,
            influxdb_v2_token,
            bucket_org_pairs,
            backup_path,
            start_time,
            end_time,
            num_export_lp_workers,
            line_protocol_filename,
        )
    )

    if not export_lp_result[0]:
        logger.error("Exporting to line protocol failed")
        return 1

    # Line protocol files are kept in directories using bucket IDs
    # as their names, so bucket IDs are required for later writing.
    # Bucket names are required for creating new databases in InfluxDB v3.
    bucket_name_id_pairs: list[tuple[str, int]] = export_lp_result[1]
    bucket_name_id_pairs_str: str = f"{args.bucket_separator}".join(
        f"{bucket_name}{args.bucket_org_separator}{bucket_id}"
        for bucket_name, bucket_id in bucket_name_id_pairs
    )

    influxdb_v3_ingestion_arguments: list[str] = [
        "--influxdb-v3-url",
        args.influxdb_v3_url,
        "--tokens-secret-name",
        args.tokens_secret_name,
        "--num-workers",
        str(args.num_ingestion_workers),
        "--influxdb-v2-buckets-and-ids",
        bucket_name_id_pairs_str,
        "--bucket-separator",
        args.bucket_separator,
        "--bucket-id-separator",
        args.bucket_org_separator,
        "--backup-path",
        str(backup_path),
        "--line-protocol-filename",
        args.line_protocol_filename,
        "--retention-period",
        args.influxdb_v3_database_retention_period,
    ]

    ingestion_result = influxdb_v3_ingestion.main(influxdb_v3_ingestion_arguments)

    if not ingestion_result:
        logger.error("Ingestion failed")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
