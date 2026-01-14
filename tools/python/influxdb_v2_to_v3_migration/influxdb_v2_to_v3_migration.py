#!/usr/bin/env python3
"""
Script for migrating InfluxDB v2 bucket data to InfluxDB v3.
"""

import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
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
from influxdb_client.domain.buckets import Buckets
import httpx

import influxdb_v3_ingestion
import utils

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger: logging.Logger = logging.getLogger("influxdb_v2_to_v3_migration")


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
    """
    Verifies that start and/or end timestamp strings are valid RFC 3339 timestamps.

    Args:
        start_time (str | None): The start time.
        end_time (str | None): The end time. Must be later than start_time.

    Returns:
        bool: Whether start_time and/or end_time are valid RFC 3339 timestamps.
    """
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


def health_check(host: str, token: str) -> bool:
    """
    Pings an InfluxDB instance to determine health. Compatible with InfluxDB v2 and v3.

    Args:
        host (str): The address of the host to ping.
        token (str): The token for the instance.

    Returns:
        bool: Whether the InfluxDB v2 or v3 instance can be reached.
    """
    # Ensure host can be connected to.
    logger.info(f"Checking connectivity to {host}")
    health_check_response = httpx.get(
        f"{host}/health",
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
    )
    try:
        _ = health_check_response.raise_for_status()
    except httpx.HTTPStatusError as error:
        logger.error(str(error))
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
    Backs up InfluxDB v2 data from multiple buckets to a local directory using the InfluxDB v2 CLI.

    Args:
        influxdb_v2_url (str): The InfluxDB v2 URL, including scheme and port.
        influxdb_v2_token (str): The InfluxDB v2 token.
        bucket_org_pairs (list[tuple[str, ...]]): A list of bucket names and their organization names to back up. For example, [("bucket-one", "org-one"), ("bucket-two", "org-two")].
        backup_path (Path): The path to backup data to.
        num_backup_workers (int): The number of workers to use to back up data in parallel. Defaults to 5.

    Returns:
        bool: Whether all backups succeeded.
    """
    backup_path.mkdir(parents=True, exist_ok=True)
    results: list[str] = list()
    failed_buckets: list[tuple[str, ...]] = list()

    with ProcessPoolExecutor(max_workers=num_backup_workers) as executor:
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
    """
    Backs up a single InfluxDB v2 bucket to a local directory using the InfluxDB v2 CLI.

    Args:
        influxdb_v2_url (str): The InfluxDB v2 URL, including scheme and port.
        influxdb_v2_token (str): The InfluxDB v2 token.
        bucket_org_pair (tuple[str, ...]): A bucket name and org pair to back up. For example, ("bucket-one", "org-one").
        backup_path (str): The path to backup data to.

    Returns:
        str: A success message.

    Raises:
        RuntimeError: If the backup fails.
    """
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

    subprocess_error_output: str = ""
    try:
        call_result = subprocess.run(
            bucket_backup_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=False,
            env=env,
        )
        subprocess_error_output = call_result.stderr
        call_result.check_returncode()
        logger.info("Backup command completed successfully")
    except subprocess.CalledProcessError as e:
        error_message: str = (
            f"Backup failed for bucket {bucket_name}: {e}:\n{subprocess_error_output}"
        )
        logger.error(error_message)
        raise RuntimeError(error_message)

    return f"Backed up {bucket_name} in org {org_name} to {backup_path}"


def export_influxdb_v2_buckets_to_lp(
    influxdb_v2_url: str,
    influxdb_v2_token: str,
    bucket_org_pairs: list[tuple[str, str]],
    backup_path: Path,
    start_time: str | None = None,
    end_time: str | None = None,
    num_export_lp_workers: int = 5,
) -> tuple[bool, list[tuple[str, str]]]:
    """
    Exports backed up InfluxDB v2 bucket data from multiple buckets to line protocol in parallel
    using the InfluxDB v2 daemon (influxd).

    Args:
        influxdb_v2_url (str): The InfluxDB v2 URL, including scheme and port.
        influxdb_v2_token (str): The InfluxDB v2 token.
        bucket_org_pairs (list[tuple[str, str]]): A list of bucket names and their organization names
            to export to line protocol. For example, [("bucket-one", "org-one"), ("bucket-two", "org-two")].
        backup_path (Path): The path where the backed up data resides.
        start_time (str | None): The start time of the backed up data to export. Must be a valid RFC 3339 timestamp.
        end_time (str | None): The end time of the backed up data to export. Must be a valid RFC 3339 timestamp.
        num_export_lp_workers (int): The number of workers to use to export data to line protocol in
            parallel. Defaults to 5.
        lp_filename (str): The filename to use for all exported line protocol files. Data is differenciated by
            the directories they reside in, which correspond to bucket IDs.

    Returns:
        tuple[bool, list[tuple[str, str]]]: A tuple containing a boolean value, indicating whether all data was
            exported successfully, and a list of (str, str) tuples, bucket names and their IDs, for example,
            ("bucket-one", "0af435lsdjfm").
    """
    if not backup_path.exists():
        raise RuntimeError(f"Backup path {backup_path} does not exist")

    bucket_name_id_pairs: list[tuple[str, str]] = list()
    failed_buckets: list[tuple[str, str]] = list()

    with ProcessPoolExecutor(max_workers=num_export_lp_workers) as executor:
        futures = {
            executor.submit(
                export_influxdb_v2_bucket_to_lp,
                influxdb_v2_url,
                influxdb_v2_token,
                bucket_org_pair,
                backup_path,
                start_time,
                end_time,
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
    bucket_org_pair: tuple[str, str],
    backup_path: Path,
    start_time: str | None = None,
    end_time: str | None = None,
) -> tuple[str, str]:
    """
    Exports backed up InfluxDB v2 bucket data from a single bucket to line protocol using
    the InfluxDB v2 daemon (influxd).

    Args:
        influxdb_v2_url (str): The InfluxDB v2 URL, including scheme and port.
        influxdb_v2_token (str): The InfluxDB v2 token.
        bucket_org_pair (tuple[str, str]): Tuple containing a bucket name and its organization name
            to export to line protocol. For example, ("bucket-one", "org-one").
        backup_path (Path): The path where the backed up data resides.
        start_time (str | None): The start time of the backed up data to export. Must be a valid RFC 3339 timestamp.
        end_time (str | None): The end time of the backed up data to export. Must be a valid RFC 3339 timestamp.

    Returns:
        tuple[str, str]: A tuple of a bucket name and its ID. For example,
            ("bucket-one", "0af435lsdjfm").

    Raises:
        RuntimeError: If exporting to line protocol fails.
    """
    bucket_name, org_name = bucket_org_pair
    client: InfluxDBClient = InfluxDBClient(
        url=influxdb_v2_url, org=org_name, token=influxdb_v2_token
    )
    bucket_api: BucketsApi = client.buckets_api()
    bucket: Bucket | None = bucket_api.find_bucket_by_name(bucket_name)
    if bucket is None:
        raise RuntimeError(f"Could not find bucket {bucket_name} in org {org_name}")
    bucket_id: str = bucket.id

    lp_output_path = backup_path / Path(str(bucket_id)) / Path(f"{bucket_name}.lp")

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
        bucket_id,
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
    return (bucket_name, bucket_id)


def get_buckets_from_orgs(
    influxdb_v2_url: str, influxdb_v2_token: str, orgs_str: str, org_separator: str
) -> list[tuple[str, str]]:
    """
    Given a list of org names as a string, retrieves the names of all buckets in
    an organization and produces list of bucket name and org names.

    Args:
        influxdb_v2_url (str): The InfluxDB v2 URL to use.
        influxdb_v2_token (str): The InfluxDB v2 token to use.
        orgs_str (str): A list of InfluxDB v2 organization names to retrieve all
            buckets from as a single string.
        separator (str): The separator used to separate org names in orgs_str.
            For example, ','.

    Returns:
        list[tuple[str, str]]: A list of bucket names and organization name pairs.
    """
    bucket_org_pairs: list[tuple[str, str]] = []
    for org_name in orgs_str.split(org_separator):
        client: InfluxDBClient = InfluxDBClient(
            url=influxdb_v2_url, org=org_name, token=influxdb_v2_token
        )
        bucket_api: BucketsApi = client.buckets_api()
        buckets: Buckets = bucket_api.find_buckets(org=org_name)
        buckets_list: list[Bucket] | None = buckets.buckets
        if buckets_list is not None:
            for bucket in buckets_list:
                # Skip system buckets.
                if not bucket.name.startswith("_"):
                    bucket_org_pairs.append((bucket.name, org_name))

    print(bucket_org_pairs)
    return bucket_org_pairs


def main(input_args: list[str]) -> int:
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        prog="influxdb_v2_to_v3_migration",
        description="A script that migrates data from InfluxDB v2 to InfluxDB v3",
    )
    _ = parser.add_argument(
        "--influxdb-v2-url",
        required=True,
        help="The InfluxDB v2 URL to migrate from. Example: https://example.com:8086",
    )
    _ = parser.add_argument(
        "--influxdb-v3-url",
        required=True,
        help="The InfluxDB v3 URL to migrate to. Example: https://example.com:8181",
    )
    _ = parser.add_argument(
        "--backup-path-root",
        required=False,
        default="~",
        help=(
            "The root of the backup path. The backup directory must have the following "
            "structure and naming: 'engine/data/'. By default, the 'engine' directory is "
            "assumed to be in the home directory (~/engine). This option allows this to be "
            "changed, for example, to '/', '/tmp', or '/home/ec2-user/some-directory'. If the 'engine' "
            "and 'data' directories do not exist, they will be created."
        ),
    )
    _ = parser.add_argument(
        "--bucket-separator",
        required=False,
        default=",",
        help="The character used to separate buckets in the --influxdb-v2-buckets-and-orgs argument. Defaults to ','",
    )
    _ = parser.add_argument(
        "--org-separator",
        required=False,
        default=",",
        help="The character used to separate buckets in the --influxdb-v2-orgs argument. Defaults to ','",
    )
    _ = parser.add_argument(
        "--bucket-org-separator",
        required=False,
        default=":",
        help="The character used to separate a bucket and its organization in the --influxdb-v2-buckets-and-orgs argument. Defaults to ':'",
    )
    _ = parser.add_argument(
        "--num-backup-workers",
        required=False,
        default=5,
        help="The number of workers to use in parallel to backup buckets from InfluxDB v2.",
    )
    _ = parser.add_argument(
        "--num-export-lp-workers",
        required=False,
        default=5,
        help="The number of workers to use in parallel to transform data to line protocol before ingestion to InfluxDB v3.",
    )
    _ = parser.add_argument(
        "--num-ingestion-workers",
        required=False,
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
        required=True,
        help=(
            "The name of the AWS Secret Manager secret in which both the InfluxDB v2 and v3 tokens have been placed. "
            "Defaults to the value of the TOKEN_SECRET_NAME environment variable."
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
    _ = parser.add_argument(
        "--region",
        default="us-west-2",
        required=False,
        help="The AWS region to use for AWS Secrets Manager.",
    )
    mutually_exclusive_group = parser.add_mutually_exclusive_group(required=True)
    _ = mutually_exclusive_group.add_argument(
        "--influxdb-v2-buckets-and-orgs",
        help=(
            "A list of bucket names paired with the organization each bucket resides in. "
            "Example: 'bucket-one:org-one,bucket-two:org-two'. The separators used in this "
            "list can be changed with the --bucket-separator and --bucket-org-separator arguments."
        ),
    )
    _ = mutually_exclusive_group.add_argument(
        "--influxdb-v2-orgs",
        help=(
            "A list of organization names from which to migrate all buckets. "
            "Example: org-one,org-two,org-three"
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
    backup_path_root: str = args.backup_path_root
    region_name: str = args.region

    backup_path: Path = Path(backup_path_root).expanduser() / Path("engine/data")

    if not verify_required_subprocess_tools():
        logger.error("Could not verify all required subprocess tools are installed")
        return 1

    if not verify_timestamps(start_time, end_time):
        logger.error("Could not verify that timestamps are valid")
        return 1

    try:
        tokens: dict[str, str] = utils.get_secret(
            secret_name=tokens_secret_name, region_name=region_name
        )
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

    if not health_check(influxdb_v2_url, influxdb_v2_token):
        logger.error("Unable to reach InfluxDB v2 instance")
        return 1
    else:
        logger.info("InfluxDB v2 instance is reachable")

    if not health_check(influxdb_v3_url, influxdb_v3_token):
        logger.error("Unable to reach InfluxDB v3 instance")
        return 1
    else:
        logger.info("InfluxDB v3 instance is reachable")

    if args.influxdb_v2_orgs is not None:
        bucket_org_pairs: list[tuple[str, str]] = get_buckets_from_orgs(
            influxdb_v2_url,
            influxdb_v2_token,
            args.influxdb_v2_orgs,
            args.org_separator,
        )

    if args.influxdb_v2_buckets_and_orgs is not None:
        bucket_org_pairs: list[tuple[str, str]] = [
            (bucket_name, org_name)
            for bucket_name, org_name in (
                pair.split(args.bucket_org_separator, 1)
                for pair in args.influxdb_v2_buckets_and_orgs.split(
                    args.bucket_separator
                )
            )
        ]

    backup_result: bool = backup_influxdb_v2_buckets(
        influxdb_v2_url,
        influxdb_v2_token,
        bucket_org_pairs,
        backup_path,
        num_backup_workers,
    )

    if backup_result:
        logger.info("Backing up bucket data complete")
    if not backup_result:
        logger.error("Backup failed")
        return 1

    utils.extract_all_tar_files_in_path(backup_path)

    export_lp_result: tuple[bool, list[tuple[str, str]]] = (
        export_influxdb_v2_buckets_to_lp(
            influxdb_v2_url,
            influxdb_v2_token,
            bucket_org_pairs,
            backup_path,
            start_time,
            end_time,
            num_export_lp_workers,
        )
    )

    if export_lp_result[0]:
        logger.info("Exporting all bucket data to line protocol complete")
    else:
        if not export_lp_result[1]:
            logger.error("Exporting to line protocol failed")
            return 1
        else:
            # Exporting to line protocol can fail for some buckets and not others.
            # Empty buckets will fail during line protocol exporting.
            logger.warning(
                f"Partial failure during line protocol exporting detected. Continuing migration of {len(export_lp_result[1])} buckets"
            )

    ingestion_result: bool = influxdb_v3_ingestion.ingest_line_protocol_files(
        influxdb_v3_url=args.influxdb_v3_url,
        influxdb_v3_token=influxdb_v3_token,
        backup_path=backup_path,
        bucket_id_pairs=export_lp_result[1],
        num_workers=args.num_ingestion_workers,
        retention_period=args.influxdb_v3_database_retention_period,
    )

    if ingestion_result:
        logger.info("Ingesting line protocol files complete")
    else:
        logger.error("Ingestion failed")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
