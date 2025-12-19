"""
Parallel processing of ingesting gzip line protocol files to InfluxDB.
This module extracts and ingests gzip files using multiple processes.
"""

from concurrent.futures import ProcessPoolExecutor, as_completed
from io import TextIOWrapper
from multiprocessing import current_process
import os
import sys
import argparse
import time
import logging
import random
import requests
from pathlib import Path
from influxdb_client.client.write_api import WriteApi
from influxdb_client_3 import (
    write_client_options,
    InfluxDBClient3,
    WriteOptions,
    WriteType,
)
from requests.models import HTTPError

import utils


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger: logging.Logger = logging.getLogger("influxdb_v3_ingestion")


# Custom exceptions for better error handling
class InfluxDBIngestionError(Exception):
    """Exception raised for errors during InfluxDB ingestion."""

    pass


class FileExtractionError(Exception):
    """Exception raised for errors during file extraction."""

    pass


def ingest_batch(
    client: InfluxDBClient3,
    batch: list[str],
    batch_id: str,
    process_name: str,
    max_retries: int,
) -> int:
    """
    Ingest a single batch of line protocol data into InfluxDB with retry logic.

    Args:
        write_api: InfluxDB write API client
        batch: List of lines to ingest
        batch_id: Identifier for the batch
        process_name: Name of the current process
        bucket_name: Name of the InfluxDB bucket
        max_retries: Maximum number of retry attempts

    Returns:
        int: Number of lines successfully ingested

    Raises:
        InfluxDBIngestionError: If the batch couldn't be ingested after max retries
    """
    retry_interval_ms: int = 5000
    max_retry_delay_ms: int = 30000
    exponential_base: int = 2

    batch_content: str = "".join(batch).rstrip()
    ingestion_attempt: int = 1

    batch_length = 0

    while ingestion_attempt <= max_retries:
        try:
            if ingestion_attempt == 1:
                logger.info(
                    f"{process_name} - Writing batch {batch_id} of {len(batch)} lines"
                )
            else:
                logger.warning(
                    f"{process_name} - Retry attempt {ingestion_attempt} for batch {batch_id}"
                )

            client.write(batch_content, write_precision="ns")

            logger.info(
                f"{process_name} - Successfully wrote batch {batch_id} on attempt #{ingestion_attempt}"
            )
            batch_length = len(batch)
            break

        except Exception as write_exc:
            ingestion_attempt += 1
            if ingestion_attempt > max_retries:
                logger.error(
                    f"{process_name} - Failed to write batch {batch_id} after {max_retries} retries: {write_exc}"
                )
                raise InfluxDBIngestionError(
                    f"Failed to write batch {batch_id} after {max_retries} retries"
                ) from write_exc

            # Calculate backoff with jitter
            delay_ms: int = min(
                retry_interval_ms * (exponential_base ** (ingestion_attempt - 1)),
                max_retry_delay_ms,
            )
            jitter_ms: int = int(delay_ms * 0.2)  # 20% jitter
            random_jitter: float = random.uniform(0, jitter_ms)
            delay_with_jitter: float = delay_ms + random_jitter

            logger.warning(
                f"{process_name} - Retry attempt for batch {batch_id} due to error: {write_exc}"
            )
            logger.warning(
                f"{process_name} - Waiting {delay_with_jitter / 1000:.2f} seconds before retry #{ingestion_attempt + 1}"
            )

            # Sleep before retry (convert ms to seconds)
            time.sleep(delay_with_jitter / 1000)
    return batch_length


def batch_outer_chunk(
    client: InfluxDBClient3,
    outer_chunk: list[str],
    lines_per_batch: int,
    process_name: str,
    line_count: int,
    max_retries: int,
) -> tuple[int, int]:
    """
    Batch a large outer chunk by breaking it into smaller batches for ingestion. Using a larger
    outer chunk reduces disk I/O when reading from the line protocol file.

    Args:
        write_api: InfluxDB write API client
        outer_chunk: List of lines read from file
        lines_per_batch: Number of lines to ingest in each batch
        process_name: Name of the current process
        line_count: Current line count for batch ID generation
        max_retries: Maximum number of retry attempts

    Returns:
        tuple: (total_lines_ingested, updated_line_count)

    Raises:
        InfluxDBIngestionError: If any batch fails to ingest
    """
    total_lines: int = 0
    current_line_count: int = line_count

    # Process the large chunk in smaller batches
    for i in range(0, len(outer_chunk), lines_per_batch):
        batch = outer_chunk[i : i + lines_per_batch]
        if batch:
            current_line_count += len(batch)
            batch_id: str = f"{process_name}-batch-{current_line_count}"

            lines_ingested = ingest_batch(
                client,
                batch,
                batch_id,
                process_name,
                max_retries,
            )
            total_lines += lines_ingested

    return total_lines, current_line_count


def read_outer_chunks(
    file_handle: TextIOWrapper, lines_per_batch: int, io_multiplier: int
) -> list[str]:
    """
    Read a chunk of lines from a file to optimize I/O operations.

    Args:
        file_handle: Open file handle to read from
        lines_per_batch: Number of lines in a single batch
        io_multiplier: How many batches to read at once

    Returns:
        list: List of non-empty lines read from the file
    """
    # Read multiple batches at once to reduce disk I/O
    outer_chunk = [
        file_handle.readline() for _ in range(lines_per_batch * io_multiplier)
    ]
    return [line for line in outer_chunk if line]


def ingest_line_protocol_files(
    influxdb_v3_url: str,
    influxdb_v3_token: str,
    backup_path: Path,
    bucket_id_pairs: list[tuple[str, str]],
    lines_per_batch: int = 10_000,
    io_multiplier: int = 10,
    max_retries: int = 20,
    num_workers: int = 5,
    lp_filename: str = "output.lp",
    retention_period: str | None = None,
) -> bool:
    if not os.path.isdir(backup_path):
        logger.error(f"Error: {backup_path} is not a valid directory")
        return False

    results: list[str] = list()
    failed_buckets: list[tuple[str, str]] = list()

    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(
                ingest_line_protocol_file,
                influxdb_v3_url,
                influxdb_v3_token,
                backup_path,
                bucket_id_pair,
                lines_per_batch,
                io_multiplier,
                max_retries,
                lp_filename,
                retention_period,
            ): bucket_id_pair
            for bucket_id_pair in bucket_id_pairs
        }

        for future in as_completed(futures):
            pair = futures[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Error processing {pair}: {e}")
                failed_buckets.append(pair)

    if failed_buckets:
        logger.error(
            "Ingesting line protocol failed for %d bucket(s): %s",
            len(failed_buckets),
            ", ".join(map(str, failed_buckets)),
        )
        return False

    return True


def ingest_line_protocol_file(
    influxdb_v3_url: str,
    influxdb_v3_token: str,
    backup_path: Path,
    bucket_name_id_pair: tuple[str, str],
    lines_per_batch: int = 10_000,
    io_multiplier: int = 10,
    max_retries: int = 20,
    lp_filename: str = "output.lp",
    retention_period: str | None = None,
) -> str:
    """
    Ingest a line protocol file into InfluxDB.

    Args:
        extracted_file_path: Path to the line protocol file to ingest
        lines_per_batch: Number of lines to ingest in each batch
        io_multiplier: Multiplier for I/O chunking optimization
        bucket_name: Name of the InfluxDB bucket
        max_retries: Maximum number of retry attempts

    Returns:
        int: The number of lines ingested

    Raises:
        InfluxDBIngestionError: If any batch fails to ingest
        IOError: If file operations fail
    """
    bucket_name, bucket_id = bucket_name_id_pair

    # Databases in InfluxDB v3 cannot contain underscores (_), must start
    # and begin with an alphanumeric character, and are allowed to use hyphens.
    database_name: str = bucket_name.replace("_", "-")

    lp_file_path: Path = backup_path / Path(bucket_id) / Path(lp_filename)

    response: requests.Response = requests.get(
        f"{influxdb_v3_url}/api/v3/configure/database?format=json",
        headers={"Authorization": f"Bearer {influxdb_v3_token}"},
    )
    try:
        response.raise_for_status()
    except HTTPError as e:
        error_message = f"Failed to list existing InfluxDB v3 databases: {e}"
        logger.error(error_message)
        raise RuntimeError(error_message)

    list_databases_response = response.json()

    if any(database_name in database.values() for database in list_databases_response):
        logger.info(f"Database {database_name} exists")
    else:
        logger.info(f"Creating database {database_name}")

        database_creation_body = {"db": database_name}
        if retention_period is not None:
            database_creation_body["retention_period"] = retention_period

        response = requests.post(
            f"{influxdb_v3_url}/api/v3/configure/database",
            headers={"Authorization": f"Bearer {influxdb_v3_token}"},
            json=database_creation_body,
        )
        try:
            response.raise_for_status()
        except HTTPError as e:
            error_message = f"Failed to create database {database_name}: {e.response}"
            logger.error(error_message)
            raise RuntimeError(error_message)

    process_name = current_process().name
    total_lines = 0
    line_count = 0

    with InfluxDBClient3(
        host=influxdb_v3_url, token=influxdb_v3_token, database=database_name
    ) as client:
        logger.info(f"Ingesting contents of {str(lp_file_path)}")
        with open(lp_file_path, "r", encoding="utf-8") as file_reader:
            while True:
                outer_chunk = read_outer_chunks(
                    file_reader, lines_per_batch, io_multiplier
                )

                if not outer_chunk:
                    break

                lines_ingested, line_count = batch_outer_chunk(
                    client,
                    outer_chunk,
                    lines_per_batch,
                    process_name,
                    line_count,
                    max_retries,
                )
                total_lines += lines_ingested

    return f"Ingested {total_lines} into {database_name}"


def main(input_args: list[str]) -> int:
    _ = parser = argparse.ArgumentParser(
        description="Ingest line protocol files from an InfluxDB v2 engine directory into InfluxDB v3 using multiple processes."
    )
    _ = parser.add_argument(
        "--influxdb-v3-url",
        help="The InfluxDB v3 URL to ingest data into. Example: 'https://example.com:8181'.",
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
        "--num-workers",
        type=int,
        default=10,
        help="Number of workers to use (default: 10)",
    )
    _ = parser.add_argument(
        "-l",
        "--lines",
        type=int,
        default=10000,
        help="Number of lines for a batch (default: 10000)",
    )
    _ = parser.add_argument(
        "-m",
        "--multiplier",
        type=int,
        default=10,
        help="I/O multiplier: how many batches to read at once (default: 10)",
    )
    _ = parser.add_argument(
        "-r",
        "--retries",
        type=int,
        default=20,
        help="Maximum number of retry attempts for failed batches (default: 20)",
    )
    _ = parser.add_argument(
        "--influxdb-v2-buckets-and-ids",
        help=(
            "A list of bucket names paired with their IDs. "
            "Example: 'bucket-one:12fzmskpe435,bucket-two:shmflq24jaml3'. The separators used in this "
            "list can be changed with the --bucket-separator and --bucket-id-separator arguments."
        ),
    )
    _ = parser.add_argument(
        "--bucket-separator",
        default=",",
        help="The character used to separate buckets in the --influxdb-v2-buckets-and-ids argument. Defaults to ','",
    )
    _ = parser.add_argument(
        "--bucket-id-separator",
        default=":",
        help="The character used to separate a bucket and its ID in the --influxdb-v2-buckets-and-ids argument. Defaults to ':'",
    )
    _ = parser.add_argument(
        "--backup-path",
        help=(
            "The the backup path where line protocol data exists. "
            "This path must end with engine/data, for example: '~/engine/data'. "
            "Within the backup path, line protocol files (.lp) must be "
            "within directories that use bucket IDs as names."
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
        "--retention-period",
        required=False,
        help=(
            "The retention period to use for all new databases. Retention periods can be updated later. "
            "For example, '1d', '30m'. By default, this is infinity."
        ),
    )

    args = parser.parse_args(input_args)
    tokens_secret_name: str = args.tokens_secret_name

    try:
        tokens: dict[str, str] = utils.get_secret(tokens_secret_name)
        influxdb_v3_token: str | None = tokens.get("INFLUXDB_V3_TOKEN")
        assert influxdb_v3_token is not None

        logger.info(
            f"Successfully retrieved tokens from Secrets Manager: {tokens_secret_name}"
        )
    except Exception as e:
        logger.error(f"Failed to retrieve tokens from Secrets Manager: {str(e)}")
        return 1

    backup_path = Path(args.backup_path).expanduser()

    print(args.influxdb_v2_buckets_and_ids)

    bucket_id_pairs: list[tuple[str, str]] = [
        (bucket_name, bucket_id)
        for bucket_name, bucket_id in (
            pair.split(args.bucket_id_separator, 1)
            for pair in args.influxdb_v2_buckets_and_ids.split(args.bucket_separator)
        )
    ]

    ingestion_result = ingest_line_protocol_files(
        args.influxdb_v3_url,
        influxdb_v3_token,
        backup_path,
        bucket_id_pairs,
        args.lines,
        args.multiplier,
        args.retries,
        args.num_workers,
        args.line_protocol_filename,
        args.retention_period,
    )

    if not ingestion_result:
        logger.error("Ingestion failed")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
