"""
Parallel processing of ingesting gzip line protocol files to InfluxDB.
This module extracts and ingests gzip files using multiple processes.
"""

import os
import sys
import gzip
import shutil
import argparse
import time
import logging
import random
import multiprocessing
from multiprocessing import Pool, current_process, Value
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from unload.utils.logger_utils import update_logger

from dotenv import load_dotenv

ingestion_logger = logging.getLogger("ingestion")

# Custom exceptions for better error handling
class InfluxDBIngestionError(Exception):
    """Exception raised for errors during InfluxDB ingestion."""
    pass


class FileExtractionError(Exception):
    """Exception raised for errors during file extraction."""
    pass


# Check that all environment variables have been set
def check_required_env_vars(skip_bucket_check):
    required_vars = [
        'INFLUXDB_V2_URL',
        'INFLUXDB_V2_TOKEN'
    ]
    if not skip_bucket_check:
        required_vars.append('INFLUXDB_V2_ORG')

    missing_vars = []
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)

    if missing_vars:
        print(f"Error: The following required environment variables are not set: {', '.join(missing_vars)}")
        sys.exit(1)

    return True


def check_bucket_exists(bucket_name):
    """
    Check if the specified InfluxDB bucket exists.

    Args:
        bucket_name: Name of the InfluxDB bucket to check

    Returns:
        bool: True if bucket exists, else False
    """
    client = None
    try:
        client = InfluxDBClient.from_env_properties()
        buckets_api = client.buckets_api()
        buckets = buckets_api.find_buckets().buckets

        bucket_exists = any(bucket.name == bucket_name for bucket in buckets)

        if bucket_exists:
            ingestion_logger.info(f"Bucket '{bucket_name}' exists. Proceeding with ingestion.")
            return True
        else:
            ingestion_logger.error(f"Bucket '{bucket_name}' does not exist. Create the bucket before ingestion.")
            return False
    except Exception as e:
        ingestion_logger.error(f"Error checking if bucket '{bucket_name}' exists: {e}")
        return False
    finally:
        if client:
            client.close()


def check_org_exists():
    """
    Check if InfluxDB organization exists.

    Returns:
        bool: True if organization exists, else False
    """
    try:
        client = InfluxDBClient.from_env_properties()
        org_api = client.organizations_api()
        orgs = org_api.find_organizations(org=client.org)
        if orgs:
            ingestion_logger.info("Organization '%s' exists – proceeding.", client.org)
            return True
        return False
    except Exception as exc:
        ingestion_logger.error("Error checking if organization '%s' exists: %s", client.org, exc)
        return False
    finally:
        if client:
            client.close()

def update_tracking_file(file_path, file_name, success=True):
    """
    Update the tracking file with the ingested file name.

    Args:
        file_path: Path to the tracking file
        file_name: Name of the ingested file to record
        success: Boolean indicating if the file was ingested successfully
    """
    try:
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(f"{file_name}\n")
            f.flush()
        log_level = logging.INFO if success else logging.ERROR
        log_message = f"{'Successfully ingested' if success else 'Failed to ingest'}: {file_name}"
        ingestion_logger.log(log_level, log_message)
    except Exception as e:
        ingestion_logger.error(f"Error updating tracking file {file_path}: {e}")


def find_latest_ingested_files(tracking_directory):
    """
    Reads the successfuly ingested files list from a tracking directory.

    Args:
        tracking_directory: Directory containing ingested_files.txt

    Returns:
        set: Set of filenames that were successfully ingested in a previous run
    """
    ingested_files = set()
    prev_success_log = os.path.join(tracking_directory, "ingested_files.txt")
    if os.path.exists(prev_success_log):
        try:
            with open(prev_success_log, 'r', encoding='utf-8') as f:
                ingested_files = set(line.strip() for line in f)
            ingestion_logger.info(f"Found {len(ingested_files)} previously ingested files in {prev_success_log}")
        except Exception as e:
            ingestion_logger.error(f"Error reading previous successful files: {e}")
    else:
        ingestion_logger.info(f"No successful files log found in {tracking_directory}")
    return ingested_files


def decompress_gzip_file(gz_file_path):
    """
    Decompress a gzip file to the temporary .line file.

    Args:
        gz_file_path: Path to the gzip file to decompress

    Returns:
        str: Path to the extracted file

    Raises:
        FileExtractionError: If decompression fails
    """
    # Remove .gz extension and add .line
    extracted_file_path = gz_file_path[:-3] if gz_file_path.endswith('.gz') else f"{gz_file_path}.line"
    ingestion_logger.info(f"Extracting {gz_file_path} to {extracted_file_path}")
    try:
        with gzip.open(gz_file_path, 'rb') as f_in:
            with open(extracted_file_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        return extracted_file_path
    except Exception as exc:
        ingestion_logger.error(f"Error decompressing file {gz_file_path}: {exc}")
        raise FileExtractionError(f"Failed to decompress {gz_file_path}") from exc


def ingest_batch(write_api, batch, batch_id, process_name, bucket_name, max_retries, precision):
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
    retry_interval_ms = 5000
    max_retry_delay_ms = 30000
    exponential_base = 2

    batch_content = ''.join(batch).rstrip()
    ingestion_attempt = 1

    while ingestion_attempt <= max_retries:
        try:
            if ingestion_attempt == 1:
                ingestion_logger.info(f"{process_name} - Writing batch {batch_id} of {len(batch)} lines")
            else:
                ingestion_logger.warning(f"{process_name} - Retry attempt {ingestion_attempt} for batch {batch_id}")

            write_api.write(bucket=bucket_name, record=batch_content, write_precision=precision)

            ingestion_logger.info(f"{process_name} - Successfully wrote batch {batch_id} on attempt #{ingestion_attempt}")
            return len(batch)

        except Exception as write_exc:
            ingestion_attempt += 1
            if ingestion_attempt > max_retries:
                ingestion_logger.error(f"{process_name} - Failed to write batch {batch_id} after {max_retries} retries: {write_exc}")
                raise InfluxDBIngestionError(f"Failed to write batch {batch_id} after {max_retries} retries") from write_exc

            # Calculate backoff with jitter
            delay_ms = min(retry_interval_ms * (exponential_base ** (ingestion_attempt - 1)), max_retry_delay_ms)
            jitter_ms = int(delay_ms * 0.2)  # 20% jitter
            random_jitter = random.uniform(0, jitter_ms)
            delay_with_jitter = delay_ms + random_jitter

            ingestion_logger.warning(f"{process_name} - Retry attempt for batch {batch_id} due to error: {write_exc}")
            ingestion_logger.warning(f"{process_name} - Waiting {delay_with_jitter/1000:.2f} seconds before retry #{ingestion_attempt +1}")

            # Sleep before retry (convert ms to seconds)
            time.sleep(delay_with_jitter / 1000)


def batch_outer_chunk(write_api, outer_chunk, lines_per_batch, process_name, line_count, bucket_name, max_retries, precision):
    """
    Batch a large outer chunk by breaking it into smaller batches for ingestion. Using a larger
    outer chunk reduces disk I/O when reading from the line protocol file.

    Args:
        write_api: InfluxDB write API client
        outer_chunk: List of lines read from file
        lines_per_batch: Number of lines to ingest in each batch
        process_name: Name of the current process
        line_count: Current line count for batch ID generation
        bucket_name: Name of the InfluxDB bucket
        max_retries: Maximum number of retry attempts

    Returns:
        tuple: (total_lines_ingested, updated_line_count)

    Raises:
        InfluxDBIngestionError: If any batch fails to ingest
    """
    total_lines = 0
    current_line_count = line_count

    # Process the large chunk in smaller batches
    for i in range(0, len(outer_chunk), lines_per_batch):
        batch = outer_chunk[i:i + lines_per_batch]
        if batch:
            current_line_count += len(batch)
            batch_id = f"{process_name}-batch-{current_line_count}"

            lines_ingested = ingest_batch(write_api, batch, batch_id, process_name, bucket_name, max_retries, precision)
            total_lines += lines_ingested

    return total_lines, current_line_count


def read_outer_chunks(file_handle, lines_per_batch, io_multiplier):
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
    outer_chunk = [file_handle.readline() for _ in range(lines_per_batch * io_multiplier)]
    return [line for line in outer_chunk if line]


def ingest_line_protocol_file(extracted_file_path, lines_per_batch, io_multiplier, bucket_name, max_retries, precision):
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
    process_name = current_process().name
    total_lines = 0
    line_count = 0

    with InfluxDBClient.from_env_properties() as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        ingestion_logger.info(f"ingesting contents of {extracted_file_path}")
        with open(extracted_file_path, 'r', encoding='utf-8') as f:
            while True:
                outer_chunk = read_outer_chunks(f, lines_per_batch, io_multiplier)

                if not outer_chunk:
                    break

                lines_ingested, line_count = batch_outer_chunk(
                    write_api, outer_chunk, lines_per_batch, process_name, line_count, bucket_name, max_retries, precision
                )
                total_lines += lines_ingested

    return total_lines


def ingest_gzip_file(gz_file_path, lines_per_batch, io_multiplier, bucket_name, max_retries, precision, logs_dir):
    """
    Ingest a gzip file to InfluxDB.

    Args:
        gz_file_path: Path to the gzip file to ingest
        lines_per_batch: Number of lines to ingest in each batch
        io_multiplier: Multiplier for batches read from file at a time
        bucket_name: Name of the InfluxDB bucket
        max_retries: Maximum number of retry attempts
        precision: Timestamp precision for InfluxDB write

    Returns:
        int: The number of lines ingested

    Raises:
        Exception: If any error occurs during ingestion
    """
    update_logger(ingestion_logger, logs_dir, f"worker-{current_process().pid}.log")
    process_name = current_process().name
    start_time = time.time()
    total_lines = 0
    file_name = os.path.basename(gz_file_path)
    extracted_file_path = None

    extracted_file_path = decompress_gzip_file(gz_file_path)

    total_lines = ingest_line_protocol_file(extracted_file_path, lines_per_batch, io_multiplier, bucket_name, max_retries, precision)

    duration = time.time() - start_time
    lines_per_sec = total_lines/duration if duration > 0 else 0
    ingestion_logger.info(
        "Process %s finished ingesting %s: %d lines in %.2f seconds (%.2f lines/sec)",
        process_name, file_name, total_lines, duration, lines_per_sec
    )

    if extracted_file_path and os.path.exists(extracted_file_path):
        try:
            os.remove(extracted_file_path)
            ingestion_logger.info("%s deleted.", extracted_file_path)
        except Exception as exc:
            ingestion_logger.error("Error deleting extracted file %s: %s", extracted_file_path, exc)


    return total_lines

def poll_for_result(result, failure_flag, continue_on_error, failed_log, file_name, poll_interval=5.0):
    """
    Poll for a result from an async task, checking for failures between attempts.

    Args:
        result: AsyncResult object to poll
        failure_flag: Shared Value to check for failures during ingestion
        continue_on_error: Whether to continue on error
        failed_log: Failed file ingestion log
        file_name: File being ingestion
        poll_interval: How often to check for results (seconds)

    Returns:
        The result of the async task or 0 if there was an error and continue_on_error is True

    Raises:
        Exception: If a failure is detected and continue_on_error is False
    """
    while True:
        try:
            return result.get(timeout=poll_interval)
        except multiprocessing.TimeoutError:
            if failure_flag.value == 1:
                update_tracking_file(failed_log, file_name, success=False)
                if continue_on_error:
                    ingestion_logger.warning(f"Error detected in file {file_name}, but continuing with next file")
                    return 0
                else:
                    ingestion_logger.error("Stopping ingestion due to error in file %s.", file_name)
                    ingestion_logger.error("Fix the issue and run with --resume-from with the path to the previous tracking folder.")
                    sys.exit(1)
            continue
            # We also need this exception block to handle InfluxDBIngestionError
        except Exception as e:
            update_tracking_file(failed_log, file_name, success=False)
            if continue_on_error:
                ingestion_logger.warning(f"Continuing with next file after error {e} in {file_name}")
                return 0
            else:
                ingestion_logger.error(f"Stopping ingestion due to error {e} in file {file_name}.")
                ingestion_logger.error("Fix the issue and run with --resume-from with the path to the previous tracking folder.")
                sys.exit(1)

def main(input_args):
    load_dotenv()

    parser = argparse.ArgumentParser(description='Process gzip files in a directory using multiple processes')
    parser.add_argument('bucket', help='InfluxDB bucket name')
    parser.add_argument('data_directory', help='Directory containing .gz files')
    parser.add_argument('-w', '--workers', type=int, default=10,
                        help='Number of workers to use (default: 10)')
    parser.add_argument('-l', '--lines', type=int, default=10000,
                        help='Number of lines for a batch (default: 10000)')
    parser.add_argument('-m', '--multiplier', type=int, default=10,
                        help='I/O multiplier: how many batches to read at once (default: 10)')
    parser.add_argument('-r', '--retries', type=int, default=20,
                        help='Maximum number of retry attempts for failed batches (default: 20)')
    parser.add_argument('--logs-dir', type=str, default='influxdb-ingestion-logs',
                        help='Directory for ingestion logs (default: influxdb-ingestion-logs)')
    parser.add_argument('--resume-from', type=str,
                        help='Resume from a previous run, providing the path to the previous tracking_<run_id> directory')
    parser.add_argument('--continue-on-error', action='store_true',
                        help='Continue ingesting remaining files even if one fails')
    parser.add_argument('-p', '--precision', type=str, default='ns',
                        choices=['ns', 'ms', 'us', 's'],
                        help='Timestamp precision for InfluxDB write. Note that this must align with the timestamp precision from the data being ingested (default: ns)')
    parser.add_argument('--skip-bucket-check', action='store_true',
                        help='Skips bucket and organization check (for ingestions to V3)')
    args = parser.parse_args(input_args)

    log_file_name = f'ingestion_{time.strftime("%Y%m%d_%H%M%S")}.log'
    update_logger(ingestion_logger, args.logs_dir, log_file_name)

    check_required_env_vars(args.skip_bucket_check)
    if not args.skip_bucket_check and (not check_bucket_exists(args.bucket) or not check_org_exists()):
        sys.exit(1)

    if not os.path.isdir(args.data_directory):
        ingestion_logger.error(f"Error: {args.data_directory} is not a valid directory")
        sys.exit(1)

    # Create a success and failure file tracking directory
    run_id = time.strftime("%Y%m%d_%H%M%S")
    tracking_dir = os.path.join(args.logs_dir, f"tracking_{run_id}")
    os.makedirs(tracking_dir, exist_ok=True)

    success_log = os.path.join(tracking_dir, "ingested_files.txt")
    failed_log = os.path.join(tracking_dir, "failed_files.txt")

    # Get previously ingested files if resuming
    ingested_files = set()
    if args.resume_from:
        ingested_files = find_latest_ingested_files(args.resume_from)
    gz_files = []
    for file in os.listdir(args.data_directory):
        if file.endswith('.gz'):
            file_path = os.path.join(args.data_directory, file)
            # Skip already ingested files if in resume mode
            if os.path.basename(file_path) not in ingested_files:
                gz_files.append(file_path)
            else:
                ingestion_logger.info(f"Skipping already ingested file: {file}")

    if not gz_files:
        ingestion_logger.warning(f"No .gz files found to ingest in {args.data_directory}")
        return 0

    ingestion_logger.info(f"Found {len(gz_files)}.gz files in directory {args.data_directory}")
    ingestion_logger.info(f"Using {args.workers} workers to handle extraction and ingestion")

    start_time = time.time()
    total_lines_ingested = 0
    ingestion_logger.info(f"Continue-on-error mode: {'enabled' if args.continue_on_error else 'disabled'}")

    with Pool(processes=args.workers) as pool:
        async_results = []
        failure_flags = {}

        for file in gz_files:
            # Create a failure flag for each file
            file_name = os.path.basename(file)
            failure_flags[file_name] = Value('i', 0)

            # Closure to capture the specific file's failure flag
            def make_error_callback(file_flag):
                def error_callback(e):
                    ingestion_logger.error(f"Worker ingestion error: {e}")
                    with file_flag.get_lock():
                        file_flag.value = 1
                return error_callback

            result = pool.apply_async(
                ingest_gzip_file,
                args=(file, args.lines, args.multiplier, args.bucket, args.retries, args.precision, args.logs_dir),
                error_callback=make_error_callback(failure_flags[file_name])
            )
            async_results.append((file, result))
        for file_path, result in async_results:
            file_name = os.path.basename(file_path)
            lines = poll_for_result(result, failure_flags[file_name], args.continue_on_error, failed_log, file_name)
            if lines > 0:
                total_lines_ingested += lines
                update_tracking_file(success_log, file_name, success=True)
                ingestion_logger.info(f"Successfully ingested file: {file_name}")
            else:
                ingestion_logger.error(f"Failed to ingest: {file_name}")
                ingestion_logger.warning(f"Continuing with next file after error in {file_name}")

    # Log summary statistics
    total_time = time.time() - start_time
    ingestion_logger.info("Unload ingestion complete with a processing time: %.2f seconds", total_time)
    ingestion_logger.info("Total number of lines ingested: %d", total_lines_ingested)
    if total_time > 0:
        ingestion_logger.info("Overall ingestion rate: %.2f lines/second", total_lines_ingested / total_time)

    successful_count = sum(1 for _ in open(success_log, encoding='utf-8')) if os.path.exists(success_log) else 0
    failed_count = sum(1 for _ in open(failed_log, encoding='utf-8')) if os.path.exists(failed_log) else 0

    ingestion_logger.info(f"Successfully ingested {successful_count} files.")
    if failed_count > 0:
        ingestion_logger.error(f"Failed to ingest {failed_count} files.")
        ingestion_logger.error("To retry failed files, run the script with the --resume-from with the path to the previous tracking folder.")
    return total_lines_ingested

if __name__ == "__main__":
    main(sys.argv[1:])
