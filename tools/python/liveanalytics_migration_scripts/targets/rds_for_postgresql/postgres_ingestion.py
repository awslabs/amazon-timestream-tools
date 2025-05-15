import os
import time
import boto3
import psycopg2
import glob
import queue
from datetime import datetime
import argparse
import sys
import json
import logging 
import shutil
import re
import concurrent.futures
from psycopg2 import pool
import gzip

sys.path.append("../../unload/utils/")
from logger_utils import create_logger
from timestream_utils import TimestreamUtility

class FileExtractionError(Exception):
    """Exception raised for errors during file extraction."""
    pass

def _extract_column_names(csv_filepath, delimiter=','):
    """
    Extract column names from the header of a CSV file.

    Args:
        csv_filepath (str): Path to the CSV file
        delimiter (str): Delimiter used in the CSV file

    Returns:
        list: List of column names
    """
    with open(csv_filepath, 'r') as f:
        header_line = f.readline().strip()
        return [col.strip() for col in header_line.split(delimiter)]

def _execute_copy_command(cur, table_name, csv_file, column_names, logger):
    """
    Execute the COPY command to load data from a CSV file into PostgreSQL.

    Args:
        cur: Database cursor
        table_name (str): Target table name
        csv_file: File object for the CSV file
        column_names (list): List of column names
        logger: Logger instance

    Returns:
        int: Number of rows ingested
    """
    col_string = ', '.join(column_names)
    copy_sql = f"""COPY {table_name} ({col_string}) FROM STDIN WITH (FORMAT csv,HEADER true,NULL '');"""
    logger.info(copy_sql)

    start_time = time.time()
    cur.copy_expert(sql=copy_sql, file=csv_file)
    rows_ingested = cur.rowcount

    end_time = time.time()
    elapsed_seconds = end_time - start_time
    minutes = int(elapsed_seconds // 60)
    seconds = int(elapsed_seconds % 60)
    logger.info(f"Rows ingested: {rows_ingested}, Time taken: {minutes} minutes and {seconds} seconds")

    return rows_ingested

def _handle_retry_backoff(retry_count, max_retries, initial_backoff, backoff_multiplier, logger):
    """
    Handle exponential backoff between retry attempts.

    Args:
        retry_count (int): Current retry attempt number
        max_retries (int): Maximum number of retry attempts
        initial_backoff (int): Initial backoff time in seconds
        backoff_multiplier (int): Multiplier for exponential backoff
        logger: Logger instance
    """
    if retry_count > 0:
        backoff_time = initial_backoff * (backoff_multiplier ** (retry_count - 1))
        logger.info(f"Retry attempt {retry_count}/{max_retries} after {backoff_time} seconds...")
        time.sleep(backoff_time)

def _handle_failure(csv_filepath, exception, retry_count, max_retries, logger, timestream_utility=None, is_connection_error=False):
    """
    Handle failure during copy operation.

    Args:
        csv_filepath (str): Path to the CSV file
        exception: The exception that occurred
        retry_count (int): Current retry attempt number
        max_retries (int): Maximum number of retry attempts
        logger: Logger instance
        timestream_utility: Timestream utility instance
        is_connection_error (bool): Whether this was a connection error
    """
    # Only send SNS notification on final retry failure
    if retry_count > max_retries:
        error_type = "Database connection error" if is_connection_error else "Error loading"
        message = f"{error_type} with error: {str(exception)} for {csv_filepath} after {max_retries} retries"
        logger.info(message)
        if timestream_utility is not None:
            timestream_utility.sns_publish_message(message, "Ingestion to postgres failed")
    else:
        attempt_type = "Connection attempt" if is_connection_error else "Attempt"
        logger.info(f"{attempt_type} {retry_count}/{max_retries} failed: {str(exception)}")

def copy_to_postgres(conn_pool, table_name, csv_filepath, processed_dir, logger, timestream_utility=None, delimiter=',', header=True, 
                     max_retries=3, initial_backoff=1, backoff_multiplier=2):
    """
    Copies data from a CSV file into a PostgreSQL table with retry logic.

    Args:
        conn_pool (dict): connecton pool
        table_name (str): Target table name
        csv_filepath (str): Path to the CSV file
        processed_dir (str): Directory to move processed files to
        logger: Logger instance
        timestream_utility: Timestream utility instance
        delimiter (str, optional): Delimiter used in the CSV file (default: ',')
        header (bool, optional): Whether the CSV file has a header row (default: True)
        max_retries (int, optional): Maximum number of retry attempts (default: 3)
        initial_backoff (int, optional): Initial backoff time in seconds (default: 1)
        backoff_multiplier (int, optional): Multiplier for exponential backoff (default: 2)

    Returns:
        tuple: (success (bool), rows_ingested (int))
    """
    success = False
    retry_count = 0
    last_exception = None
    conn = None
    rows_ingested = 0

    while retry_count <= max_retries and not success:
        _handle_retry_backoff(retry_count, max_retries, initial_backoff, backoff_multiplier, logger)

        try:
            conn = conn_pool.getconn()
            try:
                with conn.cursor() as cur:
                    logger.info(f"Processing file: {csv_filepath}")

                    column_names = _extract_column_names(csv_filepath, delimiter)

                    with open(csv_filepath, 'r') as f:
                        # Rewind to beginning to include header
                        f.seek(0)
                        rows_ingested = _execute_copy_command(cur, table_name, f, column_names, logger)

                    conn.commit()
                    logger.info(f"Successfully copied: {csv_filepath}")
                    success = True
                    break  # Exit the retry loop on success

            except psycopg2.Error as e:
                conn.rollback()
                last_exception = e
                retry_count += 1
                _handle_failure(csv_filepath, e, retry_count, max_retries, logger, timestream_utility)

            finally:
                if conn:
                    conn_pool.putconn(conn)
                    conn = None

        except psycopg2.Error as e:
            last_exception = e
            retry_count += 1
            _handle_failure(csv_filepath, e, retry_count, max_retries, logger, timestream_utility, is_connection_error=True)
 
    if success:
        move_to_processed_directory(csv_filepath, processed_dir, logger)
    elif last_exception:
        logger.info(f"All {max_retries} retry attempts failed for {csv_filepath}")
        logger.info(f"Last error: {str(last_exception)}")

    return success, rows_ingested


def decompress_gzip_files(gz_files):
    """
    Decompress a list of gzip files.

    Args:
        gz_files: List of paths to gzip files to decompress, or a single file path

    Returns:
        list: Paths to the extracted files

    Raises:
        FileExtractionError: If decompression fails
    """

    if isinstance(gz_files, str):
        gz_files = [gz_files]

    extracted_files = []
    for gz_file_path in gz_files:
        extracted_file_path = gz_file_path[:-3] if gz_file_path.endswith('.gz') else gz_file_path
        logging.info(f"Extracting {gz_file_path} to {extracted_file_path}")
        try:
            with gzip.open(gz_file_path, 'rb') as f_in:
                with open(extracted_file_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            extracted_files.append(extracted_file_path)
        except Exception as exc:
            logging.error(f"Error decompressing file {gz_file_path}: {exc}")
            raise FileExtractionError(f"Failed to decompress {gz_file_path}") from exc

    return extracted_files


def list_files(glob_pattern, file_extension, logger):
    """
    List all files with the supplied file_extension in the specified directory matching the glob pattern.

    Args:
        glob_pattern (str): Glob pattern with extension
        file_extension (str): Extension to search for files
        logger: Logger instance

    Returns:
        list: List of file paths
    """
    try:

        all_files = glob.glob(glob_pattern)
        files_with_extension = [file for file in all_files if file.lower().endswith(file_extension)]

        logger.info(f"Found {len(files_with_extension)} {file_extension} files matching pattern {glob_pattern}")
        files_with_extension.sort()
        return files_with_extension


    except Exception as e:
        message = f"Error listing {file_extension} files with pattern {glob_pattern}: {str(e)}"
        logger.error(message)
        return []

def check_table_exists(conn_pool, table_name, schema):
    """
    Check if a table exists in the specified schema.

    Args:
        conn_pool: Database connection pool
        table_name (str): Name of the table to check
        schema (str): Schema name where the table should exist

    Returns:
        bool: True if the table exists, False otherwise
    """
    conn = conn_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                );
            """, (schema, table_name))
            return cur.fetchone()[0]
    finally:
        conn_pool.putconn(conn)


def thread_handler(conn_pool, table_name, processed_dir, custom_file_queue, logger, timestream_utility=None):
    """
    Process files from the queue using a dedicated thread.

    Args:
        conn_pool: Database connection pool
        table_name (str): Target table name
        processed_dir (str): Directory to move processed files to
        custom_file_queue (Queue): Queue containing files to process
        logger: Logger instance
        timestream_utility: Optional Timestream utility instance for notifications

    Returns:
        int: Total number of rows ingested by this thread
    """
    total_rows_ingested = 0
    files_processed = 0

    while True:
        try:
            csv_file = custom_file_queue.get_nowait()
        except queue.Empty:
            break

        success, rows = copy_to_postgres(conn_pool, table_name, csv_file, processed_dir, logger, timestream_utility)
        if success:
            total_rows_ingested += rows
            files_processed += 1

        custom_file_queue.task_done()

    logger.info(f"Thread completed: Processed {files_processed} files with {total_rows_ingested} total rows")
    return total_rows_ingested


def handle_ingestion(threads_count, conn_pool, table_name, csv_files, processed_dir, custom_file_queue, logger, timestream_utility=None):
    """
    Manage the multi-threaded ingestion of CSV files into PostgreSQL.

    Args:
        threads_count (int): Number of threads to use for ingestion
        conn_pool: Database connection pool
        table_name (str): Target table name
        csv_files (list): List of CSV file paths to process
        processed_dir (str): Directory to move processed files to
        custom_file_queue (Queue): Queue to hold files for processing
        logger: Logger instance
        timestream_utility: Optional Timestream utility instance for notifications

    Returns:
        int: Total number of rows ingested across all threads
    """
    for file in csv_files:
        custom_file_queue.put(file)

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads_count) as executor:
        future_results = []
        for i in range(threads_count):
            future = executor.submit(thread_handler, conn_pool, table_name, processed_dir, custom_file_queue, logger, timestream_utility)
            future_results.append(future)

        thread_results = [future.result() for future in concurrent.futures.as_completed(future_results)]

    total_rows = sum(thread_results)
    logger.info(f"Total rows ingested across all threads: {total_rows}")
    return total_rows


def move_to_processed_directory(file_path, processed_dir, logger):
    """
    Move a file to the processed directory.

    Args:
        file_path (str): Path to the file to move
        processed_dir (str): Path to the processed directory
        logger: Logger instance

    Returns:
        bool: True if successful, False otherwise
    """
    try:

        filename = os.path.basename(file_path)
        destination = os.path.join(processed_dir, filename)

        # If the file already exists in the processed directory, add a timestamp
        if os.path.exists(destination):
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename_parts = os.path.splitext(filename)
            new_filename = f"{filename_parts[0]}_{timestamp}{filename_parts[1]}"
            destination = os.path.join(processed_dir, new_filename)

        shutil.move(file_path, destination)
        logger.info(f"Moved {file_path} to {destination}")
        return True
    except Exception as e:
        logger.error(f"Failed to move {file_path} to processed directory: {str(e)}")
        return False

def get_secret(secret_arn, logger):
    """
    Retrieve a secret value from AWS Secrets Manager.

    Args:
        secret_arn (str): The ARN of the secret to retrieve
        logger (logger): Logger instance

    Returns:
        dict: The secret value as a dictionary

    Raises:
        Exception: If the secret cannot be retrieved
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager'
    )

    try:
        response = client.get_secret_value(
            SecretId=secret_arn
        )
    except Exception as e:
        sanitized_secret_arn = "***" + secret_arn[-6:] if secret_arn else "None"
        logger.info(f"Error retrieving secret (ARN ending in {sanitized_secret_arn}): {str(e)}")
        raise

    if 'SecretString' in response:
        secret = response['SecretString']
        return json.loads(secret)

def validate_sql_identifier(identifier):
    """
    Validate that a SQL identifier contains only allowed characters.
    
    Args:
        identifier (str): The SQL identifier to validate
        
    Returns:
        bool: True if the identifier is valid, False otherwise
    """
    # Validation - alphanumeric and underscore only
    return bool(re.match(r'^[a-zA-Z0-9_]+$', identifier))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--table", help="postgres table name", required=True)
    parser.add_argument("-t", "--database", help="postgres database name", required=True)
    parser.add_argument("-s", "--schema", help="Postgres schema where table resides", default='public', required=False)
    parser.add_argument("-u", "--user", help="User to login to postgres", default='postgres', required=False)
    parser.add_argument("-f", "--input-files", help="Directory or glob pattern for CSV files(Compressed on uncompressed), for example \"./data/*.gz\"", required=True) 
    parser.add_argument("-e", "--host", help="Postgres Writer Endpoint", required=True)
    parser.add_argument("-p", "--port", help="Postgres Port", default='5432', required=False)
    parser.add_argument("-sm", "--secret-arn", help="Secrets Manager secret arn if secret stored in Secrets Manager. Omit if using self managed credentials and enter the database password through standard input.", required=False)
    parser.add_argument("-pt", "--parallel-threads", help = "Number of threads that will ingest CSV files in parallel", default=10, required = False)
    parser.add_argument("-pd", "--processed-dir", help = "location for moving the processed files",default = None, required = False)
    parser.add_argument("-ld", "--logs_dir", help='Directory for postgres ingestion logs (default: postgres-ingestion-logs)', default = None, required = False)
    parser.add_argument("-sns","--sns_topic-arn", help="SNS topic ARN for sending any batchload failures", default=None, required=False)

    args = parser.parse_args()
    
    secret = None
    if args.secret_arn is None:
        secret = input("No value provided for secret-arn. Add the secret-arn for the database credentials, "
        "unless you are using self managed credentials in which case enter your database password now: ")

    log_dir = args.logs_dir

    if log_dir is None:
        log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'postgres-ingestion-logs')
    os.makedirs(log_dir, exist_ok=True)
    custom_logger_file = f"postgres_ingestion_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger = create_logger('postgres_ingestion', log_file=f"{log_dir}/{custom_logger_file}" )
    logger.info(f"logging into {log_dir}/{custom_logger_file}")

    table_name = args.table
    database_name = args.database
    schema = args.schema
    host = args.host
    port = args.port
    user = args.user
    num_of_threads = int(args.parallel_threads)
    sns_topic_arn = args.sns_topic_arn
    timestream_utility = None
    if args.sns_topic_arn is not None:
        timestream_utility = TimestreamUtility(sns_topic_arn=sns_topic_arn)
        if not timestream_utility.init_sns_topic(args.sns_topic_arn):
            sys.exit(1)


    if not validate_sql_identifier(table_name) or not validate_sql_identifier(schema):
        logger.error(f"Invalid table name or schema. Must contain only alphanumeric characters and underscores.")
        sys.exit(1)

    try:
        port = int(args.port)
        if not (1 <= port <= 65535):
            raise ValueError("Port must be between 1 and 65535")
    except ValueError:
        logger.error(f"Invalid port number: {args.port}")
        sys.exit(1)

    db_params = {
        'dbname': database_name ,
        'user': user,
        'host': host,
        'port': port
    }

    # Retrieve secret value if secret-arn has been provided
    if secret is None:
        try:
            secret_arn = args.secret_arn
            sanitized_secret_arn = "***" + secret_arn[-6:] if secret_arn else "None"
            secret = get_secret(secret_arn, logger)
            if 'password' not in secret:
                error_message = f"Password not found in secret (ARN ending in {sanitized_secret_arn})"
                logger.error(error_message)
                sys.exit(1)
            db_params['password'] = secret['password']
        except Exception as e:
            sanitized_secret_arn = "***" + secret_arn[-6:] if secret_arn else "None"
            error_message = f"Error retrieving secret (ARN ending in {sanitized_secret_arn}): {str(e)}"
            logger.error(error_message)
            timestream_utility.sns_publish_message(error_message, "Failed to retrieve database credentials")
            sys.exit(1)


    logger.info(f"Connecting to {host} as user {user} for database {database_name}")

    try:
        conn_pool = psycopg2.pool.SimpleConnectionPool(1, num_of_threads, **db_params)
        logger.info("Successfully created database connection pool")
    except psycopg2.Error as e:
        logger.error(f"Failed to create connection pool: {str(e)}")
        message = f"Failed to connect to database: {str(e)}"
        timestream_utility.sns_publish_message(sns_topic_arn, message, "Database connection failed")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error creating connection pool: {str(e)}")
        message = f"Unexpected error while connecting to database: {str(e)}"
        timestream_utility.sns_publish_message(sns_topic_arn, message, "Database connection failed with unexpected error")
        sys.exit(1)

    schema = args.schema 
    if not check_table_exists(conn_pool, table_name, schema):
        logger.error(f"Table {table_name} does not exist or doesn't exist under provided schema. Please create it first.")
        sys.exit(1)

    try:
        if args.processed_dir is not None:
            processed_dir = args.processed_dir
        else:
            input_dir = os.path.dirname(args.input_files)    
            processed_dir = os.path.join(input_dir, f'processed_{table_name}_{database_name}')
            logger.info(f"Processed files will be moved to: {processed_dir} after processing")

        # Create the directory if it doesn't exist
        if not os.path.exists(processed_dir):
            os.makedirs(processed_dir)
            logger.info(f"Created processed directory: {processed_dir}")
    except Exception as e:
        logger.error(f"Failed to create processed directory: {str(e)}")
        sys.exit(1)



    start_time = datetime.now()
    custom_file_queue = queue.Queue()

    gz_files = list_files(args.input_files, ".gz", logger)
    if gz_files:
        logger.info(f"Found {len(gz_files)} .gz files to decompress")
        decompress_gzip_files(gz_files)

    csv_files = list_files(args.input_files, ".csv", logger)
    if not csv_files:
        logger.error(f"No CSV files found for pattern {args.input_files}")
        sys.exit(1)
    logger.info(f"Starting ingestion of {len(csv_files)} files in {min({len(csv_files)},{num_of_threads})} threads")
    handle_ingestion(num_of_threads, conn_pool, table_name, csv_files, processed_dir, custom_file_queue, logger, timestream_utility)
    conn_pool.closeall()
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Start time: {start_time}")
    logger.info(f"End time: {end_time}")
    logger.info(f"Duration: {duration}")

