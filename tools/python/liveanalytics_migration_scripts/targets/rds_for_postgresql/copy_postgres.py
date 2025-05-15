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

sys.path.append("../../unload/utils/")
from logger_utils import create_logger

def sns_publish_message(sns_topic_arn, message, subject, message_structure='email'):
    """
    Publish a message to an SNS topic

    Args:
        message: Message to publish
        subject: Subject of the message
        message_structure: Message structure (default: 'email')
    """
    if not sns_topic_arn:
        logger.info(f"SNS notification skipped (no topic ARN provided): {subject} - {message}")
        return None

    subject =subject[:100]
    sns_client = boto3.client('sns')

    try:
        response = sns_client.publish(
            TopicArn=sns_topic_arn, Message=message, Subject=subject, MessageStructure=message_structure)
        logger.info(response)
    except Exception as err:
        logger.error(f"Error publishing message to SNS topic: {str(err)}", exc_info=True)

def init_sns_topic(sns_topic_arn):
    """
    Validates that the SNS topic exists and is accessible. Sends initialization message.

    Args:
        sns_topic_arn (str): The ARN of the SNS topic to validate

    Returns:
        bool: True if the topic is valid and accessible, False otherwise
    """
    sns_client = boto3.client('sns')
    try:
        sns_client.get_topic_attributes(
            TopicArn=sns_topic_arn
        )

        # Topic exists if we don't run into exception
        sns_init_message = {
            "validation": "test",
            "timestamp": datetime.now().isoformat(),
            "message": f"Migration initiated for RDS for PostgreSQL at {datetime.now().isoformat()}"
        }

        response = sns_client.publish(
            TopicArn=sns_topic_arn,
            Message=json.dumps(sns_init_message),
            Subject="SNS Topic Validation Test",
            MessageAttributes={
                'TestMessage': {
                    'DataType': 'String',
                    'StringValue': 'true'
                }
            }
        )

        # Check if we got a message ID, which indicates successful publishing
        if 'MessageId' in response:
            logger.info(f"Successfully initialized SNS topic with test message: {sns_topic_arn} (Message ID: {response['MessageId']})")
            return True
        else:
            logger.error(f"Failed to publish test message to SNS topic: {sns_topic_arn}")
            return False

    except boto3.exceptions.botocore.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', 'Unknown error')

        if error_code == 'AuthorizationErrorException':
            logger.error(f"Authorization error accessing SNS topic {sns_topic_arn}: {error_message}")
        elif error_code == 'NotFound':
            logger.error(f"SNS topic {sns_topic_arn} not found: {error_message}")
        else:
            logger.error(f"Error validating SNS topic {sns_topic_arn}: {error_code} - {error_message}")

        return False
    except Exception as e:
        logger.error(f"Unexpected error validating SNS topic {sns_topic_arn}: {str(e)}")
        return False

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

def _execute_copy_command(cur, table_name, csv_file, column_names):
    """
    Execute the COPY command to load data from a CSV file into PostgreSQL.

    Args:
        cur: Database cursor
        table_name (str): Target table name
        csv_file: File object for the CSV file
        column_names (list): List of column names

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

def _handle_retry_backoff(retry_count, max_retries, initial_backoff, backoff_multiplier):
    """
    Handle exponential backoff between retry attempts.

    Args:
        retry_count (int): Current retry attempt number
        max_retries (int): Maximum number of retry attempts
        initial_backoff (int): Initial backoff time in seconds
        backoff_multiplier (int): Multiplier for exponential backoff
    """
    if retry_count > 0:
        backoff_time = initial_backoff * (backoff_multiplier ** (retry_count - 1))
        logger.info(f"Retry attempt {retry_count}/{max_retries} after {backoff_time} seconds...")
        time.sleep(backoff_time)

def _handle_failure(csv_filepath, exception, retry_count, max_retries, is_connection_error=False):
    """
    Handle failure during copy operation.

    Args:
        csv_filepath (str): Path to the CSV file
        exception: The exception that occurred
        retry_count (int): Current retry attempt number
        max_retries (int): Maximum number of retry attempts
        is_connection_error (bool): Whether this was a connection error
    """
    # Only send SNS notification on final retry failure
    if retry_count > max_retries:
        error_type = "Database connection error" if is_connection_error else "Error loading"
        message = f"{error_type} with error: {str(exception)} for {csv_filepath} after {max_retries} retries"
        logger.info(message)
        sns_publish_message(sns_topic_arn, message, "Ingestion to postgres failed")
    else:
        attempt_type = "Connection attempt" if is_connection_error else "Attempt"
        logger.info(f"{attempt_type} {retry_count}/{max_retries} failed: {str(exception)}")

def copy_to_postgres(conn_pool, table_name, csv_filepath, processed_dir, delimiter=',', header=True, 
                     max_retries=3, initial_backoff=1, backoff_multiplier=2):
    """
    Copies data from a CSV file into a PostgreSQL table with retry logic.

    Args:
        conn_pool (dict): connecton pool
        csv_filepath (str): Path to the CSV file
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
        _handle_retry_backoff(retry_count, max_retries, initial_backoff, backoff_multiplier)

        try:
            conn = conn_pool.getconn()
            try:
                with conn.cursor() as cur:
                    logger.info(f"Processing file: {csv_filepath}")

                    column_names = _extract_column_names(csv_filepath, delimiter)

                    with open(csv_filepath, 'r') as f:
                        # Rewind to beginning to include header
                        f.seek(0)
                        rows_ingested = _execute_copy_command(cur, table_name, f, column_names)

                    conn.commit()
                    logger.info(f"Successfully copied: {csv_filepath}")
                    success = True
                    break  # Exit the retry loop on success

            except psycopg2.Error as e:
                conn.rollback()
                last_exception = e
                retry_count += 1
                _handle_failure(csv_filepath, e, retry_count, max_retries)

            finally:
                if conn:
                    conn_pool.putconn(conn)
                    conn = None

        except psycopg2.Error as e:
            last_exception = e
            retry_count += 1
            _handle_failure(csv_filepath, e, retry_count, max_retries, is_connection_error=True)
 
    if success:
        move_to_processed_directory(csv_filepath, processed_dir)
    elif last_exception:
        logger.info(f"All {max_retries} retry attempts failed for {csv_filepath}")
        logger.info(f"Last error: {str(last_exception)}")

    return success, rows_ingested


def list_csv_file(directory):
    """
    List all CSV files in the specified directory.

    Args:
        directory (str): Directory path or glob pattern

    Returns:
        list: List of CSV file paths
    """
    try:
        all_files = glob.glob(directory)
        csv_files = [file for file in all_files if file.lower().endswith('.csv')]
        logger.info(f"Found {len(csv_files)} CSV files in {directory}")
        csv_files.sort()
        return csv_files
    except Exception as e:
        message = f"Error listing CSV files in {directory}: {str(e)}"
        logger.error(message)
        return []

def check_table_exists(conn_pool, table_name, schema):
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


def thread_handler(conn_pool, table_name, processed_dir):
    total_rows_ingested = 0
    files_processed = 0

    while True:
        try:
            csv_file = custom_file_queue.get_nowait()
        except queue.Empty:
            break

        success, rows = copy_to_postgres(conn_pool, table_name, csv_file, processed_dir)
        if success:
            total_rows_ingested += rows
            files_processed += 1

        custom_file_queue.task_done()

    logger.info(f"Thread completed: Processed {files_processed} files with {total_rows_ingested} total rows")
    return total_rows_ingested


def multi_thread_handler(threads_count, conn_pool, table_name, csv_files, processed_dir):
    for file in csv_files:
        custom_file_queue.put(file)

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads_count) as executor:
        future_results = []
        for i in range(threads_count):
            future = executor.submit(thread_handler, conn_pool, table_name, processed_dir)
            future_results.append(future)

        thread_results = [future.result() for future in concurrent.futures.as_completed(future_results)]

    total_rows = sum(thread_results)
    logger.info(f"Total rows ingested across all threads: {total_rows}")
    return total_rows


def move_to_processed_directory(file_path, processed_dir):
    """
    Move a file to the processed directory.

    Args:
        file_path (str): Path to the file to move
        processed_dir (str): Path to the processed directory

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

def get_secret(secret_arn):
    """
    Retrieve a secret value from AWS Secrets Manager.

    Args:
        secret_arn (str): The ARN of the secret to retrieve

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
        secret_id_safe = "***" + secret_arn[-6:] if secret_arn else "None"
        logger.info(f"Error retrieving secret (ARN ending in {secret_id_safe}): {str(e)}")
        raise

    if 'SecretString' in response:
        secret = response['SecretString']
        return json.loads(secret)
    else:
        decoded_binary_secret = base64.b64decode(response['SecretBinary'])
        return json.loads(decoded_binary_secret)

def validate_sql_identifier(identifier):
    # Validation - alphanumeric and underscore only
    return bool(re.match(r'^[a-zA-Z0-9_]+$', identifier))


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument("-d", "--table", help="postgres table name", required=True)
    parser.add_argument("-t", "--database", help="postgres database name", required=True)
    parser.add_argument("-s", "--schema", help="Postgres schema where table resides", default='public', required=False)
    parser.add_argument("-u", "--user", help="User to login to postgres", default='postgres', required=False)
    parser.add_argument("-f", "--csv-files-dir", help="CSV files to feed into Postgres", required=True) 
    parser.add_argument("-e", "--host", help="Postgres Writer Endpoint", required=True)
    parser.add_argument("-p", "--port", help="Postgres Port", default='5432', required=False)
    parser.add_argument("-sm", "--secret-arn", help="Secrets Manager secret arn if secret stored in Secrets Manager. Omit if using self managed credentials and enter through standard input.", required=False)
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
    directory = args.csv_files_dir
    schema = args.schema
    host = args.host
    port = args.port
    user = args.user
    num_of_threads = int(args.parallel_threads)
    sns_topic_arn = args.sns_topic_arn

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


    if args.sns_topic_arn is not None:
        if not init_sns_topic(sns_topic_arn):
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
            # Log with redacted ARN
            secret_id_safe = "***" + secret_arn[-6:] if secret_arn else "None"
            secret = get_secret(secret_arn)
            if 'password' not in secret:
                error_message = f"Password not found in secret (ARN ending in {secret_id_safe})"
                logger.error(error_message)
                sys.exit(1)
            db_params['password'] = secret['password']
        except Exception as e:
            # Log with redacted ARN
            secret_id_safe = "***" + secret_arn[-6:] if secret_arn else "None"
            error_message = f"Error retrieving secret (ARN ending in {secret_id_safe}): {str(e)}"
            logger.error(error_message)
            sns_publish_message(error_message, "Failed to retrieve database credentials")
            sys.exit(1)


    logger.info(f"Connecting to {host} as user {user} for database {database_name}")

    try:
        conn_pool = psycopg2.pool.SimpleConnectionPool(1, num_of_threads, **db_params)
        logger.info("Successfully created database connection pool")
    except psycopg2.Error as e:
        logger.error(f"Failed to create connection pool: {str(e)}")
        message = f"Failed to connect to database: {str(e)}"
        sns_publish_message(sns_topic_arn, message, "Database connection failed")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error creating connection pool: {str(e)}")
        message = f"Unexpected error while connecting to database: {str(e)}"
        sns_publish_message(sns_topic_arn, message, "Database connection failed with unexpected error")
        sys.exit(1)

    schema = args.schema 
    if not check_table_exists(conn_pool, table_name, schema):
        logger.error(f"Table {table_name} does not exist or doesn't exist under provided schema. Please create it first.")
        sys.exit(1)

    try:
        if args.processed_dir is not None:
            processed_dir = args.processed_dir
        else:
            # Get the directory part of the input path
            input_dir = os.path.dirname(directory)    
            # Go one level up to create the processed directory
            parent_dir = os.path.dirname(input_dir)
            processed_dir = os.path.join(parent_dir, f'processed_{table_name}_{database_name}')
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
    csv_files = list_csv_file(directory)
    if not csv_files:
        logger.error(f"No CSV files found in {directory}")
        sys.exit(1)
    logger.info(f"Starting ingestion of {len(csv_files)} files in {min({len(csv_files)},{num_of_threads})} threads")
    multi_thread_handler(num_of_threads, conn_pool, table_name, db_params, csv_files, processed_dir)
    conn_pool.closeall()
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Start time: {start_time}")
    logger.info(f"End time: {end_time}")
    logger.info(f"Duration: {duration}")

