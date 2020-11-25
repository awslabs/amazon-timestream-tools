import boto3
import argparse

from CrudAndSimpleIngestionExample import CrudAndSimpleIngestionExample
from CsvIngestionExample import CsvIngestionExample
from QueryExample import QueryExample
from botocore.config import Config

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--csv_file_path", help="This file will be used for ingesting records")
    parser.add_argument("-k", "--kmsId", help="This will be used for updating the database")
    args = parser.parse_args()

    session = boto3.Session()

    # Recommended Timestream write client SDK configuration:
    #  - Set SDK retry count to 10.
    #  - Use SDK DEFAULT_BACKOFF_STRATEGY
    #  - Set RequestTimeout to 20 seconds .
    #  - Set max connections to 5000 or higher.
    write_client = session.client('timestream-write', config=Config(read_timeout=20, max_pool_connections=5000,
                                                                    retries={'max_attempts': 10}))
    query_client = session.client('timestream-query')

    crud_and_simple_ingestion_example = CrudAndSimpleIngestionExample(write_client)
    csv_ingestion_example = CsvIngestionExample(write_client)
    query_example = QueryExample(query_client)

    crud_and_simple_ingestion_example.create_database()
    crud_and_simple_ingestion_example.describe_database()
    crud_and_simple_ingestion_example.list_databases()

    if args.kmsId is not None:
        crud_and_simple_ingestion_example.update_database(args.kmsId)
        crud_and_simple_ingestion_example.describe_database()

    crud_and_simple_ingestion_example.create_table()
    crud_and_simple_ingestion_example.describe_table()
    crud_and_simple_ingestion_example.list_tables()
    crud_and_simple_ingestion_example.update_table()

    crud_and_simple_ingestion_example.write_records()
    crud_and_simple_ingestion_example.write_records_with_common_attributes()

    crud_and_simple_ingestion_example.write_records_with_upsert()

    if args.csv_file_path is not None:
        csv_ingestion_example.bulk_write_records(args.csv_file_path)

    query_example.run_all_queries()

    # Try cancelling a query
    query_example.cancel_query()

    # Try a query with multiple pages
    query_example.run_query_with_multiple_pages(20000)

    # Cleanup commented out
    # crud_and_simple_ingestion_example.delete_table()
    # crud_and_simple_ingestion_example.delete_database()
