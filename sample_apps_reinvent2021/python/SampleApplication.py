#!/usr/bin/python

import boto3
import argparse
from enum import Enum
from botocore.config import Config

from utils.Constant import *

from examples.BasicExample import BasicExample
from examples.CsvIngestionExample import CsvIngestionExample
from examples.ScheduledQueryExample import ScheduledQueryExample
from examples.Cleanup import Cleanup


def main(app_type, csv_file_path, kms_id, stage, region, skip_deletion_string):
    session = boto3.Session()
    skip_deletion = skip_deletion_string == "true"

    # Recommended Timestream write client SDK configuration:
    #  - Set SDK retry count to 10
    #  - Use SDK DEFAULT_BACKOFF_STRATEGY
    #  - Set RequestTimeout to 20 seconds
    #  - Set max connections to 5000 or higher
    write_client = session.client('timestream-write',
                                  config=Config(region_name=region, read_timeout=20, max_pool_connections=5000,
                                                retries={'max_attempts': 10}))
    query_client = session.client('timestream-query',
                                  config=Config(region_name=region))

    app_type = AppType(app_type)
    if app_type is AppType.BASIC:
        basic_example = BasicExample(DATABASE_NAME, TABLE_NAME, write_client, query_client, skip_deletion)
        basic_example.run(kms_id)
    elif app_type is AppType.CSV:
        table_example = CsvIngestionExample(DATABASE_NAME, TABLE_NAME, write_client, query_client, skip_deletion)
        table_example.run(csv_file_path)
    elif app_type is AppType.SCHEDULED_QUERY:
        scheduled_query_example = ScheduledQueryExample(
            stage,
            region,
            DATABASE_NAME,
            TABLE_NAME,
            SQ_RESULT_DATABASE,
            SQ_RESULT_TABLE,
            write_client,
            query_client,
            skip_deletion
        )
        scheduled_query_example.run()
    elif app_type is AppType.SCHEDULED_QUERY_ERROR:
        scheduled_query_error_example = ScheduledQueryExample(
            stage,
            region,
            DATABASE_NAME,
            TABLE_NAME,
            SQ_RESULT_DATABASE,
            SQ_RESULT_TABLE,
            write_client,
            query_client,
            skip_deletion,
            fail_on_execution=True
        )
        scheduled_query_error_example.run()
    elif app_type is AppType.CLEANUP:
        cleanup = Cleanup(DATABASE_NAME, TABLE_NAME, write_client)
        cleanup.run()


class BaseEnum(Enum):
    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))


class AppType(BaseEnum):
    BASIC = 'basic'
    CSV = 'csv'
    SCHEDULED_QUERY = 'sq'
    SCHEDULED_QUERY_ERROR = 'sq-error'
    CLEANUP = 'cleanup'


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-t",
                        "--type",
                        default=AppType.BASIC.value,
                        nargs="?",
                        choices=AppType.list(),
                        help="choose type of workload to run (default: %(default)s)")
    parser.add_argument("-f", "--csv_file_path", help="file to ingest")
    parser.add_argument("-k", "--kmsId", help="KMS key for updating the database")
    parser.add_argument("-s", "--stage", default="prod")
    parser.add_argument("-r", "--region", default="us-east-1")
    parser.add_argument("-sd",
                        "--skip_deletion",
                        default="true",
                        choices=("true", "false"),
                        help="skip deletion of table and database created by this script")
    args = parser.parse_args()

    main(args.type, args.csv_file_path, args.kmsId, args.stage, args.region, args.skip_deletion)
