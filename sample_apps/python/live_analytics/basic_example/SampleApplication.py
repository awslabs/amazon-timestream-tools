#!/usr/bin/python

import sys
import boto3
import argparse

from enum import Enum
from botocore.config import Config
from utils.WriteUtil import WriteUtil
from UnloadExample import UnloadExample
from BasicExample import BasicExample
from CompositePartitionKeyExample import CompositePartitionKeyExample
from Constant import *

def main(app_type, csv_file_path, kmsId, region, skip_deletion_string):
    skip_deletion = skip_deletion_string == "true"
    session = boto3.Session()

    # Recommended Timestream write client SDK configuration:
    #  - Set SDK retry count to 10.
    #  - Use SDK DEFAULT_BACKOFF_STRATEGY
    #  - Set RequestTimeout to 20 seconds .
    #  - Set max connections to 5000 or higher.
    write_client = session.client('timestream-write', config=Config(region_name=region, read_timeout=20, max_pool_connections = 5000, retries={'max_attempts': 10}))
    query_client = session.client('timestream-query', config=Config(region_name=region))

    app_type = AppType(app_type)
    if app_type is AppType.BASIC:
        basic_example = BasicExample(write_client, query_client, kmsId, csv_file_path, skip_deletion)
        basic_example.run()
    elif app_type is AppType.UNLOAD:
        unload_example = UnloadExample(args.region, write_client, query_client)
        unload_example.run(args.csv_file_path, skip_deletion)
    elif app_type is AppType.COMPOSITE_PARTITION_KEY:
        composite_partition_key_example = CompositePartitionKeyExample(write_client, query_client,
                                                                       skip_deletion, args.region)
        composite_partition_key_example.run()
    elif app_type is AppType.CLEANUP:
        write_util = WriteUtil(write_client)
        write_util.delete_table(DATABASE_NAME, TABLE_NAME)
        write_util.delete_database(DATABASE_NAME)

class BaseEnum(Enum):
    @classmethod
    def list(cls):
        return list(map(lambda c: c.value, cls))

class AppType(BaseEnum):
    BASIC = 'basic'
    UNLOAD = 'unload'
    COMPOSITE_PARTITION_KEY = 'composite_partition_key'
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
    parser.add_argument("-r", "--region", default="us-west-2")
    parser.add_argument("-sd",
                        "--skip_deletion",
                        default="true",
                        choices=("true", "false"),
                        help="skip deletion of table and database created by this script")
    args = parser.parse_args()

    main(args.type, args.csv_file_path, args.kmsId, args.region, args.skip_deletion)
