import boto3
from logger_utils import create_logger
from botocore.exceptions import ClientError
import botocore
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from s3_utils import S3Utility
import time
from datetime import timezone
import json
import re


class TimestreamUtility:
    def __init__(self, region=None, sns_topic_arn=None, enable_dynamodb_logger=False, log_file=None, s3_util=None):
        """
        Initialize the TimestreamUtility class.

        Args:
            region (str): The AWS region.
            sns_topic_arn (str): The ARN of the SNS topic.
            enable_dynamodb_logger (bool): Whether to enable DynamoDB logging.
        """
        botocore_config = botocore.config.Config(
            max_pool_connections=5000, retries={"max_attempts": 10}
        )
        self.timestream_write_client = boto3.client(
            "timestream-write", region_name=region
        )
        self.timestream_read_client = boto3.client(
            "timestream-query", region_name=region
        )
        self.sns_client = boto3.client(
            "sns", region_name=region, config=botocore_config
        )
        self.dynamodb_client = boto3.client("dynamodb")
        self.dynamodb = boto3.resource("dynamodb")
        self.logger = create_logger("timestream_logger", log_file=log_file)
        self.sns_topic_arn = sns_topic_arn
        if s3_util is None:
            self.s3_util = S3Utility(region)
        else:
            self.s3_util = s3_util
        self.enable_dynamodb_logger = enable_dynamodb_logger

    @staticmethod
    def comma_separated_list(arg):
        return arg.split(",")

    @staticmethod
    def is_valid_timestream_dimension_name(dimension_name: str) -> bool:
        """
        Validates a Timestream dimension using Timestream's naming
        restrictions.

        Args:
            dimension_name (str): The Timestream dimension name to validate.

        Returns:
            bool: Whether the Timestream dimension name is valid
                and formatted in accordance with Timestream's restrictions.
        """
        reserved_prefixes = ("ts_", "measure_name")
        VALID_DIMENSION_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{1,256}$")
        return VALID_DIMENSION_NAME.match(
            dimension_name
        ) is not None and not dimension_name.startswith(reserved_prefixes)

    @staticmethod
    def is_valid_timestream_database_or_table_name(name: str) -> bool:
        """
        Validates Timestream database or table names using Timestream's naming
        restrictions. Timestream table and database names have the same naming
        restrictions.

        Args:
            name (str): The Timestream database or table name to validate.

        Returns:
            bool: Whether the Timestream database or table name is valid
                and formatted in accordance with Timestream's restrictions.
        """
        VALID_NAME = re.compile(r"^[A-Za-z_\-.0-9]{1,256}$")
        if not VALID_NAME.match(name):
            return False
        if len(name.encode("utf-8")) > 256:
            return False
        return True

    def get_all_databases(self):
        """
        Get all Timestream databases and return as a list

        Returns:
            list: List of database names
        """
        databases = []
        self.logger.info("Getting all databases")
        try:
            next_token = None
            while True:
                if next_token:
                    response = self.timestream_write_client.list_databases(
                        NextToken=next_token
                    )
                else:
                    response = self.timestream_write_client.list_databases()
                for db in response["Databases"]:
                    databases.append(db["DatabaseName"])
                if "NextToken" in response:
                    next_token = response["NextToken"]
                else:
                    break
            return databases
        except Exception as e:
            self.logger.error(f"Error listing databases: {str(e)}", exc_info=True)
            raise

    @staticmethod
    def validate_timestamp(ts):
        """
        Validate timestamp format

        Args:
            ts (str): Timestamp string

        Returns:
            None
        """
        assert isinstance(ts, str), f"Timestamp must be a string, got {type(ts)}"
        datetime.strptime(
            ts, "%Y-%m-%d %H:%M:%S"
        )  # Raises ValueError if format is invalid

    def generate_time_partitions(self, start_time_str, end_time_str, partition_by, custom_partition_count):
        """
        Generate time partitions for a given time range and partition type.
        This method is used to generate time partitions for unloading data from Timestream to S3.
        Args:
            start_time_str (str): Start time in format 'YYYY-MM-DD HH:MM:SS'
            end_time_str (str): End time in format 'YYYY-MM-DD HH:MM:SS'
            partition_by (str): Partition by 'hour', 'day', 'month', or 'year'

        """
        start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
        end_time = datetime.strptime(end_time_str, "%Y-%m-%d %H:%M:%S")

        partitions = []
        current = start_time

        while current < end_time:
            # You cannot have more than 100 partitions for single unload.
            partition_count = custom_partition_count 
            if partition_by == 'hour':
                next_time = current + timedelta(hours=partition_count)
            if partition_by == "day":
                next_time = current + timedelta(days=partition_count)
            elif partition_by == "month":
                next_time = current + relativedelta(months=partition_count)
            elif partition_by == "year":
                next_time = current + relativedelta(years=partition_count)

            end_partition = min(next_time, end_time)
            # Append as formatted strings
            partitions.append(
                [
                    current.strftime("%Y-%m-%d %H:%M:%S"),
                    end_partition.strftime("%Y-%m-%d %H:%M:%S"),
                ]
            )
            current = next_time

        return partitions

    def get_all_tables(self, database_name):
        """
        Get all Timestream tables for a given database and return as a list

        Args:
            database_name: Timestream database name

        Returns:
            list: List of table names
        """
        self.logger.info(f"Getting all tables for database: {database_name}")
        tables_list = []
        try:
            next_token = None
            while True:
                if next_token:
                    response = self.list_tables(
                        database_name=database_name, next_token=next_token
                    )
                else:
                    response = self.list_tables(
                        database_name=database_name
                    )

                for table in response["Tables"]:
                    tables_list.append(table["TableName"])
                    self.logger.debug(f"Found table: {table['TableName']}")

                if "NextToken" in response:
                    next_token = response["NextToken"]
                else:
                    break
            return tables_list
        except Exception as e:
            self.logger.error(f"Error getting tables: {str(e)}", exc_info=True)
            raise   

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
                self.logger.info(f"Successfully initialized SNS topic with test message: {sns_topic_arn} (Message ID: {response['MessageId']})")
                return True
            else:
                self.logger.error(f"Failed to publish test message to SNS topic: {sns_topic_arn}")
                return False

        except boto3.exceptions.botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', 'Unknown')
            error_message = e.response.get('Error', {}).get('Message', 'Unknown error')

            if error_code == 'AuthorizationErrorException':
                self.logger.error(f"Authorization error accessing SNS topic {sns_topic_arn}: {error_message}")
            elif error_code == 'NotFound':
                self.logger.error(f"SNS topic {sns_topic_arn} not found: {error_message}")
            else:
                self.logger.error(f"Error validating SNS topic {sns_topic_arn}: {error_code} - {error_message}")

            return False
        except Exception as e:
            self.logger.error(f"Unexpected error validating SNS topic {sns_topic_arn}: {str(e)}")
            return False

    def list_tables(self, database_name, next_token=None):
        if next_token is not None:
            return self.timestream_write_client.list_tables(
                DatabaseName=database_name, NextToken=next_token
            )
        else:
            return self.timestream_write_client.list_tables(DatabaseName=database_name)

    def sns_publish_message(self, message, subject, message_structure="email"):
        """
        Publish a message to an SNS topic

        Args:
            message: Message to publish
            subject: Subject of the message
            message_structure: Message structure (default: 'email')
        """
        subject =subject[:100]
        self.logger.info(f"Publishing message to SNS topic: {self.sns_topic_arn}")
        try:
            response = self.sns_client.publish(
                TopicArn=self.sns_topic_arn, Message=message, Subject=subject, MessageStructure=message_structure)
            self.logger.info(response)
        except Exception as err:
            self.logger.error(f"Error publishing message to SNS topic: {str(err)}", exc_info=True)

    def timestream_unload(self, database, table, bucket_s3_uri, partition, export_format, start_time, end_time, compression, migration_tag, max_file_size, kms_key, encryption, escaped_by, field_delimiter, recent_first, custom_partition_count, order_by_asc, append_timestamps):
        """
        Unload data from Timestream to S3

        Args:
            database: Timestream database name
            table: Timestream table name
            bucket_s3_uri: S3 bucket URI
            partition: Partition by day, month, or year
            export_format: Export format (default: 'csv')
            start_time: Start time for export
            end_time: End time for export
            compression: Compression type (default: 'gzip')
            migration_tag: Migration tag for logging
            max_file_size: Maximum file size for S3
            kms_key: KMS key for encryption
            encryption: Encryption type (default: 'SSE_KMS')
            escaped_by: Escaped by character (default: '')
            field_delimiter: Field delimiter (default: ',')
            append_timestamps: Whether timestamps should be preserved
        """
        self.logger.info(f"Starting unload for {database}.{table}")
        total_rows_exported = 0
        if partition == 'hour':
            batches=self.generate_time_partitions(start_time, end_time, 'hour', custom_partition_count)
        elif partition == 'day':
            batches=self.generate_time_partitions(start_time, end_time, 'day', custom_partition_count)
        elif partition == 'month':
            batches=self.generate_time_partitions(start_time, end_time, 'month', custom_partition_count)
        else:
            batches=self.generate_time_partitions(start_time, end_time, 'year', custom_partition_count)
        #descend the batches if user chooses recent_time_first=true
        if (recent_first):
            batches.reverse()
        self.logger.info(f"Unload will be performed in batches:{batches}")
        self.log_unload(
            database=database,
            table=table,
            migration_tag=migration_tag,
            configuration=f"Unload will be performed in {len(batches)} batches",
        )
        self.log_unload(
            database=database,
            table=table,
            migration_tag=migration_tag,
            time_range=f"start_time >= {start_time} and end_time < {end_time}",
            status="unload_started",
        )
        timestamp_measures = []
        if append_timestamps:
            timestamp_measures = self.list_timestamp_columns(database, table)
        for index, start_end_pair in enumerate(batches, start=1):
            batch_start_time = start_end_pair[0]  # Gets first timestamp (start time)
            batch_end_time = start_end_pair[1]    # Gets second timestamp (end time)
            query = self.build_query(migration_tag, database, table, bucket_s3_uri, partition, export_format, batch_start_time, batch_end_time, compression, max_file_size, kms_key, encryption, escaped_by, field_delimiter, order_by_asc, timestamp_measures)
            self.log_unload(database=database, table=table, migration_tag=migration_tag, time_range=f'start_time >= {batch_start_time} and end_time < {batch_end_time}', status=f"batch{index}_started")
            rows_exported = self.run_unload_query(query, database, table, batch_start_time, batch_end_time, migration_tag,batch_number=index)
            self.log_unload(database=database, table=table, migration_tag=migration_tag, time_range=f'start_time >= {batch_start_time} and end_time < {batch_end_time}', rows_exported=rows_exported, status=f"batch{index}_completed")
            total_rows_exported += rows_exported
        self.logger.info(f"Unload completed for {database}.{table}")
        self.logger.info(
            f"Total rows exported for start_time >= {start_time} and end_time < {end_time} for  {database}.{table} : {total_rows_exported}"
        )
        self.log_unload(
            database=database,
            table=table,
            migration_tag=migration_tag,
            time_range=f"start_time >= {start_time} and end_time < {end_time}",
            rows_exported=total_rows_exported,
            status="unload_completed",
        )

    def build_query(
        self,
        migration_tag,
        database,
        table,
        bucket_s3_uri,
        partition,
        export_format,
        start_time,
        end_time,
        compression,
        max_file_size,
        kms_key,
        encryption,
        escaped_by,
        field_delimiter,
        order_by_asc,
        timestamp_measures,
    ):
        """
        Build the Timestream unload query with the given parameters

        Args:
            migration_tag : migrationt tag to place files uniquely
            database: Timestream database name
            table: Timestream table name
            bucket_s3_uri: S3 bucket URI
            partition: Partition by hour, day, month, or year
            export_format: Export format (default: 'csv')
            start_time: Start time for export
            end_time: End time for export
            compression: Compression type (default: 'gzip')
            max_file_size: Maximum file size for S3
            kms_key: KMS key for encryption
            encryption: Encryption type (default: 'SSE_KMS')
            escaped_by: Escaped by character (default: '')
            field_delimiter: Field delimiter (default: ', ')
            timestamp_measures: Comma-separated list of timestamp columns that will be cast to VARCHAR and appended to export.

        Returns:
            str: Timestream unload query
        """

        self.logger.info(f"Building unload query for {database}.{table}")
        unload_query = "UNLOAD("
        unload_query += " SELECT *"
        if timestamp_measures:
            for timestamp_measure in timestamp_measures:
                unload_query += f", CAST(to_nanoseconds({timestamp_measure}) AS VARCHAR) as {timestamp_measure}_ns"
        if partition:
            if partition == "hour":
                unload_query += ", DATE_FORMAT(time, '%Y-%m-%d %H') as partition_date"
            elif partition == "day":
                unload_query += ", DATE_FORMAT(time,'%Y-%m-%d') as partition_date"
            elif partition == "month":
                unload_query += ", DATE_FORMAT(time,'%Y-%m') as partition_date"
            elif partition == "year":
                unload_query += ", DATE_FORMAT(time,'%Y') as partition_date"

        unload_query += f' FROM "{database}"."{table}"'   
        unload_query += f" WHERE time >= '{start_time}' AND time < '{end_time}'"

        if (order_by_asc):
            unload_query += "ORDER BY time asc"
        unload_query += f")"

        unload_query += f" TO '{bucket_s3_uri}/{database}/{table}/{migration_tag}'"
        unload_query += " WITH ("

        if partition:
            unload_query += " partitioned_by = ARRAY['partition_date'],"

        if kms_key:
            unload_query += f"kms_key='{kms_key}',"

        if export_format == "CSV":
            unload_query += " include_header='true',"
            unload_query += f" escaped_by='{escaped_by}',"
            unload_query += f" field_delimiter='{field_delimiter}',"

        unload_query += f" max_file_size='{max_file_size}',"
        unload_query += f" format='{export_format}',"
        unload_query += f" encryption='{encryption}',"
        unload_query += f" compression='{compression}')"

        return unload_query

    def run_unload_query(
        self, query, database, table, start_time, end_time, migration_tag, batch_number
    ):
        """
        Run the given Timestream unload query and return the number of rows exported

        Args:
            query: Timestream unload query
            database: Timestream database name
            table: Timestream table name
            start_time: Start time for export
            end_time: End time for export
            migration_tag: Migration tag for logging
            batch_number: Batch number for logging

        Returns:
            int: Number of rows exported
        """

        self.logger.info(f"Running unload query for {database}.{table}")
        paginator = self.timestream_read_client.get_paginator("query")
        try:
            self.logger.info("QUERY EXECUTING: " + query)
            page_iterator = iter(paginator.paginate(QueryString=query))
            self.logger.info(
                f"UNLOAD IN PROGRESS for batch start_time >= {start_time} and end_time < {end_time} for {database}.{table} "
            )
            next_token = None

            while True:
                if next_token:
                    page = self.query(query_string=query, next_token=next_token)
                else:
                    page = next(page_iterator)
                self.logger.info(
                    f"Progress Percentage for batch start_time >= {start_time} and end_time < {end_time} for {database}.{table} : "
                    + str(page["QueryStatus"]["ProgressPercentage"])
                    + "%"
                )
                self.logger.debug(page)

                if page["QueryStatus"]["ProgressPercentage"] == 100.0:
                    if "Rows" in page and page["Rows"]:
                        manifest_file = page["Rows"][0]["Data"][2]["ScalarValue"]
                        break
                    elif "NextToken" in page:
                        next_token = page["NextToken"]
                        self.logger.info(
                            "Manifest file not yet available. Waiting for next page..."
                        )
                        continue
                    else:
                        self.logger.error("Manifest file not found in the response.")
                        return None

            file = "/".join(manifest_file.split("/")[3:])
            s3_manifest_bucket_name = manifest_file.split('s3://')[-1].split('/')[0]
            manifest_file_response = self.s3_util.fetch_json_from_s3(s3_manifest_bucket_name, file)
            exported_rows = manifest_file_response['query_metadata']['total_row_count']
            export_files = {}
            for file in manifest_file_response['result_files']:
                export_file = file['url']
                export_files[export_file] = file['file_metadata']['row_count']
            self.logger.info(export_files)  
            self.logger.info(f"Rows exported for batch start_time >= {start_time} and end_time < {end_time} for {database}.{table}: {exported_rows}")
            return exported_rows
        except Exception as err:
            self.logger.error("Exception while running query: ", err)
            exception_message = (
                f"Unload job failed for {database}.{table} with error: {str(err)}"
            )
            if self.sns_topic_arn is not None:
                self.sns_publish_message(exception_message, "Unload Script Failed")

            self.log_unload(
                database=database,
                table=table,
                migration_tag=migration_tag,
                time_range=f"start_time >= {start_time} and end_time < {end_time}",
                status=f"batch{batch_number}_failed",
                error_message=f"{str(err)}",
            )
            raise

    def query(self, query_string, next_token=None):
        if next_token is not None:
            return self.timestream_read_client.query(
                QueryString=query_string, NextToken=next_token
            )
        else:
            return self.timestream_read_client.query(QueryString=query_string)

    def log_unload(
        self,
        dynamodb_table="timestream_unload_tracker",
        database="",
        table="unload_configuration",
        migration_tag=None,
        start_time=None,
        end_time=None,
        time_range=None,
        rows_exported=None,
        status=None,
        error_message=None,
        configuration=None,
    ):
        """
        Log a migration batch to DynamoDB

        Args:
            database_table (str): Database and table name (e.g., 'database.table')
            migration_tag (str): Migration identifier
            time_range (str): Time range for the batch (e.g., '2024-03-26 17:24:38 to 2024-07-03 17:24:38')
            rows_exported (int): Number of rows exported in this batch
            status (str): Status of the batch ('SUCCESS' or 'FAILED')
            error_message (str, optional): Error message if status is 'FAILED'
            configuration (dict, optional): Configuration details

        Returns:
            bool: True if logging successful, False otherwise
        """
        if self.enable_dynamodb_logger:
            try:
                item = {
                    "DatabaseName.TableName": table
                    if table == "unload_configuration"
                    else f"{database}.{table}",
                    "timestamp_epoch": str(int(time.time() * 1000)),
                    "MigrationTag": migration_tag,
                }

                # Add optional fields if provided
                if start_time is not None:
                    item["BatchStartTime"] = start_time
                if end_time is not None:
                    item["BatchEndTime"] = end_time
                if time_range is not None:
                    item["TimeRange"] = time_range
                if rows_exported is not None:
                    item["RowsExported"] = rows_exported
                if status is not None:
                    item["Status"] = status
                if error_message:
                    item["ErrorMessage"] = error_message
                if configuration:
                    item["Configuration"] = configuration

                self.logger.info(item)

                dynamo_table = self.dynamodb.Table(dynamodb_table)

                dynamo_table.put_item(Item=item)
                return True

            except ClientError as e:
                self.logger.info(f"Error logging to DynamoDB: {str(e)}")
                return False
        else:
            self.logger.info(
                "DynamoDB logging is disabled. Skipping logging to DynamoDB."
            )

    def create_dynamodb_logger_table(self, table_name, partition_key, sort_key=None):
        """
        Create DynamoDB table with on-demand capacity

        Args:
            table_name (str): Name of the table
            partition_key (str): Name of partition key
            sort_key (str, optional): Name of sort key
        """
        if self.enable_dynamodb_logger:
            key_schema = [
                {
                    "AttributeName": partition_key,
                    "KeyType": "HASH",  # Partition key
                }
            ]

            attribute_definitions = [
                {
                    "AttributeName": partition_key,
                    "AttributeType": "S",  # String type
                }
            ]

            if sort_key:
                key_schema.append(
                    {
                        "AttributeName": sort_key,
                        "KeyType": "RANGE",  # Sort key
                    }
                )
                attribute_definitions.append(
                    {"AttributeName": sort_key, "AttributeType": "S"}
                )

            try:
                self.dynamodb_client.create_table(
                    TableName=table_name,
                    KeySchema=key_schema,
                    AttributeDefinitions=attribute_definitions,
                    BillingMode="PAY_PER_REQUEST",  # On-demand capacity
                )

                self.logger.info(
                    f"Waiting for DynamoDB logger table {table_name} to be created..."
                )
                waiter = self.dynamodb_client.get_waiter("table_exists")
                waiter.wait(TableName=table_name)
                self.logger.info(f"DynamoDB Table {table_name} created successfully")

            except self.dynamodb_client.exceptions.ResourceInUseException:
                self.logger.info(
                    f"DynamoDB Table {table_name} already exists. Skipping table creation"
                )
            except Exception as err:
                self.logger.error(f"Create table failed: {err}")
                raise
        else:
            self.logger.info("DynamoDB logging is disabled. Skipping table creation.")

    def list_timestamp_columns(self, database: str, table: str):
        """
        Retrieves all columns of type TIMESTAMP from the specified Timestream table.

        Args:
            database (str): The name of the Timestream database.
            table (str): The name of the Timestream table.

        Returns:
            List[str]: A list of column names with type TIMESTAMP.
        """
        query_string = f'DESCRIBE "{database}"."{table}"'
        try:
            response = self.timestream_read_client.query(QueryString=query_string)
            rows = response.get("Rows", [])
            timestamp_columns = [
                row["Data"][0]["ScalarValue"]
                for row in rows
                if row["Data"][1]["ScalarValue"].upper() == "TIMESTAMP"
            ]
            self.logger.info(f"Timestamp columns in {database}.{table}: {timestamp_columns}")
            return timestamp_columns
        except Exception as e:
            self.logger.error(f"Failed to list timestamp columns for {database}.{table}: {str(e)}", exc_info=True)
            raise
