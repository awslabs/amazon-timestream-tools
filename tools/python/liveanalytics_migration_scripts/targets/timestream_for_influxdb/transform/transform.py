# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import argparse
from dataclasses import dataclass
import logging
import time
import os
import sys
import pyarrow.parquet as pq
import pyarrow.fs as fs

sys.path.insert(
    0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
)

from unload.utils.timestream_utils import TimestreamUtility
from unload.utils.s3_utils import S3Utility
from unload.utils.athena_utils import AthenaUtility, MAX_WAIT_SECONDS
from unload.utils.logger_utils import update_logger


transform_logger = logging.getLogger("transform")

@dataclass
class LineProtocolTranslationResult:
    """
    Contains information relating to a line protocol translation.
    """

    timestream_database_name: str
    timestream_table_name: str
    created_athena_table_name: str
    s3_bucket_destination: str
    tags: list[str]
    fields: list[str]

    def __init__(
        self,
        timestream_database_name: str = "",
        timestream_table_name: str = "",
        created_athena_table_name: str = "",
        s3_bucket_destination: str = "",
        tags=None,
        fields=None,
    ):
        self.timestream_database_name = timestream_database_name
        self.timestream_table_name = timestream_table_name
        self.created_athena_table_name = created_athena_table_name
        self.s3_bucket_destination = s3_bucket_destination
        self.tags = tags if tags is not None else []
        self.fields = fields if fields is not None else []

    def __repr__(self) -> str:
        repr = ""

        repr += "Timestream for LiveAnalytics database: "
        if self.timestream_database_name:
            repr += f"{self.timestream_database_name}\n"
        else:
            repr += "None\n"

        repr += "Timestream for LiveAnalytics table: "
        if self.timestream_table_name:
            repr += f"{self.timestream_table_name}\n"
        else:
            repr += "None\n"

        repr += "Created Athena table name: "
        if self.created_athena_table_name:
            repr += f"{self.created_athena_table_name}\n"
        else:
            repr += "None\n"

        repr += "S3 bucket destination: "
        if self.s3_bucket_destination:
            repr += f"{self.s3_bucket_destination}\n"
        else:
            repr += "None\n"

        repr += "Tags:\n\t"
        if self.tags:
            quoted_tags = []
            for tag in self.tags:
                if "," in tag:
                    quoted_tags.append(f'"{tag}"')
                else:
                    quoted_tags.append(tag)
            repr += f"{','.join(quoted_tags)}\n"
        else:
            repr += "[]\n"

        repr += "Fields:\n\t"
        if self.fields:
            quoted_fields = []
            for field in self.fields:
                if "," in field:
                    quoted_fields.append(f'"{field}"')
                else:
                    quoted_fields.append(field)
            repr += f"{','.join(quoted_fields)}\n"
        else:
            repr += "[]\n"

        return repr


timestream_to_athena_ddl_type_mappings = {
    "VARCHAR": "STRING",
    "BOOLEAN": "BOOLEAN",
    "BIGINT": "BIGINT",
    "DOUBLE": "DOUBLE",
    "TIMESTAMP": "TIMESTAMP",
    "DATE": "DATE",
    "INTEGER": "INTEGER",
    "INT": "INTEGER",
}

arrow_to_athena_ddl_type_mappings = {
    "BOOL": "BOOLEAN",
    "INT8": "TINYINT",
    "INT16": "SMALLINT",
    "INT32": "INTEGER",
    "INT64": "BIGINT",
    "FLOAT": "FLOAT",
    "FLOAT16": "FLOAT",  # Athena float is 32 bits.
    "FLOAT32": "FLOAT",
    "FLOAT64": "DOUBLE",
    "DOUBLE": "DOUBLE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMP[S]": "TIMESTAMP",
    "TIMESTAMP[MS]": "TIMESTAMP",
    "DATE32": "DATE",
    "BINARY": "BINARY",
    "STRING": "STRING",
    "UTF8": "STRING",
}


def parse_bool_cli_argument(arg: str) -> bool:
    if arg.lower() == "true" or arg == "1":
        return True
    elif arg.lower() == "false" or arg == "0":
        return False
    else:
        raise ValueError(f"Unknown argument: {arg}")


def get_timestream_to_athena_ddl_type_mapping(timestream_type: str) -> str:
    athena_type = timestream_to_athena_ddl_type_mappings.get(
        timestream_type.upper(), ""
    )
    if not athena_type:
        raise RuntimeError(
            f"Failed to match Timestream type to Athena type: {timestream_type}"
        )
    return athena_type

def get_parquet_column_details(s3_uri: str):
    s3, path = fs.FileSystem.from_uri(s3_uri)
    with s3.open_input_file(path) as file:
        parquet_file = pq.ParquetFile(file)
        schema = parquet_file.schema_arrow
        all_field_names = {field.name for field in schema}

        formatted_columns = []
        processed_fields = set()

        for field in schema:
            field_name = field.name
            if field_name in processed_fields:
                continue

            column_type = str(field.type).upper()

            if column_type == "TIMESTAMP[NS]":
                ns_field_name = f"{field_name}_ns"
                # Check if corresponding _ns field exists
                if ns_field_name in all_field_names:
                    # Use the _ns field instead and mark both as processed
                    formatted_columns.append(f"`{ns_field_name}` STRING")
                    processed_fields.add(field_name)
                    processed_fields.add(ns_field_name)
                else:
                    # No _ns field exists, use the original field
                    formatted_columns.append(f"`{field_name}` TIMESTAMP")
                    processed_fields.add(field_name)
            elif column_type == "INT64":
                formatted_columns.append(f"`{field_name}` BIGINT")
                processed_fields.add(field_name)
            else:
                formatted_columns.append(f"`{field_name}` {column_type}")
                processed_fields.add(field_name)
        return formatted_columns

def get_arrow_to_athena_ddl_type_mapping(arrow_type: str) -> str:
    athena_type = arrow_to_athena_ddl_type_mappings.get(arrow_type.upper(), "")
    if not athena_type:
        raise RuntimeError(f"Failed to match Arrow type to Athena type: {arrow_type}")
    return athena_type


def get_parquet_column_details_in_glue_format(s3_uri: str) -> list[dict]:
    s3, path = fs.FileSystem.from_uri(s3_uri)
    with s3.open_input_file(path) as file:
        parquet_file = pq.ParquetFile(file)
        schema = parquet_file.schema_arrow
        all_field_names = {field.name for field in schema}

        formatted_columns = []
        processed_fields = set()

        for field in schema:
            field_name = field.name
            if field_name in processed_fields:
                continue

            column_type = str(field.type).upper()

            if column_type == "TIMESTAMP[NS]":
                ns_field_name = f"{field_name}_ns"
                # Check if corresponding _ns field exists
                if ns_field_name in all_field_names:
                    # time is a reserved column name.
                    if field_name == "time":
                        formatted_columns.append(
                            {"Name": field_name, "Type": "TIMESTAMP"}
                        )
                        formatted_columns.append(
                            {"Name": ns_field_name, "Type": "STRING"}
                        )
                        processed_fields.add(field_name)
                        processed_fields.add(ns_field_name)
                    else:
                        # Use the _ns field instead and mark both as processed
                        formatted_columns.append(
                            {"Name": ns_field_name, "Type": "STRING"}
                        )
                        processed_fields.add(field_name)
                        processed_fields.add(ns_field_name)
                else:
                    # No _ns field exists, use the original field
                    formatted_columns.append({"Name": field_name, "Type": "TIMESTAMP"})
                    processed_fields.add(field_name)
            else:
                column_type = get_arrow_to_athena_ddl_type_mapping(column_type)
                formatted_columns.append({"Name": field_name, "Type": column_type})
                processed_fields.add(field_name)
        return formatted_columns


def create_and_load_athena_table(
    s3_utility: S3Utility,
    athena_utility: AthenaUtility,
    timestream_database_name: str,
    timestream_table_name: str,
    s3_bucket_path: str,
    athena_database_name="default",
    athena_table_name=None,
) -> list[dict]:
    """
    Creates and loads an Athena table, importing data from an S3 bucket.

    Args:
        timestream_utility (TimestreamUtility): The TimestreamUtility to use to execute queries.
        s3_utility (S3Utility): The S3Utility to use to check the existence of S3 buckets.
        athena_utility (AthenaUtility): The AthenaUtility used to execute Athena queries.
        timestream_database_name (str): The Timestream database to retrieve the table schema from.
        timestream_table_name (str): The Timestream table to replicate the schema of.
        s3_bucket_path (str): The S3 bucket path containing Timestream data.
        athena_table_name (str): Optional. The name of the Athena table to
            create. Defaults to <timestream_database_name>_<timestream_table_name>.

    Returns:
        List(str): Columns from loaded athena table
    """

    # Timestream database and table names are passed directly into a Timestream
    # query -- they must be validated.
    if not TimestreamUtility.is_valid_timestream_database_or_table_name(
        timestream_database_name
    ):
        raise RuntimeError(
            f"Timestream database name is invalid: {timestream_database_name}"
        )

    if not TimestreamUtility.is_valid_timestream_database_or_table_name(
        timestream_table_name
    ):
        raise RuntimeError(f"Timestream table name is invalid: {timestream_table_name}")

    if athena_table_name is None:
        athena_table_name = (
            f"{timestream_database_name.lower()}_{timestream_table_name.lower()}"
        )
        athena_table_name = athena_table_name.replace("-", "_")

    if not AthenaUtility.is_valid_athena_table_name(athena_table_name):
        raise RuntimeError(f"Athena table name {athena_table_name} is invalid")

    if s3_bucket_path.lower().startswith("s3://"):
        s3_bucket_path = s3_bucket_path[5:]

    if not s3_bucket_path:
        raise RuntimeError("S3 bucket path was empty")

    s3_bucket_path_parts = s3_bucket_path.split("/")
    s3_bucket_name = s3_bucket_path_parts[0]

    # S3 bucket names are easier to check by simply checking if the
    # bucket already exists
    if not s3_utility.s3_bucket_exists(bucket_name=s3_bucket_name):
        raise RuntimeError(f"S3 bucket {s3_bucket_name} does not exist")

    try:
        # An S3 bucket name has been provided,
        # search for a results path within.
        if len(s3_bucket_path_parts) == 1:
            # Expect the unload path to be
            # unload-%Y-%m-%d %H:%M:S
            latest_unload_path = s3_utility.get_latest_unload_path(
                bucket_name=s3_bucket_name,
                timestream_database_name=timestream_database_name,
                timestream_table_name=timestream_table_name,
            )
            s3_unload_path = f"s3://{s3_bucket_name}/{timestream_database_name}/{timestream_table_name}/{latest_unload_path}"
            s3_results_path = s3_unload_path + "/results"
        else:
            if not s3_utility.s3_bucket_path_exists(
                bucket_name=s3_bucket_name, prefix="/".join(s3_bucket_path_parts[1:])
            ):
                raise RuntimeError(
                    f"S3 bucket path does not exist or is unavailable: {s3_bucket_path}"
                )
            s3_unload_path = f"s3://{'/'.join(s3_bucket_path_parts[:-1])}"
            s3_results_path = f"s3://{s3_bucket_path}"
    except Exception as e:
        transform_logger.error(f"Failed to get S3 path: {e}")
        raise

    athena_columns = []
    try:
        first_parquet = s3_utility.get_first_of_type(s3_results_path)
        athena_columns = get_parquet_column_details_in_glue_format(first_parquet)
    except FileNotFoundError:
        transform_logger.warning(
            f'Table "{timestream_database_name}"."{timestream_table_name}" is empty, skipping Athena table creation'
        )
        return athena_columns
    except Exception as e:
        transform_logger.error(
            f'Error getting table details from "{timestream_database_name}"."{timestream_table_name}": {e}'
        )
        return athena_columns

    try:
        athena_utility.create_glue_table_from_parquet(
            database_name=athena_database_name,
            table_name=athena_table_name,
            columns=athena_columns,
            s3_bucket_path=s3_results_path,
        )
    except Exception as e:
        transform_logger.error(f"Athena CREATE EXTERNAL TABLE query failed: {e}")
        return athena_columns
    return athena_columns


def translate_athena_table_to_line_protocol(
    timestream_utility: TimestreamUtility,
    s3_utility: S3Utility,
    athena_utility: AthenaUtility,
    timestream_database_name: str,
    timestream_table_name: str,
    s3_output_path: str,
    dimensions_to_fields=[],
    athena_database_name: str = "default",
    athena_table_name=None,
    lp_athena_table_name=None,
    wait_for_completion: bool = True,
    max_wait_seconds: int = MAX_WAIT_SECONDS,
    add_validation_field: bool = False,
    use_ns_precision: bool = False,
) -> LineProtocolTranslationResult:
    """
    Translates the contents of an Athena table to line protocol and stores the
    resulting line protocol data in an S3 bucket.

    Args:
        session (Session): The boto3 Session used for all clients.
        timestream_database_name (str): The Timestream for LiveAnalytics
            database in which the data originates.
        timestream_table_name (str): The Timestream for LiveAnalytics
            table in which the data originates.
        s3_output_path (str): The S3 bucket path to add line protocol
            data to.
        dimensions_to_fields (list[str]): A list of dimension names to turn
            into fields.
        athena_database_name (str): The Athena database name to use for Athena
            queries. Defaults to "default".
        athena_table_name (str): The name of the existing Athena table containing
            data to be translated. Defaults to
            <timestream_database_name>_<timestream_table_name>.
        lp_athena_table_name (str): The name to give the new Athena
            table containing translated line protocol data. Defaults to
            lp_<athena_table_name>.
        wait_for_completion (bool): Whether to wait for the line protocol
            translation query to complete, checking its status periodically.
        max_wait_seconds (int): The maximum number of seconds to wait
            for line protocol translation to complete.
        add_validation_field (bool): Whether to add a new field to the
            created line protocol data to help with validation. The
            new field will be la_unload=1.

    Returns:
        LineProtocolTranslationResult: Information relating to the line protocol
            translation.
    """
    # Timestream database and table names are passed directly into a Timestream
    # query -- they must be validated.
    if not TimestreamUtility.is_valid_timestream_database_or_table_name(
        timestream_database_name
    ):
        raise RuntimeError(
            f"Timestream database name is invalid: {timestream_database_name}"
        )

    if not TimestreamUtility.is_valid_timestream_database_or_table_name(
        timestream_table_name
    ):
        raise RuntimeError(f"Timestream table name is invalid: {timestream_table_name}")

    if athena_table_name is None:
        athena_table_name = (
            f"{timestream_database_name.lower()}_{timestream_table_name.lower()}"
        )
        athena_table_name = athena_table_name.replace("-", "_")

    if not AthenaUtility.is_valid_athena_table_name(athena_table_name):
        raise RuntimeError(f"Athena table name {athena_table_name} is invalid")

    if lp_athena_table_name is None:
        lp_athena_table_name = f"lp_{athena_table_name}"

    if not AthenaUtility.is_valid_athena_table_name(lp_athena_table_name):
        raise RuntimeError(f"Athena table name {lp_athena_table_name} is invalid")

    if s3_output_path.lower().startswith("s3://"):
        s3_output_path = s3_output_path[5:]

    if not s3_output_path:
        raise RuntimeError("S3 line protocol output path was empty")

    s3_output_path_parts = s3_output_path.split("/")
    s3_bucket_name = s3_output_path_parts[0]

    if not s3_utility.s3_bucket_exists(bucket_name=s3_bucket_name):
        raise RuntimeError(f"S3 bucket {s3_bucket_name} does not exist")

    line_protocol_translation_result = LineProtocolTranslationResult(
        timestream_database_name=timestream_database_name,
        timestream_table_name=timestream_table_name,
    )

    measure_name = ""
    # List of dimension names.
    # All dimensions are of type VARCHAR
    dimension_names = []
    # List of tuples (column_name, column_type), using Timestream types
    measures = []

    try:
        describe_query = (
            f'DESCRIBE "{timestream_database_name}"."{timestream_table_name}"'
        )
        transform_logger.info(f"Executing query: {describe_query}")
        describe_response = timestream_utility.query(query_string=describe_query)
        next_token = describe_response.get("NextToken", None)
        schema = describe_response["Rows"]
        while next_token:
            describe_response = timestream_utility.query(
                query_string=describe_query, next_token=next_token
            )
            next_token = describe_response.get("NextToken", None)
            schema.extend(describe_response["Rows"])
    except Exception as e:
        transform_logger.error(f"Failed to describe table: {e}")
        raise
    if "Rows" not in describe_response or len(describe_response["Rows"]) <= 0:
        raise RuntimeError("Failed to access returned rows in describe table result")

    for column in schema:
        if "Data" in column:
            scalar_values = column["Data"]
            if len(scalar_values) != 3:
                transform_logger.warning(
                    f"Unexpected number of scalar values in column: {scalar_values}"
                )
                continue
            # Columns are returned in the following order:
            # column name, type, Timestream attribute type ()
            column_name = scalar_values[0]["ScalarValue"]
            column_type = scalar_values[1]["ScalarValue"]
            column_attribute_type = scalar_values[2]["ScalarValue"]
            if column_attribute_type.lower() == "measure_name":
                measure_name = column_name
            elif column_attribute_type.lower() == "dimension":
                if dimensions_to_fields and column_name in dimensions_to_fields:
                    measures.append((column_name, "varchar"))
                    line_protocol_translation_result.fields.append(column_name)
                    transform_logger.info(f"Using dimension {column_name} as a field")
                else:
                    dimension_names.append(column_name)
                    line_protocol_translation_result.tags.append(column_name)
            elif column_attribute_type.lower() != "timestamp":
                measures.append((column_name, column_type))
                line_protocol_translation_result.fields.append(column_name)
        else:
            transform_logger.warning(f"No data entry in column: {column}")

    # Empty table.
    # This is not an error, but produces no line protocol.
    if len(measures) == 0:
        transform_logger.warning(
            f'Table "{timestream_database_name}"."{timestream_table_name}" is empty, skipping line protocol translation'
        )
        return line_protocol_translation_result

    line_protocol_translation_result.tags.append(measure_name)

    try:
        if len(s3_output_path_parts) == 1:
            # A bucket name has been provided, search within for the
            # latest unload path.
            latest_unload_path = s3_utility.get_latest_unload_path(
                bucket_name=s3_bucket_name,
                timestream_database_name=timestream_database_name,
                timestream_table_name=timestream_table_name,
            )
            s3_unload_path = f"s3://{s3_bucket_name}/{timestream_database_name}/{timestream_table_name}/{latest_unload_path}"
        else:
            if not s3_utility.s3_bucket_path_exists(
                bucket_name=s3_bucket_name,
                prefix="/".join(s3_output_path_parts[1:]),
                delimiter="/",
            ):
                raise RuntimeError(
                    f"S3 bucket path does not exist or is unavailable: {s3_output_path}"
                )
            # Assume that the path provided is a "results" path, containing valid parquet files.
            # Avoid adding line protocol output to the same path, to not clobber the parquet files.
            s3_unload_path = f"s3://{'/'.join(s3_output_path_parts[:-1])}"

        line_protocol_translation_result.s3_bucket_destination = (
            s3_unload_path + "/line-protocol-output"
        )

        # Translate to line protocol.
        #
        # All column names are wrapped in quotes.
        lp_translation_query = f"""
            CREATE TABLE "{athena_database_name}"."{lp_athena_table_name}"
            WITH (
                format = 'TEXTFILE',
                external_location = '{s3_unload_path}/line-protocol-output'
            ) AS
            SELECT
                -- Measurement, from table name
                    '{timestream_table_name}' ||
                -- Tags
                    CASE WHEN \"{measure_name}\" IS NOT NULL THEN ',measure_name=' || REGEXP_REPLACE(CAST(\"{measure_name}\" AS VARCHAR), '([, =])', '\\\\$1') ELSE '' END ||
        """

        for dimension_name in dimension_names:
            lp_translation_query += f"""
                CASE WHEN \"{dimension_name}\" IS NOT NULL THEN ',' || REGEXP_REPLACE('{dimension_name}', '([, =])', '\\\\$1') || '=' || REGEXP_REPLACE(CAST(\"{dimension_name}\" AS VARCHAR), '([, =])', '\\\\$1') ELSE '' END ||
            """
        lp_translation_query += """
                -- Fields
                ' ' ||
                TRIM(TRAILING ',' FROM(
        """

        if add_validation_field:
            validation_field_key = "la_unload"
            lp_translation_query += f"""
                '{validation_field_key}=1,' ||
            """
            line_protocol_translation_result.fields.append(validation_field_key)

        for i in range(len(measures)):
            measure_value_name = measures[i][0]
            measure_value_type = measures[i][1]

            if i < len(measures) - 1:
                delimiter = "||"
            else:
                delimiter = ")) ||"
            if measure_value_type == "varchar":
                lp_translation_query += f"""
                CASE WHEN \"{measure_value_name}\" IS NOT NULL
                THEN REGEXP_REPLACE('{measure_value_name}', '([, =])', '\\\\$1') || '="' || 
                     REGEXP_REPLACE(REGEXP_REPLACE(CAST(\"{measure_value_name}\" AS VARCHAR), '\\\\', '\\\\\\\\'), '"', '\\\\"') || '",'
                ELSE ''
                END {delimiter}
                """
            elif measure_value_type == "bigint":
                lp_translation_query += f"""
                CASE WHEN \"{measure_value_name}\" IS NOT NULL THEN '{measure_value_name}=' || CAST(\"{measure_value_name}\" AS VARCHAR) || 'i,' ELSE '' END {delimiter}
                """
            elif measure_value_type == "timestamp":
                if use_ns_precision:
                    lp_translation_query += f"""
                    CASE WHEN \"{measure_value_name}_ns\" IS NOT NULL THEN '{measure_value_name}=' || {measure_value_name}_ns || 'i,' ELSE '' END {delimiter}
                    """
                else:
                    lp_translation_query += f"""
                    CASE WHEN \"{measure_value_name}\" IS NOT NULL THEN '{measure_value_name}=' || CAST(CAST(TO_UNIXTIME({measure_value_name}) * 1000 AS BIGINT) AS VARCHAR) || 'i,' ELSE '' END {delimiter}
                    """
            else:
                lp_translation_query += f"""
                CASE WHEN \"{measure_value_name}\" IS NOT NULL THEN REGEXP_REPLACE('{measure_value_name}', '([, =])', '\\\\$1') || '=' || CAST(\"{measure_value_name}\" AS VARCHAR) || ',' ELSE '' END {delimiter}
                """

        if use_ns_precision:
            # Nanosecond precision, expects time_ns column from unload
            time_query = "time_ns"
        else:
            # Millisecond precision, the most fine-grain precision that Athena supports
            time_query = "CAST(CAST(TO_UNIXTIME(time) * 1000 AS BIGINT) AS VARCHAR)"
        lp_translation_query += f"""
            ' ' || {time_query} AS lp_record
            FROM "{athena_database_name}"."{athena_table_name}";
        """

        transform_logger.info(f"Executing query: {lp_translation_query}")
        response = athena_utility.start_query_execution(
            query_string=lp_translation_query,
            output_location=f"{s3_unload_path}/athena-query-results",
            database_name=athena_database_name,
        )
        query_execution_id = response["QueryExecutionId"]
        transform_logger.info(f"Query execution ID: {query_execution_id}")
        if wait_for_completion:
            athena_utility.wait_for_athena_query(
                query_execution_id=query_execution_id,
                max_wait_seconds=max_wait_seconds,
            )
        line_protocol_translation_result.created_athena_table_name = (
            lp_athena_table_name
        )
    except Exception as e:
        transform_logger.error(f"Line protocol translation failed: {e}")
        raise
    return line_protocol_translation_result


def parse_table_dimensions(arg):
    try:
        table_name, dimensions = arg.split("=", 1)
        return table_name, dimensions.split(",")
    except ValueError:
        raise argparse.ArgumentTypeError("Use format table1=dimension1,dimension2")


def main(input_args):
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="""A sample application that translates all data in a Timestream for LiveAnalytics
                table to line protocol using Amazon Athena.
            """,
    )
    parser.add_argument(
        "--tables",
        help="Optional. A comma-separated list of Timestream for LiveAnalytics tables to translate.",
        required=False,
        type=TimestreamUtility.comma_separated_list,
    )
    parser.add_argument(
        "--database-name",
        help="The Timestream for LiveAnalytics database that your table(s) resides in.",
        required=True,
    )
    parser.add_argument(
        "--all-tables",
        help="Optional. Whether to translate all tables in the database.",
        required=False,
        action="store_true",
    )
    parser.add_argument(
        "--s3-bucket-path",
        help="The S3 bucket path in which "
        "to load data from. This bucket must already "
        "exist. If this is an S3 bucket name or URI, "
        "for example, s3://example_bucket, then the path "
        "s3://example_bucket/database_name/table_name/unload-latest-timestamp/results "
        "will be used to load data.",
        required=True,
    )
    parser.add_argument(
        "--athena-database-name",
        help="Optional. The name of the Athena database to use "
        'when creating any new Athena tables. Defaults to "default".',
        required=False,
        default="default",
    )
    parser.add_argument(
        "--athena-table-name",
        help="Optional. The name to use "
        "for a new Athena table, used for the translation of "
        "LiveAnalytics records to line protocol. Defaults to "
        "the Timestream for LiveAnalytics database and "
        "table name connected with a hyphen, without "
        "dashes.",
        required=False,
    )
    parser.add_argument(
        "--dimensions-to-fields",
        help="Optional. The tables and names of "
        "dimensions within to change to fields in resulting line "
        "protocol. Dimensions are usually mapped to tags. "
        "Mapping dimensions to fields can lower cardinality. "
        "The required format is "
        "--dimensions-to-fields table1=dimension1,dimension2 "
        "--dimensions-to-fields table2=dimension3,dimension4.",
        required=False,
        type=parse_table_dimensions,
        action="append",
    )
    parser.add_argument(
        "--add-validation-field",
        help="Whether to add an additional "
        "field to all translated line protocol points "
        "to help with post-migration validation. "
        "The field will be 'la_unload=1'.",
        required=True,
        type=parse_bool_cli_argument,
    )
    parser.add_argument(
        "--logs-dir",
        help="Directory for export logs .",
        default="transform-logs",
        required=False
    )

    args = parser.parse_args(input_args)

    log_file_name = f'transform_{time.strftime("%Y%m%d_%H%M%S")}.log'
    update_logger(transform_logger, args.logs_dir, log_file_name)

    timestream_database_name = args.database_name
    s3_bucket_path = args.s3_bucket_path.rstrip("/")
    if s3_bucket_path.lower().startswith("s3://"):
        s3_bucket_path = s3_bucket_path[5:]

    dimensions_to_fields_map = (
        dict(args.dimensions_to_fields) if args.dimensions_to_fields else {}
    )

    timestream_utility = TimestreamUtility()
    s3_utility = S3Utility()
    athena_utility = AthenaUtility()

    timestream_table_names = []
    line_protocol_results = []
    if args.all_tables:
        try:
            list_tables_response = timestream_utility.list_tables(
                database_name=args.database_name
            )
            for table in list_tables_response.get("Tables", []):
                timestream_table_names.append(table["TableName"])
            next_token = list_tables_response.get("NextToken", None)
            if next_token:
                while next_token:
                    list_tables_response = timestream_utility.list_tables(
                        database_name=args.database_name, next_token=next_token
                    )
                    for table in list_tables_response.get("Tables", []):
                        timestream_table_names.append(table["TableName"])
                    next_token = list_tables_response.get("NextToken", None)
        except Exception as e:
            transform_logger.error(e)
            exit(1)
    elif args.tables is not None:
        timestream_table_names = args.tables
    else:
        transform_logger.error(
            "Neither --tables nor --all-tables have been provided. One is required."
        )
        exit(1)

    for timestream_table_name in timestream_table_names:
        try:
            athena_columns = create_and_load_athena_table(
                s3_utility=s3_utility,
                athena_utility=athena_utility,
                timestream_database_name=timestream_database_name,
                timestream_table_name=timestream_table_name,
                s3_bucket_path=s3_bucket_path,
                athena_database_name=args.athena_database_name,
                athena_table_name=args.athena_table_name,
            )
            if not athena_columns:
                continue

            line_protocol_result = translate_athena_table_to_line_protocol(
                timestream_utility=timestream_utility,
                s3_utility=s3_utility,
                athena_utility=athena_utility,
                timestream_database_name=timestream_database_name,
                timestream_table_name=timestream_table_name,
                s3_output_path=s3_bucket_path,
                dimensions_to_fields=dimensions_to_fields_map.get(
                    timestream_table_name, []
                ),
                athena_database_name=args.athena_database_name,
                athena_table_name=args.athena_table_name,
                add_validation_field=args.add_validation_field,
                use_ns_precision=any(
                    column.get("Name") == "time_ns" and column.get("Type") == "STRING"
                    for column in athena_columns
                ),
            )
            line_protocol_results.append(line_protocol_result)
        except Exception as e:
            transform_logger.error(
                f"Encountered error transforming {timestream_table_name}: {e}"
            )
            continue

    print("Line protocol translation results:")
    for line_protocol_result in line_protocol_results:
        print(line_protocol_result)
        print()


if __name__ == "__main__":
    main(sys.argv[1:])
