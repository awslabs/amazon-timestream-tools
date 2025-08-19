import argparse
from datetime import datetime
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unload.utils.timestream_utils import TimestreamUtility
from unload.utils.logger_utils import create_logger


cardinality_logger = create_logger("cardinality")


def get_recommended_influxdb_instance(cardinality):
    # Based on the "InfluxDB instance sizing" section in the
    # Amazon Timestream developer guide.
    # https://docs.aws.amazon.com/timestream/latest/developerguide/timestream-for-influxdb.html#timestream-for-influx-dbi-storage

    # This is a guess, the developer guide
    # doesn't have an entry for db.influx.mediumn
    if cardinality < 10_000:
        return "db.influx.medium"
    elif cardinality < 100_000:
        return "db.influx.large"
    elif cardinality < 1_000_000:
        return "db.influx.2xlarge"
    elif cardinality < 5_000_000:
        return "db.influx.4xlarge"
    elif cardinality < 10_000_000:
        return "db.influx.8xlarge"
    elif cardinality < 10_300_000:
        return "db.influx.12xlarge"
    else:
        # This is a guess, there is no entry
        # for db.influx.16xlarge
        return "db.influx.16xlarge"


def get_live_analytics_cardinality(
    timestream_utility: TimestreamUtility,
    database_name: str,
    table_name: str,
    excluded_dimension_names=[],
    start_time: datetime | None = None,
    end_time: datetime | None = None,
) -> int:
    """
    Determines the cardinality of a Timestream for LiveAnalytics table, in accordance with
    InfluxData's definition of cardinality.

    Args:
        timestream_utility (TimestreamUtility): The TimestreamUtility object to use for queries and validation.
        database_name (str): The Timestream for LiveAnalytics database name to use.
        table_name (str): The Timestream for LiveAnalytics table to check the
            cardinality of.
        excluded_dimension_names (str[]): An array of dimension names to
            exclude from the calculation.

    Returns:
        int: The cardinality of the Timestream for LiveAnalytics table.
    """
    if not timestream_utility.is_valid_timestream_database_or_table_name(database_name):
        raise RuntimeError(f"Database name is invalid: {database_name}")

    if not timestream_utility.is_valid_timestream_database_or_table_name(table_name):
        raise RuntimeError(f"Table name is invalid: {database_name}")

    for dimension_name in excluded_dimension_names:
        if not timestream_utility.is_valid_timestream_dimension_name(dimension_name):
            raise RuntimeError(
                f"Dimension name to exclude is invalid: {dimension_name}"
            )
    try:
        describe_query = f'DESCRIBE "{database_name}"."{table_name}"'
        cardinality_logger.info(f"Executing query: {describe_query}")
        describe_response = timestream_utility.query(query_string=describe_query)
        next_token = describe_response.get("NextToken", None)
        schema = describe_response["Rows"]
        while next_token is not None:
            describe_response = timestream_utility.query(query_string=describe_query)
            next_token = describe_response.get("NextToken", None)
            schema.extend(describe_response["Rows"])
    except Exception as e:
        cardinality_logger.error(f"Failed to describe table: {e}")
        raise
    if "Rows" not in describe_response or len(describe_response["Rows"]) <= 0:
        raise RuntimeError("Failed to access returned rows in describe table result")

    cardinality = 0

    # Get dimensions
    dimensions = []
    for column in schema:
        if "Data" in column:
            scalar_values = column["Data"]
            if len(scalar_values) != 3:
                cardinality_logger.warning(
                    f"Unexpected number of scalar values in column: {scalar_values}"
                )
                continue
            if any(
                scalar_value.get("ScalarValue") == "DIMENSION"
                for scalar_value in scalar_values
            ):
                dimension_name = next(
                    (
                        scalar_value["ScalarValue"]
                        for scalar_value in scalar_values
                        if scalar_value["ScalarValue"] not in ("DIMENSION", "varchar")
                    ),
                    None,
                )
                if dimension_name and dimension_name not in excluded_dimension_names:
                    dimensions.append(dimension_name)
        else:
            cardinality_logger.warning(f"No data entry in column: {column}")

    dimensions_string = ", " + ", ".join(dimensions) if dimensions else ""
    """
    InfluxData defines cardinality as

        The number of unique database, measurement,
        tag set, and field key combinations in an InfluxDB instance.

    The following query determines cardinality in Timestream for LiveAnalytics,
    based on how InfluxData defines cardinality:

    SELECT 
        COUNT(
            DISTINCT(
                measure_name, dimension_name1, dimension_name2, 
                dimension_name3
            )
        ) AS cardinality
    FROM 
        "database_name"."table_name"
    """
    cardinality_query = f'SELECT COUNT(DISTINCT(measure_name{dimensions_string})) AS cardinality FROM "{database_name}"."{table_name}"'

    where_clauses = []
    if start_time is not None:
        where_clauses.append(f"time >= '{str(start_time)}'")
    if end_time is not None:
        where_clauses.append(f"time <= '{str(end_time)}'")

    if where_clauses:
        cardinality_query += " WHERE " + " AND ".join(where_clauses)

    cardinality_logger.info(f"Executing query: {cardinality_query}")
    try:
        query_response = timestream_utility.query(query_string=cardinality_query)
        next_token = query_response.get("NextToken", None)
        if (
            len(query_response["Rows"]) > 0
            and len(query_response["Rows"][0]["Data"]) > 0
        ):
            cardinality = int(query_response["Rows"][0]["Data"][0]["ScalarValue"])
        while next_token is not None:
            query_response = timestream_utility.query(
                query_string=cardinality_query, next_token=next_token
            )
            next_token = query_response.get("NextToken", None)
            if (
                len(query_response["Rows"]) > 0
                and len(query_response["Rows"][0]["Data"]) > 0
            ):
                cardinality = int(query_response["Rows"][0]["Data"][0]["ScalarValue"])
    except Exception as e:
        cardinality_logger.error(f"Failed to execute cardinality query: {e}")
        raise

    return cardinality


def main(input_args):
    parser = argparse.ArgumentParser(
        prog="cardinality.py",
        description="A sample application that determines the cardinality of a Timestream for LiveAnalytics table.",
    )
    parser.add_argument(
        "--table-name",
        help="The Timestream for LiveAnalytics table to determine the cardinality of.",
        required=True,
    )
    parser.add_argument(
        "--database-name",
        help="The Timestream for LiveAnalytics database that your table resides in.",
        required=True,
    )
    parser.add_argument(
        "--exclude-dimensions",
        help="Optional. A list of "
        "dimension names to exclude from the cardinality "
        "calculation separated by commas. In a real-world "
        "scenario, changing Timestream for LiveAnalytics "
        "dimensions to InfluxDB fields rather than InfluxDB "
        "tags when translating Timestream for LiveAnalytics "
        "records to line protocol lowers the cardinality.",
        required=False,
        type=TimestreamUtility.comma_separated_list,
    )
    parser.add_argument(
        "--start-time",
        help="Optional. Inclusive lower time bound for cardinality check in "
        "ISO-8601 format (e.g., '2024-08-01T00:00:00Z').",
        required=False,
    )
    parser.add_argument(
        "--end-time",
        help="Optional. Exclusive upper time bound for cardinality check in "
        "ISO-8601 format (e.g., '2024-08-02T00:00:00Z').",
        required=False,
    )
    args = parser.parse_args(input_args)

    database_name = args.database_name
    table_name = args.table_name
    excluded_dimension_names = args.exclude_dimensions
    start_time = (
        datetime.strptime(args.start_time, "%Y-%m-%dT%H:%M:%SZ")
        if args.start_time
        else None
    )
    end_time = (
        datetime.strptime(args.end_time, "%Y-%m-%dT%H:%M:%SZ")
        if args.end_time
        else None
    )

    timestream_utility = TimestreamUtility()

    # Actual cardinality
    table_cardinality = get_live_analytics_cardinality(
        timestream_utility=timestream_utility,
        database_name=database_name,
        table_name=table_name,
        start_time=start_time,
        end_time=end_time,
    )
    print(f'Cardinality of "{database_name}"."{table_name}": {table_cardinality}')
    print(
        "Your recommended Timestream for InfluxDB instance type is: "
        f"{get_recommended_influxdb_instance(table_cardinality)}"
    )

    # Hypothetical cardinality, if a dimension or dimensions are changed to fields
    if excluded_dimension_names and len(excluded_dimension_names) > 0:
        hypothetical_cardinality = get_live_analytics_cardinality(
            timestream_utility=timestream_utility,
            database_name=database_name,
            table_name=table_name,
            excluded_dimension_names=excluded_dimension_names,
            start_time=start_time,
            end_time=end_time,
        )
        if len(excluded_dimension_names) == 1:
            print(
                f'Hypothetical cardinality of "{database_name}"."{table_name}" '
                f"if the dimension {excluded_dimension_names[0]} became a field: "
                f"{hypothetical_cardinality}"
            )
        if len(excluded_dimension_names) == 2:
            excluded_dimension_names_string = ", ".join(excluded_dimension_names[:-1])
            print(
                f'Hypothetical cardinality of "{database_name}"."{table_name}" '
                f"if the dimensions {excluded_dimension_names_string} and "
                f"{excluded_dimension_names[-1]} became fields: {hypothetical_cardinality}"
            )
        if len(excluded_dimension_names) > 2:
            excluded_dimension_names_string = ", ".join(excluded_dimension_names[:-1])
            print(
                f'Hypothetical cardinality of "{database_name}"."{table_name}" '
                f"if the dimensions {excluded_dimension_names_string}, and "
                f"{excluded_dimension_names[-1]} became fields: {hypothetical_cardinality}"
            )
        print(
            "Your hypothetical recommended Timestream for InfluxDB instance "
            f"type is: {get_recommended_influxdb_instance(hypothetical_cardinality)}"
        )


if __name__ == "__main__":
    main(sys.argv[1:])
