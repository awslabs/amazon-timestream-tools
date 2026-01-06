import csv
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import random
import string


def get_random_string(length: int):
    """
    Gets a random string.
    Args:
        length (int): The length of the random string to get.
    Returns:
        str: A random string.
    """
    return "".join(
        random.SystemRandom().choice(string.ascii_lowercase + string.digits)
        for _ in range(length)
    )


def parse_time(time_str: str) -> timedelta | None:
    """
    Parses a time string into a timedelta.

    Args:
        time_str (str): The time representation to parse. Supported
            time formats are hr, m, and s. For
            example, 5h2m3s.

    Returns:
        timedelta: If the string can be parsed.
        None: If the string cannot be parsed.
    """
    regex = re.compile(
        r"^(?=.*\d)((?P<hours>\d+?)hr)?((?P<minutes>\d+?)m)?((?P<seconds>\d+?)s)?$"
    )
    parts = regex.match(time_str)
    if parts is None:
        return
    parts = parts.groupdict()
    time_params = {}
    for name, param in parts.items():
        if param:
            time_params[name] = int(param)
    return timedelta(**time_params)


if __name__ == "__main__":
    parser = ArgumentParser("generate_csv_file")
    parser.add_argument(
        "--output-path",
        required=False,
        default="./data/generated_data.csv",
        help='The path to place the generated CSV file. For example, "./data/generated_data.csv".',
    )
    parser.add_argument(
        "--timestamp-column",
        default="timestamp_utc",
        required=False,
        help="The name of the column to use as the time column in the data file. Defaults to timestamp_utc.",
    )
    parser.add_argument(
        "--tag-columns",
        nargs="+",
        default=["region", "meter_id", "project_id", "olc_id"],
        required=False,
        help='The names of the columns to use as tags in the data file as a list. For example, --tag-columns "region" "meter_id". Values will be random strings.',
    )
    parser.add_argument(
        "--field-columns",
        nargs="+",
        default=["kwh", "interval_kwh"],
        required=False,
        help='The names of the columns to use as fields in the data file as a list. For example, --field-columns "kwh" "interval_kwh". Values will be random integers.',
    )
    parser.add_argument(
        "--num-rows",
        default=25,
        required=False,
        type=int,
        help="The number of rows to generate.",
    )
    parser.add_argument(
        "--start-time",
        default=(datetime.now(timezone.utc) - timedelta(hours=24)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
        help="The time to use as the intial generation point as an RFC 3339 timestamp. Defaults to 24 hours ago. For example, '2026-01-01T00:00:00Z' for UTC or '2026-01-01T00:00:00-08:00' for PST.",
    )
    parser.add_argument(
        "--time-increment",
        default="1m",
        help="The amount of time to increment between records. Defaults to one minute (1m). Supported time formats are hr, m, and s.",
    )

    args = parser.parse_args()

    output_path: Path = Path(args.output_path).expanduser()
    timestamp_column: str = args.timestamp_column
    tag_columns: list[str] = args.tag_columns
    field_columns: list[str] = args.field_columns
    num_rows: int = args.num_rows
    start_time: datetime = datetime.fromisoformat(args.start_time)
    time_increment: timedelta | None = parse_time(args.time_increment)
    if time_increment is None:
        print(f"Time increment {args.time_increment} could not be parsed")
        exit(1)

    print(f"Writing to {str(output_path)}")
    with open(output_path, "w", newline="") as csv_file:
        writer = csv.writer(csv_file)
        headers: list[str] = [timestamp_column]
        headers.extend(tag_columns)
        headers.extend(field_columns)
        writer.writerow(headers)

        current_record_time: datetime = start_time
        for i in range(num_rows):
            row = [str(current_record_time)]
            for _ in tag_columns:
                row.append(get_random_string(7))
            for _ in field_columns:
                row.append(str(random.randint(0, 1_000_000)))
            current_record_time += time_increment
            writer.writerow(row)
    print(f"{str(output_path)} written")
