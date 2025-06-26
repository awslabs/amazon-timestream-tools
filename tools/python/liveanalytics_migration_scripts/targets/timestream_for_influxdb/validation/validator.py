"""
Compare row/point counts between an Amazon Athena (or Timestream) table and an
InfluxDB bucket measurement, optionally within a specific time-range.

Example CLI usage:

    python validate.py \
        --source-engine timestream \
        --timestream-db benchmark3 \
        --timestream-table cpu \
        --influxdb-v2-url https://example.com:8086 \
        --influxdb-v2-token MYTOKEN \
        --influxdb-v2-org my-org \
        --influxdb-v2-bucket bucket3 \
        --influxdb-v2-measurement cpu \
        --schema-tags service_environment,os,arch \
        --start-time 2024-08-01T00:00:00Z \
        --end-time 2024-08-02T00:00:00Z

Or simply:
    # Define variables in .env file (see example.env)
    python validate.py
"""

from __future__ import annotations

import argparse
from io import StringIO
import os
import logging
import sys
import time
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, List, Sequence, Tuple, Dict

from pandas.io.parsers.readers import csv
import requests
from dotenv import load_dotenv
import boto3
import influxdb_client
from unload.utils.logger_utils import update_logger

validation_logger = logging.getLogger("validation")


# ───────────────────────── Helpers ──────────────────────────

def get_quoted_tags(dimensions: list) -> str:
    """
    Produces line protocol tags in a format that the validation script
    expects for its --schema-tags argument. To do this, this function
    builds a string comprised of comma-separated dimension names, adding
    quotes to any dimension name that includes commas.

    Args:
        dimensions (list[dict]): A list of dimensions where each dimension
            is a dict with the key "Name".

    Returns:
        str
    """
    # measure_name is assumed to always be present as a tag.
    quoted_tags = ["measure_name"]
    for dimension in dimensions:
        if "," in dimension:
            quoted_tags.append(f'"{dimension}"')
        else:
            quoted_tags.append(dimension)
    return ",".join(quoted_tags)

def timed(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Tuple[Any, float]:
    """Execute *func* and measure its runtime."""
    start = time.perf_counter()
    result = func(*args, **kwargs)
    return result, time.perf_counter() - start


def initialize_session() -> boto3.Session:
    """Create a fresh `boto3.Session`."""
    return boto3.Session()


def extract_wal_values(payload: str) -> List[float]:
    """
    Return a list of non-zero floats found on storage_wal_size lines.
    Splitting on whitespace is valid per the Prometheus exposition spec.
    """
    vals: List[float] = []
    for line in payload.splitlines():
        if line.startswith("storage_wal_size"):
            try:
                val = float(line.split(None, 1)[1])
            except (IndexError, ValueError):
                continue
            if val != 0:
                vals.append(val)
    return vals


def poll_metrics(session, url, timeout=60) -> List[float]:
    """Fetch /metrics and return non-zero WAL sizes (may be empty)."""
    r = session.get(f"{url}/metrics", timeout=timeout)
    r.raise_for_status()
    return extract_wal_values(r.text)


# ───────────────── Timestream utilities ─────────────────────


def count_timestream_rows(
    session: boto3.Session,
    database: str,
    table: str,
    dimensions: Sequence[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
) -> int | None:
    """Count rows in an Amazon Timestream table.

    Args:
        session: Active boto3 session.
        database: Timestream database name.
        table: Timestream table name.
        dimensions: Dimension (tag) columns to use in case of transformed schema. When
            provided the query counts distinct `(measure_name, *dimensions,
            time)` tuples; otherwise a plain `COUNT(*)` is executed.
        start_time: Inclusive lower-bound (ISO-8601).  None = no lower bound.
        end_time:   Exclusive upper-bound (ISO-8601).  None = no upper bound.

    Returns:
        Row count, or `None` on failure.
    """
    if dimensions is None:
        dimensions = []

    client = session.client("timestream-query")

    select_expr = (
        f"COUNT(DISTINCT({', '.join(f'"{dimension}"' for dimension in dimensions)}, time))"
        if dimensions
        else "COUNT(*)"
    )
    query = f'SELECT {select_expr} AS c FROM "{database}"."{table}"'

    where_clauses: list[str] = []
    if start_time:
        where_clauses.append(f"time >= from_iso8601_timestamp('{start_time}')")
    if end_time:
        where_clauses.append(f"time <  from_iso8601_timestamp('{end_time}')")
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    rows: list[dict] = []
    next_token: str | None = None

    validation_logger.info(f"--- Timestream ---\n\nRunning query:\n{query}\n")
    while True:
        params: Dict[str, str] = {"QueryString": query}
        if next_token:
            params["NextToken"] = next_token

        resp = client.query(**params)
        rows.extend(resp.get("Rows", []))
        next_token = resp.get("NextToken")
        if not next_token:
            break

    if not rows:
        return 0

    total = int(rows[0]["Data"][0]["ScalarValue"])
    label = f"{database}.{table} ({start_time or 'begin'} - {end_time or 'now'})"
    validation_logger.info(f"[TIMESTREAM] Total records in {label}: {total}\n")
    return total


# ───────────────── Athena utilities ─────────────────────


def count_athena_rows(
    session: boto3.Session,
    database: str,
    table: str,
    output_location: str,
    dimensions: List[str] | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    poll_interval: float = 2.0,
) -> int | None:
    """Count rows in an Amazon Athena table.

    Args:
        session: Active boto3 session.
        database: Athena database name.
        table: Athena table name.
        output_location: S3 location where Athena will store query results.
        dimensions: Dimension (tag) columns to use in case of transformed schema. When
            provided the query counts distinct `(measure_name, *dimensions,
            time)` tuples; otherwise a plain `COUNT(*)` is executed.
        start_time: Inclusive lower-bound (ISO-8601).  None = no lower bound.
        end_time:   Exclusive upper-bound (ISO-8601).  None = no upper bound.
        poll_interval: Seconds to wait between status checks.

    Returns:
        Row count, or None on failure.
    """
    if dimensions is None:
        dimensions = []

    if "time" in dimensions:
        raise ValueError("Please exclude `time` from the SCHEMA_TAGS list.")

    select_expr = (
        f"COUNT(DISTINCT({', '.join(f'"{dimension}"' for dimension in dimensions)}, time)) AS c"
        if dimensions
        else "COUNT(*) AS c"
    )
    query = f'SELECT {select_expr} FROM "{database}"."{table}"'

    where_clauses: list[str] = []
    if start_time:
        where_clauses.append(f"time >= from_iso8601_timestamp('{start_time}')")
    if end_time:
        where_clauses.append(f"time <  from_iso8601_timestamp('{end_time}')")
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    client = session.client("athena")

    validation_logger.info(f"--- Athena ---\n\nRunning query:\n{query}\n")
    execution = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    qid = execution["QueryExecutionId"]

    while True:
        status = client.get_query_execution(QueryExecutionId=qid)
        state = status["QueryExecution"]["Status"]["State"]
        if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(poll_interval)

    if state != "SUCCEEDED":
        err_msg = (
            status.get("QueryExecution", {})
            .get("Status", {})
            .get("AthenaError", {})
            .get("ErrorMessage", "UNKNOWN")
        )
        raise RuntimeError(err_msg)

    rows = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    count = int(rows[1]["Data"][0]["VarCharValue"])
    label = f"{database}.{table} ({start_time or 'begin'} - {end_time or 'now'})"
    validation_logger.info(f"[ATHENA] Total records in {label}: {count}\n")
    return count


# ────────────────── InfluxDB utilities ──────────────────────


def _build_range_clause(start_time: str | None, end_time: str | None) -> str:
    """Return the Flux `range()` clause respecting start_time/end_time."""
    if not start_time and not end_time:
        return " |> range(start: 0)"

    start_filter = f'time(v: "{start_time}")' if start_time else "0"
    stop_filter = f'time(v: "{end_time}")' if end_time else "now()"
    return f" |> range(start: {start_filter}, stop: {stop_filter})"


def count_influx_rows(
    url: str,
    token: str,
    org: str,
    bucket: str,
    measurement: str,
    row_identifier: str = "migration_label",
    start_time: str | None = None,
    end_time: str | None = None,
    timeout: int = 30_000,
) -> int | None:
    """Count points in an InfluxDB measurement.

    Args:
        url: Base URL of the InfluxDB instance.
        token: InfluxDB access token.
        org: InfluxDB organisation name.
        bucket: Target bucket.
        measurement: Measurement to count.
        row_identifier: Name of a field that appears exactly once per logical
            row. Using a single field avoids double-counting where multiple
            fields exist.
        start_time: Inclusive lower-bound (ISO-8601).  None = no lower bound.
        end_time:   Exclusive upper-bound (ISO-8601).  None = no upper bound.
        timeout: InfluxDB client timeout.

    Returns:
        Point count, or `None` on failure.
    """
    with influxdb_client.InfluxDBClient(
        url=url, token=token, org=org, timeout=timeout
    ) as client:
        range_clause = _build_range_clause(start_time, end_time)
        query = (
            f'from(bucket: "{bucket}")'
            f"{range_clause}"
            f' |> filter(fn: (r) => r._measurement == "{measurement}")'
            f' |> filter(fn: (r) => r._field == "{row_identifier}")'
            " |> group() |> count()"
        )
        validation_logger.info(f"--- InfluxDB ---\n\nRunning query:\n{query}\n")
        tables = client.query_api().query(org=org, query=query)

    total = sum(int(rec.get_value()) for tbl in tables for rec in tbl.records)
    label = f"{bucket}.{measurement} ({start_time or 'begin'} - {end_time or 'now'})"
    validation_logger.info(f"[INFLUXDB] Total LP points in {label}: {total}\n")
    return total


# ────────────────────── CLI parsing ─────────────────────────


def parse_args(input_args: list[str]) -> argparse.Namespace:
    """
    Precedence order:
        1. command-line flags
        2. environment variables
    """
    load_dotenv()

    source_engine_arg = argparse.ArgumentParser(add_help=False)
    source_engine_arg.add_argument(
        "--source-engine",
        choices=["timestream", "athena"],
        default=os.getenv("SOURCE_ENGINE", "athena"),
        help="Data source engine, 'timestream' or 'athena'. (Defaults to 'athena')",
    )

    prelim, remaining = source_engine_arg.parse_known_args(input_args)

    env = os.getenv
    missing = lambda var: env(var) is None

    parser = argparse.ArgumentParser(
        parents=[source_engine_arg],
        description=(
            "Validate that Timestream/Athena and InfluxDB have identical "
            "row/point counts, optionally within a time range."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # - Timestream
    parser.add_argument(
        "--timestream-database-name",
        default=env("TIMESTREAM_DATABASE_NAME"),
        required=prelim.source_engine == "timestream"
        and missing("TIMESTREAM_DATABASE_NAME"),
        help="Timestream database name (required if SOURCE_ENGINE=timestream)",
    )
    parser.add_argument(
        "--timestream-table-name",
        default=env("TIMESTREAM_TABLE_NAME"),
        required=prelim.source_engine == "timestream"
        and missing("TIMESTREAM_TABLE_NAME"),
        help="Timestream table name (required if SOURCE_ENGINE=timestream)",
    )

    # - Athena
    parser.add_argument(
        "--athena-database-name",
        default=env("ATHENA_DATABASE_NAME"),
        required=prelim.source_engine == "athena" and missing("ATHENA_DATABASE_NAME"),
        help="Athena database name (required if SOURCE_ENGINE=athena)",
    )
    parser.add_argument(
        "--athena-table-name",
        default=env("ATHENA_TABLE_NAME"),
        required=prelim.source_engine == "athena" and missing("ATHENA_TABLE_NAME"),
        help="Athena table name (required if SOURCE_ENGINE=athena)",
    )
    parser.add_argument(
        "--athena-output",
        default=env("ATHENA_OUTPUT"),
        required=prelim.source_engine == "athena" and missing("ATHENA_OUTPUT"),
        help="Athena query results S3 output location (required if SOURCE_ENGINE=athena)",
    )

    # - InfluxDB
    parser.add_argument(
        "--influxdb-v2-url",
        default=env("INFLUXDB_V2_URL"),
        required=missing("INFLUXDB_V2_URL"),
        help="InfluxDB URL (e.g., 'https://example.com:8086')",
    )
    parser.add_argument(
        "--influxdb-v2-token",
        default=env("INFLUXDB_V2_TOKEN"),
        required=missing("INFLUXDB_V2_TOKEN"),
        help="InfluxDB API token",
    )
    parser.add_argument(
        "--influxdb-v2-org",
        default=env("INFLUXDB_V2_ORG"),
        required=missing("INFLUXDB_V2_ORG"),
        help="InfluxDB organization name",
    )
    parser.add_argument(
        "--influxdb-v2-bucket",
        default=env("INFLUXDB_V2_BUCKET"),
        required=missing("INFLUXDB_V2_BUCKET"),
        help="InfluxDB bucket name",
    )
    parser.add_argument(
        "--influxdb-v2-measurement",
        default=env("INFLUXDB_V2_MEASUREMENT"),
        required=missing("INFLUXDB_V2_MEASUREMENT"),
        help="InfluxDB measurement to validate",
    )

    # - Optional
    parser.add_argument(
        "--schema-tags",
        default=env("SCHEMA_TAGS", ""),
        help="Comma-separated list of dimension/tag names. "
        "If a tag includes commas, surround the tag with quotes. For "
        'example: --schema-tags tag1,"tag2,with,commas",tag3',
    )
    parser.add_argument(
        "--start-time",
        default=env("START_TIME"),
        help="Inclusive lower time bound in ISO-8601 format (e.g., '2024-08-01T00:00:00Z')",
    )
    parser.add_argument(
        "--end-time",
        default=env("END_TIME"),
        help="Exclusive upper time bound in ISO-8601 format (e.g., '2024-08-02T00:00:00Z')",
    )
    parser.add_argument(
        "--poll-metrics-interval",
        default=env("POLL_METRICS_INTERVAL", 30),
        help="Polling interval (seconds) for post-ingestion validation to /metrics endpoint",
    )
    parser.add_argument(
        "--skip-wal-check",
        action="store_true",
        default=env("SKIP_WAL_CHECK", False),
        help="Skip waiting for the WAL to complete flushing.",
    )
    parser.add_argument(
        "--influx-only",
        action="store_true",
        default=env("INFLUX_ONLY", False),
        help="Skip querying the source engine (Athena/Timestream) and only "
        "return the InfluxDB row count.",
    )
    parser.add_argument(
        "--logs-dir",
        help="Directory for export logs .",
        default="transform-logs",
        required=False
    )

    args = parser.parse_args(remaining, namespace=prelim)
    return args


# ────────────────────────── Main ────────────────────────────


def main(input_args) -> None:
    args = parse_args(input_args)

    log_file_name = f'validation_{time.strftime("%Y%m%d_%H%M%S")}.log'
    update_logger(validation_logger, args.logs_dir, log_file_name)

    validation_logger.info("-" * 20)
    validation_logger.info("Starting validation")
    validation_logger.info("-" * 20)

    poll_interval = int(args.poll_metrics_interval)
    session = requests.Session()

    if not args.skip_wal_check:
        validation_logger.info(
            f"\nPolling {args.influxdb_v2_url}/metrics to wait for WAL to complete flushing ...\n"
        )
        while True:
            timestamp = dt.datetime.now().isoformat(sep=" ", timespec="seconds")
            try:
                nz_wals = poll_metrics(session=session, url=args.influxdb_v2_url)
            except requests.RequestException as exc:
                validation_logger.error(
                    f"{timestamp}  request failed ({exc}); retrying in {poll_interval}s"
                )
                time.sleep(poll_interval)
                continue

            if nz_wals:
                validation_logger.info(
                    f"{timestamp}  {len(nz_wals):>2} shards with non-zero WAL "
                    f"(largest={max(nz_wals) / 1024:,.1f} KiB)"
                )
                time.sleep(poll_interval)
            else:
                validation_logger.info(f"{timestamp}  WAL empty on all shards — ready for validation.\n")
                break
    else:
        validation_logger.info("\nSkipping check for InfluxDB WAL to complete flushing ...\n")

    if args.schema_tags:
        reader = csv.reader(StringIO(args.schema_tags))
        schema_tags = [tag.strip() for tag in next(reader) if tag.strip()]
    else:
        schema_tags = []

    boto3_session = initialize_session()

    validation_logger.info("Starting validation ...\n")

    futures: Dict[str, Any] = {}
    results: Dict[str, Tuple[Any, float]] = {}
    errors: Dict[str, Exception] = {}

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures["InfluxDB"] = pool.submit(
            timed,
            count_influx_rows,
            args.influxdb_v2_url,
            args.influxdb_v2_token,
            args.influxdb_v2_org,
            args.influxdb_v2_bucket,
            args.influxdb_v2_measurement,
            "la_unload",
            args.start_time,
            args.end_time,
        )

        if not args.influx_only:
            if args.source_engine == "athena":
                futures["Source Engine"] = pool.submit(
                    timed,
                    count_athena_rows,
                    boto3_session,
                    args.athena_database_name,
                    args.athena_table_name,
                    args.athena_output,
                    schema_tags,
                    args.start_time,
                    args.end_time,
                )
            else:  # Timestream
                futures["Source Engine"] = pool.submit(
                    timed,
                    count_timestream_rows,
                    boto3_session,
                    args.timestream_database_name,
                    args.timestream_table_name,
                    schema_tags,
                    args.start_time,
                    args.end_time,
                )

        for name, fut in futures.items():
            try:
                results[name] = fut.result()
            except Exception as exc:
                errors[name] = exc

    infl_count, infl_elapsed = results.get("InfluxDB", (None, None))
    src_count, src_elapsed = results.get("Source Engine", (None, None))

    if infl_count is None:
        validation_logger.error("\n❌ InfluxDB query failed - cannot continue comparison.")
    elif infl_count == 0:
        validation_logger.info(f"\n❗ InfluxDB returned 0 points in {args.influxdb_v2_bucket}.")
        return

    if infl_count is not None and args.influx_only:
        validation_logger.info(f"\n⏱ InfluxDB query time: {infl_elapsed:.2f}s")
        validation_logger.info("\n--------- Influx-Only Results ---------\n")
        validation_logger.info(f"InfluxDB row count: {infl_count}\n")

    if not errors:
        validation_logger.info("--------- Migration Results ---------\n")
        if src_elapsed is not None:
            src_label = args.source_engine.title()
            validation_logger.info(f"⏱ {src_label} query time: {src_elapsed:.2f}s")
        if infl_elapsed is not None:
            validation_logger.info(f"⏱ InfluxDB query time:   {infl_elapsed:.2f}s\n")

        if src_count is not None and infl_count is not None:
            if src_count == infl_count:
                validation_logger.info(f"🎉  {src_label} and InfluxDB row counts match.\n")
            else:
                sign = ">" if src_count > infl_count else "<"
                validation_logger.info(f"⚠️  {src_label} ({src_count}) {sign} InfluxDB ({infl_count})\n")
                return
    else:
        validation_logger.info("\n--------- Exceptions ---------\n")
        for name, exc in errors.items():
            validation_logger.error(f"{name} query failed: {exc}")
        return


if __name__ == "__main__":
    try:
        main(sys.argv[1:])
    except KeyboardInterrupt:
        sys.exit("Interrupted by user.")
