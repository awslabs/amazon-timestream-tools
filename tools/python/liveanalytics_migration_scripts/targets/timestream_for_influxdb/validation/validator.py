"""
Compare row/point counts between an Amazon Athena (or Timestream) table and an InfluxDB
bucket measurement, optionally within a specific time‑range.

Example CLI usage:

    python validate.py \
        --engine timestream \
        --timestream-db benchmark3 \
        --timestream-table cpu \
        --influx-url https://example.com:8086 \
        --influx-token MYTOKEN \
        --influx-org my-org \
        --influx-bucket bucket3 \
        --influx-measurement cpu \
        --schema-tags service_environment,os,arch \
        --start-time 2024-08-01T00:00:00Z \
        --end-time 2024-08-02T00:00:00Z

Or simply:
    # Define variables in .env file (see example.env)
    python validate.py
"""
from __future__ import annotations
import argparse
import os
import sys
import time
import datetime as dt
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, List, Sequence, Tuple
import requests

# Third‑party deps
from dotenv import load_dotenv
import boto3
import influxdb_client

# ───────────────────────── Helpers ──────────────────────────

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
            # "<metric>{labels} <value>"
            try:
                value_str = line.split(None, 1)[1]
                val = float(value_str)
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
        f"COUNT(DISTINCT(measure_name, {', '.join(dimensions)}, time))"
        if dimensions
        else "COUNT(*)"
    )
    query = f'SELECT {select_expr} AS c FROM "{database}"."{table}"'

    where_clauses: list[str] = []
    if start_time:
        where_clauses.append(
            f"time >= from_iso8601_timestamp('{start_time}')")
    if end_time:
        where_clauses.append(
            f"time <  from_iso8601_timestamp('{end_time}')")
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    rows: list[dict] = []
    next_token: str | None = None

    try:
        while True:
            params: dict[str, str] = {"QueryString": query}
            if next_token:
                params["NextToken"] = next_token

            resp = client.query(**params)
            rows.extend(resp.get("Rows", []))
            next_token = resp.get("NextToken")
            if not next_token:
                break
    except Exception as e:
        print(f"[Timestream] Error: {e}")
        return None

    if not rows:
        return 0

    total = int(rows[0]["Data"][0]["ScalarValue"])
    label = f"{database}.{table} ({start_time or 'begin'} – {end_time or 'now'})"
    print(f"--- Timestream ---\nTotal records in {label}: {total}\n")
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

    if "time" in dimensions or "measure_name" in dimensions:
        raise ValueError(
            "Please exclude `time` and `measure_name` from the SCHEMA_TAGS list."
        )

    if dimensions:
        select_expr = (
            f"COUNT(DISTINCT(measure_name, {', '.join(dimensions)}, time)) AS c"
        )
    else:
        select_expr = "COUNT(*) AS c"

    query = f'SELECT {select_expr} FROM "{database}"."{table}"'

    # ─ Optional date filtering ─
    where_clauses: list[str] = []
    if start_time:
        where_clauses.append(
            f"time >= from_iso8601_timestamp('{start_time}')"
        )
    if end_time:
        where_clauses.append(
            f"time <  from_iso8601_timestamp('{end_time}')"
        )
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)

    client = session.client("athena")

    try:
        execution = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
        )
        qid = execution["QueryExecutionId"]

        # ─ Poll until finished ─
        while True:
            state = client.get_query_execution(QueryExecutionId=qid)[
                "QueryExecution"
            ]["Status"]["State"]
            if state in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(poll_interval)

        if state != "SUCCEEDED":
            print(f"[Athena] Query {state}")
            return None

        rows = client.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        count = int(rows[1]["Data"][0]["VarCharValue"])
        label = (
            f"{database}.{table} ({start_time or 'begin'} – {end_time or 'now'})"
        )
        print(f"--- Athena ---\nTotal records in {label}: {count}\n")
        return count
    except Exception as e:
        print(f"[Athena] Error: {e}")
        return None


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
            row. Using a single field avoids double‑counting where multiple
            fields exist.
        start_time: Inclusive lower-bound (ISO-8601).  None = no lower bound.
        end_time:   Exclusive upper-bound (ISO-8601).  None = no upper bound.
        timeout: InfluxDB client timeout.

    Returns:
        Point count, or `None` on failure.
    """
    try:
        with influxdb_client.InfluxDBClient(url=url, token=token, org=org, timeout=timeout) as client:
            range_clause = _build_range_clause(start_time, end_time)
            query = (
                f'from(bucket: "{bucket}")'
                f"{range_clause}"
                f' |> filter(fn: (r) => r._measurement == "{measurement}")'
                f' |> filter(fn: (r) => r._field == "{row_identifier}")'
                ' |> group() |> count()'
            )
            tables = client.query_api().query(org=org, query=query)

        total = sum(int(rec.get_value()) for tbl in tables for rec in tbl.records)
        label = f"{bucket}.{measurement} ({start_time or 'begin'} – {end_time or 'now'})"
        print(f"--- InfluxDB ---\nTotal LP points in {label}: {total}\n")
        return total
    except Exception as e:
        print(f"[InfluxDB] Error: {e}")
        return None


# ────────────────────── CLI parsing ─────────────────────────

def parse_args() -> argparse.Namespace:
    """
    Precedence order:
        1. command-line flags
        2. environment variables
    """
    load_dotenv()

    engine_arg = argparse.ArgumentParser(add_help=False)
    engine_arg.add_argument(
        "--engine",
        choices=["timestream", "athena"],
        default=os.getenv("ENGINE", "athena"),
        help="Data source engine, 'timestream' or 'athena'. (Defaults to 'athena')",
    )

    prelim, remaining = engine_arg.parse_known_args()

    env = os.getenv
    missing = lambda var: env(var) is None

    parser = argparse.ArgumentParser(
        parents=[engine_arg],
        description=(
            "Validate that Timestream/Athena and InfluxDB have identical "
            "row/point counts, optionally within a time range."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # – Timestream
    parser.add_argument(
        "--timestream-db",
        default=env("TIMESTREAM_DB"),
        required=prelim.engine == "timestream" and missing("TIMESTREAM_DB"),
        help="Timestream database name (required if ENGINE=timestream)",
    )
    parser.add_argument(
        "--timestream-table",
        default=env("TIMESTREAM_TABLE"),
        required=prelim.engine == "timestream" and missing("TIMESTREAM_TABLE"),
        help="Timestream table name (required if ENGINE=timestream)",
    )

    # – Athena
    parser.add_argument(
        "--athena-db",
        default=env("ATHENA_DB"),
        required=prelim.engine == "athena" and missing("ATHENA_DB"),
        help="Athena database name (required if ENGINE=athena)",
    )
    parser.add_argument(
        "--athena-table",
        default=env("ATHENA_TABLE"),
        required=prelim.engine == "athena" and missing("ATHENA_TABLE"),
        help="Athena table name (required if ENGINE=athena)",
    )
    parser.add_argument(
        "--athena-output",
        default=env("ATHENA_OUTPUT"),
        required=prelim.engine == "athena" and missing("ATHENA_OUTPUT"),
        help="Athena query results S3 output location (required if ENGINE=athena)",
    )

    # – InfluxDB
    parser.add_argument(
        "--influx-url",
        default=env("INFLUX_URL"),
        required=missing("INFLUX_URL"),
        help="InfluxDB URL (e.g., 'https://example.com:8086')",
    )
    parser.add_argument(
        "--influx-token",
        default=env("INFLUX_TOKEN"),
        required=missing("INFLUX_TOKEN"),
        help="InfluxDB API token",
    )
    parser.add_argument(
        "--influx-org",
        default=env("INFLUX_ORG"),
        required=missing("INFLUX_ORG"),
        help="InfluxDB organization name",
    )
    parser.add_argument(
        "--influx-bucket",
        default=env("INFLUX_BUCKET"),
        required=missing("INFLUX_BUCKET"),
        help="InfluxDB bucket name",
    )
    parser.add_argument(
        "--influx-measurement",
        default=env("INFLUX_MEASUREMENT"),
        required=missing("INFLUX_MEASUREMENT"),
        help="InfluxDB measurement to validate",
    )

    # – Optional
    parser.add_argument(
        "--schema-tags",
        default=env("SCHEMA_TAGS", ""),
        help="Comma-separated list of dimension/tag names",
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

    args = parser.parse_args(remaining, namespace=prelim)
    return args


# ────────────────────────── Main ────────────────────────────

def main() -> None:
    args = parse_args()
    print("-"*20)
    print("Starting validation")
    print("-"*20)

    # First, check WAL across shards to ensure post-ingestion
    # is complete
    poll_interval = args.poll_metrics_interval
    session = requests.Session()

    if not args.skip_wal_check:
        print(f"\nPolling {args.influx_url}/metrics to wait for WAL to complete flushing ... \n")
        while True:
            timestamp = dt.datetime.now().isoformat(sep=" ", timespec="seconds")
            try:
                nz_wals = poll_metrics(session=session, url=args.influx_url)
            except requests.RequestException as exc:
                print(f"{timestamp}  request failed ({exc}); retrying in {poll_interval}s")
                time.sleep(poll_interval)
                continue

            if nz_wals:
                print(f"{timestamp}  {len(nz_wals):>2} shards with non-zero WAL "
                      f"(largest={max(nz_wals)/1024:,.1f} KiB)")
                time.sleep(poll_interval)
            else:
                print(f"{timestamp}  WAL empty on all shards — ready for validation.\n")
                break
    else:
        print(f"\nSkipping check for InfluxDB WAL to complete flushing ...\n")

    schema_tags = [t.strip() for t in args.schema_tags.split(",") if t.strip()]
    boto3_session = initialize_session()

    # Begin validation
    print(f"Starting validation ...\n")
    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_influx = pool.submit(
            timed,
            count_influx_rows,
            args.influx_url,
            args.influx_token,
            args.influx_org,
            args.influx_bucket,
            args.influx_measurement,
            "la_unload",
            args.start_time,
            args.end_time,
        )


        # Check Influx result first
        infl_count, infl_elapsed = fut_influx.result()
        if infl_count == 0:
            print(f"❗ InfluxDB returned 0 points in {args.influx_bucket}.")
            sys.exit(1)

        if args.influx_only:
            print(f"\n⏱ InfluxDB query time: {infl_elapsed:.2f} s")
            print("\n--------- Influx-Only Results ---------\n")
            print(f"InfluxDB row count: {infl_count}\n")
            return 

        if args.engine == "athena":
            fut_source = pool.submit(
                timed,
                count_athena_rows,
                boto3_session,
                args.athena_db,
                args.athena_table,
                args.athena_output,
                schema_tags,
                args.start_time,
                args.end_time,
            )
        else:  # Timestream
            fut_source = pool.submit(
                timed,
                count_timestream_rows,
                boto3_session,
                args.timestream_db,
                args.timestream_table,
                schema_tags,
                args.start_time,
                args.end_time,
            )

        src_count,  src_elapsed  = fut_source.result()

    # ─ Timings ─
    src_label = args.engine.title()
    print(f"⏱ {src_label} query time: {src_elapsed:.2f} s")
    print(f"⏱ InfluxDB query time:   {infl_elapsed:.2f} s\n")

    # ─ Report ─
    print("--------- Migration Results ---------\n")
    if src_count is None:
        print(f"⚠️  {src_label} query failed.")
    if infl_count is None:
        print("⚠️  InfluxDB query failed.")

    if src_count is not None and infl_count is not None:
        if src_count == infl_count:
            print(f"🎉  {src_label} and InfluxDB row counts match.\n")
        else:
            sign = ">" if src_count > infl_count else "<"
            print(
                f"⚠️  {src_label} ({src_count}) {sign} InfluxDB ({infl_count})\n"
            )

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("Interrupted by user.")
