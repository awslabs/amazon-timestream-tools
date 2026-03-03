from enum import IntEnum
import io
import json
import re
import sys
import time
import numpy
import pandas
import pyarrow.parquet as pq
import pyarrow as pa
import requests


METADATA_TABLE_NAME = "liveanalytics_migration_metadata"
MIGRATION_PENDING = "pending"
MIGRATION_IN_PROGRESS = "in progress"
MIGRATION_NEEDS_VERIFICATION = "needs verification"
MIGRATION_COMPLETED = "completed"
MIGRATION_FAILED = "failed"

# 10 minutes.
GET_PARQUET_TIMEOUT_SECONDS = 600

# Items placed in the cache will stay for
# seven days.
CACHE_PUT_TTL_SECONDS = 604800


class HttpStatus(IntEnum):
    ACCEPTED = 202
    OK = 200
    INTERNAL_ERROR = 500
    INVALID_REQUEST = 400
    NOT_FOUND = 404


class PresignedRangeReader(io.RawIOBase):
    def __init__(
        self, presigned_get_url, buffer_size=8 * 1024 * 1024
    ):  # 8MB default buffer
        self.url = presigned_get_url
        self.pos = 0
        self.buffer_size = buffer_size

        # Reuse session for connection pooling
        self.session = requests.Session()

        head = self.session.get(
            self.url,
            headers={"Range": "bytes=0-0"},
            timeout=GET_PARQUET_TIMEOUT_SECONDS,
        )
        head.raise_for_status()
        self.length = int(head.headers["Content-Range"].split("/")[-1])

        self.buffer = b""
        self.buffer_start = 0

    def read(self, size=-1):
        if size == -1:
            size = self.length - self.pos

        if size == 0:
            return b""

        # Check if data is in buffer
        buffer_end = self.buffer_start + len(self.buffer)
        if self.buffer_start <= self.pos < buffer_end:
            offset = self.pos - self.buffer_start
            available = len(self.buffer) - offset

            if available >= size:
                # Return all data in buffer
                data = self.buffer[offset : offset + size]
                self.pos += len(data)
                return data

        fetch_size = max(size, self.buffer_size)
        start = self.pos
        end = min(self.pos + fetch_size - 1, self.length - 1)

        headers = {"Range": f"bytes={start}-{end}"}
        response = self.session.get(self.url, headers=headers, stream=True)
        response.raise_for_status()

        # Update buffer
        self.buffer = response.content
        self.buffer_start = start

        # Return requested amount
        data = self.buffer[:size]
        self.pos += len(data)
        return data

    def close(self):
        self.session.close()
        super().close()

    def readable(self):
        return True

    def seekable(self):
        return True

    def seek(self, offset, whence=0):
        if whence == 0:  # SEEK_SET
            self.pos = offset
        elif whence == 1:  # SEEK_CUR
            self.pos += offset
        elif whence == 2:  # SEEK_END
            self.pos = self.length + offset
        return self.pos


def create_http_response(
    status: HttpStatus, message: str, table_row_counts: dict = None
):
    """
    Creates an HTTP response with sanitized message.

    Args:
        status (HttpStatus): HTTP status code.
        message (str): Response message.
        table_row_counts (dict, optional): Dict mapping table names to expected cumulative row counts.

    Returns:
        dict: Response with status, message, and optional table row counts.
    """
    presigned_url_regex = r"(http|https)://.*"
    token_regex = r"apiv3_.*"
    sanitized_message = re.sub(presigned_url_regex, "*****", message)
    sanitized_message = re.sub(token_regex, "*****", sanitized_message)
    response = {"status": status, "message": sanitized_message}

    # Include table row counts if provided (for client to verify at end of migration)
    if table_row_counts is not None:
        response["table_row_counts"] = table_row_counts

    return response


def process_request(
    influxdb3_local, query_parameters, request_headers, request_body, args=None
):
    """
    Handle HTTP requests to a custom endpoint.
    """
    influxdb3_local.info("Processing request")

    # Args from the trigger.
    try:
        db_name = parse_arg(influxdb3_local, "db_name", args)
        migration_id = parse_arg(influxdb3_local, "migration_id", args)
    except RuntimeError as e:
        return create_http_response(
            HttpStatus.INVALID_REQUEST, f"Invalid argument: {str(e)}"
        )

    if "verify" in query_parameters:
        response = verify_previous_migrations(influxdb3_local, migration_id)
        if "delete_cache" in query_parameters:
            deleted_migration_records = influxdb3_local.cache.delete(
                "migration-records"
            )
            if deleted_migration_records:
                influxdb3_local.info("Deleted migration records from in-memory cache")
            else:
                influxdb3_local.error(
                    "Failed to delete migration records from in-memory cache"
                )
            deleted_table_counts = influxdb3_local.cache.delete(
                "migration-table-counts"
            )
            if deleted_table_counts:
                influxdb3_local.info("Deleted table counts from in-memmory cache")
            else:
                influxdb3_local.error(
                    "Failed to delete table counts from in-memory cache"
                )
        return response

    # Body of the incoming request.
    data = {}
    if request_body:
        data = json.loads(request_body)
        return migrate_parquet_file(
            influxdb3_local, migration_id, db_name, data.get("parquet_path", "")
        )
    return create_http_response(HttpStatus.INVALID_REQUEST, "Invalid request")


def migrate_parquet_file(influxdb3_local, migration_id, db_name, current_parquet_path):
    if not current_parquet_path:
        influxdb3_local.warn("Parquet path is empty, aborting")
        return create_http_response(
            HttpStatus.INVALID_REQUEST, "Invalid body, Parquet path is missing"
        )

    migration_records = influxdb3_local.cache.get("migration-records", default={})

    # Shared migration_records dict has not been populated. We will assume this is the
    # first invocation and populate it from the metadata table. The metadata table only exists for an hour.
    if not migration_records:
        try:
            migration_records = get_migration_metadata(influxdb3_local, migration_id)
        except Exception as e:
            return create_http_response(HttpStatus.INTERNAL_ERROR, str(e))
        influxdb3_local.cache.put(
            key="migration-records",
            value=migration_records,
            ttl=CACHE_PUT_TTL_SECONDS,
        )
        influxdb3_local.info(f"{migration_id}: Retrieved data from metadata table")
    else:
        verification = verify_previous_migrations(influxdb3_local, migration_id)
        if (
            verification["status"] != HttpStatus.OK
            and verification["status"] != HttpStatus.ACCEPTED
        ):
            influxdb3_local.error(f"{migration_id}: Previous migration failed")
            return verification

    if not current_parquet_path:
        error_message = f"{migration_id}: Migration failed: Parquet file path missing, aborting migration"
        influxdb3_local.error(error_message)
        return create_http_response(HttpStatus.INVALID_REQUEST, error_message)

    if current_parquet_path not in migration_records:
        error_message = f"{migration_id}: Migration failed: Parquet path {current_parquet_path} not found"
        influxdb3_local.error(error_message)
        return create_http_response(HttpStatus.NOT_FOUND, error_message)

    if (
        migration_records.get(current_parquet_path, {}).get("status")
        == MIGRATION_COMPLETED
    ):
        return create_http_response(
            HttpStatus.OK, "Parquet file was previously migrated, skipping migration"
        )

    try:
        presigned_get_url = migration_records[current_parquet_path]["presigned_get_url"]
        ingestion_stats = write_to_ingestion_buffer(
            influxdb3_local=influxdb3_local,
            migration_id=migration_id,
            db_name=db_name,
            presigned_get_url=presigned_get_url,
            parquet_path=current_parquet_path,
        )
        migration_records[current_parquet_path]["status"] = MIGRATION_NEEDS_VERIFICATION
        # Store ingestion stats (row count and time bounds) for row count verification
        migration_records[current_parquet_path]["row_count"] = ingestion_stats[
            "row_count"
        ]
        migration_records[current_parquet_path]["min_time_ns"] = ingestion_stats[
            "min_time_ns"
        ]
        migration_records[current_parquet_path]["max_time_ns"] = ingestion_stats[
            "max_time_ns"
        ]
        influxdb3_local.cache.put(
            key="migration-records",
            value=migration_records,
            ttl=CACHE_PUT_TTL_SECONDS,
        )
        influxdb3_local.info(
            f"{migration_id}: Migration complete, pending verification"
        )
    except Exception as e:
        return create_http_response(
            HttpStatus.INTERNAL_ERROR, f"{migration_id}: Migration failed: {str(e)}"
        )

    return create_http_response(HttpStatus.ACCEPTED, "Request processed")


def get_migration_metadata(influxdb3_local, migration_id):
    """
    Gets migration metadata from the migration metadata table.

    Args:
        influxdb3_local: An InfluxDB v3 local client.
        migration_id (str): The ID for the current migration.

    Returns:
        dict[str, dict[str, str]]: Migration metadata.
    """
    # Shared migration_records dict has not been populated. We will assume this is the
    # first invocation and populate it from the metadata table. The metadata table only exists for an hour.
    migration_records = {}
    try:
        query: str = f'SELECT * FROM "{METADATA_TABLE_NAME}"'
        tracking_data = influxdb3_local.query(query)
        if not tracking_data:
            return create_http_response(
                HttpStatus.INTERNAL_ERROR,
                f"{migration_id}: Query for migration metadata failed",
            )

        for record in tracking_data:
            s3_key = record["s3_key"]
            presigned_get_url = record["presigned_get_url"]
            presigned_done_url = record["presigned_done_url"]
            influxdb3_local.info(f"S3 key: {s3_key}")
            migration_records[s3_key] = {
                "status": MIGRATION_PENDING,
                "presigned_get_url": presigned_get_url,
                "presigned_done_url": presigned_done_url,
            }
        return migration_records
    except Exception as e:
        raise Exception(f"{migration_id}: Failed to retrieve migration metadata")


def verify_previous_migrations(influxdb3_local, migration_id):
    migration_records = influxdb3_local.cache.get("migration-records", default={})
    table_counts = influxdb3_local.cache.get("migration-table-counts", default={})

    # Since influxdb3_local.write_to_db() writes to a buffer that is later ingested, each invocation must
    # verify the previous invocation's migration.
    all_parquet_files_migrated = True
    for parquet_path, migration_record in migration_records.items():
        # If any previous failure is detected, abort the migration.
        if migration_record["status"] == MIGRATION_FAILED:
            error_message = f"{migration_id}: Migration failed: Previous migration of {parquet_path} failed"
            influxdb3_local.error(error_message)
            return create_http_response(HttpStatus.INTERNAL_ERROR, error_message)

        # Since each invocation writes to a buffer, we can only determine whether a previous migration
        # has succeeded or failed. Migrations can fail during the writing stage, after an invocation has
        # ended, when line protocol is invalid or does not match the table schema.
        if migration_record["status"] == MIGRATION_NEEDS_VERIFICATION:
            influxdb3_local.info(
                f"{migration_id}: Verifying migration for Parquet file {parquet_path}"
            )
            if not parquet_path:
                return create_http_response(
                    HttpStatus.INVALID_REQUEST, "Parquet file path was empty"
                )
            table_name_parts = parquet_path.split("/")
            if len(table_name_parts) < 2:
                return create_http_response(
                    HttpStatus.INVALID_REQUEST,
                    "Parquet file path was incorrectly formatted. Path should start wtih database-name/table-name",
                )
            table_name = table_name_parts[1]

            # Get row count and time bounds from cache stored during previous file ingestion
            current_parquet_row_count = migration_record.get("row_count")
            min_time_ns = migration_record.get("min_time_ns")
            max_time_ns = migration_record.get("max_time_ns")

            if current_parquet_row_count is None:
                error_message = (
                    f"{migration_id}: Row count not in cache for {parquet_path}. "
                    f"This file was not properly ingested - cache entry missing row_count."
                )
                influxdb3_local.error(error_message)
                return create_http_response(HttpStatus.INTERNAL_ERROR, error_message)

            if min_time_ns is None or max_time_ns is None:
                error_message = (
                    f"{migration_id}: Time bounds not in cache for {parquet_path}. "
                    f"This file was not properly ingested - cache entry missing time bounds."
                )
                influxdb3_local.error(error_message)
                return create_http_response(HttpStatus.INTERNAL_ERROR, error_message)

            # Convert nanoseconds to RFC3339 timestamp for InfluxDB query
            min_time_str = pandas.Timestamp(min_time_ns, unit="ns").isoformat() + "Z"
            max_time_str = pandas.Timestamp(max_time_ns, unit="ns").isoformat() + "Z"

            row_count_query = (
                f'SELECT COUNT(*) AS row_count FROM "{table_name}" '
                f"WHERE time >= '{min_time_str}' AND time <= '{max_time_str}'"
            )
            expected_row_count = current_parquet_row_count
            influxdb3_local.info(
                f"{migration_id}: Verifying {parquet_path} with time-bounded query "
                f"(time range: {min_time_str} to {max_time_str}, expected {expected_row_count} rows)"
            )

            # Verify row count with retry logic to handle WAL flush timing
            max_verification_attempts = 3
            verification_retry_delay_seconds = 5
            actual_row_count = None

            for attempt in range(max_verification_attempts):
                query_response = influxdb3_local.query(row_count_query)

                if not query_response:
                    error_message = f"{migration_id}: Unable to verify record count for table {table_name} from Parquet file {parquet_path}"
                    influxdb3_local.error(error_message)
                    return create_http_response(
                        HttpStatus.INTERNAL_ERROR, error_message
                    )

                actual_row_count = query_response[0]["row_count"]

                # For time-bounded queries: actual >= expected is success
                # because other parquet files may have overlapping time ranges
                # Timestream UNLOAD doesn't guarantee non-overlapping timestamps
                # Only fail if actual < expected and data is missing
                if actual_row_count >= expected_row_count:
                    # Verification passed
                    if actual_row_count > expected_row_count:
                        influxdb3_local.info(
                            f"{migration_id}: Verification passed for {parquet_path}. "
                            f"Got {actual_row_count} rows (>= expected {expected_row_count}). "
                            f"Extra rows likely from overlapping time ranges in other parquet files."
                        )
                    break
                elif attempt < max_verification_attempts - 1:
                    # Row count less than expected - WAL may not have flushed yet, retry after delay
                    influxdb3_local.info(
                        f"{migration_id}: Row count less than expected for {parquet_path} "
                        f"(expected {expected_row_count}, got {actual_row_count}). "
                        f"Waiting {verification_retry_delay_seconds}s for WAL flush... "
                        f"(attempt {attempt + 1}/{max_verification_attempts})"
                    )
                    time.sleep(verification_retry_delay_seconds)

            if actual_row_count < expected_row_count:
                error_message = f"{migration_id}: Migration failed for Parquet file {parquet_path}: expected at least {expected_row_count} rows, got {actual_row_count} rows (after {max_verification_attempts} attempts)"
                influxdb3_local.error(error_message)
                migration_record["status"] = MIGRATION_FAILED
                migration_records[parquet_path] = migration_record
                influxdb3_local.cache.put(
                    key="migration-records",
                    value=migration_records,
                    ttl=CACHE_PUT_TTL_SECONDS,
                )
                return create_http_response(HttpStatus.INTERNAL_ERROR, error_message)
            else:
                migration_record["status"] = MIGRATION_COMPLETED
                migration_records[parquet_path] = migration_record
                table_tally = table_counts.get(table_name, 0)
                table_counts[table_name] = table_tally + current_parquet_row_count
                influxdb3_local.cache.put(
                    key="migration-table-counts",
                    value=table_counts,
                    ttl=CACHE_PUT_TTL_SECONDS,
                )
                influxdb3_local.cache.put(
                    key="migration-records",
                    value=migration_records,
                    ttl=CACHE_PUT_TTL_SECONDS,
                )
                put_done_file(
                    influxdb3_local,
                    parquet_path,
                    migration_record["presigned_done_url"],
                )
                influxdb3_local.info(
                    f"{migration_id}: Migration complete and verified for Parquet file {parquet_path}"
                )
        if migration_record["status"] != MIGRATION_COMPLETED:
            all_parquet_files_migrated = False

    if all_parquet_files_migrated:
        success_message = f"{migration_id}: All Parquet files migrated"
        influxdb3_local.info(success_message)
        # Return final table row counts for client to verify against InfluxDB
        return create_http_response(
            HttpStatus.OK, success_message, table_row_counts=table_counts
        )
    return create_http_response(
        HttpStatus.ACCEPTED,
        "Verified outstanding migrations. Migrations are still in progress or pending verification",
        table_row_counts=table_counts,
    )


def write_to_ingestion_buffer(
    influxdb3_local, migration_id, db_name, presigned_get_url, parquet_path
):
    """
    Writes parquet data to the InfluxDB ingestion buffer.

    Args:
        influxdb3_local: InfluxDB v3 local client.
        migration_id (str): The migration ID.
        db_name (str): Database name.
        presigned_get_url (str): Pre-signed GET URL for the Parquet file.
        parquet_path (str): S3 path to the parquet file.

    Returns:
        dict: Ingestion statistics including row_count, min_time_ns, max_time_ns
    """
    influxdb3_local.info(f"{migration_id}: Starting migration")

    # Assuming Parquet path, within S3 bucket, is organized as database-name/table-name/results/. . .
    if not parquet_path:
        raise RuntimeError("Parquet file path was empty")
    table_name_parts = parquet_path.split("/")
    if len(table_name_parts) < 2:
        raise RuntimeError(
            "Parquet file path was incorrectly formatted. Path should start wtih database-name/table-name"
        )
    table_name = table_name_parts[1]
    ingestion_stats = ingest_parquet_file_in_chunks(
        influxdb3_local, presigned_get_url, db_name, table_name, parquet_path
    )
    influxdb3_local.info(f"{migration_id}: Wrote {parquet_path} to buffer")
    return ingestion_stats


def put_done_file(influxdb3_local, s3_key: str, presigned_done_url: str):
    """
    Using a pre-signed URL, PUTs a done.ack file in an S3 bucket. For a Parquet file,
    done.ack will be placed in example.parquet/done.ack.

    Args:
        influxdb3_local: The local InfluxDB v3 client.
        s3_key (str): The S3 key that identifies the Parquet file.
        presigned_done_url (str): The pre-signed URL that allows PUTting a done.ack file
            in a Parquet file's object key.

    Returns:
        None
    """
    try:
        influxdb3_local.info(f"Putting done file for {s3_key}")
        response: requests.Response = requests.put(presigned_done_url, data=b"")
        response.raise_for_status()
        influxdb3_local.info(f"Put done file for {s3_key}")
    except Exception as e:
        influxdb3_local.error(f"Error putting done file for {s3_key}: {str(e)}")
    return


def read_parquet_in_batches(
    influxdb3_local, presigned_get_url: str, s3_key: str, batch_size
):
    reader = PresignedRangeReader(presigned_get_url)
    parquet_file = pq.ParquetFile(reader)

    influxdb3_local.info(f"Reading S3 file: {s3_key}")

    try:
        for batch in parquet_file.iter_batches(batch_size=batch_size):
            # Verify batch is not empty
            if len(batch) == 0:
                continue

            yield batch
    finally:
        reader.close()


def ingest_parquet_file_in_chunks(
    influxdb3_local,
    presigned_get_url: str,
    db_name: str,
    table_name: str,
    s3_key: str,
    chunk_size: int = 2000,
):
    """
    Read parquet file in chunks and format output as requested.
    Format: <table-name>, <Dimension keys and values>: <measure_name>-<measure_name_value>: <timestamp>

    Args:
        influxdb3_local: InfluxDB v3 local client.
        presigned_get_url (str): Pre-signed GET URL for a Parquet file.
        db_name (str): Database name.
        table_name (str): Table name.
        s3_key (str): S3 object key.
        chunk_size (int): Number of records to read at a time (default: 10000).

    Returns:
        dict: Ingestion statistics including row_count, min_time_ns, max_time_ns
    """

    influxdb3_local.info("Processing Parquet files in chunks")

    start_time = time.time()

    records_processed = 0
    chunk_number = 0
    processing_start_time = time.time()

    min_time_ns = None
    max_time_ns = None

    # Read the file in batches.
    for batch in read_parquet_in_batches(
        influxdb3_local, presigned_get_url, s3_key, chunk_size
    ):
        chunk_number += 1
        batch_schema = batch.schema
        column_types = []
        time_col_idx = None
        for idx, field in enumerate(batch_schema):
            # We only care about double and int64 as other panda data types can be inferred during line protocol generation
            if pa.types.is_floating(field.type):
                column_types.append(("double", field.name))
            elif pa.types.is_integer(field.type):
                column_types.append(("int64", field.name))
            else:
                column_types.append(("skip", field.name))
            if field.name.lower() == "time":
                time_col_idx = idx

        df_chunk = batch.to_pandas(types_mapper={pa.int64(): pandas.Int64Dtype()}.get)

        # Track min/max time from this batch for verification
        if time_col_idx is not None and "time" in df_chunk.columns:
            batch_min = df_chunk["time"].min()
            batch_max = df_chunk["time"].max()
            if pandas.notna(batch_min):
                batch_min_ns = pandas.Timestamp(batch_min).value
                if min_time_ns is None or batch_min_ns < min_time_ns:
                    min_time_ns = batch_min_ns
            if pandas.notna(batch_max):
                batch_max_ns = pandas.Timestamp(batch_max).value
                if max_time_ns is None or batch_max_ns > max_time_ns:
                    max_time_ns = batch_max_ns

        influxdb3_local.info(
            f"Processing Chunk {chunk_number} ({len(df_chunk):,} records)"
        )

        # Process each record in the chunk.
        for row in df_chunk.itertuples(index=False, name=None):
            line_protocol = transform_row_to_lp(
                influxdb3_local, row, table_name, column_types
            )
            if len(line_protocol.fields.items()) == 0:
                influxdb3_local.info(
                    f"Line protocol was ignored as no fields were set: {line_protocol}"
                )
            else:
                try:
                    influxdb3_local.write_to_db(db_name, line_protocol)
                    records_processed += 1
                except Exception as e:
                    influxdb3_local.error(f"Error writing: {str(e)}")

        influxdb3_local.info(
            f"Completed chunk {chunk_number} - Total processed records: {records_processed:,}"
        )

    end_time = time.time()
    total_time = end_time - start_time
    processing_time = end_time - processing_start_time

    influxdb3_local.info(f"Completed processing all {records_processed:,} records")
    influxdb3_local.info(f"Records processed: {records_processed:,}")
    influxdb3_local.info(f"Processing time: {processing_time:.2f} seconds")
    influxdb3_local.info(f"Total: {total_time:.2f} seconds")
    influxdb3_local.info(
        f"Processing rate: {records_processed / processing_time:.0f} records/second"
    )

    return {
        "row_count": records_processed,
        "min_time_ns": min_time_ns,
        "max_time_ns": max_time_ns,
    }


def transform_row_to_lp(influxdb3_local, row, table_name, column_types):
    """
    Transforms data into LineBuilder objects for writing to InfluxDB.

    Args:
        influxdb3_local (InfluxDB client): Logging and ingestion client.
        row (str): Row in parquet file.
        table_name (str): Table name.
        column_types(dict): Column types and names (only includes types double and int64)

    Returns:
        LineBuilder: LineBuilder object ready for writing to InfluxDB.
    """

    builder = LineBuilder(table_name)
    parse_dimensions = True

    for (column_type, column_name), val in zip(column_types, row):
        col_lower = column_name.lower()
        if pandas.isna(val):
            continue
        elif col_lower in ["measure_name"]:
            builder.tag(column_name, val)
        elif col_lower in ["time"]:
            try:
                ts = pandas.to_datetime(val)
                # Get the Unix timestamp in nanoseconds
                unix_timestamp_ns = ts.value
                builder.time_ns(str(unix_timestamp_ns))
            except:
                influxdb3_local.error("Failed to convert timestamp to unix timestamp")
                sys.exit()

            # All columns prior to time and measure_name are dimensions.
            parse_dimensions = False
        elif parse_dimensions and isinstance(
            val, (str, numpy.bool_, pandas.StringDtype().type)
        ):
            builder.tag(column_name, val)
        elif not parse_dimensions and (pandas.isna(val) or val is None):
            influxdb3_local.info(
                f"Skipping field value with nulled or missing value for column {column_name}"
            )
        elif isinstance(val, pandas.Timestamp):
            builder.string_field(column_name, str(val))
        elif column_type == "double":
            builder.float64_field(column_name, float(val))
        elif column_type == "int64":
            builder.int64_field(column_name, numpy.int64(val))
        elif isinstance(val, str):
            builder.string_field(column_name, val)
        elif isinstance(val, (bool, numpy.bool_, pandas.BooleanDtype().type)):
            builder.bool_field(column_name, bool(val))
        else:
            influxdb3_local.error(
                f"Failed to parse column name: {column_name} value: {val} type: {column_type}"
            )

    return builder


def parse_arg(influxdb3_local, arg_key, args):
    if args and arg_key in args:
        return str(args[arg_key])

    error_message = f"{arg_key} not supplied in args"
    influxdb3_local.error(error_message)
    raise RuntimeError(error_message)
