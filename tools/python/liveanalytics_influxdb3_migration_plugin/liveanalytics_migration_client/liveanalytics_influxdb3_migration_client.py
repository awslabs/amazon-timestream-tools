"""
InfluxDB Migration Plugin Wrapper Script

This script handles Timestream to InfluxDB migration operations including:
- Timestream unload operations to S3.
- S3 parquet file discovery and pre-signed URL generation.
- InfluxDB metadata management.

Usage:
    python influxdb_migration_wrapper.py --live-analytics-database-name <Timestream for LiveAnalytics database name> --s3-bucket-name <S3 bucket name> [--migration-state <migration state>]
"""

import argparse
import sys
import os
import time
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from influxdb_client_3 import InfluxDBClient3, WritePrecision
import json
import logging
import requests
from datetime import datetime, timezone

MIGRATION_METADATA_TABLE: str = "liveanalytics_migration_metadata"
TRIGGER_NAME: str = "migration_trigger"
UNLOAD_WAIT_SECONDS: int = 10


class InfluxDBMigrationWrapper:
    def __init__(
        self,
        liveanalytics_database: str,
        s3_bucket_name: str,
        resume_migration: bool = False,
        timeout_seconds: int = 120,
        region: str = "us-west-2",
        max_parquet_files: int | None = None,
    ) -> None:
        """
        Initialize

        Args:
            liveanalytics_database (str): Timestream for LiveAnalytics database name.
            s3_bucket (str): S3 bucket name.
            resume_migration (bool): Whether to resume an existing migration, skipping unload operations.
            timeout_seconds (int): The number of seconds to wait for each migration request.
            region (str): The AWS Region to use.
            max_parquet_files (int | None): The maximum number of Parquet files to migrate.

        Returns:
            None
        """
        self.liveanalytics_database: str = liveanalytics_database
        self.s3_bucket_name: str = s3_bucket_name
        self.resume_migration: bool = resume_migration
        self.timeout_seconds: int = timeout_seconds
        self.region: str = region
        self.max_parquet_files: int | None = max_parquet_files

        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger: logging.Logger = logging.getLogger(__name__)

        migration_date = int(datetime.now(tz=timezone.utc).timestamp())
        self.migration_id = f"migration-{migration_date}"

        self.setup_clients()

    def setup_clients(self):
        try:
            self.s3_client = boto3.client("s3", region_name=self.region)
            self.timestream_write_client = boto3.client(
                "timestream-write", region_name=self.region
            )
            self.timestream_query_client = boto3.client(
                "timestream-query", region_name=self.region
            )

            self.influx_host = os.getenv("INFLUXDB3_HOST_URL")
            self.influx_token = os.getenv("INFLUXDB3_AUTH_TOKEN")
            self.influx_database = os.getenv("INFLUXDB3_DATABASE_NAME")
            self.influxdb_client = InfluxDBClient3(
                host=self.influx_host,
                token=self.influx_token,
                database=self.influx_database,
            )

            self.logger.info("Successfully initialized clients")

        except Exception as e:
            self.logger.error(f"Failed to initialize clients: {str(e)}")
            sys.exit(1)

    def info(self, message: str) -> None:
        self.logger.info(message)

    def debug(self, message: str) -> None:
        self.logger.debug(message)

    def error(self, message: str, error_details: str = "") -> None:
        self.logger.error(f"{message} {error_details}")

    def warning(self, message: str) -> None:
        self.logger.warning(message)

    def unload_db(self, resume: bool = False) -> None:
        """
        Unloads an entire database from Timestream for LiveAnalytics to S3.

        Returns:
            None
        """
        self.info(
            f"Starting unload operation for database: {self.liveanalytics_database}"
        )

        tables = []
        next_token = None

        try:
            while True:
                params = {"DatabaseName": self.liveanalytics_database}
                if next_token:
                    params["NextToken"] = next_token

                response = self.timestream_write_client.list_tables(**params)

                tables.extend([t["TableName"] for t in response.get("Tables", [])])

                next_token = response.get("NextToken")
                if not next_token:
                    break
        except (ClientError, BotoCoreError) as e:
            self.error("Listing tables in database query failed: ", str(e))
            sys.exit(1)

        self.info(f"Found {len(tables)} tables to unload: {tables}")

        for table in tables:
            self.unload_table(table, resume)

            # Set manifest information.
            manifest_info = self.get_manifest_files(table)
            if manifest_info:
                if not hasattr(self, "table_manifests"):
                    self.table_manifests = {}
                self.table_manifests[table] = manifest_info
                file_count = 0
                row_count = 0
                for manifest in manifest_info:
                    file_count += manifest.get("file_count", 0)
                    row_count += manifest.get("total_rows", 0)
                self.info(
                    f"Manifest information for {table}: {file_count} files, "
                    f"{row_count} rows"
                )

    def unload_table(self, table_name: str, resume: bool = False) -> None:
        """
        Unloads a specific table from Timestream for LiveAnalytics to S3.
        Splits the UNLOAD into multiple queries, each covering at most 99 days
        to avoid the 100 partition limit with partition_by.

        Args:
            table_name (str): Name of the table to unload.
            resume (bool): Whether a migration is being resumed. If this is
                True, no UNLOAD queries will be sent.

        Returns:
            None

        Raises:
            RuntimeError: If UNLOAD fails, indicating partial writes may exist in S3.
        """
        try:
            time_range = self.get_table_time_range(table_name)
            if not time_range:
                self.warning(f"No data found in table {table_name}, skipping UNLOAD")
                return

            min_time, max_time = time_range
            self.info(f"Table {table_name} time range: {min_time} to {max_time}")

            # Calculate total days and number of UNLOAD chunks needed
            from datetime import timedelta

            total_days = (max_time - min_time).days + 1
            max_days_per_chunk = 99
            num_chunks = (total_days + max_days_per_chunk - 1) // max_days_per_chunk

            self.info(
                f"Dataset will be chronologically split into {num_chunks} chunk(s)"
            )

            chunk_start = min_time
            chunk_num = 0

            while chunk_start <= max_time:
                chunk_num += 1
                chunk_end = min(
                    chunk_start + timedelta(days=max_days_per_chunk - 1), max_time
                )

                start_str = chunk_start.strftime("%Y-%m-%d 00:00:00")
                end_str = (chunk_end + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

                self.info(
                    f"UNLOAD chunk {chunk_num}/{num_chunks}: {chunk_start.date()} to {chunk_end.date()}"
                )

                self.unload_table_chunk(
                    table_name, start_str, end_str, chunk_num, resume
                )

                chunk_start = chunk_end + timedelta(days=1)

            self.info(
                f"UNLOAD complete for table {table_name}: {chunk_num} chunk(s) processed"
            )

        except Exception as e:
            self.error(
                f"UNLOAD operation failed for s3://{self.s3_bucket_name}/{self.liveanalytics_database}/{table_name}/"
            )
            self.error(f"Error: {str(e)}")

            raise RuntimeError(
                f"UNLOAD failed for table {table_name}: {str(e)}. "
                f"Partial writes may exist in S3. Migration cancelled."
            )

    def get_table_time_range(self, table_name: str) -> tuple | None:
        """
        Gets the min and max time values from a Timestream table.

        Args:
            table_name (str): Name of the table.

        Returns:
            tuple: (min_datetime, max_datetime) or None if no data.
        """
        try:
            response = self.timestream_query_client.query(
                QueryString=f"""
                    SELECT min(time) as min_time, max(time) as max_time 
                    FROM "{self.liveanalytics_database}"."{table_name}"
                """
            )

            next_token = response.get("NextToken")
            while next_token is not None:
                response = self.timestream_query_client.query(
                    NextToken=next_token,
                    QueryString=f"""
                    SELECT min(time) as min_time, max(time) as max_time 
                    FROM "{self.liveanalytics_database}"."{table_name}"
                """,
                )
                next_token = response.get("NextToken")
                time.sleep(UNLOAD_WAIT_SECONDS)

            rows = response.get("Rows", [])
            if not rows:
                return None

            row_data = rows[0].get("Data", [])
            if len(row_data) < 2:
                return None

            min_time_str = row_data[0].get("ScalarValue")
            max_time_str = row_data[1].get("ScalarValue")

            if not min_time_str or not max_time_str:
                return None

            from datetime import datetime

            min_time = datetime.fromisoformat(
                min_time_str.replace(" ", "T").split(".")[0]
            )
            max_time = datetime.fromisoformat(
                max_time_str.replace(" ", "T").split(".")[0]
            )

            return (min_time, max_time)

        except Exception as e:
            self.warning(f"Failed to get time range for table {table_name}: {e}")
            return None

    def unload_table_chunk(
        self,
        table_name: str,
        start_time: str,
        end_time: str,
        chunk_num: int,
        resume: bool = False,
    ) -> None:
        """
        Unloads a time-bounded chunk of a table to S3 with partition_by day.
        Each chunk writes to a unique path to get its own manifest file.

        Args:
            table_name (str): Name of the table.
            start_time (str): Start time (inclusive) in format 'YYYY-MM-DD HH:MM:SS'.
            end_time (str): End time (exclusive) in format 'YYYY-MM-DD HH:MM:SS'.
            chunk_num (int): Chunk number for unique S3 path.
            resume (bool): Whether a migration is being resumed. If this is True, no UNLOAD
                queries will be sent.
        """
        # Unload from Timestream for LiveAnalytics to S3.
        if not resume:
            self.info(f"Unloading table: {table_name}")
            try:
                chunk_path = f"s3://{self.s3_bucket_name}/{self.liveanalytics_database}/{table_name}/chunk_{chunk_num:03d}"

                # Use partition_by for smaller files (one per day)
                response = self.timestream_query_client.query(
                    QueryString=f"""
                        UNLOAD (
                            SELECT *, DATE_FORMAT(time, '%y-%m-%d') as partition_date 
                            FROM "{self.liveanalytics_database}"."{table_name}"
                            WHERE time >= '{start_time}' AND time < '{end_time}'
                        ) 
                        TO '{chunk_path}' 
                        WITH (partitioned_by = ARRAY['partition_date'], 
                              format = 'PARQUET', 
                              max_file_size='16MB', 
                              compression = 'NONE')
                        """
                )

                http_status = response.get("ResponseMetadata", {}).get(
                    "HTTPStatusCode", 0
                )
                if http_status < 200 or http_status >= 300:
                    raise RuntimeError(f"UNLOAD chunk returned HTTP {http_status}")

                # Wait for UNLOAD to complete.
                next_token = response.get("NextToken")
                while next_token is not None:
                    # Note: whitespace must match the original query.
                    response = self.timestream_query_client.query(
                        NextToken=next_token,
                        QueryString=f"""
                        UNLOAD (
                            SELECT *, DATE_FORMAT(time, '%y-%m-%d') as partition_date 
                            FROM "{self.liveanalytics_database}"."{table_name}"
                            WHERE time >= '{start_time}' AND time < '{end_time}'
                        ) 
                        TO '{chunk_path}' 
                        WITH (partitioned_by = ARRAY['partition_date'], 
                              format = 'PARQUET', 
                              max_file_size='16MB', 
                              compression = 'NONE')
                        """,
                    )
                    next_token = response.get("NextToken")
                    unload_progress = response.get("QueryStatus", {}).get(
                        "ProgressPercentage"
                    )
                    self.info(f"Unload progress: {str(unload_progress)}%")
                    if unload_progress >= 100.0:
                        break
                    time.sleep(UNLOAD_WAIT_SECONDS)
            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "")
                raise RuntimeError(f"UNLOAD chunk failed: {error_code} - {str(e)}")

    def get_manifest_files(
        self,
        table_name: str,
    ) -> list[dict] | None:
        """
        Gets the manifest file for a table and each chunk from S3.
        The manifest file contains the complete list of parquet files and row counts.

        Args:
            table_name (str): Name of the table being unloaded.
            chunk_num (int): Chunk number to find specific manifest.

        Returns:
            dict: Manifest info with 'file_count', 'total_rows', 'files' or None if not found.
        """
        prefix = f"{self.liveanalytics_database}/{table_name}/"

        self.info(f"Getting manifest files for table {table_name}...")

        manifests = []

        try:
            paginator = self.s3_client.get_paginator("list_objects_v2")

            for page in paginator.paginate(Bucket=self.s3_bucket_name, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key = obj.get("Key", "")
                    if "_manifest" in key:
                        self.info(f"Found manifest file: {key}")
                        manifests.append(self.parse_manifest_file(key))

        except Exception as e:
            self.warning(f"Error checking for manifest file: {e}")

        if not manifests:
            self.error(f"Manifest file not found for table {table_name}")
            return None
        return manifests

    def parse_manifest_file(self, manifest_key: str) -> dict | None:
        """
        Parses the Timestream UNLOAD manifest file to get file list and row counts.

        Manifest format:
        {
          "result_files": [
            {"url": "s3://...", "file_metadata": {"row_count": 10}},
            ...
          ],
          "query_metadata": {"total_row_count": 30},
          "author": {"name": "Amazon Timestream", "manifest_file_version": "1.0"}
        }

        Args:
            manifest_key (str): S3 key of the manifest file.

        Returns:
            dict: Contains 'file_count', 'total_rows', 'files' list.
        """
        try:
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket_name, Key=manifest_key
            )
            manifest_content = response["Body"].read().decode("utf-8")
            manifest = json.loads(manifest_content)

            result_files = manifest.get("result_files", [])
            query_metadata = manifest.get("query_metadata", {})
            total_rows = query_metadata.get("total_row_count", 0)

            if total_rows == 0:
                raise RuntimeError(
                    f"Failed to get row count for manifest {manifest_key}"
                )

            return {
                "file_count": len(result_files),
                "total_rows": total_rows,
                "files": result_files,
            }

        except Exception as e:
            self.warning(f"Failed to parse manifest file {manifest_key}: {e}")
            return None

    def get_parquet_files_from_manifests(self) -> list[str]:
        """
        Gets the list of parquet files from manifest files generated during UNLOAD.

        Returns:
            list[str]: List of parquet file S3 keys

        Raises:
            RuntimeError: If no manifest files are available.
        """
        if not hasattr(self, "table_manifests") or not self.table_manifests:
            raise RuntimeError(
                "No manifest files found. UNLOAD must complete successfully "
                "and generate manifest files before migration can proceed."
            )

        parquet_keys: list[str] = []
        total_rows = 0

        for table_name, manifest_list in self.table_manifests.items():
            table_files = 0
            table_rows = 0

            for manifest_info in manifest_list:
                files = manifest_info.get("files", [])
                table_files += len(files)
                table_rows += manifest_info.get("total_rows", 0)

                for file_entry in files:
                    url = file_entry.get("url", "")
                    if url:
                        parts = url.replace("s3://", "").split("/", 1)
                        if len(parts) == 2:
                            s3_key = parts[1]
                            parquet_keys.append(s3_key)

            self.info(
                f"Table {table_name}: {table_files} files, {table_rows:,} rows from {len(manifest_list)} chunks"
            )
            total_rows += table_rows

        if not parquet_keys:
            raise RuntimeError(
                "No parquet files found in manifest files. "
                "UNLOAD may have produced empty results."
            )

        self.info(f"Total: {len(parquet_keys)} parquet files, {total_rows:,} rows")
        return parquet_keys

    def generate_metadata(
        self, parquet_file_names: list[str], expiration: int = 604_800
    ) -> dict[str, dict[str, str]]:
        """
        Generates metadata, including presigned URLs for S3 objects.
        This metadata will be a dict[str, dict[str, str]] with the following structure:

            {
                "S3 object key": {
                    "presigned_get_url": "pre-signed GET URL",
                    "presigned_done_url": "pre-signed done (PUT) URL"
                },
                . . .
            }

        In InfluxDB v3, the metadata table will hold the same information, with "migration_id"
        as a tag and "presigned_url" and "migration_status" as fields.

        Args:
            parquet_file_names (list[str]): List of parquet file names.
            expiration (int): URL expiration time in seconds. Defaults to 604_800 (7 days).

        Returns:
            dict[str, dict[str, str]]: S3 object key mapped to a file's pre-signed GET
                and PUT URLs.
        """

        # Metadata is eventually ingested into InfluxDB v3 and is used by the plugin
        # to manage states and access S3 using presigned (GET and PUT) URLs.
        metadata = {}

        for file_key in parquet_file_names:
            try:
                _ = self.s3_client.put_object_legal_hold(
                    Bucket=self.s3_bucket_name, Key=file_key, LegalHold={"Status": "ON"}
                )

                # Generate presigned URLs.
                presigned_get_url: str = self.s3_client.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": self.s3_bucket_name, "Key": file_key},
                    ExpiresIn=expiration,
                )
                # "Done" files (done.ack) will not have an object lock. Object locks for
                # presigned put_object calls are not supported.
                presigned_done_url: str = self.s3_client.generate_presigned_url(
                    "put_object",
                    Params={
                        "Bucket": self.s3_bucket_name,
                        "Key": f"{file_key}/done.ack",
                    },
                    ExpiresIn=expiration,
                )
                metadata[file_key] = {}
                metadata[file_key]["presigned_get_url"] = presigned_get_url
                metadata[file_key]["presigned_done_url"] = presigned_done_url

                self.info(f"Generated metadata for: {file_key}")
            except ClientError as e:
                self.error(f"Error generating metadata for {file_key}: ", str(e))

        return metadata

    def setup_influxdb_metadata(self) -> bool:
        """
        Creates an InfluxDB v3 database and metadata table with retention policy using the InfluxDB v3 HTTP API.
        """
        try:
            headers = {
                "Authorization": f"Bearer {self.influx_token}",
                "Content-Type": "application/json",
            }

            self.info(f"Creating InfluxDB database: {self.influx_database}")
            db_url = f"{self.influx_host}/api/v3/configure/database"
            db_payload = {"db": self.influx_database}

            try:
                response: requests.Response = requests.post(
                    db_url,
                    json=db_payload,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
                if response.status_code == 201 or response.status_code == 200:
                    self.info(f"Successfully created database: {self.influx_database}")
                elif response.status_code == 409:
                    self.info(f"Database already exists: {self.influx_database}")
                else:
                    self.error(
                        f"Failed to create database. Status: {response.status_code}, Response: {response.text}"
                    )
                    return False
            except requests.exceptions.RequestException as e:
                self.error(f"Error creating database: ", str(e))
                return False

            if not self.resume_migration:
                self.delete_metadata_table(silence_errors=True)

            # Create metadata table with 1h retention period.
            self.info(f"Creating {MIGRATION_METADATA_TABLE} table with 1h retention")
            table_url: str = f"{self.influx_host}/api/v3/configure/table"
            table_payload = {
                "db": self.influx_database,
                "table": MIGRATION_METADATA_TABLE,
                "retention_period": "1h",
                "tags": ["s3_key"],
                "fields": [
                    {"name": "presigned_get_url", "type": "utf8"},
                    {"name": "presigned_done_url", "type": "utf8"},
                ],
            }

            try:
                response = requests.post(table_url, json=table_payload, headers=headers)
                if response.status_code == 201 or response.status_code == 200:
                    self.info(f"Successfully created {MIGRATION_METADATA_TABLE} table")
                    return True
                elif response.status_code == 409:
                    self.info(f"Table {MIGRATION_METADATA_TABLE} already exists")
                else:
                    self.error(
                        f"Failed to create table. Status: {response.status_code}, Response: {response.text}"
                    )
                    return False
            except requests.exceptions.RequestException as e:
                self.error(f"Error creating table: ", str(e))
                return False
            return True

        except Exception as e:
            self.error(f"Failed to setup InfluxDB metadata: ", str(e))
            sys.exit(1)

    def write_metadata_to_influxdb(self, metadata):
        """
        Writes metadata to InfluxDB v3 using line protocol.

        Args:
            metadata (dict[str, dict[str, str]]): Mapping of random UUIDs to a file's
                pre-signed (GET and DELETE) URLs and migration status.

        Returns:
            None
        """
        try:
            self.info("Writing metadata to InfluxDB")
            lines: list[str] = []

            for s3_key, migration_metadata in metadata.items():
                lines.append(
                    f"{MIGRATION_METADATA_TABLE},"
                    f"s3_key={s3_key} "
                    f'presigned_get_url="{migration_metadata["presigned_get_url"]}",'
                    f'presigned_done_url="{migration_metadata["presigned_done_url"]}"\n'
                )

            self.influxdb_client.write(
                database=self.influx_database,
                record=lines,
                write_precision=WritePrecision.NS,
            )
            self.info(
                f"Successfully wrote {len(metadata)} metadata entries to InfluxDB"
            )

        except Exception as e:
            self.error(f"Failed to write metadata to InfluxDB: ", str(e))
            sys.exit(1)

    def bulk_invoke_http_trigger(self, metadata):
        """
        Invokes the HTTP migration processing engine trigger for all Parquet files to be migrated.

        Args:
            metadata (dict[str, dict[str, str]]): Mapping of S3 keys to presigned URLs.

        Returns:
            None
        """
        session = requests.Session()

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=requests.adapters.Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[500, 502, 503, 504],
                allowed_methods=["POST"],
            ),
        )
        session.mount("https://", adapter)
        session.headers.update({"Connection": "keep-alive"})

        url = f"{self.influx_host}/api/v3/engine/{TRIGGER_NAME}"
        headers = {"Authorization": f"Bearer {self.influx_token}"}

        # For a new migration, delete the trigger cache.
        if not self.resume_migration:
            params = {"delete_cache": True}
            _ = session.post(
                url=url,
                headers=headers,
                params=params,
                timeout=self.timeout_seconds,
            )
        try:
            metadata_table_deleted: bool = False
            num_parquet_files_submitted = 0
            for s3_key in metadata:
                if (
                    self.max_parquet_files is not None
                    and num_parquet_files_submitted >= self.max_parquet_files
                ):
                    break
                table_name = s3_key.split("/")[1]
                self.info(
                    f'Migrating {s3_key} to "{self.influx_database}"."{table_name}"'
                )
                json_body = {"parquet_path": s3_key}

                trigger_invocation_response = session.post(
                    url=url,
                    json=json_body,
                    headers=headers,
                    timeout=self.timeout_seconds,
                )
                trigger_invocation_response.raise_for_status()
                response_body = trigger_invocation_response.json()

                if response_body["status"] != 200 and response_body["status"] != 202:
                    raise RuntimeError(
                        f"Migrating {s3_key} failed: {response_body['message']}"
                    )

                if not metadata_table_deleted:
                    self.delete_metadata_table()
                    metadata_table_deleted = True
                num_parquet_files_submitted += 1

            # Final verification invocation.
            verification_params = {"verify": True, "delete_cache": True}
            final_invocation_response = session.post(
                url=url,
                headers=headers,
                params=verification_params,
                timeout=self.timeout_seconds,
            )
            final_invocation_response.raise_for_status()
            response_json = final_invocation_response.json()

            if response_json["status"] != 200:
                raise Exception(
                    f"Final verification failed: {response_json['message']}"
                )

            # Store expected table row counts for final verification
            self.expected_table_row_counts = response_json.get("table_row_counts", {})
            self.debug(f"Invocation response: {response_json}")

        except Exception as e:
            self.error(
                f"HTTP invocation failed: {e}. View processing engine logs for more information"
            )
            raise
        finally:
            session.close()

        # Trigger is only deleted when a migration is successful. This allows users to resume a
        # migration if an error occurs.
        self.delete_trigger()

    def get_num_completed_and_total_parquet_files(self):
        """
        Gets the number of migrated and total Parquet files in an S3 bucket.

        Returns:
            tuple[int, int]: The number of completed (number of .ack files) and number of
                total Parquet files.
        """
        s3 = boto3.resource("s3", region_name=self.region)
        bucket = s3.Bucket(self.s3_bucket_name)
        parquet_count: int = 0
        completed_count: int = 0
        for obj in bucket.objects.filter(
            Prefix=f"{self.liveanalytics_database}/"
        ).all():
            if obj.key.endswith(".parquet"):
                parquet_count += 1
            if obj.key.endswith(".ack"):
                completed_count += 1
        return (completed_count, parquet_count)

    def delete_trigger(self):
        try:
            self.info("Deleting processing engine trigger")

            headers = {
                "Authorization": f"Bearer {self.influx_token}",
                "Content-Type": "application/json",
            }

            disable_trigger_url = (
                f"{self.influx_host}/api/v3/configure/processing_engine_trigger/disable"
            )

            trigger_payload = {
                "db": self.influx_database,
                "trigger_name": TRIGGER_NAME,
                "plugin_filename": "liveanalytics_migration_plugin/liveanalytics_migration_plugin.py",
                "trigger_specification": f"request:{TRIGGER_NAME}",  # Creates /api/v3/engine/<TRIGGER_NAME> endpoint.
                "trigger_settings": {"run_async": False, "error_behavior": "log"},
                "disabled": "true",
                "trigger_arguments": {
                    "db_name": self.influx_database,
                    "s3_bucket": self.s3_bucket_name,
                    "migration_id": self.migration_id,
                },
            }

            disable_response: requests.Response = requests.post(
                disable_trigger_url, params=trigger_payload, headers=headers
            )
            disable_response.raise_for_status()

            delete_trigger_url = (
                f"{self.influx_host}/api/v3/configure/processing_engine_trigger"
            )

            delete_body = {"db": self.influx_database, "trigger_name": TRIGGER_NAME}

            delete_response = requests.delete(
                delete_trigger_url, json=delete_body, headers=headers
            )
            delete_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.error("Error deleting processing engine trigger:", str(e))
        except Exception as e:
            self.error("Failed to delete processing engine trigger:", str(e))
        return

    def delete_metadata_table(self, silence_errors: bool = False):
        """
        Deletes the InfluxDB v3 migration metadata table.

        Returns:
            None
        """
        self.info(
            f'Deleting InfluxDB v3 metadata table "{self.influx_database}"."{MIGRATION_METADATA_TABLE}"'
        )

        try:
            deletion_date = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            query_params = {
                "db": self.influx_database,
                "hard_delete_at": deletion_date,
                "table": MIGRATION_METADATA_TABLE,
            }
            headers = {"Authorization": f"Bearer {self.influx_token}"}
            delete_table_response: requests.Response = requests.delete(
                url=f"{self.influx_host}/api/v3/configure/table",
                headers=headers,
                params=query_params,
            )
            delete_table_response.raise_for_status()
            self.info(f'Deleted "{self.influx_database}"."{MIGRATION_METADATA_TABLE}"')
        except Exception as e:
            if not silence_errors:
                self.error(
                    f'Failed to delete "{self.liveanalytics_database}"."{MIGRATION_METADATA_TABLE}": {e}'
                )

    def create_processing_engine_trigger(self):
        """
        Creates a processing engine trigger scheduled for 10 seconds in the future UTC.

        Returns:
            None
        """
        try:
            self.info(f"Creating processing engine HTTP trigger")

            headers = {
                "Authorization": f"Bearer {self.influx_token}",
                "Content-Type": "application/json",
            }

            trigger_url = (
                f"{self.influx_host}/api/v3/configure/processing_engine_trigger"
            )
            trigger_payload = {
                "db": self.influx_database,
                "trigger_name": TRIGGER_NAME,
                "plugin_filename": "liveanalytics_migration_plugin/liveanalytics_migration_plugin.py",
                "trigger_specification": f"request:{TRIGGER_NAME}",  # Creates /api/v3/engine/<TRIGGER_NAME> endpoint.
                "trigger_settings": {"run_async": False, "error_behavior": "log"},
                "disabled": False,
                "trigger_arguments": {
                    "db_name": self.influx_database,
                    "s3_bucket": self.s3_bucket_name,
                    "migration_id": self.migration_id,
                },
            }

            response: requests.Response = requests.post(
                trigger_url, json=trigger_payload, headers=headers
            )
            if response.status_code == 200 or response.status_code == 201:
                self.info(
                    f"Successfully created processing engine trigger: {TRIGGER_NAME}"
                )
            elif response.status_code == 409:
                self.info("Processing engine trigger already exists")
            else:
                self.error(
                    f"Failed to create trigger. Status: {response.status_code}, Response: {response.text}"
                )
        except requests.exceptions.RequestException as e:
            self.error("Error creating processing engine trigger: ", str(e))

        except Exception as e:
            self.error("Failed to create processing engine trigger: ", str(e))

    def run(self):
        """
        Main execution method.

        Returns:
            None

        Raises:
            RuntimeError: If verification of buckets fails or metadata setup fails.
        """
        self.info("Starting InfluxDB Migration Wrapper")
        self.info(f"Database: {self.liveanalytics_database}")
        self.info(f"S3 Bucket: {self.s3_bucket_name}")
        self.info(f"Resuming migration: {self.resume_migration}")

        if not self.verify_bucket():
            raise RuntimeError("Unable to verify bucket")

        if self.resume_migration:
            self.info("Resuming migration and skipping unload operations")
        self.unload_db(self.resume_migration)

        # Get parquet file list from manifest files (generated during each UNLOAD chunk)
        parquet_file_names: list[str] = self.get_parquet_files_from_manifests()

        metadata: dict[str, dict[str, str]] = self.generate_metadata(
            parquet_file_names=parquet_file_names
        )

        # Setup InfluxDB metadata.
        is_metadata_setup_complete: bool = self.setup_influxdb_metadata()
        if not is_metadata_setup_complete:
            raise RuntimeError("Failed to setup database and table in InfluxDB")

        self.write_metadata_to_influxdb(metadata)
        self.create_processing_engine_trigger()
        self.bulk_invoke_http_trigger(metadata)

        # Final verification: compare expected row counts from plugin with actual InfluxDB counts
        self.verify_final_row_counts()

        self.delete_unloaded_data()

        self.info("Migration wrapper completed successfully")

    def delete_unloaded_data(self):
        """
        Deletes all unloaded data from an S3 bucket.

        Returns:
            None
        """
        self.info(
            f"Deleting all S3 objects in s3://{self.s3_bucket_name}/{self.liveanalytics_database}/"
        )
        paginator = self.s3_client.get_paginator("list_object_versions")
        pages = paginator.paginate(
            Bucket=self.s3_bucket_name, Prefix=f"{self.liveanalytics_database}/"
        )

        for page in pages:
            for version in page.get("Versions", []):
                # Disable object lock. Object lock is only applied to Parquet files.
                if version["Key"].endswith(".parquet"):
                    try:
                        object_lock_response = self.s3_client.get_object_legal_hold(
                            Bucket=self.s3_bucket_name,
                            Key=version["Key"],
                            VersionId=version["VersionId"],
                        )
                        if object_lock_response["LegalHold"]["Status"] == "ON":
                            self.s3_client.put_object_legal_hold(
                                Bucket=self.s3_bucket_name,
                                Key=version["Key"],
                                VersionId=version["VersionId"],
                                LegalHold={"Status": "OFF"},
                            )
                    except Exception:
                        pass

                # Delete object.
                try:
                    self.s3_client.delete_object(
                        Bucket=self.s3_bucket_name,
                        Key=version["Key"],
                        VersionId=version["VersionId"],
                        BypassGovernanceRetention=True,
                    )
                except Exception as e:
                    self.error(
                        f"Failed to delete object {version['Key']} with version ID {version['VersionId']}: {e}"
                    )

            for marker in page.get("DeleteMarkers", []):
                try:
                    self.s3_client.delete_object(
                        Bucket=self.s3_bucket_name,
                        Key=marker["Key"],
                        VersionId=marker["VersionId"],
                    )
                except Exception as e:
                    self.error(
                        f"Failed to delete deletion marker for object {marker['Key']} with version ID {marker['VersionId']}"
                    )

        return

    def get_expected_row_counts_from_manifests(self) -> dict:
        """
        Aggregates expected row counts per table from manifest files in S3 bucket from UNLOAD operation.

        Returns:
            dict: Table name to expected total row count.
        """
        if not hasattr(self, "table_manifests") or not self.table_manifests:
            return {}

        expected_counts = {}
        for table_name, manifest_list in self.table_manifests.items():
            total_rows = 0
            for manifest_info in manifest_list:
                total_rows += manifest_info.get("total_rows", 0)
            expected_counts[table_name] = total_rows

        return expected_counts

    def verify_final_row_counts(self) -> bool:
        """
        Verifies that all tables in the migrated database have the correct row counts
        by comparing expected counts from S3 manifest files with actual InfluxDB counts.

        Returns:
            bool: True if counts match, False otherwise.
        """
        manifest_counts = self.get_expected_row_counts_from_manifests()
        plugin_counts = getattr(self, "expected_table_row_counts", {})

        if not manifest_counts:
            self.warning("No manifest data available for verification")
            return True

        if not plugin_counts:
            self.warning("No plugin row counts available for verification")
            return True

        self.info("Comparing unload manifest and ingested plugin row counts")

        verification_results = []
        total_manifest = 0
        total_plugin = 0

        for table_name, manifest_count in manifest_counts.items():
            plugin_count = plugin_counts.get(table_name, 0)
            total_manifest += manifest_count
            total_plugin += plugin_count

            if plugin_count == manifest_count:
                status = "MATCH"
            elif plugin_count < manifest_count:
                status = "ROWS_SKIPPED"
            else:
                status = "DEDUP_NEEDED"

            verification_results.append(
                {
                    "table": table_name,
                    "manifest": manifest_count,
                    "plugin": plugin_count,
                    "status": status,
                }
            )

        self.info(f"{'Table':<40} {'Manifest':>15} {'Ingested':>15} {'Status':>15}")

        for r in verification_results:
            self.info(
                f"{r['table']:<40} {r['manifest']:>15,} {r['plugin']:>15,} {r['status']:>15}"
            )

        self.info(f"{'Total':<40} {total_manifest:>15,} {total_plugin:>15,}")

        skipped = [r for r in verification_results if r["status"] == "ROWS_SKIPPED"]
        if skipped:
            self.info("Some rows are missing during migration")
            for r in skipped:
                self.info(f"  {r['table']}: {r['manifest'] - r['plugin']:,} skipped")

        dedup = [r for r in verification_results if r["status"] == "DEDUP_NEEDED"]
        if dedup:
            self.info(
                "Deduplication may be needed as extra row counts were encountered during migration"
            )
            for r in dedup:
                self.info(f"  {r['table']}: {r['plugin'] - r['manifest']:,} extra rows")

        return True

    def verify_bucket(self):
        """
        Verifies that an S3 bucket has all required settings for a migration: versioning and object lock enabled.

        Returns:
            bool: Whether the S3 bucket has all necessary settings.
        """
        versioning_response = self.s3_client.get_bucket_versioning(
            Bucket=self.s3_bucket_name
        )
        status = versioning_response.get("Status")
        if status != "Enabled":
            self.error(
                f"Bucket versioning is not enabled for bucket {self.s3_bucket_name}. Bucket versioning must be enabled in order to use object locks"
            )
            return False

        object_lock_response = self.s3_client.get_object_lock_configuration(
            Bucket=self.s3_bucket_name
        )
        config = object_lock_response.get("ObjectLockConfiguration", {})
        enabled = config.get("ObjectLockEnabled")

        if enabled is None or enabled != "Enabled":
            self.error(
                f"Object lock is not enabled for bucket {self.s3_bucket_name}. Object lock must be enabled"
            )
            return False

        rule = config.get("Rule")

        if rule is not None:
            default_retention = rule.get("DefaultRetention")
            if default_retention is not None:
                mode = default_retention.get("Mode")
                if mode != "GOVERNANCE":
                    self.error(
                        f"Bucket {self.s3_bucket_name} has a default object lock mode of {mode} but GOVERNANCE is required"
                    )
                    return False

        encrypted_properly = False
        encryption_response = self.s3_client.get_bucket_encryption(
            Bucket=self.s3_bucket_name
        )
        if (
            "ServerSideEncryptionConfiguration" in encryption_response
            and "Rules" in encryption_response["ServerSideEncryptionConfiguration"]
        ):
            for rule in encryption_response["ServerSideEncryptionConfiguration"][
                "Rules"
            ]:
                if (
                    rule["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
                    == "AES256"
                ):
                    encrypted_properly = True
                    break
        else:
            return False

        if not encrypted_properly:
            self.error("S3 bucket must use S3-SSE (AE256) encryption")
            return False

        tls_configured = False
        try:
            tls_response = self.s3_client.get_bucket_policy(Bucket=self.s3_bucket_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchBucketPolicy":
                self.error(
                    "S3 bucket did not have a policy. A policy that denies all non-TLS traffic is required"
                )
                return False
            raise
        policy = json.loads(tls_response["Policy"])

        for statement in policy.get("Statement", []):
            condition = statement.get("Condition", {})
            bool_condition = condition.get("Bool", {})
            if (
                bool_condition.get("aws:SecureTransport") == "false"
                and statement.get("Effect") == "Deny"
            ):
                tls_configured = True
                break

        if not tls_configured:
            self.error("S3 bucket policy does not deny all non-TLS traffic")
            return False

        return True


def main(input_args) -> int:
    """
    Main entry point.

    Args:
        input_args (list[str]): A list of input CLI arguments, to be parsed by argparse.

    Returns:
        int: The status code, zero for success, non-zero for failure.
    """
    parser: argparse.ArgumentParser = argparse.ArgumentParser(
        description="InfluxDB Migration Plugin Wrapper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Environment Variables Required:
    INFLUXDB3_HOST_URL      - InfluxDB host URL
    INFLUXDB3_AUTH_TOKEN    - InfluxDB authentication token
    INFLUXDB3_DATABASE_NAME - InfluxDB database name
    
    AWS_ACCESS_KEY_ID     - AWS access key
    AWS_SECRET_ACCESS_KEY - AWS secret key
    AWS_SESSION_TOKEN     - AWS session token
    AWS_DEFAULT_REGION    - AWS region (optional, defaults to us-east-1)

Examples:
    # Perform unload and metadata operations
    python liveanalytics_influxdb3_migration_client.py --live-analytics-database-name MyDatabase --s3-bucket-name my-s3-bucket
    
    # Skip unload operations (MigrationState set)
    python liveanalytics_influxdb3_migration_client.py --live-analytics-database-name MyDatabase --s3-bucket-name my-s3-bucket --resume
        """,
    )

    parser.add_argument(
        "--live-analytics-database-name",
        required=True,
        help="Name of the Timestream for LiveAnalytics database to migrate.",
    )
    parser.add_argument(
        "--s3-bucket-name", required=True, help="The S3 bucket name for parquet files."
    )
    parser.add_argument(
        "--timeout-seconds",
        default=120,
        type=int,
        required=False,
        help="The number of seconds to wait for each Parquet file migration. Defaults to 120.",
    )
    parser.add_argument(
        "--resume",
        required=False,
        action="store_true",
        help="Optional. If set, unload operations will be skipped.",
    )
    parser.add_argument(
        "--region",
        required=False,
        default="us-west-2",
        help="Optional. The AWS Region to use. Defaults to us-west-2.",
    )
    parser.add_argument(
        "--max-parquet-files",
        required=False,
        type=int,
        help="Optional. The maximum number of Parquet files to migrate. Default behaviour is to migrate all Parquet files.",
    )

    args = parser.parse_args(input_args)

    live_analytics_database_name: str = args.live_analytics_database_name
    s3_bucket_name: str = args.s3_bucket_name
    resume_migration: bool = args.resume
    timeout_seconds: int = args.timeout_seconds
    region: str = args.region
    max_parquet_files: int | None = args.max_parquet_files

    required_env_vars = [
        "INFLUXDB3_HOST_URL",
        "INFLUXDB3_AUTH_TOKEN",
        "INFLUXDB3_DATABASE_NAME",
    ]
    missing_vars = [var for var in required_env_vars if not os.getenv(var)]

    if missing_vars:
        print(
            f"Error: Missing required environment variables: {', '.join(missing_vars)}"
        )
        print("Please set the required environment variables and try again.")
        sys.exit(1)

    wrapper = InfluxDBMigrationWrapper(
        liveanalytics_database=live_analytics_database_name,
        s3_bucket_name=s3_bucket_name,
        resume_migration=resume_migration,
        timeout_seconds=timeout_seconds,
        region=region,
        max_parquet_files=max_parquet_files,
    )

    try:
        wrapper.run()
    except Exception as e:
        logging.error(f"Failed migration: {e}")
        return 1
    return 0


if __name__ == "__main__":
    main(sys.argv[1:])
