import argparse
import shutil
import os
import yaml
import logging
from boto3 import Session
from datetime import datetime, timezone
from pathlib import Path
from unload import main as unload_main
from unload.utils.s3_utils import S3Utility
from unload.utils.timestream_utils import TimestreamUtility
from targets.timestream_for_influxdb.transform import transform
from targets.timestream_for_influxdb.ingestion import influxdb_ingestion
from targets.timestream_for_influxdb.validation import validator
from influxdb_client import InfluxDBClient
from concurrent.futures import ThreadPoolExecutor
from unload.utils.logger_utils import update_logger

migration_logger = logging.getLogger("e2e-migration")

def to_iso_z(dt_str):
    return datetime.fromisoformat(dt_str).replace(tzinfo=timezone.utc).isoformat().replace('+00:00', 'Z')

def echo_config(d, indent=0):
    """Recursively print nested dictionary values with indentation."""
    for key, value in d.items():
        prefix = '  ' * indent
        if isinstance(value, dict):
            migration_logger.info(f"{prefix}{key}:")
            echo_config(value, indent + 1)
        else:
            migration_logger.info(f"{prefix}{key}: {value}")

def cleanup_athena(glue, athena_database, timestream_database, timestream_tables):
    for table in timestream_tables:
        athena_table_name = (
            timestream_database.replace("-", "_")
            + "_"
            + table.replace("-", "_")
        )
        athena_lp_table_name = f"lp_{athena_table_name}"
        try:
            migration_logger.info(f"Deleting {athena_table_name} from {athena_database}")
            glue.delete_table(
                DatabaseName=athena_database, Name=athena_table_name
            )
        except Exception as e:
            migration_logger.error(f"Error deleting {athena_table_name} from {athena_database}")
        try:
            migration_logger.info(f"Deleting {athena_lp_table_name} from {athena_database}")
            glue.delete_table(
                DatabaseName=athena_database, Name=athena_lp_table_name
            )
        except Exception as e:
            migration_logger.error(f"Error deleting {athena_table_name} from {athena_database}")

def create_athena_db_if_not_exists(boto_session, athena_database_name):
    glue_client = boto_session.client("glue")
    migration_logger.info(f"Checking Athena for database: {athena_database_name}")
    try:
        glue_client.get_database(Name=athena_database_name)
        migration_logger.info(f"Database '{athena_database_name}' already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        glue_client.create_database(
            DatabaseInput={"Name": athena_database_name}
        )
        migration_logger.info(f"Database '{athena_database_name}' created successfully.")
    finally:
        glue_client.close()

def execute_jobs(func, func_args, num_threads=4):
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(func, args) for args in func_args]
        results = [future.result() for future in futures]
        migration_logger.info(f"Finished {len(results)} runs.")

def load_config(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, 'r') as file:
        if path.endswith(('.yaml', '.yml')):
            return yaml.safe_load(file)
        else:
            raise ValueError("Unsupported config format. Use .yaml/.yml")

def main():
    parser = argparse.ArgumentParser(description="Load config from a given file path.")
    parser.add_argument(
        "--config",
        nargs="?",
        const="config.yaml",
        default="config.yaml",
        help="Optional path to config file (default: config.yaml)"
    )
    args = parser.parse_args()
    config = load_config(args.config)

    # ---------
    # global configs
    # ---------
    aws_region = config["global"]["aws_region"]
    os.environ["AWS_DEFAULT_REGION"] = aws_region

    s3_uri = config["global"]["s3_uri"]
    base_logs_dir = config["global"]["logs_dir"]
    num_threads = config["global"]["num_threads"]

    # setup logger
    log_file_name = f'migration_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log'
    update_logger(migration_logger, base_logs_dir, log_file_name)

    # ---------
    # source database configs
    # ---------
    all_databases = config['source'].get("all_databases", False)
    db_table_map = config["source"].get("databases", None)

    migration_logger.info(f"Beginning migration to InfluxDB")
    migration_logger.info("-"*20)
    migration_logger.info(f"Loaded configs from '{args.config}'")
    echo_config(config)
    migration_logger.info("-"*20)

    if not all_databases and not db_table_map:
        migration_logger.error(f"Specify source databases to migrate in {args.config}")
    
    # ---------
    # UNLOAD configs
    # ---------
    migration_tag = config["stage"]["unload"].get(
        "migration_tag",
        f"unload-{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    )
    encryption = config["stage"]["unload"]["encryption"]
    kms_key = config["stage"]["unload"]["kms_key"]
    sns_topic_arn = config["stage"]["unload"]["sns_topic_arn"]
    unload_start_time = config["stage"]["unload"]["start_time"]
    unload_end_time = config["stage"]["unload"]["end_time"]
    partition = config["stage"]["unload"]["partition"]
    export_format = config["stage"]["unload"]["export_format"]
    compression = config["stage"]["unload"]["compression"]
    custom_partition_count = config["stage"]["unload"]["custom_partition_count"]
    recent_first = config["stage"]["unload"]["recent_first"]
    append_timestamps = config["stage"]["unload"]["append_timestamps"]
    max_file_size = config["stage"]["unload"]["max_file_size"]
    enable_dynamodb_logger = config["stage"]["unload"]["enable_dynamodb_logger"]
    field_delimiter = config["stage"]["unload"]["field_delimiter"]
    escaped_by = config["stage"]["unload"]["escaped_by"]
    order_by_asc = config["stage"]["unload"]["order_by_asc"]
    unload_logs_dir = os.path.join(base_logs_dir, config["stage"]["unload"]["logs_dir"])

    unload_common_args = [
        "--s3-uri", s3_uri,
        "--migration-tag", migration_tag,
        "--encryption", encryption,
        "--kms-key", kms_key, 
        "--sns-topic-arn", sns_topic_arn, 
        "--start-time", unload_start_time, 
        "--end-time", unload_end_time, 
        "--partition", partition,
        "--export-format", export_format, 
        "--compression", compression,
        "--custom-partition-count", str(custom_partition_count),
        "--recent-first", str(recent_first), 
        "--append-timestamps", str(append_timestamps), 
        "--max-file-size", max_file_size,
        "--enable-dynamodb-logger", str(enable_dynamodb_logger), 
        "--field-delimiter", field_delimiter, 
        "--escaped-by", escaped_by, 
        "--order-by-asc", str(order_by_asc),
        "--logs-dir", unload_logs_dir, 
    ]
    
    # ---------
    # TRANSFORM configs
    # ---------
    athena_database_name = config["stage"]["transform"]["athena_database_name"]
    dimensions_to_fields_map = config["stage"]["transform"].get("dimensions_to_fields", {})
    add_validation_field = config["stage"]["transform"]["add_validation_field"]
    transform_logs_dir = os.path.join(base_logs_dir, config["stage"]["transform"]["logs_dir"])

    transform_common_args = [
        "--s3-bucket-path", s3_uri,
        "--athena-database-name", athena_database_name,
        "--add-validation-field", str(add_validation_field),
        "--logs-dir", transform_logs_dir,
    ]

    # ---------
    # INGEST configs
    # ---------
    lines_per_batch = config["stage"]["ingest"]["lines_per_batch"]
    io_multiplier = config["stage"]["ingest"]["io_multiplier"]
    retries = config["stage"]["ingest"]["retries"]
    precision = config["stage"]["ingest"]["precision"]
    ingest_logs_dir = os.path.join(base_logs_dir, config["stage"]["ingest"]["logs_dir"])
    continue_on_error = config["stage"]["ingest"]["continue_on_error"]
    skip_bucket_check = config["stage"]["ingest"]["skip_bucket_check"]

    # TODO: rm; here for convenience
    # InfluxV2
    # os.environ["INFLUXDB_V2_URL"] = "https://l75mpzejl3-zcipc7m4j2l72o.timestream-influxdb.us-west-2.on.aws:8086"
    # os.environ["INFLUXDB_V2_ORG"] = "org"
    # os.environ["INFLUXDB_V2_TOKEN"] = "xx=="

    # InfluxV3
    # os.environ["INFLUXDB_V2_URL"] = "https://m1xovyvziu-izdmnhd7injid2.timestream-influxdb-alpha.us-west-2.on.aws:8181"
    # os.environ["INFLUXDB_V2_TOKEN"] = "xx"

    influx_args = [
        "-w", str(num_threads),
        "-l", str(lines_per_batch),
        "-m", str(io_multiplier),
        "-r", str(retries),
        "-p", precision,
        "--logs-dir", str(ingest_logs_dir),
    ]
    if continue_on_error:
        influx_args.append("--continue-on-error")
    if skip_bucket_check:
        influx_args.append("--skip-bucket-check")


    # ---------
    # VALIDATION configs
    # ---------
    validation_logs_dir = os.path.join(base_logs_dir, config["stage"]["validation"]["logs_dir"])

    validation_common_args = [
        "--logs-dir", validation_logs_dir,
    ]

    # ---------
    # Setup clients and util classes
    # ---------
    session = Session()
    timestream_utility = TimestreamUtility(
        aws_region, sns_topic_arn, enable_dynamodb_logger)
 
    # ========================
    # gather runs for unload, transform
    # ========================
    unload_runs = []
    transform_runs = []

    # Migrate all databases
    if all_databases:
        unload_flags = ["--export-all-databases"]
        unload_runs.append(unload_flags + unload_common_args)

        # fetch all databases
        all_databases = timestream_utility.get_all_databases()
        db_table_map = {}

        for db_name in all_databases:
            # save database names for later
            db_table_map[db_name] = []

            transform_flags = [
                "--database", db_name,
                 "--all-tables",
            ]
            dimensions_to_fields = []
            if db_name in dimensions_to_fields_map.keys():
                dimensions_to_fields_db = dimensions_to_fields_map[db_name]
                for table_name, dimensions in dimensions_to_fields_db.items():
                    dimensions_csv = ",".join(dimensions)
                    dimensions_to_fields.append(f"{table_name}={dimensions_csv}")

            for table_dimensions in dimensions_to_fields:
                transform_flags += [
                    "--dimensions-to-fields", table_dimensions,
                ]

            transform_runs.append(transform_flags + transform_common_args)

    # migrate specific databases/tables
    else:
        for db_name, tables in db_table_map.items():
            if tables:
                for table in tables:
                    unload_flags = [
                        "--database", db_name,
                        "--table", table, "--export-table"
                    ]
                    unload_runs.append(unload_flags + unload_common_args)

                transform_flags = [
                    "--database", db_name,
                    "--tables", ",".join(tables)
                ]
            # export full database
            else:
                unload_flags = [
                    "--database", db_name,
                    "--export-database"
                ]
                unload_runs.append(unload_flags + unload_common_args)

                transform_flags = [
                    "--database", db_name,
                    "--all-tables"
                ]

            dimensions_to_fields = []
            if db_name in dimensions_to_fields_map.keys():
                dimensions_to_fields_db = dimensions_to_fields_map[db_name]
                for table_name, dimensions in dimensions_to_fields_db.items():
                    dimensions_csv = ",".join(dimensions)
                    dimensions_to_fields.append(f"{table_name}={dimensions_csv}")

            for table_dimensions in dimensions_to_fields:
                transform_flags += [
                    "--dimensions-to-fields", table_dimensions,
                ]
            transform_runs.append(transform_flags + transform_common_args)

    migration_logger.info(f"UNLOAD batch size: {len(unload_runs)}")
    migration_logger.info(f"TRANSFORM batch size: {len(transform_runs)}")

    # create athena database if not exists
    create_athena_db_if_not_exists(session, athena_database_name)

    # ---------
    # Run unloads
    # ---------
    migration_logger.info(f"Executing UNLOAD(s)")
    execute_jobs(unload_main, unload_runs, num_threads)

    # ---------
    # Run transforms
    # ---------
    migration_logger.info(f"Executing TRANSFORM(s)")
    execute_jobs(transform.main, transform_runs, num_threads)


    # create directory to store LP
    lp_base_directory = "lp_output"
    os.makedirs(lp_base_directory, exist_ok=True)

    client = InfluxDBClient.from_env_properties()
    s3_utility = S3Utility()
    validation_logs_dir = os.path.join(base_logs_dir, config["stage"]["validation"]["logs_dir"])
    for db_name, tables in db_table_map.items():
        lp_directory = f"{lp_base_directory}/{db_name}"
        if not tables:
            tables = timestream_utility.get_all_tables(db_name)
            db_table_map[db_name] = tables

        # Sync from s3
        for table in tables:
            migration_logger.info(f"Downloading LP for table '{table}' from database '{db_name}'...")
            os.makedirs(lp_directory, exist_ok=True)
            s3_utility.sync_line_protocol_to_storage(
                s3_bucket_path=s3_uri,
                directory=lp_directory,
                timestream_database_name=db_name,
                timestream_table_name=table,
            )
        try:
            # ---------
            # Run ingestion & validation
            # ---------
            # Create bucket if not exists
            if not skip_bucket_check:
                migration_logger.info(f"Checking if bucket: '{db_name}' exists")
                buckets = client.buckets_api().find_buckets().buckets
                if not any(bucket.name == db_name for bucket in buckets):
                    client.buckets_api().create_bucket(bucket_name=db_name)
                    migration_logger.info(f"Bucket '{db_name}' created.")

            migration_logger.info(f"Begin ingestion to '{db_name}' from {lp_directory}")
            influxdb_ingestion.main(influx_args + [db_name, lp_directory])

            migration_logger.info(f"Ingestion complete.")

            # make room by clearing LP
            if os.path.exists(lp_directory):
                shutil.rmtree(lp_directory)
                migration_logger.info(f"Deleted {lp_directory}")
        except Exception as e:
            migration_logger.error(f"Error during ingestion: {e}")

        # TODO: add v3 support in validation
        if not skip_bucket_check:
            migration_logger.info(f"Executing validation(s) for {db_name}")
            for table in tables:
                validator_args = [
                    "--source-engine",
                    "timestream",
                    "--athena-output",
                    s3_uri,
                    "--timestream-database-name",
                    db_name,
                    "--timestream-table-name",
                    table,
                    "--influxdb-v2-url",
                    os.environ["INFLUXDB_V2_URL"],
                    "--influxdb-v2-token",
                    os.environ["INFLUXDB_V2_TOKEN"],
                    "--influxdb-v2-org",
                    os.environ["INFLUXDB_V2_ORG"],
                    "--influxdb-v2-bucket",
                    db_name,
                    "--influxdb-v2-measurement",
                    table,
                    "--start-time",
                    to_iso_z(unload_start_time),
                    "--end-time",
                    to_iso_z(unload_end_time),
                    "--skip-wal-check",
                    "--logs-dir", validation_logs_dir,
                ]
                if db_name in dimensions_to_fields_map.keys():
                    dims = timestream_utility.list_dimension_columns(db_name, table)
                    for table_name, dimensions in dimensions_to_fields_map[db_name].items():
                        if table_name == table:
                            schema_tags = [dim for dim in dims if dim not in dimensions]
                            tags = validator.get_quoted_tags(schema_tags)
                            validator_args += ["--schema-tags", tags]

                validator.main(validator_args)

    migration_logger.info(f"Validation complete.")
    # ---------
    # Perform clean up
    # ---------
    migration_logger.info("-"*20)
    migration_logger.info(f"Performing cleanup")
    if client:
        client.close()

    # Delete LP directory
    try:
        if os.path.exists(lp_base_directory):
            shutil.rmtree(lp_base_directory)
            migration_logger.info(f"Deleted {lp_base_directory}")
    except Exception as e:
        migration_logger.error(f"Error deleting {lp_base_directory}: {e}")

    # Delete Athena tables
    glue_client = session.client("glue")
    try:
        for db_name, tables in db_table_map.items():
            if tables:
                cleanup_athena(
                    glue=glue_client,
                    athena_database="default",
                    timestream_database=db_name,
                    timestream_tables=tables,
                )
            # no tables = all tables; clean all tables
            else:
                all_tables = timestream_utility.get_all_tables(db_name)
                cleanup_athena(
                    glue=glue_client,
                    athena_database="default",
                    timestream_database=db_name,
                    timestream_tables=all_tables,
                )
        migration_logger.info(f"Deleted athena tables for {db_name}")

    except Exception as e:
        migration_logger.error(f"Error deleting Athena tables: {e}")
    finally:
        glue_client.close()

    return

if __name__ == "__main__":
    main()
