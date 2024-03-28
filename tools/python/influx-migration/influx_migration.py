# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance with
# the License. A copy of the License is located at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
# and limitations under the License.

"""
py:module: influx_migration

:synopsis: Migrates data from InfluxDB OSS 2.x to Amazon Timestream for InfluxDB
:platform: macOS, Linux, Windows
"""
import atexit
import argparse
import boto3
import botocore
import glob
import json
from json import JSONDecodeError
import logging
import os
from pathlib import Path
from shutil import which
import signal
import ssl
import subprocess
from subprocess import CalledProcessError
import sys
from sys import platform
import textwrap
import time
import urllib
import urllib3

from influxdb_client import BucketRetentionRules, InfluxDBClient
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.rest import ApiException

# Maximum number of retries for attempting to mount an S3 bucket
MAX_RETRIES = 20
# The timeout value for all requests
MILLISECOND_TIMEOUT = 30_000
# The name of the temporary directory to be created and used to
# mount an S3 bucket
MOUNT_POINT_NAME = "influxdb-backups"

script_duration = 0

# The user is already warned for using --skip-verify, this will de-clutter output
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def backup(backup_path, root_token, src_host, bucket_name=None, full=False, skip_verify=False, src_org=None):
    """
    Backups data and metadata stored in InfluxDB. Data and metadata are copied to
    a set of files stored in a specified directory on a filesystem.

    :param backup_path: The path to send all backup files.
    :type backup_path: class: pathlib.Path
    :param str root_token: The admin token for the source InfluxDB server.
    :param str src_host: The address of the source InfluxDB server.
    :param bucket_name: Optional, the name of the singular bucket to backup in the source server.
    :type bucket_name: str or None
    :param full: Optional, whether to do a full backup, storing all buckets and all key-value.
        variables like users and tokens.
    :type full: bool or None
    :param skip_verify: Optional, whether to skip TLS certificate verification.
    :type skip_verify: bool or None
    :param src_org: The name of the organization used in the source instance.
    :type src-org: str or None
    :return: None
    :raise ValueError: If bucket_name and full are both missing.
    :raise RuntimeError: If the backup fails
    """
    if bucket_name is None and not full:
        raise ValueError("bucket_name and full not provided, one must be provided")

    logging.info("Backing up bucket data and metadata using the InfluxDB CLI")
    start_time = time.time()

    bucket_backup_command = ['influx', 'backup', backup_path, '-t', root_token,
        '--host', src_host]
    if skip_verify:
        bucket_backup_command.append('--skip-verify')
    if bucket_name is not None and not full:
        bucket_backup_command.extend(['--bucket', bucket_name])
    if src_org is not None:
        bucket_backup_command.extend(['--org', src_org])
    try:
        subprocess.run(bucket_backup_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True)
    except CalledProcessError:
        raise RuntimeError("Backup CLI command failed")
    duration = time.time() - start_time
    log_performance_metrics("backup", start_time, duration)

def backup_csv(backup_path, root_token, src_host, bucket_name=None, full=False, skip_verify=False, src_org=None):
    """
    Backups data and metadata stored in InfluxDB to a specified directory using csv for each
    bucket. Uses the InfluxDB v2 API to query buckets and store results as csv files.

    :param str backup_path: The path to send all backup files.
    :param str root_token: The admin token for the source InfluxDB server.
    :param str src_host: The address of the source InfluxDB server.
    :param bucket_name: Optional, the name of the bucket to backup.
    :type bucket_name: str or None
    :param full: Optional, whether to do a full backup, storing all user-defined buckets.
    :type full: bool or None
    :param skip_verify: Optional, whether to skip TLS certificate verification
    :type skip_verify: bool or None
    :param src_org: Optional, the name of the organization used in the source instance to be used for a single-bucket backup
    :type src_org: str or None
    :returns: None
    :raises InfluxDBError: If backup using the InfluxDB client fails
    :raises OSError: If writing bucket data to csv fails
    """
    logging.info("Backing up bucket data and metadata using the InfluxDB v2 API")
    start_time = time.time()

    try:
        client = InfluxDBClient(url=src_host, token=root_token,
            verify_ssl=not skip_verify, timeout=MILLISECOND_TIMEOUT)
        if src_org is not None:
            client.org = src_org
        # Backup all user-defined buckets
        if full:
            buckets = client.buckets_api().find_buckets().to_dict()['buckets']
            for bucket in buckets:
                if bucket['type'] == "user":
                    bucket_org = client.organizations_api().find_organization(bucket['org_id']).name
                    write_bucket_to_csv(backup_path=backup_path, token=root_token, bucket=bucket, org_name=bucket_org, host=src_host, skip_verify=skip_verify)
        # Backup a single bucket
        elif bucket_name is not None:
            bucket = client.buckets_api().find_bucket_by_name(bucket_name).to_dict()
            bucket_org = client.organizations_api().find_organization(bucket['org_id']).name
            write_bucket_to_csv(backup_path=backup_path, token=root_token, bucket=bucket, org_name=bucket_org, host=src_host, skip_verify=skip_verify)
    except (OSError, InfluxDBError):
        raise
    finally:
        client.close()
    duration = time.time() - start_time  # Calculate the duration
    log_performance_metrics("backup_csv", start_time, duration)

def bucket_create_rollback(host, token, bucket_name, org, skip_verify):
    """
    Rolls back bucket creation.

    :param str host: The host of the instance to do the rollback on.
    :param str token: The InfluxDB token to use for the rollback.
    :param bucket_name: The name of the bucket to delete.
    :param str org: The name of the org associated with the bucket.
    :param bool skip_verify: Whether to verify TLS certificates during deletion request.
    :returns: Whether rollback succeeded.
    :rtype: bool
    """
    try:
        client = InfluxDBClient(url=host,
            token=token, timeout=MILLISECOND_TIMEOUT, verify_ssl=not skip_verify, org=org)
        bucket = client.buckets_api().find_bucket_by_name(bucket_name)
        if bucket is not None:
            client.buckets_api().delete_bucket(bucket)
    except (ApiException, InfluxDBError) as error:
        logging.debug(repr(error))
        logging.debug(f"Rollback for bucket {bucket.name} could not be deleted")
        return False
    finally:
        client.close()
    return True

def bucket_exists(host, token, bucket_name, skip_verify=False, org=None):
    """
    Checks for the existence of a bucket.

    :param str host: The host for the InfluxDB instance.
    :param str token: The token to use for verification.
    :param str bucket_name: The name of the bucket to verify.
    :param bool skip_verify: Whether to skip TLS certificate verification.
    :param org: The name of the org to use for bucket verification
    :type org: str or None
    :returns: Whether the bucket exists in the instance.
    :rtype: bool
    """
    try:
        client = InfluxDBClient(url=host,
            token=token, timeout=MILLISECOND_TIMEOUT, verify_ssl=not skip_verify, org=org)
        if client.buckets_api().find_bucket_by_name(bucket_name) is None:
            return False
    except InfluxDBError as error:
        logging.error(str(error))
        return False
    finally:
        client.close()
    return True

def cleanup(mount_point=None, exec_s3_bucket_mount=None):
    """
    Coordinates unmounting S3 bucket and deleting temporary mount directory. Called at exit.

    :param exec_s3_bucket_mount: The subprocess.CompletedProcess object used to mount the S3
        bucket, must be terminated.
    :type exec_s3_bucket_mount: class: subprocess.CompletedProcess
    :param mount_point: The temporary local directory used as a mounting point for the S3
        bucket, must be removed.
    :type mount_point: class: pathlib.Path
    :returns: None
    """
    if mount_point is None and exec_s3_bucket_mount is None:
        logging.info("cleanup: all args None")
        return
    if platform == "win32" and exec_s3_bucket_mount is not None:
        logging.info("Terminating rclone mount")
        exec_s3_bucket_mount.send_signal(signal.CTRL_C_EVENT)
    elif exec_s3_bucket_mount is not None:
        logging.info("Terminating S3 mount process")
        exec_s3_bucket_mount.kill()
    if platform != "win32" and mount_point is not None and os.path.isdir(mount_point) and os.path.ismount(mount_point):
        unmount_s3_bucket_unix(mount_point)
    if mount_point is not None and os.path.exists(mount_point) and os.path.isdir(mount_point) \
            and not os.path.ismount(mount_point):
        try:
            logging.info("Removing temporary mount directory")
            os.rmdir(mount_point)
        except OSError as error:
            logging.error(str(error))
    return

def create_backup_directory(backup_directory, mount_point=None, bucket_name=None):
    """
    Creates the backup directory in local storage or within a mounted S3 bucket.

    :param backup_directory: The Path object to use for creating the backup directory.
    :type backup_directory: class: pathlib.Path
    :param mount_point: The Path object that is directory the S3 bucket is mounted to.
    :type mount_point: class: pathlib.Path or None
    :param bucket_name: The name of the S3 bucket, needed for creating the backup directory
        within the mounted S3 bucket.
    :type bucket_name: str or None
    :returns: The backup_directory.
    :rtype: class: pathlib.Path
    :raises RuntimeError: If the backup directory cannot be created.
    """
    if os.path.exists(backup_directory):
        logging.info(f"Using existing backup directory {backup_directory}")
        return backup_directory
    if mount_point is not None and not os.path.ismount(mount_point):
        raise RuntimeError("Mount point is not mounted, cannot "
            "create backup directory within mount point")
    if mount_point is not None:
        backup_directory = Path(mount_point / bucket_name / backup_directory) \
            if os.path.exists(Path(mount_point / bucket_name / backup_directory)) else \
            Path(mount_point / backup_directory)
    os.mkdir(backup_directory)

    # There may be a delay in creating the backup directory within
    # an S3 bucket
    retries = 0
    if mount_point is not None:
        while retries < MAX_RETRIES and not os.path.exists(backup_directory):
            time.sleep(0.1)
            retries += 1
    if retries >= MAX_RETRIES and not os.path.exists(backup_directory):
        raise RuntimeError("Backup directory could not be created within S3 bucket")
    return backup_directory

def health_check(host, token, skip_verify, debug=False):
    """
    Pings the InfluxDB instance to determine health.
    
    :param str host: The address of the host to ping.
    :param str src_token: The token for the database instance.
    :param bool skip_verify: Whether to skip TLS certificate verification.
    :returns: Whether the health check succeeded.
    :rtype: bool
    """
    # Ensure host can be connected to
    try:
        client = InfluxDBClient(url=host,
            token=token, timeout=MILLISECOND_TIMEOUT, verify_ssl=not skip_verify, debug=debug)
        if not client.ping():
            url_parse = urllib.parse.urlparse(host)
            if url_parse.scheme == "":
                logging.error("Host is missing scheme")
            if url_parse.port is None:
                logging.error("Host is missing port")
            if url_parse.scheme == "" or url_parse.port is None:
                logging.error("The expected format for host urls is <scheme>:<domain>:<port>. "
                    "For example, http://127.0.0.1:8086")
            raise InfluxDBError(message=f"InfluxDB API call to {host}/ping failed")
    except InfluxDBError as error:
        logging.error(str(error))
        return False
    finally:
        client.close()
    return True

def log_performance_metrics(func_name, start_time, duration):
    """
    Logs performance metrics if debug logging is enabled.

    :param float start_time: The time, in seconds since the epoch, that performance measurements began.
    :param float duration: The duration in seconds.
    :returns: None
    """
    global script_duration
    script_duration += duration
    if logging.root.isEnabledFor(logging.DEBUG):
        formatted_start_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))
        message = f"{func_name} started at {formatted_start_time} and took {duration:.2f} seconds to run."
        logging.debug(message)
        try:
            with open("performance.txt", "a") as file:
                file.write(message + "\n")
        except (IOError, OSError):
            logging.debug(f"Writing performance metrics for {func_name} failed")

def mount_s3_bucket(bucket_name, mount_point):
    """
    Mounts S3 bucket with the name bucket_name as a directory within mount_point,
    using mount-s3 on Linux and rclone on Windows and macOS.

    :param str bucket_name: Name of the S3 bucket. On Linux this is simply the name of the
        bucket. On Windows and macOS this is the configured rclone remote name and bucket as
        remote-name:bucket-name.
    :param mount_point: An existing directory to mount the S3 bucket into
    :type mount_point: class: pathlib.Path
    :returns: None
    :raises OSError: If mount point could not be created.
    :raises ValueError: If S3 bucket fails to mount.
    """
    logging.info(f"Mounting {bucket_name}")
    # On Linux and macOS a temporary directory needs to be created, on Windows this is
    # done automatically
    if platform != "win32":
        try:
            logging.info(f"Creating mount point {mount_point}")
            os.mkdir(mount_point)
        except OSError as error:
            if os.path.exists(mount_point):
                logging.info(f"Using existing mount point {mount_point}")
            else:
                raise error
    proc = None
    if platform == "win32":
        proc = subprocess.Popen(['rclone', 'mount', bucket_name, mount_point],
            creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP)
    elif platform == "darwin":
        proc = subprocess.Popen(['rclone', 'mount', '--vfs-cache-mode', 'full', bucket_name, mount_point])
    elif platform == "linux":
        unix_mount_command = ['mount-s3', bucket_name, mount_point]
        proc = subprocess.Popen(unix_mount_command)
    if proc is not None and proc.returncode is not None and proc.returncode != 0:
        raise ValueError("S3 bucket mount command failed")
    atexit.register(cleanup, mount_point=mount_point, exec_s3_bucket_mount=proc)

    # Confirm S3 bucket has mounted
    retries = 0
    while retries < MAX_RETRIES and not os.path.ismount(mount_point):
        time.sleep(1)
        retries += 1
    if retries >= MAX_RETRIES and not os.path.ismount(mount_point):
        raise ValueError("S3 bucket failed to mount")

def parse_args(args):
    """
    Takes arguments and parses according to argparse rules, setting argparse variables such as
    source host, whether to use csv, etc.

    :param args: The arguments from main to parse.
    :type args: list[str]
    :returns: The parsed arguments.
    :rtype: class: argparse.Namespace
    """
    parser = argparse.ArgumentParser(
        prog="influx_migration.py",
        description="Migrates InfluxDB data and metadata.")

    parser.add_argument("--src-bucket", help="Optional. The name of the InfluxDB bucket in the "
        "source server. If not provided, then --full must be provided.",
        required=False)
    parser.add_argument("--dest-bucket", help="Optional. The name of the InfluxDB bucket in the "
        "destination server, must not be an already existing bucket. Defaults to value of "
        "--src-bucket or None if --src-bucket not provided.",
        required=False)
    parser.add_argument("--src-host", help="Optional. The host for the source server. "
        "Must have a scheme, domain or IP address, and port, e.g., http://127.0.0.1:8086 or "
        "https://<domain>:<port>. Defaults to http://localhost:8086 if no value is specified.",
        default="http://localhost:8086", required=False)
    parser.add_argument("--dest-host", help="The host for the destination server. "
        "Must have a scheme, domain or IP address, and port, e.g., http://127.0.0.1:8086 or "
        "https://<domain:<port>.",
        required=True)
    parser.add_argument("--full", help="Optional. Whether to perform a full restore, replacing all "
        "data on destination server with all data from source server from all organizations, "
        "including all key-value data such as tokens, dashboards, users, etc. "
        "Overrides --src-bucket and --dest-bucket. If used with --csv, only migrates "
        "data and metadata of buckets. Defaults to false.",
        default=False, required=False, action='store_true')
    parser.add_argument("--confirm-full", help="Optional. Using --full without --csv will "
        "replace all tokens, users, buckets, dashboards, and any other key-value data in the destination "
        "database with the tokens, users, buckets, dashboards, and any other key-value data in the source "
        "database. --full with --csv only migrates all bucket and bucket metadata, including bucket "
        "organizations. This option (--confirm-full) will confirm a full migration and proceed without "
        "user input. If this option is not provided, and --full has been provided and --csv not "
        "provided, then the script will pause for execution and wait for user confirmation. "
        "This is a critical action, proceed with caution. Defaults to false.",
        default=False, required=False, action='store_true')
    parser.add_argument("--src-org", help="Optional. The name of the organization in the source instance "
        "to use during migration. If this is omitted, the default organization associated with the source token "
        "will be used.",
        required=False)
    parser.add_argument("--dest-org", help="Optional. The name of the organization to restore "
        "buckets to in the destination server. If this is omitted, then all migrated buckets "
        "from the source server will retain their original organization and migrated buckets may "
        "not be visible in the destination server without creating and switching organizations. "
        "This value will be used in all forms of restoration whether a single bucket, a full "
        "migration, or any migration using csv files for backup and restoration. --src-org is "
        "required for the --dest-org option to work when not using --csv.",
        required=False)
    parser.add_argument("--csv", help="Optional. Whether to use csv files for backing up and restoring. "
        "If --full is passed as well then all user-defined buckets in all organizations will be migrated, "
        "not system buckets, users, tokens, or dashboards. If a singular organization is desired for all "
        "buckets in the destination server instead of their already-existing source organizations, use "
        "--dest-org.",
        default=False, required=False, action='store_true')
    parser.add_argument("--retry-restore-dir", help="Optional. Directory to use for restoration when a "
        "previous restore failed, will skip backup and directory creation, will fail if the directory "
        "doesn't exist, can be a directory within an S3 bucket. If a restoration fails, the backup directory "
        "path that can be used for restoration will be indicated relative to the current directory. " 
        "S3 buckets will be in the form influxdb-backups/<s3 bucket>/influxdb-backup-<timestamp>.",
        required=False)
    parser.add_argument("--dir-name", help="Optional. The name of the backup directory to create. Defaults to "
        "influxdb-backup-<timestamp>. Must not already exist.",
        required=False)
    parser.add_argument("--log-level", help="Optional. The log level to be used during execution. "
        "Options are debug, error, and info. Defaults to info.",
        default="info", required=False)
    parser.add_argument("--skip-verify", help="Optional. Skip TLS certificate verification.",
        default=False, required=False, action='store_true')
    parser.add_argument("--s3-bucket", help="Optional. The name of the S3 bucket to use to store "
        "backup files. On Linux this is simply the name of the S3 bucket, such as "
        "my-bucket, given AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables have "
        "been set or ${HOME}/.aws/credentials exists. On Windows and macOS, this is the rclone configured "
        "remote and bucket name, such as my-remote:my-bucket. All backup files will be left in "
        "the S3 bucket after migration in a created influxdb-backups-<timestamp> directory. "
        "A temporary mount directory named influx-backups will be created in the "
        "directory from where this script is ran. If not provided, then all backup files will "
        "be stored locally in a created influxdb-backups-<timestamp> directory from where "
        "this script is ran.",
        required=False)
    parser.add_argument("--allow-unowned-s3-bucket", help="Optional. Whether to automatically allow the "
        "use of an unowned S3 bucket, skipping prompt verification. Using an S3 bucket you own is recommended. "
        "If you choose to continue the migration with an S3 bucket you do not own, proceed with caution.",
        default=False, required=False, action='store_true')

    return parser.parse_args(args)

def parse_bucket_name(s3_bucket):
    """
    Parses the args.s3_bucket argument, returning the bucket name. This is required as
    on macOS and Windows the s3_bucket argument will be formatted as <remote>:<s3-bucket-name>,
    while on Linux it will be <s3-bucket-name>.

    :param str s3_bucket: s3_bucket argument to be parsed.
    :returns: The bucket name.
    :rtype: str
    """
    if s3_bucket is None or platform == "linux":
        return s3_bucket
    elif platform == "win32" or platform == "darwin":
        verify_s3_bucket_syntax(s3_bucket)
        return s3_bucket[s3_bucket.find(":") + 1:]

def restore(src_path, dest_token, dest_host, src_bucket=None,
        new_bucket=None, full=False, dest_org=None, skip_verify=False, src_org=None):
    """
    Restores backup data and metadata from InfluxDB OSS using the influx CLI as a subprocess.

    :param str src_path: The directory where InfluxDB backup files exists to be used for restoration.
    :param str dest_token: The admin token for the destination InfluxDB server.
    :param str dest_host: The host address of the destination InfluxDB server.
    :param src_bucket: Optional, the name of the bucket in the source InfluxDB server to
        restore from.
    :type src_bucket: str or None
    :param new_bucket: Optional, the name of the bucket to be created and populated with data
        in the source InfluxDB server.
    :type new_bucket: str or None
    :param bool full: Optional, whether to do a full restoration, replacing all data on the destination server
        with all data on the source server.
    :param dest_org: Optional, the organization name to use for restoring, instead of the bucket's original
        organization name.
    :type dest_org: str or None
    :param  bool skip_verify: Optional, whether to skip TLS certificate verification
    :returns: None
    :raises RuntimeError: If the Influx CLI command to restore fails.
    """
    logging.info("Restoring bucket data and metadata using the InfluxDB CLI")
    start_time = time.time()
    command = ['influx', 'restore', src_path, '-t', dest_token,
        '--host', dest_host]
    if skip_verify:
        command.append('--skip-verify')
    if full:
        command.append('--full')
    elif src_bucket is not None and new_bucket is not None:
        command.extend(['--bucket', src_bucket, '--new-bucket',
            new_bucket])
    else:
        logging.error("restore: src_bucket or new_bucket were not provided "
            "when doing a single bucket restoration")
        return False
    # influx full restore can't be restricted to one organization or bucket
    if dest_org is not None and src_org is not None and full is False and src_bucket is not None:
        command.extend(['--new-org', dest_org, '--org', src_org])
    try:
        subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, check=True)
    except CalledProcessError:
        raise RuntimeError("Restore CLI command failed")
    try:
        shards = glob.glob(str(src_path) + '/[0-9]*T[0-9]*Z.[0-9]*.tar.gz')
        if len(shards) == 1:
            logging.info(f"{len(shards)} shard migrated")
        else:
            logging.info(f"{len(shards)} shards migrated")
    except OSError as error:
        logging.debug(str(error))

    duration = time.time() - start_time
    log_performance_metrics("restore", start_time, duration)

def restore_csv(src_path, dest_token, dest_host, src_bucket=None, new_bucket=None,
        full=False, dest_org=None, skip_verify=False):
    """
    Restores backup data and metadata from InfluxDB OSS using csv files for buckets.

    :param src_path: The directory where InfluxDB backup files exists to be used for restoration
    :param dest_token: The admin token for the destination InfluxDB server
    :param dest_host: The host address of the destination InfluxDB server
    :param src_bucket: The name of the bucket in the source InfluxDB server to
        restore from
    :param new_bucket: The name of the bucket to be created and populated with data
        in the source InfluxDB server
    :param full: Whether to do a full restoration, replacing all data on the destination server
        with all data on the source server
    :param dest_org: The name of the organization to use for restoring buckets in the destination server
    :param skip_verify: Whether to skip TLS certificate verification
    :returns: boolean value of whether the restoration succeeded, or partially succeed for a full backup
    """

    logging.info("Restoring bucket data and metadata from csv")
    start_time = time.time()

    # Partial migrations are acceptable if it's made clear only a partial migration occurred.
    # If no buckets were migrated, report the failure
    migrated_buckets = 0
    if full:
        for file in glob.glob(str(src_path) + "/bucket_*"):
            filepath_str = str(Path(file))
            bucket_name = filepath_str[filepath_str.find("/bucket_") + len("/bucket_") : filepath_str.find(".csv")]
            try:
                write_bucket_from_csv(path=src_path, bucket_name=bucket_name, host=dest_host,
                    token=dest_token, org=dest_org, skip_verify=skip_verify)
            except (ApiException, CalledProcessError, OSError, InfluxDBError) as error:
                if type(error) == ApiException:
                    error_body = json.loads(error.body)
                    logging.error(f'Status {error.response.status}: {error.reason}: {error_body["message"]}')
                    if error.response.status == 401:
                        logging.error("The required permissions for the destination token are:\n"
                            "\tread:/orgs\n\tread:orgs/<destination org>/buckets\n\twrite:orgs/<destination org>/buckets.\n"
                            "Ensure your token has the required permissions.")
                    if error.response.status == 422:
                        logging.error("Ensure the buckets to be migrated do not share their names with "
                            "buckets that already exist in the destination instance. Individual buckets "
                            "can be migrated with a new name using the --src-bucket and --dest-bucket options.")
                elif type(error) != CalledProcessError:
                    logging.error(str(error))
                logging.error(f"Bucket migration failed for bucket {bucket_name}, skipping to next bucket")
                continue
            migrated_buckets += 1
        if migrated_buckets <= 0:
            raise RuntimeError("No buckets were migrated during full migration")

    # Single bucket restoration, restores all data and metadata
    elif src_bucket is not None and new_bucket is not None:
        try:
            write_bucket_from_csv(path=src_path, host=dest_host, token=dest_token,
                                  org=dest_org, bucket_name=src_bucket, new_bucket_name=new_bucket, skip_verify=skip_verify)
        except (ApiException, CalledProcessError, OSError, InfluxDBError):
            logging.error(f"Restoration failed for bucket {src_bucket}")
            raise
    else:
        raise ValueError("restore_csv: new_bucket is None and full is False")

    duration = time.time() - start_time
    log_performance_metrics("restore_csv", start_time, duration)

def set_logging(log_level):
    """
    Sets log level. 

    :param str log_level: The log level to set.
    :returns: None
    """
    grey = "\x1b[38;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    
    logging.addLevelName(logging.WARNING, yellow + logging.getLevelName(logging.WARNING) + reset)
    logging.addLevelName(logging.ERROR, bold_red + logging.getLevelName(logging.ERROR) + reset)
    log_format = '%(levelname)s: %(filename)s: %(message)s'

    log_level = log_level.lower()
    if log_level == "debug":
        logging.basicConfig(format=log_format, level=logging.DEBUG)
    elif log_level == "error":
        logging.basicConfig(format=log_format, level=logging.ERROR)
    else:
        logging.basicConfig(format=log_format, level=logging.INFO)

def subprocess_check(use_s3_bucket):
    """
    Ensures the required tools exist as part of the user's PATH.

    :param bool use_s3_bucket: Whether to check for tools required to mount and
        unmount an s3 bucket.
    :returns: None
    :raises EnvironmentError: If any required tools are missing
    """
    if use_s3_bucket:
        if (platform == "win32" or platform == "darwin") and not which("rclone"):
            raise EnvironmentError("rclone is missing from PATH, unable to mount S3 bucket")
        if platform == "linux" and not which("mount-s3"):
            raise EnvironmentError("mount-s3 is missing from PATH, unable to mount S3 bucket")
        if (platform == "linux" or platform == "darwin") and not which("umount"):
            raise EnvironmentError("umount is missing from PATH, unable to mount S3 bucket")
    if not which("influx"):
        raise EnvironmentError("influx is missing from PATH")

def unmount_s3_bucket_unix(mount_point):
    """
    Unmounts S3 bucket located at mount_point on Linux and macOS.

    :param mount_point: The directory used to mount an S3 bucket using either mount-s3 or rclone.
    :type mount_point: class: pathlib.Path
    :returns: None
    """
    logging.info(f"Unmounting {mount_point}")

    retries = 0
    proc = subprocess.run(['umount', mount_point], check=False)
    # umount can fail because the mount point can be busy temporarily
    while retries < MAX_RETRIES and proc.returncode != 0:
        time.sleep(0.005)
        proc = subprocess.run(['umount', mount_point], check=False)
        retries += 1
    # If the mount point fails to unmount leave it to the user to unmount themselves
    if proc.returncode != 0:
        logging.error(f"Mount point {mount_point} could not be unmounted.")

def verify_args(args):
    """
    Verifies the arguments for the program are correct.

    :param args: The program arguments.
    :type args: class: argparse.Namespace
    :returns: None
    :raises ValueError: If any of the arguments are invalid.
    """
    if args.full and not args.csv and not args.confirm_full:
        warning_message = ("WARNING: if you proceed with full migration, then all tokens, users, buckets, "
            "dashboards, and other key-value data in the destination database will be replaced, "
            "permanently, with the tokens, users, buckets, dashboards, and other key-value data "
            "in the source database. If you wish to fully migrate only all buckets from the source "
            "database to the destination database, using --full with --csv will do so.")
        print(textwrap.fill(warning_message, 60), end='\n\n')
        confirmation = input("Type YES to proceed with full migration, NO to abort: ")
        confirmation = confirmation.strip()
        if confirmation != "YES":
            print("Aborting . . .")
            sys.exit(1)

    if args.dest_org is not None and args.src_org is None and args.csv is False:
        raise ValueError("Remote migration with --dest-org requires --src-org to work")

    if args.skip_verify:
        logging.warning("TLS certificate verification will be skipped, proceed with caution")

    if args.src_bucket is None and args.full is False:
        raise ValueError("Both --src-bucket and --full have been left empty, at least one or the "
            "other is required.")

    # Use the same name for buckets if destination bucket hasn't been provided
    if args.dest_bucket is None and args.src_bucket is not None:
        args.dest_bucket = args.src_bucket

    if args.s3_bucket is not None:
        verify_s3_bucket_syntax(args.s3_bucket)

def verify_s3_bucket_ownership(s3_bucket_name, allow_unowned_s3_bucket):
    """
    Verifies the user owns a given named S3 bucket.

    :param str s3_bucket_name: The name of the S3 bucket to verify ownership of.
    :param bool allow_unowned_s3_bucket: Whether to automatically allow the use of
        an unowned S3 bucket. When this option is False, users will be prompted for input.
    :returns: None
    :raises botocore.exceptions.ClientError: If the user does not own the named bucket or the bucket
        doesn't exist.
    """
    sts_client = boto3.client('sts')
    s3_client = boto3.client('s3')
    try:
        expected_owner = sts_client.get_caller_identity().get('Account')
        s3_client.head_bucket(Bucket=s3_bucket_name, ExpectedBucketOwner=expected_owner)
    except botocore.exceptions.ClientError as error:
        if "Error" not in error.response or "Code" not in error.response["Error"] or \
            "Message" not in error.response["Error"]:
            raise
        elif error.response["Error"]["Code"] == "403" and not allow_unowned_s3_bucket:
            warning_message = ("WARNING: the S3 bucket you are attempting to use is not owned by "
                "the current boto3 user. An attacker may be attempting to intercept your migration data. "
                "If this is unexpected or you do not understand, abort, otherwise, proceed with caution.")
            print(textwrap.fill(warning_message, 60), end='\n\n')
            confirmation = input("Type YES to proceed with migration, NO to abort: ")
            confirmation = confirmation.strip()
            if confirmation != "YES":
                print("Aborting . . .")
                sys.exit(1)
        elif error.response["Error"]["Code"] == "403" and allow_unowned_s3_bucket:
            logging.warning("Proceeding automatically with an unowned S3 bucket")
            return
        else:
            raise
    finally:
        sts_client.close()
        s3_client.close()

def verify_environment_tokens(skip_backup):
    """
    Verifies the existence of required token environment variables and returns them.

    :param bool skip_backup: Whether the backup process will be skipped, if so, then the
        INFLUX_SRC_TOKEN environment variable won't be checked.
    :returns: str and str or None and str if skip_backup is True
    :raises EnvironmentError: If any required environment variables are missing.
    """
    src_token = None
    dest_token = None
    if not skip_backup:
        if 'INFLUX_SRC_TOKEN' not in os.environ:
            raise EnvironmentError("INFLUX_SRC_TOKEN environment variable has not been set")
        else:
            src_token = os.environ['INFLUX_SRC_TOKEN']
    if 'INFLUX_DEST_TOKEN' not in os.environ:
        raise EnvironmentError("INFLUX_DEST_TOKEN environment variable has not been set")
    else:
        dest_token = os.environ['INFLUX_DEST_TOKEN']
    return src_token, dest_token

def verify_instances(args, src_token, dest_token):
    """
    Handles all host-related verification such as host health checks, bucket
    existence, org existence, and token verification.

    :param args: The script arguments.
    :type args: class: argparse.Namespace
    :param src_token: The token for the source instance. If None, then backup is being
        skipped and only the destination instance will be verified.
    :type src_token: str or None
    :param str dest_token: The token for the destination instance.
    :returns: None
    :raises InfluxDBError: If any check fails.
    """
    debug = args.log_level.lower() == "debug"
    # Source checks
    if src_token is not None:
        if not health_check(args.src_host, src_token, args.skip_verify, debug):
            raise InfluxDBError(message="Health check for source host failed")
        if not verify_token(args.src_host, src_token, args.skip_verify):
            raise InfluxDBError(message="Could not verify source token")
        if args.src_org is not None and not verify_org(args.src_host, src_token, args.src_org, args.skip_verify):
            raise InfluxDBError(message="The source org could not be verified")
        if args.src_bucket is not None and args.full is False and \
            not bucket_exists(args.src_host, src_token, args.src_bucket, args.skip_verify, args.src_org):
            raise InfluxDBError(message="The source bucket could not be found")
        if not args.skip_verify and not verify_tls(args.src_host):
            raise InfluxDBError(message="TLS certificate could not be verified for source host")

    # Destination checks
    if not health_check(args.dest_host, dest_token, args.skip_verify, debug):
        raise InfluxDBError(message="Health check for destination host failed")
    if not args.skip_verify and not verify_tls(args.dest_host):
        raise InfluxDBError(message="TLS certificate could not be verified for destination host")
    if not verify_token(args.dest_host, dest_token, args.skip_verify):
        raise InfluxDBError(message="Could not verify destination token")
    if args.dest_org is not None and not verify_org(args.dest_host, dest_token, args.dest_org, args.skip_verify):
        raise InfluxDBError(message="The destination org could not be verified")
    if args.dest_bucket is not None and args.full is False and \
        bucket_exists(args.dest_host, dest_token, args.dest_bucket, args.skip_verify, args.dest_org):
        message = (f"The destination bucket {args.dest_bucket} already exists in the "
            "destination instance")
        if args.dest_org is not None:
            message += f" in the {args.dest_org} organization. "
        else:
            message += ". "
        raise InfluxDBError(message=message +
            "Destination buckets must be unique")

def verify_org(host, token, org, skip_verify):
    """
    Verifies the existence of an org in an instance.

    :param str host: The host to use to verify an org's existence.
    :param str token: The token to use to verify an org's existence.
    :param str org: The org to verify the existence of.
    :param bool skip_verify: Whether to verify TLS certificates when making requests.
    :returns: Whether the org exists in the instance.
    :rtype: bool
    """
    try:
        client = InfluxDBClient(url=host,
            token=token, org=org, timeout=MILLISECOND_TIMEOUT, verify_ssl=not skip_verify)
        client.organizations_api().find_organizations(org=org)
    except Exception as error:
        logging.error(str(error))
        return False
    finally:
        client.close()
    return True

def verify_s3_bucket_syntax(s3_bucket_str):
    """
    Verifies the syntax of an S3 bucket name argument. On Windows and macOS the argument,
    passed in as a CLI argument, must be in the form remote-name:s3-bucket-name, since this
    is the format rclone expects. On Linux, only the name of the S3 bucket is required.

    :param str s3_bucket_str: The --s3-bucket argument string.
    :returns: None
    :raises ValueError: If the syntax of s3_bucket_str is invalid for the current platform.
    """
    if (platform == "win32" or platform == "darwin") and \
            (s3_bucket_str.find(":") == -1 or
             s3_bucket_str.find(":") == 0 or
             s3_bucket_str.find(":") == len(s3_bucket_str) - 1):
        raise ValueError(f"S3 bucket was {s3_bucket_str} but expected value on {platform} is "
          "an rclone configured remote and bucket name, i.e., remote-name:s3-bucket-name")

def verify_tls(host):
    """
    Verifies the TLS certificates for an instance.

    :param str host: The address of the instance.
    :returns: Whether the TLS certificate could be verified.
    :rtype: bool
    """
    try:
        url_parse = urllib.parse.urlparse(host)
        if url_parse.scheme == "https":
            ssl.get_server_certificate(addr=(url_parse.hostname, url_parse.port))
    except Exception as error:
        logging.error(str(error))
        return False
    return True

def verify_token(host, token, skip_verify):
    """
    Verifies that a token has authentication to access the InfluxDB instance at the given host.

    :param str host: The host of the InfluxDB instance.
    :param str token: The token to verify.
    :param bool skip_verify: Whether to verify TLS certificates.
    :returns: Whether the token could be verified.
    :rtype: bool
    """
    try:
        client = InfluxDBClient(url=host,
            token=token, timeout=MILLISECOND_TIMEOUT, verify_ssl= not skip_verify)
        client.organizations_api().find_organizations()
    except Exception as error:
        logging.error(str(error))
        return False
    finally:
        client.close()
    return True

def write_bucket_from_csv(path, host, token, bucket_name, new_bucket_name=None, org=None, skip_verify=False):
    """
    Creates a bucket in the destination instance and writes bucket data from a csv file to that
    created bucket.

    :param path: The path to the backed up data.
    :type path: class: pathlib.Path
    :param str host: The host of the destination instance.
    :param str token: The InfluxDB token for the destination instance.
    :param str bucket_name: The name of the bucket that was backed up.
    :param new_bucket_name: Optional, the name of the bucket to create in the destination instance and restore
        data to.
    :type new_bucket_name: str or None
    :param org: The name of the organization in the destination instance to restore to.
    :type org: str or None
    :param bool skip_verify: Whether to verify TLS certificates during requests.
    :returns: None
    :raises OSError: If reading from the csv file fails.
    :raises subprocess.CalledProcessError: If writing to the database fails.
    :raises RuntimeError: If no records were written to the database but no other exception was raised.
    :raises JSONDecodeError: If the metadata for the bucket could not be loaded.
    """
    try:
        with open(str(path) + f"/metadata_{bucket_name}.json") as metadatafile:
            metadata = json.load(metadatafile)
        retention_seconds = metadata["retention_rules"][0]["every_seconds"]
        shard_group_retention_seconds = metadata["retention_rules"][0]["shard_group_duration_seconds"]
        org_name = metadata['org_name'] if org is None else org
        dest_bucket_name = bucket_name

        if new_bucket_name is not None:
            dest_bucket_name = new_bucket_name
        logging.info(f"Restoring to bucket {dest_bucket_name}")

        retention_rules = BucketRetentionRules(type='expire', every_seconds=retention_seconds)

        # There is a bug with creating a bucket where if retention is 0 (infinite) and
        # shard group duration is defined at all then retention will instead be set to 720
        # hours, risking excluding data points from insertion due to being out of range of
        # retention policy
        # Thread: https://community.influxdata.com/t/bucket-created-with-retention-0-shows-720h-retention/30184
        if retention_seconds > 0:
            retention_rules.shard_group_duration_seconds = shard_group_retention_seconds

        written_records = 0
        client = InfluxDBClient(url=host, token=token,
            verify_ssl=not skip_verify, timeout=MILLISECOND_TIMEOUT, org=org_name)
        client.buckets_api().create_bucket(bucket_name=dest_bucket_name, org=org_name, retention_rules=retention_rules)
        write_command = ['influx', 'write', '--host', host, '-t', token, '--bucket', dest_bucket_name,
            '--org', org_name, '--format', 'csv', '--file', f'{path}/bucket_{bucket_name}.csv']
        if skip_verify:
            write_command.append("--skip-verify")
        proc = subprocess.run(write_command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        if len(proc.stderr) > 0:
            logging.error(proc.stderr.decode())
            error_count = proc.stderr.decode().count("\n")
            logging.error(f'{error_count} errors were reported. Batches that include '
            'any reported failed reccords will not be ingested. The total number of written records cannot be '
            f'determined, inspect the destination bucket {dest_bucket_name} to determine the number of written records '
            'and consider redoing migration')
        else:
            try:
                with open(f'{path}/bucket_{bucket_name}.csv', 'r') as csv_file:
                    for line in csv_file:
                        # Annotations begin with "#", headers begin with ",result,", and tables are separated by an empty line
                        if line.find("#") != 0 and line.find(",result,") != 0 and line != "\n":
                            written_records += 1
                logging.info(f"{written_records} records migrated")
                logging.info(f"{len(proc.stderr)} errors were reported")
            # Catch file read error here as we don't want to rollback the bucket if reading the csv file to report
            # written records is the only error that occurs
            except (IOError, OSError) as error:
                logging.error(str(error))
                logging.error("The number of migrated records could not be determined")
    except ApiException:
        # An ApiException will happen when the call to create the bucket fails. This could
        # happen because the bucket already exists, the user doesn't have the required
        # permissions, etc., therefore the bucket shouldn't be attempted to be deleted
        raise
    except (CalledProcessError, OSError, InfluxDBError, RuntimeError):
        bucket_create_rollback(host, token, dest_bucket_name, org_name, skip_verify)
        raise
    finally:
        client.close()

def write_bucket_to_csv(backup_path, bucket, org_name, token, host, skip_verify=False):
    """
    Writes bucket metadata and data to a csv file. Data will be stored in
    bucket_<bucket name>.csv and metadata will be stored in metadata_<bucket name>.json.

    :param backup_path: The path used for backing up, where files will be written to.
    :type backup_path: class: pathlib.Path
    :param dict bucket: The bucket to write to file.
    :param str org: The organization to associate with the bucket in the metadata file.
    :param str token: The token to use to query the instance.
    :param str host: The host for the instance to retrieve the bucket data from.
    :param bool skip_verify: Whether to verify TLS certificates during request.
    :returns: None
    :raises subprocess.CalledProcessError: If querying the bucket fails.
    :raises OSError: If writing the bucket to file fails.
    """
    try:
        # Write metadata to json
        bucket['org_name'] = org_name
        with open(f'{backup_path}/metadata_{bucket["name"]}.json', 'w') as file:
            json.dump(bucket, file, default=str)

        with open(f'{backup_path}/bucket_{bucket["name"]}.csv', 'w') as csv_file:
            query_command = ['influx', 'query', 
                f'from(bucket: "{bucket["name"]}") |> range(start: 1678-01-01T00:00:00Z, stop: 2800-01-01T00:00:00Z)',
                '--host', host, "-t", token, "--org", org_name, "--raw"]
            if skip_verify:
                query_command.append("--skip-verify")
            subprocess.run(query_command, stdout=csv_file, shell=False, check=True)
    except (CalledProcessError, KeyError, OSError, InfluxDBError) as error:
        raise error

def main(args):
    script_start_time = time.time()
    try:
        args = parse_args(args)
        set_logging(args.log_level)
        verify_args(args)
        subprocess_check(args.s3_bucket is not None)
        src_token, dest_token = verify_environment_tokens(args.retry_restore_dir is not None)
        verify_instances(args, src_token, dest_token)
        mount_point = None

        if args.s3_bucket is not None:
            verify_s3_bucket_ownership(parse_bucket_name(args.s3_bucket), args.allow_unowned_s3_bucket)
            mount_point = Path(MOUNT_POINT_NAME)
            atexit.register(cleanup, mount_point=mount_point)
            mount_s3_bucket(args.s3_bucket, Path(MOUNT_POINT_NAME))

        backup_directory = None
        if args.retry_restore_dir is not None:
            backup_directory = Path(args.retry_restore_dir)
        elif args.dir_name is not None:
            backup_directory = Path(args.dir_name)
        else:
            backup_directory = Path("influxdb-backup-" + str(int(time.time() * 1000)))

        if args.retry_restore_dir is None and src_token is not None:
            # If an S3 bucket argument is passed then this will be within the S3 bucket, otherwise
            # this is a newly-created directory in the current directory
            backup_directory = create_backup_directory(backup_directory, mount_point, parse_bucket_name(args.s3_bucket))
            try:
                if args.csv:
                    backup_csv(
                        backup_path=backup_directory,
                        root_token=src_token,
                        src_host=args.src_host,
                        bucket_name=args.src_bucket,
                        full=args.full,
                        skip_verify=args.skip_verify,
                        src_org=args.src_org)
                else:
                    backup(
                        backup_path=backup_directory,
                        root_token=src_token,
                        src_host=args.src_host,
                        bucket_name=args.src_bucket,
                        full=args.full,
                        skip_verify=args.skip_verify,
                        src_org=args.src_org)
            except (ApiException, CalledProcessError, RuntimeError, InfluxDBError, ValueError, OSError):
                logging.error("Backup has failed.")
                raise

        try:
            if args.csv:
                restore_csv(
                    src_path=backup_directory,
                    src_bucket=args.src_bucket,
                    new_bucket=args.dest_bucket,
                    dest_token=dest_token,
                    dest_host=args.dest_host,
                    full=args.full,
                    dest_org=args.dest_org,
                    skip_verify=args.skip_verify)
            else:
                restore(
                    src_path=backup_directory,
                    src_bucket=args.src_bucket,
                    new_bucket=args.dest_bucket,
                    dest_token=dest_token,
                    dest_host=args.dest_host,
                    full=args.full,
                    dest_org=args.dest_org,
                    skip_verify=args.skip_verify,
                    src_org=args.src_org)
        except (ApiException, CalledProcessError, RuntimeError, InfluxDBError, OSError, ValueError):
            logging.error(f"Restore has failed.\nThe backup directory is\n\n{backup_directory}\n\n"
                "If you want to skip backing up data and retry restoration use the "
                "--retry-restore-dir option and provide the previously-mentioned backup directory.")
            raise

        logging.info("Migration complete")
        log_performance_metrics("influx_migration.py", script_start_time, script_duration)
    except (ApiException, CalledProcessError, botocore.exceptions.ClientError, OSError, ValueError,
            EnvironmentError, RuntimeError, InfluxDBError, JSONDecodeError) as error:
        if type(error) == ApiException:
            error_body = json.loads(error.body)
            logging.error(f'Status {error.response.status}: {error.reason}: {error_body["message"]}')
            if error.response.status == 401:
                logging.error("The required permissions for the source token are:\n"
                    "\tread:/orgs\n\tread:orgs/<source org>/buckets.\n"
                    "The required permissions for the destination token are:\n"
                    "\tread:/orgs\b\tread:orgs/<destination org>/buckets\n\twrite:orgs/<destination org>/buckets.\n"
                    "Ensure your tokens have the required permissions.")
            if error.response.status == 422:
                logging.error("Ensure the buckets to be migrated do not share their names with "
                    "buckets that already exist in the destination instance. Individual buckets "
                    "can be migrated with a new name using the --src-bucket and --dest-bucket options.")
        elif type(error) == botocore.exceptions.ClientError:
            if "Error" not in error.response or "Code" not in error.response["Error"] or \
                "Message" not in error.response["Error"]:
                logging.error("An unknown error occurred when checking permissions for the S3 bucket "
                    "indicated with --s3-bucket. Verify the S3 bucket exists and the current boto3 user "
                    "has permission to access it and try again.")
            elif error.response["Error"]["Code"] == "403":
                logging.error("The current boto3 user does not own the S3 bucket indicated with --s3-bucket. "
                    "Check ownership and try again.")
            elif error.response["Error"]["Code"] == "404":
                logging.error("The S3 bucket indicated with --s3-bucket does not exist. "
                    "Check the existence of the S3 bucket and try again.")
            else:
                logging.error(f"Could not access S3 bucket: {error.response['Error']['Code']}: {error.response['Error']['Message']}")
        # subprocess.CalledProcessError exceptions will contain tokens, therefore, this check
        # will ensure we do not log subprocess.CalledProcessError exceptions
        elif type(error) != CalledProcessError:
            logging.error(str(error))
        logging.error("Migration failed. Exiting . . .")
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv[1:])
