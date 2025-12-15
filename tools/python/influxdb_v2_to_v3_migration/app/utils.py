import boto3
import json
from mypy_boto3_secretsmanager.client import SecretsManagerClient
from mypy_boto3_secretsmanager.type_defs import GetSecretValueResponseTypeDef
import os
from pathlib import Path
import tarfile


def get_secret(secret_name: str, region_name: str | None = None) -> dict[str, str]:
    """Retrieve a secret from AWS Secrets Manager"""
    if not region_name:
        region_name = os.environ.get("AWS_REGION", "us-east-1")

    session: boto3.Session = boto3.session.Session()
    client: SecretsManagerClient = session.client(
        service_name="secretsmanager", region_name=region_name
    )

    response: GetSecretValueResponseTypeDef = client.get_secret_value(
        SecretId=secret_name
    )
    return dict(json.loads(response["SecretString"]))


def parse_string_with_multiple_separators(
    buckets_and_ids: str, bucket_separator: str, bucket_id_separator: str
) -> list[tuple[str, ...]]:
    """
    Parses a string of buckets and their IDs separated first by a bucket separator and then a bucket and ID separator.
    """
    return [
        tuple(bucket_id_pair.split(bucket_id_separator))
        for bucket_id_pair in buckets_and_ids.split(bucket_separator)
    ]


def extract_all_tar_files_in_path(backup_path: Path):
    for tar_file_path in backup_path.glob("*.tar"):
        with tarfile.open(tar_file_path, "r") as tar_file:
            tar_file.extractall(path=backup_path)
    return
