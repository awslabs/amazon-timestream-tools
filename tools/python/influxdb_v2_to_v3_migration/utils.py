import boto3
import json
import os
from pathlib import Path
import tarfile


def get_secret(secret_name: str, region_name: str | None = None) -> dict[str, str]:
    """
    Retrieves a secret value from a secret in AWS Secrets Manager.

    Args:
        secret_name (str): The name of the secret to retrieve.
        region_name (str | None): The AWS Region to use.

    Returns:
        dict[str, str]: The secret value.
    """
    if not region_name:
        region_name = os.environ.get("AWS_REGION", "us-east-1")

    session: boto3.Session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager", region_name=region_name
    )

    response = client.get_secret_value(
        SecretId=secret_name
    )
    return dict(json.loads(response["SecretString"]))


def extract_all_tar_files_in_path(backup_path: Path) -> None:
    """
    Extracts all tar files in a path.

    Args:
        backup_path (Path): The path containing tar files

    Returns:
        None
    """
    for tar_file_path in backup_path.glob("*.tar"):
        with tarfile.open(tar_file_path, "r") as tar_file:
            tar_file.extractall(path=backup_path)
    return
