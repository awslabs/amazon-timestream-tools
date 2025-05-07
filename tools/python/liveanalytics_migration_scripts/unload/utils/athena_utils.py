import boto3
import time
import re
from logger_utils import create_logger


# 24 hours
MAX_WAIT_SECONDS = 86400


class AthenaUtility:
    def __init__(self, region=None):
        """
        Initialize the AthenaUtility class.

        Args:
            region (str): The AWS region.
        """
        self.athena_client = boto3.client("athena", region_name=region)
        self.logger = create_logger("athena_logger")

    def start_query_execution(self, query_string: str, database_name="default"):
        return self.athena_client.start_query_execution(
            QueryString=query_string,
            QueryExecutionContext={"Database": database_name},
        )

    def wait_for_athena_query(
        self, query_execution_id: str, max_wait_seconds=MAX_WAIT_SECONDS
    ):
        elapsed_seconds = 0
        time_wait_period = 15
        state = ""
        query_status = {}
        while elapsed_seconds < max_wait_seconds:
            query_status = self.athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            state = query_status["QueryExecution"]["Status"]["State"]

            self.logger.info(f"State: {state}")

            if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                break
            time.sleep(time_wait_period)
            elapsed_seconds += time_wait_period

        if elapsed_seconds >= max_wait_seconds:
            raise RuntimeError(
                f"Timed out waiting for query after {elapsed_seconds} seconds"
            )

        if state == "SUCCEEDED":
            self.logger.info("Query successful")
        else:
            raise RuntimeError(
                f"Query failed: {query_status['QueryExecution']['Status']}"
            )

    @staticmethod
    def is_valid_athena_table_name(athena_table_name: str) -> bool:
        """
        Validates an Athena table name.

        Args:
            athena_table_name (str): The Athena table name to validate.

        Returns:
            bool: Whether the Athena table name is valid.
        """
        VALID_ATHENA_NAME = re.compile(r"^[A-Za-z0-9._ ][A-Za-z0-9._]{1,255}$")
        return VALID_ATHENA_NAME.match(athena_table_name) is not None
