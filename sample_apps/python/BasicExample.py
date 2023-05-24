from Constant import DATABASE_NAME, TABLE_NAME
from utils.WriteUtil import WriteUtil
from CrudAndSimpleIngestionExample import CrudAndSimpleIngestionExample
from CsvIngestionExample import CsvIngestionExample
from QueryExample import QueryExample
class BasicExample:
    def __init__(self, write_client, query_client, kmsId, csv_file_path, skip_deletion):
        self.kmsId = kmsId
        self.csv_file_path = csv_file_path
        self.skip_deletion = skip_deletion
        self.crud_and_simple_ingestion_example = CrudAndSimpleIngestionExample(write_client)
        self.csv_ingestion_example = CsvIngestionExample(write_client)
        self.query_example = QueryExample(query_client)
        self.write_util = WriteUtil(write_client)

    def run(self):
        try:
            self.crud_and_simple_ingestion_example.run(self.kmsId)
            if self.csv_file_path != None:
                self.csv_ingestion_example.bulk_write_records(self.csv_file_path)

            self.query_example.run_all_queries()

            # Try cancelling a query
            self.query_example.cancel_query()

            # Try a query with multiple pages
            self.query_example.run_query_with_multiple_pages(20000)
        finally:
            if not self.skip_deletion:
                self.write_util.delete_table(DATABASE_NAME, TABLE_NAME)
                self.write_util.delete_database(DATABASE_NAME)

