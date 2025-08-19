import json
import pandas as pd
import sys
import traceback

class UnloadUtil:
    def __init__(self, client, timestream_dependency_helper, database_name, table_name, exported_bucket_name):
        self.client = client
        self.timestream_dependency_helper = timestream_dependency_helper

        #Exporting the data without any partitions,
        QUERY_1 = "SELECT user_id, ip_address, event, session_id, measure_name, time, query, quantity, product_id, channel FROM "
        QUERY_1 = QUERY_1 + database_name + "." + table_name
        QUERY_1 = QUERY_1 + " WHERE time BETWEEN ago(2d) AND now()"

        UNLOAD_QUERY_1 = UnloadQuery(QUERY_1, exported_bucket_name, "withoutparittion", "CSV", "GZIP", "")
        UNLOAD_PARQUET_QUERY_1 = UnloadQuery(QUERY_1, exported_bucket_name, "withoutparittion", "PARQUET", "GZIP", "")

        #Partitioning data by channel
        UNLOAD_QUERY_2 = UnloadQuery(QUERY_1, exported_bucket_name, "partitionbychannel", "CSV", "GZIP", ['channel'])

        #Partitioning data by event
        QUERY_3 = "SELECT user_id, ip_address, channel, session_id, measure_name, time, query, quantity, product_id, event FROM "
        QUERY_3 = QUERY_3 + database_name + "." + table_name
        QUERY_3 = QUERY_3 + " WHERE time BETWEEN ago(2d) AND now()"
        UNLOAD_QUERY_3 = UnloadQuery(QUERY_3, exported_bucket_name, "partitionbyevent", "CSV", "GZIP", ['event'])

        # Partitioning data by channel and event
        QUERY_4 = "SELECT user_id, ip_address, session_id, measure_name, time, query, quantity, product_id, channel,event FROM "
        QUERY_4 = QUERY_4 + database_name + "." + table_name
        QUERY_4 = QUERY_4 + " WHERE time BETWEEN ago(2d) AND now()"
        UNLOAD_QUERY_4 = UnloadQuery(QUERY_4, exported_bucket_name, "partitionbychannelandevent", "CSV", "GZIP", ['channel','event'])

        self.queries = [UNLOAD_QUERY_1, UNLOAD_PARQUET_QUERY_1, UNLOAD_QUERY_2, UNLOAD_QUERY_3, UNLOAD_QUERY_4]

    def run_all_queries(self):
        for query_id, query in enumerate(self.queries):
            print("\n")
            print("Running Unload query [%d] : [%s]" % (query_id + 1, query.build_query()))
            self.run_query(query)

    def run_single_query(self, query_id = 0):
        print("\n")
        print("Running Unload query [%d] : [%s]" % (query_id + 1, self.queries[query_id].build_query()))
        self.run_query(self.queries[query_id])

    def run_query(self, query):
        try:
            response = self.client.query(QueryString=query.build_query())
            manifest, metadata = self.__parse_query_result(response)
            self.__display_results(query, manifest, metadata)
        except Exception as err:
            print("Exception while running query:", err)
            traceback.print_exc(file=sys.stderr)

    def __parse_query_result(self, query_result):
        column_info = query_result['ColumnInfo']

        print("ColumnInfo: %s" % column_info)
        print("QueryId: %s" % query_result['QueryId'])
        print("QueryStatus:%s" % query_result['QueryStatus'])
        return self.parse_response(column_info, query_result['Rows'][0])

    def parse_response(self, column_info, row):
        response = {}
        data = row['Data']
        for i, column in enumerate(column_info):
            response[column['Name']] = data[i]['ScalarValue']

        return self.__get_manifest_metadata_file(response)

    def __get_manifest_metadata_file(self, response):
        metadata = self.timestream_dependency_helper.get_object(response['metadataFile']).read().decode('utf-8')
        manifest = self.timestream_dependency_helper.get_object(response['manifestFile']).read().decode('utf-8')
        return manifest, metadata

    def __display_results(self, query, manifest, metadata):
        if metadata is None or manifest is None:
            print("Manifest and Metadata file not found.")
            return

        parsed_manifest = json.loads(manifest)
        parsed_metadata = json.loads(metadata)
        result_files = parsed_manifest['result_files']
        print("Result files Count:", len(result_files))

        columns = self.get_column_names(parsed_metadata, query)
        print("Columns", columns)
        self.display_all_data(query, result_files, columns)

    def display_all_data(self, query, result_files, columns):
        frames = []
        for file in result_files:
            __file = self.timestream_dependency_helper.get_object(file['url'])
            df = None
            if query.format == 'CSV':
                df = pd.read_csv(__file, compression='gzip', header = None, sep = ',')
            elif query.format == 'PARQUET':
                df = pd.read_parquet(file['url'], engine='auto')
            frames.append(df)
        result = pd.concat(frames, axis=0, join="outer", ignore_index=True)
        result.columns = columns
        print(result)
    
    def get_column_names(self, metadata, query):
        columns = []
        for column in metadata['ColumnInfo']:
            if (column['Name'] not in query.partition_by):
                columns.append(column['Name'])
        return columns

class UnloadQuery:
    def __init__(self, query, s3_bucket_location, results_prefix, format, compression , partition_by):
        self.query = query
        self.s3_bucket_location = s3_bucket_location
        self.results_prefix = results_prefix
        self.format = format
        self.compression = compression
        self.partition_by = partition_by

    def build_query(self):
        query_results_s3_path = "'s3://" + self.s3_bucket_location + "/" + self.results_prefix + "/'"
        unload_query = "UNLOAD("
        unload_query = unload_query + self.query
        unload_query = unload_query + ") "
        unload_query = unload_query + " TO " + query_results_s3_path
        unload_query = unload_query + " WITH ( "

        if(len(self.partition_by) > 0) :
            unload_query = unload_query + " partitioned_by = ARRAY" + str(self.partition_by) + ","

        unload_query = unload_query + " format='" + self.format  + "', "
        unload_query = unload_query + "  compression='" + self.compression + "')"

        return unload_query