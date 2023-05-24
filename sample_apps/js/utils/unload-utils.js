import {constants} from "../constants.js";
import {QueryCommand} from "@aws-sdk/client-timestream-query";

export class UnloadUtils {
    constructor(queryClient, timestreamDependencyHelper, exported_bucket_name) {
        this.queryClient = queryClient;
        this.timestreamDependencyHelper = timestreamDependencyHelper;

        let QUERY_1 = "SELECT user_id, ip_address, event, session_id, measure_name, time, query, quantity, product_id, channel FROM "
        QUERY_1 = QUERY_1 + constants.DATABASE_NAME + "." + constants.TABLE_NAME
        QUERY_1 = QUERY_1 + " WHERE time BETWEEN ago(2d) AND now()"

        const UNLOAD_QUERY_1 = new UnloadQuery(QUERY_1, exported_bucket_name, "withoutparittion", "CSV", "GZIP", "")

        const UNLOAD_QUERY_2 = new UnloadQuery(QUERY_1, exported_bucket_name, "partitionbychannel", "CSV", "GZIP", ['channel'])

        // Partitioning data by event
        let QUERY_3 = "SELECT user_id, ip_address, channel, session_id, measure_name, time, query, quantity, product_id, event FROM "
        QUERY_3 = QUERY_3 + constants.DATABASE_NAME + "." + constants.TABLE_NAME
        QUERY_3 = QUERY_3 + " WHERE time BETWEEN ago(2d) AND now()"
        const UNLOAD_QUERY_3 = new UnloadQuery(QUERY_3, exported_bucket_name, "partitionbyevent", "CSV", "GZIP", ['event'])

        // Partitioning data by channel and event
        let QUERY_4 = "SELECT user_id, ip_address, session_id, measure_name, time, query, quantity, product_id, channel,event FROM ";
        QUERY_4 = QUERY_4 + constants.DATABASE_NAME + "." + constants.TABLE_NAME;
        QUERY_4 = QUERY_4 + " WHERE time BETWEEN ago(2d) AND now()";
        const UNLOAD_QUERY_4 = new UnloadQuery(QUERY_4, exported_bucket_name, "partitionbychannelandevent", "CSV", "GZIP", ['channel', 'event']);

        this.queries = [UNLOAD_QUERY_1, UNLOAD_QUERY_2, UNLOAD_QUERY_3, UNLOAD_QUERY_4]
    }

    async runQueries() {
        for (let i = 0; i < this.queries.length; i++) {
            await this.runQuery(this.queries[i], i);
        }
        console.log("Query execution complete!")
    }

    async runSingleQuery(queryId = 1) {
        const query = this.queries[queryId];
        await this.runQuery(query, queryId);
    }

    async runQuery(query, queryId) {
        console.log("Running Unload query [%d] : [%s]", queryId, query.buildQuery());
        const params = new QueryCommand({
            QueryString: query.buildQuery()
        })
        await this.queryClient.send(params).then(
            async (data) => {
                if (data.NextToken) {
                    this.runQuery(query, queryId);
                } else {
                    await this.parseAndDisplayResults(data, query);
                }
            },
            (err) => {
                console.error("Error while executing the query", err)
            }
        )
    }

    async parseAndDisplayResults(data, query) {
        const columnInfo = data['ColumnInfo'];
        console.log("ColumnInfo:", columnInfo)
        console.log("QueryId: %s", data['QueryId'])
        console.log("QueryStatus:", data['QueryStatus'])
        await this.parseResponse(columnInfo, data['Rows'][0], query)
    }

    async parseResponse(columnInfo, row, query) {
        let response = {}
        const data = row['Data']
        columnInfo.forEach((column, i) => {
            response[column['Name']] = data[i]['ScalarValue']
        })

        let manifest, metadata;
        await this.getManifestAndMetadataFile(response).then((data) => {
            manifest = data.manifest;
            metadata = data.metadata;
        })

        await this.displayResults(manifest, metadata, query);
    }

    async getManifestAndMetadataFile(response) {
        console.log("Getting metadata and manifest");
        let manifest, metadata;
        await this.timestreamDependencyHelper.getS3Object(response['manifestFile']).then(
            (data) => {
                manifest = JSON.parse(data);
            }
        );
        await this.timestreamDependencyHelper.getS3Object(response['metadataFile']).then(
            (data) => {
                metadata = JSON.parse(data);
            }
        );
        return {manifest, metadata}
    }

    async displayResults(manifest, metadata, query) {
        const resultFiles = manifest['result_files']
        console.log("Result files Count:", resultFiles.length)

        const columns = this.getColumnNames(metadata, query)
        console.log("Columns", columns)

        await this.displayAllData(query, resultFiles, columns);
    }

    async displayAllData(query, resultFiles, columns) {
        let mergedData = [];
        const promises = []
        resultFiles.forEach((file) => {

            const promise = this.timestreamDependencyHelper.getS3CSVObject(file['url'], columns).then((data) => {
                mergedData = mergedData.concat(data);
            })
            promises.push(promise);
        });
        await Promise.all(promises);
        console.log("Number of rows:", mergedData.length);

        console.log("Displaying first 10 entries.. \n", mergedData.slice(1, 10))
    }

    getColumnNames(metadata, query) {
        let columns = []
        metadata['ColumnInfo'].forEach((column) => {
            if (!query.partition_by.includes(column['Name'])) {
                columns.push(column['Name'])
            }
        });
        return columns
    }

    wait(time) {
        return new Promise(resolve => {
            setTimeout(resolve, time);
        });
    }
}

class UnloadQuery {
    constructor(query, s3_bucket_location, results_prefix, format, compression , partition_by) {
        this.query = query;
        this.s3_bucket_location = s3_bucket_location
        this.results_prefix = results_prefix
        this.format = format
        this.compression = compression
        this.partition_by = partition_by
    }

    buildQuery() {
        const query_results_s3_path = "'s3://" + this.s3_bucket_location + "/" + this.results_prefix + "/'"
        let unload_query = "UNLOAD("
        unload_query = unload_query + this.query
        unload_query = unload_query + ") "
        unload_query = unload_query + " TO " + query_results_s3_path
        unload_query = unload_query + " WITH ( "

        if(this.partition_by.length > 0) {
            let partitionBy = ""
            this.partition_by.forEach((str, i) => {
                partitionBy = partitionBy + (i ? ",'" : "'") + str + "'"
            })
            unload_query = unload_query + " partitioned_by = ARRAY[" + partitionBy + "],"
        }
        unload_query = unload_query + " format='" + this.format  + "', "
        unload_query = unload_query + "  compression='" + this.compression + "')"

        return unload_query
    }
}