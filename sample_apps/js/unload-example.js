import fs from 'fs';
import readline from 'readline';
import {WriteRecordsCommand} from "@aws-sdk/client-timestream-write";
import {constants} from "./constants.js";
import {UnloadUtils} from "./utils/unload-utils.js";

export class UnloadExample {
    constructor(writeClient, queryClient, timestreamDependencyHelper, csvFilePath, bucketName) {
        if (!csvFilePath) {
            console.error("CSV file path is required to run unload.")
            return
        }
        this.writeClient = writeClient;
        this.csvFilePath = csvFilePath;
        this.timestreamDependencyHelper = timestreamDependencyHelper;
        this.bucketName = bucketName;
        this.unloadUtils = new UnloadUtils(queryClient, this.timestreamDependencyHelper, bucketName);
    }

    async bulkWriteShoppingRecords() {
        try {
            await this.ingestCsvRecords();
        } catch (e) {
            console.log('Error in ingesting records', e);
        }
    }

    async ingestCsvRecords() {
        const currentTime = Date.now().toString(); // Unix time in milliseconds

        let records = [];
        console.log("Ingesting Records", this.csvFilePath)
        const rl = readline.createInterface({
            input: fs.createReadStream(this.csvFilePath),
            crlfDelay: Infinity
        });

        let header_row = [];
        let row_counter = 0
        let counter = 0;
        const promises = [];

        for await (const dataRow of rl) {
            const row = dataRow.toString().split(',');

            if (row.length === 0) continue;

            if (counter === 0) {
                header_row = row;
                counter++;
                continue;
            }

            row_counter += 1;

            const dimensions = [
                {'Name': header_row[0], 'Value': row[0]},
                {'Name': header_row[1], 'Value': row[1]},
                {'Name': header_row[2], 'Value': row[2]},
                {'Name': header_row[3], 'Value': row[3]},
                {'Name': header_row[4], 'Value': row[4]},
                {'Name': header_row[5], 'Value': row[5]}
            ];

            // override the value on the file to get an ingestion that is around current time
            const recordTime = currentTime - (counter * 50);

            let measure_values = [];
            if (row[7] !== "") {
                measure_values.push({
                    "Name": header_row[7],
                    "Value": row[7],
                    "Type": 'VARCHAR',
                })
            }

            if (row[8] !== "") {
                measure_values.push({
                    "Name": header_row[8],
                    "Value": row[8],
                    "Type": 'VARCHAR',
                })
            }

            if (row[9] !== "") {
                measure_values.push({
                    "Name": header_row[9],
                    "Value": row[9],
                    "Type": 'VARCHAR',
                })
            }

            if (row[10] !== "") {
                measure_values.push({
                    "Name": header_row[10],
                    "Value": row[10],
                    "Type": 'DOUBLE',
                })
            }

            const record = {
                'Dimensions': dimensions,
                'MeasureName': "metrics",
                'MeasureValueType': "MULTI",
                'MeasureValues': measure_values,
                'Time': recordTime.toString()
            };

            records.push(record);
            counter++;

            if (records.length === 100) {
                promises.push(this.submitBatch( records, counter));
                records = [];
            }
        }

        if (records.length !== 0) {
            promises.push(this.submitBatch(records, counter));
        }

        await Promise.all(promises);

        console.log(`Ingested ${counter} records`);
    }

    submitBatch(records, counter) {
        const params = new WriteRecordsCommand({
            DatabaseName: constants.DATABASE_NAME,
            TableName: constants.TABLE_NAME,
            Records: records
        });

        return this.writeClient.send(params).then(
            (data) => {
                console.log(`Processed ${counter} records.`);
            },
            (err) => {
                console.log("Error writing records:", err);
            }
        );
    }

    async createBucket() {
        await this.timestreamDependencyHelper.createS3Bucket(this.bucketName);
    }

    async deleteBucket() {
        await this.timestreamDependencyHelper.deleteS3Bucket(this.bucketName);
    }

    async run() {
        try {
            await this.createBucket();
            await this.bulkWriteShoppingRecords();
            await this.unloadUtils.runSingleQuery();
        } catch (e) {
            console.error(e);
        }
    }

}