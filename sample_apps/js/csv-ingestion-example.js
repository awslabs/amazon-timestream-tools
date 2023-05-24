
import fs from 'fs';
import readline from 'readline';
import { constants } from "./constants.js";
import { WriteRecordsCommand } from "@aws-sdk/client-timestream-write";

export async function processCSV(writeClient, filePath) {
    try {
        await ingestCsvRecords(writeClient, filePath);
    } catch (e) {
        console.log('e', e);
    }
}

async function ingestCsvRecords(writeClient, filePath) {
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    var records = [];
    var counter = 0;

    const rl = readline.createInterface({
        input: fs.createReadStream(filePath),
        crlfDelay: Infinity
    });

    const promises = [];

    for await ( const dataRow of rl ) {
        var row = dataRow.toString().split(',');
        const dimensions = [
            {'Name': row[0].toString(), 'Value': row[1].toString()},
            {'Name': row[2].toString(), 'Value': row[3].toString()},
            {'Name': row[4].toString(), 'Value': row[5].toString()}
        ];
        const recordTime = currentTime - counter * 50;
        const record = {
            'Dimensions': dimensions,
            'MeasureName': row[6].toString(),
            'MeasureValue': row[7].toString(),
            'MeasureValueType': row[8].toString(),
            'Time': recordTime.toString()
        };

        records.push(record);
        counter++;

        if (records.length === 100) {
            promises.push(submitBatch(writeClient, records, counter));
            records = [];
        }
    }

    if (records.length !== 0) {
        promises.push(submitBatch(writeClient, records, counter));
    }

    await Promise.all(promises);

    console.log(`Ingested ${counter} records`);
}

function submitBatch(writeClient, records, counter) {
    const params = new WriteRecordsCommand({
        DatabaseName: constants.DATABASE_NAME,
        TableName: constants.TABLE_NAME,
        Records: records
    });

    return writeClient.send(params).then(
        (data) => {
            console.log(`Processed ${counter} records.`);
        },
        (err) => {
            console.log("Error writing records:", err);
        }
    );
}