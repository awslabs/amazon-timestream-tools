
const fs = require('fs');
const readline = require('readline');

const constants = require('./constants');

async function processCSV(filePath) {
    try {
        await ingestCsvRecords(filePath);
    } catch (e) {
        console.log('e', e);
    }
}

async function ingestCsvRecords(filePath) {
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
        let record = {
            'Dimensions': dimensions,
            'MeasureName': 'metrics',
            'MeasureValues': [
                {
                    'Name': row[8],
                    'Value': row[9],
                    'Type': row[10],
                },
                {
                    'Name': row[11],
                    'Value': row[12],
                    'Type': row[13],
                }
            ],
            'MeasureValueType': 'MULTI',
            'Time': recordTime.toString()
        };

        records.push(record);
        counter++;

        if (records.length === 100) {
            promises.push(submitBatch(records, counter, constants.MEASURE_VALUE_SAMPLE_TABLE));
            records = [];
        }
    }

    if (records.length !== 0) {
        promises.push(submitBatch(records, counter, constants.MEASURE_VALUE_SAMPLE_TABLE));
    }

    await Promise.all(promises);

    console.log(`Ingested ${counter} records`);
}

function submitBatch(records, counter) {
    const params = {
        DatabaseName: constants.DATABASE_NAME,
        TableName: constants.TABLE_NAME,
        Records: records
    };

    var promise = writeClient.writeRecords(params).promise();

    return promise.then(
        (data) => {
            console.log(`Processed ${counter} records.`);
        },
        (err) => {
            console.log("Error writing records:", err);
        }
    );
}

module.exports = {processCSV};