
// Load the SDK
const AWS = require("aws-sdk");

// Loading Examples
const crudAndSimpleIngestionExample = require("./crud-and-simple-ingestion-example");
const csvIngestExample = require("./csv-ingestion-example");
const queryExample = require("./query-example");

var argv = require('minimist')(process.argv.slice(2));

var csvFilePath = null;
if (argv.csvFilePath !== undefined) {
    csvFilePath = argv.csvFilePath
}

// Configuring AWS SDK
AWS.config.update({ region: "us-east-1" });

// Creating TimestreamWrite and TimestreamQuery client

/**
 * Recommended Timestream write client SDK configuration:
 *  - Set SDK retry count to 10.
 *  - Use SDK DEFAULT_BACKOFF_STRATEGY
 *  - Set RequestTimeout to 20 seconds .
 *  - Set max connections to 5000 or higher.
 */
var https = require('https');
var agent = new https.Agent({
    maxSockets: 5000
});
writeClient = new AWS.TimestreamWrite({
        maxRetries: 10,
        httpOptions: {
            timeout: 20000,
            agent: agent
        }
    });
queryClient = new AWS.TimestreamQuery();

async function callServices() {
    await crudAndSimpleIngestionExample.createDatabase();
    await crudAndSimpleIngestionExample.describeDatabase();
    await crudAndSimpleIngestionExample.updateDatabase(argv.kmsKeyId);
    await crudAndSimpleIngestionExample.listDatabases();
    await crudAndSimpleIngestionExample.createTable();
    await crudAndSimpleIngestionExample.describeTable();
    await crudAndSimpleIngestionExample.updateTable();
    await crudAndSimpleIngestionExample.listTables();
    await crudAndSimpleIngestionExample.writeRecords();
    await crudAndSimpleIngestionExample.writeRecordsWithCommonAttributes();
    await crudAndSimpleIngestionExample.writeRecordsWithUpsert();

    if (csvFilePath != null) {
        await csvIngestExample.processCSV(csvFilePath);
    }
    await queryExample.runAllQueries();

    // Try a query with multiple pages
    await queryExample.tryQueryWithMultiplePages(20000);

    //Try cancelling a query
    //This could fail if there is no data in the table, and the example query has finished before it was cancelled.
    await queryExample.tryCancelQuery();

    // Cleanup commented out
    //await crudAndSimpleIngestionExample.deleteTable();
    //await crudAndSimpleIngestionExample.deleteDatabase();
}

callServices();
