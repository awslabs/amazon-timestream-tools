import {TimestreamWriteClient} from "@aws-sdk/client-timestream-write";
import { TimestreamQueryClient } from "@aws-sdk/client-timestream-query";
import {TimestreamDependencyHelper} from "./utils/timestream-dependency-helper.js";
import * as crudAndSimpleIngestionExample from "./crud-and-simple-ingestion-example.js";
import * as queryExample from "./query-example.js";
import * as csvIngestExample from "./csv-ingestion-example.js";
import https from 'https';
import minimist from 'minimist';
import {UnloadExample} from "./unload-example.js";
import {constants} from "./constants.js";


const appType = {
    Basic: "basic",
    Unload: "unload",
    Cleanup: "cleanup"
}

const argv = minimist(process.argv.slice(2), {
    boolean: "skipDeletion"
});

let csvFilePath = null;
let type = appType.Basic;
let region = "us-west-2";
let skipDeletion = true;
if (argv.csvFilePath !== undefined) {
    csvFilePath = argv.csvFilePath;
}

if (argv.type !== undefined) {
    type = argv.type;
}

if (argv.region != undefined) {
    region = argv.region;
}

if (argv.skipDeletion != undefined) {
    skipDeletion = argv.skipDeletion;
}

/**
 * Recommended Timestream write client SDK configuration:
 *  - Set SDK retry count to 10.
 *  - Use SDK DEFAULT_BACKOFF_STRATEGY
 *  - Set RequestTimeout to 20 seconds .
 *  - Set max connections to 5000 or higher.
 */
const agent = new https.Agent({
    maxSockets: 5000
});
const writeClient = new TimestreamWriteClient({
        maxRetries: 10,
        httpOptions: {
            timeout: 20000,
            agent: agent
        },
    region: region
    });
const queryClient = new TimestreamQueryClient({
    region: region
});

async function createResources() {
    await crudAndSimpleIngestionExample.createDatabase(writeClient);
    await crudAndSimpleIngestionExample.describeDatabase(writeClient);
    await crudAndSimpleIngestionExample.updateDatabase(argv.kmsKeyId, writeClient);
    await crudAndSimpleIngestionExample.listDatabases(writeClient);
    await crudAndSimpleIngestionExample.createTable(writeClient);
    await crudAndSimpleIngestionExample.describeTable(writeClient);
    await crudAndSimpleIngestionExample.updateTable(writeClient);
    await crudAndSimpleIngestionExample.listTables(writeClient);
}

async function callServices() {
    await createResources();
    await crudAndSimpleIngestionExample.writeRecords(writeClient);
    await crudAndSimpleIngestionExample.writeRecordsWithCommonAttributes(writeClient);
    await crudAndSimpleIngestionExample.writeRecordsWithUpsert(writeClient);

    if (csvFilePath != null) {
        await csvIngestExample.processCSV(writeClient, csvFilePath);
    }
    await queryExample.runAllQueries(queryClient);

    // Try a query with multiple pages
    await queryExample.tryQueryWithMultiplePages(queryClient, 20000);

    //Try cancelling a query
    //This could fail if there is no data in the table, and the example query has finished before it was cancelled.
    await queryExample.tryCancelQuery(queryClient);
}

async function callUnload() {
    const timestreamDependencyHelper = new TimestreamDependencyHelper(region);
    const account = await timestreamDependencyHelper.getAccount();
    const bucketName = constants.S3BUCKETPREFIX + region + "-" + account;
    const unloadExample = new UnloadExample(writeClient, queryClient, timestreamDependencyHelper, csvFilePath, bucketName);
    
    await createResources();
    await unloadExample.run();

    if (!skipDeletion) {
        await timestreamDependencyHelper.deleteS3Bucket(bucketName);
        await cleanup();
    }
}

async function cleanup() {
    await crudAndSimpleIngestionExample.deleteTable(writeClient);
    await crudAndSimpleIngestionExample.deleteDatabase(writeClient);
}

if (!type || appType.Basic === type.toLowerCase()) {
    callServices();
} else if (appType.Unload === type.toLowerCase()) {
    callUnload();
} else if (appType.Cleanup === type.toLowerCase()) {
    cleanup();
}
