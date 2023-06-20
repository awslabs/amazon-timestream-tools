import { TimestreamWriteClient } from "@aws-sdk/client-timestream-write";
import { TimestreamQueryClient } from "@aws-sdk/client-timestream-query";
import { TimestreamDependencyHelper } from "./utils/timestream-dependency-helper.js";
import * as crudAndSimpleIngestionExample from "./crud-and-simple-ingestion-example.js";
import * as queryExample from "./query-example.js";
import * as csvIngestExample from "./csv-ingestion-example.js";
import {
    runSampleWithDimensionPartitionKey,
    runSampleWithMeasureNamePartitionKey
} from "./composite-partition-key-example.js";
import https from 'https';
import minimist from 'minimist';
import { UnloadExample } from "./unload-example.js";
import { constants } from "./constants.js";

const appType = {
    Basic: "basic",
    Unload: "unload",
    Cleanup: "cleanup",
    CompositePartitionKey: "compositePartitionKey",
}

const argv = minimist(process.argv.slice(2), {
    boolean: "skipDeletion"
});

const type = argv.type ?? appType.Basic;
const region = argv.region ?? "us-west-2";
const skipDeletion = argv.skipDeletion ?? true;
const csvFilePath = argv.csvFilePath ?? null;

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

    //Try cancelling a query
    //This could fail if there is no data in the table, and the example query has finished before it was cancelled.
    await queryExample.tryCancelQuery(queryClient);

    // Try a query with multiple pages
    await queryExample.tryQueryWithMultiplePages(queryClient, 20000);
}

async function callUnload() {
    const timestreamDependencyHelper = new TimestreamDependencyHelper(region);
    const account = await timestreamDependencyHelper.getAccount();
    const bucketName = constants.S3_BUCKET_PREFIX_UNLOAD + region + "-" + account;
    const unloadExample = new UnloadExample(writeClient, queryClient, timestreamDependencyHelper, csvFilePath, bucketName);
    
    await createResources();
    await unloadExample.run();

    if (!skipDeletion) {
        await timestreamDependencyHelper.deleteS3Bucket(bucketName);
        await cleanup();
    }
}

async function callCompositePartitionKey() {
    const timestreamDependencyHelper = new TimestreamDependencyHelper(region);
    const bucketName = constants.S3_BUCKET_PREFIX_CPK + timestreamDependencyHelper.generateRandomStringWithSize(5);
    await timestreamDependencyHelper.createS3Bucket(bucketName);
    await crudAndSimpleIngestionExample.createDatabase(writeClient);

    await runSampleWithDimensionPartitionKey(writeClient, queryClient, bucketName);
    await runSampleWithMeasureNamePartitionKey(writeClient, queryClient, bucketName);

    if (!skipDeletion) {
        await timestreamDependencyHelper.deleteS3Bucket(bucketName);
        await cleanup();
    }
}

async function cleanup() {
    await crudAndSimpleIngestionExample.deleteTable(writeClient, constants.DATABASE_NAME, constants.TABLE_NAME);
    await crudAndSimpleIngestionExample.deleteTable(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_DIMENSION_TABLE_NAME);
    await crudAndSimpleIngestionExample.deleteTable(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_MEASURE_TABLE_NAME);
    await crudAndSimpleIngestionExample.deleteDatabase(writeClient);
}

switch (type) {
    case appType.Basic:
        callServices();
        break;
    case appType.Unload:
        callUnload();
        break;
    case appType.Cleanup:
        cleanup();
        break;
    case appType.CompositePartitionKey:
        callCompositePartitionKey();
        break;
}
