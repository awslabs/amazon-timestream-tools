import { constants } from "./constants.js";
import { getAllRows }  from "./query-example.js";
import { printRejectedRecordsException } from "./crud-and-simple-ingestion-example.js";
import { CreateTableCommand, DescribeTableCommand, UpdateTableCommand, WriteRecordsCommand } from "@aws-sdk/client-timestream-write";

// Composite Partition Keys are most effective when dimension has high cardinality
// and are frequently accessed in queries.
// Using dimension name with high cardinality, "hostId"
const COMPOSITE_PARTITION_KEY_DIM_NAME = "hostId";
const COMPOSITE_PARTITION_KEY_DIM_VALUE = "host1";
// Different dimension name to demonstrate enforcement level and record rejection
const COMPOSITE_PARTITION_KEY_DIFF_NAME = "hostIdDiff";

/**
 * This sample demonstrates workflow using dimension type partition key.
 *
 * 1. Create table with dimension type partition key, with optional enforcement.
 * 2. Ingest records with missing partition key. Records will be accepted as enforcement level is optional
 * 3. Update table with required enforcement.
 * 4. Ingest records with same partition key.
 * 5. Ingest records with missing partition key. This will return rejected records as they do not contain required partition key.
 * 6. Query records with same partition key.
 */
export async function runSampleWithDimensionPartitionKey(writeClient, queryClient, bucketName) {
    console.log("Starting example for partition key type dimension");

    // Create table with partition key type dimension and OPTIONAL enforcement
    const partitionKeyWithDimensionAndOptionalEnforcement = [
        {
            Type: "DIMENSION",
            Name: COMPOSITE_PARTITION_KEY_DIM_NAME,
            EnforcementInRecord: "OPTIONAL"
        }
    ];
    await createTableWithPartitionKey(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_DIMENSION_TABLE_NAME, partitionKeyWithDimensionAndOptionalEnforcement, bucketName);
    await describeTable(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_DIMENSION_TABLE_NAME);

    console.log("Writing records without partition key dimension into table with optional enforcement. " +
        "Since the enforcement level is OPTIONAL the records will be ingested");
    await writeRecords(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_DIMENSION_TABLE_NAME, COMPOSITE_PARTITION_KEY_DIFF_NAME);

    // Update partition key enforcement level to REQUIRED
    const partitionKeyWithDimensionAndRequiredEnforcement = [
        {
            Type: "DIMENSION",
            Name: COMPOSITE_PARTITION_KEY_DIM_NAME,
            EnforcementInRecord: "REQUIRED"
        }
    ];

    await updateTable(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_DIMENSION_TABLE_NAME, partitionKeyWithDimensionAndRequiredEnforcement);
    await describeTable(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_DIMENSION_TABLE_NAME);

    console.log("Writing records with partition key dimension");
    await writeRecords(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_DIMENSION_TABLE_NAME, COMPOSITE_PARTITION_KEY_DIM_NAME);

    console.log("Writing records without partition key dimension into table with required enforcement. " +
        "Since the enforcement level is REQUIRED the records will be rejected");
    await writeRecords(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_DIMENSION_TABLE_NAME, COMPOSITE_PARTITION_KEY_DIFF_NAME);

    // Query with partition key
    await runSingleQuery(queryClient, constants.DATABASE_NAME, constants.PARTITION_KEY_DIMENSION_TABLE_NAME, COMPOSITE_PARTITION_KEY_DIM_NAME, `'${COMPOSITE_PARTITION_KEY_DIM_VALUE}'`);
}

/** This sample demonstrates workflow using measure name type partition key.
 *
 * 1. Create table with measure name type partition key
 * 2. Ingest records and query
 */
export async function runSampleWithMeasureNamePartitionKey(writeClient, queryClient, bucketName) {
    console.log("Starting example for measure type partition key");

    // Create table with measure name type partition key
    const partitionKeyWithMeasure = [
        { Type: "MEASURE" }
    ];
    await createTableWithPartitionKey(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_MEASURE_TABLE_NAME, partitionKeyWithMeasure, bucketName);
    await describeTable(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_MEASURE_TABLE_NAME);
    console.log("Writing records");
    await writeRecords(writeClient, constants.DATABASE_NAME, constants.PARTITION_KEY_MEASURE_TABLE_NAME, COMPOSITE_PARTITION_KEY_DIM_NAME);
    await runSingleQuery(queryClient, constants.DATABASE_NAME, constants.PARTITION_KEY_MEASURE_TABLE_NAME, "cpu_utilization", "13.5");
}

async function createTableWithPartitionKey(writeClient, databaseName, tableName, compositePartitionKey, bucketName) {
    console.log(`Creating table with table name: ${tableName}`);

    const params = new CreateTableCommand({
        DatabaseName: databaseName,
        TableName: tableName,
        // Set RetentionProperties for the table
        RetentionProperties: {
            MemoryStoreRetentionPeriodInHours: constants.HT_TTL_HOURS,
            MagneticStoreRetentionPeriodInDays: constants.CT_TTL_DAYS
        },
        // Set MagneticStoreWriteProperties for the table
        MagneticStoreWriteProperties: {
            EnableMagneticStoreWrites: true,
            MagneticStoreRejectedDataLocation: {
                S3Configuration: {
                    BucketName: bucketName,
                    EncryptionOption: "SSE_S3"
                }
            }
        },
        // Set CompositePartitionKey for the table
        // Can use dimension or measure name as partition key
        // If using type dimension, can specify enforcement level with OPTIONAL or REQUIRED
        Schema: {
            CompositePartitionKey: compositePartitionKey
        }
    });

    await writeClient.send(params).then(
        (data) => {
            console.log(`Table ${data.Table.TableName} created successfully`);
        },
        (err) => {
            if (err.name === 'ConflictException') {
                console.log(`Table ${params.TableName} already exists on db ${params.DatabaseName}. Skipping creation.`);
            } else {
                console.log("Error creating table. ", err);
                throw err;
            }
        }
    );
}

async function updateTable(writeClient, databaseName, tableName, partitionKeyList) {
    console.log(`Updating table with table name: ${tableName}`);

    const params = new UpdateTableCommand({
        DatabaseName: databaseName,
        TableName: tableName,
        Schema: {
            CompositePartitionKey: partitionKeyList
        }
    });

    await writeClient.send(params).then(
        (data) => {
            console.log(`Table ${data.Table.TableName} updated successfully`);
        },
        (err) => {
            if (err.name === 'ConflictException') {
                console.log(`Table ${params.TableName} already exists on db ${params.DatabaseName}. Skipping creation.`);
            } else {
                console.log("Error creating table. ", err);
                throw err;
            }
        }
    );
}

async function describeTable(writeClient, databaseName, tableName) {
    console.log(`Describing table with table name: ${tableName}`);

    const params = new DescribeTableCommand({
        DatabaseName: databaseName,
        TableName: tableName,
    });

    await writeClient.send(params).then(
        (data) => {
            console.log(JSON.stringify(data.Table));
            console.log(`Table ${data.Table.TableName} has id ${data.Table.Arn}`);
            console.log(`Table ${data.Table.TableName} has composite partition key: ` + JSON.stringify(data.Table.Schema));
        },
        (err) => {
            if (err.name === "ResourceNotFoundException") {
                console.log("Table or Database doesn't exists.");
            } else {
                console.log("Describe table failed.", err);
                throw err;
            }
        }
    );
}

async function writeRecords(writeClient, databaseName, tableName, compositePartitionKeyDimName) {
    const currentTime = Date.now()

    const dimensions = [
        {"Name": "region", "Value": "us-east-1"},
        {"Name": "az", "Value": "az1"},
        {"Name": compositePartitionKeyDimName, "Value": COMPOSITE_PARTITION_KEY_DIM_VALUE}
    ];

    const commonAttributes = {
        "Dimensions": dimensions,
        "Time": currentTime.toString()
    };

    const cpuUtilization = {
        "MeasureName": "cpu_utilization",
        "MeasureValue": "13.5",
        "MeasureValueType": "DOUBLE"
    };

    const memoryUtilization = {
        "MeasureName": "memory_utilization",
        "MeasureValue": "40",
        "MeasureValueType": "DOUBLE"
    };

    // Adding multi record
    const cpuMemory = {
        "MeasureName": "cpu_memory",
        "MeasureValueType": "MULTI",
        "MeasureValues": [
            {
                "Name": "cpu_utilization",
                "Value": "13.5",
                "Type": "DOUBLE"
            },
            {
                "Name": "memory_utilization",
                "Value": "40",
                "Type": "DOUBLE"
            }
        ]
    }

    const records = [cpuUtilization, memoryUtilization, cpuMemory];

    const params = new WriteRecordsCommand({
        DatabaseName: databaseName,
        TableName: tableName,
        Records: records,
        CommonAttributes: commonAttributes
    });

    const response = await writeClient.send(params).then(
        (data) => {
            console.log("Write records successful.");
        },
        (err) => {
            if (err.name === 'RejectedRecordsException') {
                printRejectedRecordsException(err);
            } else {
                console.log("Error writing records:", err);
            }
        }
    );
}
async function runSingleQuery(queryClient, databaseName, tableName, partitionKeyName, partitionKeyValue) {
    const queryString = `SELECT * FROM "${databaseName}"."${tableName}" WHERE "${partitionKeyName}"=${partitionKeyValue}`;
    console.log(`Running query ${queryString}`);
    await getAllRows(queryClient, queryString, null);
}
