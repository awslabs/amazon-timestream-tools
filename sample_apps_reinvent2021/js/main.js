
// Load the SDK

const AWS = require("aws-sdk");
const constants = require('./constants');

// Loading Examples
const crudAndSimpleIngestionExample = require("./crud-and-simple-ingestion-example");
const multiValueAttributeExample = require("./multi-value-attributes-example");
const csvIngestExample = require("./csv-ingestion-example");
const queryExample = require("./query-example");
const scheduleQueryExample = require("./schedule-query-example");
const timestreamDependencyHelper = require("./timestream-dependency-helper");

var argv = require('minimist')(process.argv.slice(2));

var csvFilePath = null;
var AWSregion = "us-east-1";

if (argv.csvFilePath !== undefined) {
    csvFilePath = argv.csvFilePath;
}

if (argv.region !== undefined) {
    AWSregion = argv.region;
}

// Configuring AWS SDK
AWS.config.update({ region: AWSregion });

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
snsClient = new AWS.SNS({apiVersion: '2010-03-31'});
iamClient = new AWS.IAM({apiVersion: '2010-05-08'});
sqsClient = new AWS.SQS({apiVersion: '2012-11-05'});
s3Client = new AWS.S3({apiVersion: '2006-03-01'});

async function writeRecordsExample() {
    await crudAndSimpleIngestionExample.createDatabase(constants.MEASURE_VALUE_SAMPLE_DB);
    await crudAndSimpleIngestionExample.createTable(constants.MEASURE_VALUE_SAMPLE_DB, constants.MEASURE_VALUE_SAMPLE_TABLE);
    await multiValueAttributeExample.writeRecordsWithMultiMeasureValueSingleRecord();
    await multiValueAttributeExample.writeRecordsWithMultiMeasureValueMultipleRecordsMixture();
    
    if (csvFilePath != null) {
        await csvIngestExample.processCSV(csvFilePath);
    }
}

async function queryExamples() {
    if (csvFilePath != null) {
        await queryExample.runAllQueries();

        // Try a query with multiple pages
        await queryExample.tryQueryWithMultiplePages(200);

        //Try cancelling a query
        //This could fail if there is no data in the table, and the example query has finished before it was cancelled.
        await queryExample.tryCancelQuery();
    }
    else {
        console.log("Running the SELECT ALL query");
        await queryExample.tryQueryWithMultiplePages(10);
    }
}

async function createAndUpdateDatabaseExamples() {
    await crudAndSimpleIngestionExample.createDatabase(constants.DATABASE_NAME);
    await crudAndSimpleIngestionExample.describeDatabase();
    await crudAndSimpleIngestionExample.listDatabases();
    await crudAndSimpleIngestionExample.updateDatabase(argv.kmsKeyId);
    s3ErrorReportBucketName = await timestreamDependencyHelper.createS3Bucket(constants.SQ_ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX + Math.random().toString(36).slice(-5));
    await crudAndSimpleIngestionExample.createTable(constants.DATABASE_NAME, constants.TABLE_NAME);
    await crudAndSimpleIngestionExample.describeTable();
    await crudAndSimpleIngestionExample.listTables();
    await crudAndSimpleIngestionExample.updateTable();
}

async function scheduledQueryExamples() {
    try {
        await crudAndSimpleIngestionExample.createDatabase(constants.SQ_DATABASE_NAME);
        await crudAndSimpleIngestionExample.createTable(constants.SQ_DATABASE_NAME, constants.SQ_TABLE_NAME);

        var topicArn = await timestreamDependencyHelper.createSnsTopic();
        var queueUrl = await timestreamDependencyHelper.createSqsQueue();

        var queueArn = await timestreamDependencyHelper.getQueueArn(queueUrl);
        var subscriptionArn = await timestreamDependencyHelper.subscribeToSnsTopic(topicArn, queueArn);
        await timestreamDependencyHelper.setSqsAccessPolicy(queueUrl, topicArn, queueArn);

        
        var roleArn = await timestreamDependencyHelper.createIamRole();
        var policyArn = await timestreamDependencyHelper.createIAMPolicy();
        await timestreamDependencyHelper.attachIAMRolePolicy(policyArn);
        
        
        console.log("Waiting 15 seconds for newly created role to become active");
        //Waiting for newly created role to be active
        await new Promise(resolve => setTimeout(resolve, 15000));

        var scheduledQueryArn = await scheduleQueryExample.createScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName,constants.SQ_DATABASE_NAME, constants.SQ_TABLE_NAME);
        //var scheduledQueryArn = await scheduleQueryExample.createInvalidScheduleQuery(topicArn, roleArn, s3ErrorReportBucketName, constants.SQ_DATABASE_NAME, constants.SQ_TABLE_NAME);

        await scheduleQueryExample.listScheduledQueries();
        await scheduleQueryExample.describeScheduledQuery(scheduledQueryArn);

        console.log("Waiting 65 seconds for automatic ScheduledQuery executions & notifications");
        await new Promise(resolve => setTimeout(resolve, 65000));

        var didQuerySucceedManually = false;
        var wasQueryTriggeredAsExpected = false;
        var queryFailed = false;

        var response = null;
        for (let i = 0; i < 10; i++) {
            response = await timestreamDependencyHelper.receiveMessage(queueUrl);
            if (response === 'undefined') {
                continue;
            }

            var recepitHandler = response.ReceiptHandle;
            var bodyStr = response.Body;
            
            // remove all escape double backslash in nested Json, so that json parser is able to work correctly
            bodyStr = bodyStr.replace(/\\"/g, '"');
            bodyStr = bodyStr.replace(/"{/g, '{');
            bodyStr = bodyStr.replace(/}"/g, '}');
            
            var bodyJson = JSON.parse(bodyStr);
            var notificationType = bodyJson["MessageAttributes"]["notificationType"]["Value"];

            // Since its a FIFO queue, we need to delete the message to be able to receive further messages
            await timestreamDependencyHelper.deleteMessage(queueUrl, recepitHandler);

            switch(notificationType) {
                case 'SCHEDULED_QUERY_CREATING':
                    // Scheduled Query is still pending to run.
                    // Fall-through
                case 'SCHEDULED_QUERY_CREATED':
                    console.log("Scheduled Query is still pending to run");
                    break;
                case 'MANUAL_TRIGGER_SUCCESS':
                    console.log("Manual execution of Scheduled Query succeeded");
                    didQuerySucceedManually = true;
                    break;
                case 'AUTO_TRIGGER_SUCCESS':
                    console.log("Scheduled Query was triggered as expected. Now triggering another run manually");
                    wasQueryTriggeredAsExpected = true;
                    await scheduleQueryExample.executeScheduledQuery(scheduledQueryArn, new Date());
                    break;
                case 'AUTO_TRIGGER_FAILURE':
                    // Fall-through
                case 'MANUAL_TRIGGER_FAILURE':
                    queryFailed = true;
                    var scheduledQueryRunSummary = bodyJson["Message"]["scheduledQueryRunSummary"];
                    
                    console.log("Failure Reason from SQS Notification: " + scheduledQueryRunSummary["failureReason"]);
                    if (scheduledQueryRunSummary["errorReportLocation"] !== 'undefined') {
                        var errorReportPrefix = scheduledQueryRunSummary["errorReportLocation"]["s3ReportLocation"]["objectKey"];
                        await scheduleQueryExample.parseS3ErrorReport(s3ErrorReportBucketName, errorReportPrefix);
                    }
                    break;
                case 'SCHEDULED_QUERY_DELETED':
                    // Fall-through
                case 'SCHEDULED_QUERY_UPDATED':
                    // Fall-through
                default:
                    console.log("Received an unexpected message: " + response);
            }


            if ((didQuerySucceedManually && wasQueryTriggeredAsExpected) || queryFailed) {
                break;
            }
        }

        if (wasQueryTriggeredAsExpected || didQuerySucceedManually) {
            console.log("Fetching Scheduled Query execution results");
            var queryStr = `SELECT * FROM ${constants.SQ_DATABASE_NAME}.${constants.SQ_TABLE_NAME}`;
            await queryExample.getAllRows(queryStr, null);
        }

        await scheduleQueryExample.updateScheduledQueries(scheduledQueryArn);
    } catch (err) {
        console.log(err);
    }

    finally {
        await scheduleQueryExample.deleteScheduleQuery(scheduledQueryArn);
        await timestreamDependencyHelper.detachPolicy(policyArn);
        await timestreamDependencyHelper.deleteIAMPolicy(policyArn);
        await timestreamDependencyHelper.deleteIamRole();
        await timestreamDependencyHelper.deleteSqsQueue(queueUrl);
        await timestreamDependencyHelper.unsubscribeFromSnsTopic(subscriptionArn);
        await timestreamDependencyHelper.deleteSnsTopic(topicArn);
        await timestreamDependencyHelper.clearBucket();
        await timestreamDependencyHelper.deleteS3Bucket();
        await crudAndSimpleIngestionExample.deleteTable(constants.SQ_DATABASE_NAME, constants.SQ_TABLE_NAME);
        await crudAndSimpleIngestionExample.deleteDatabase(constants.SQ_DATABASE_NAME);
    }
}


(async() => {
    try {
        var s3ErrorReportBucketName = null;
        await createAndUpdateDatabaseExamples();
        await writeRecordsExample();
        await queryExamples();
        if (csvFilePath != null) {
            await scheduledQueryExamples();
        }
    }
    finally {
        // Uncomment the lines below to delete the database/table as a clean-up action
        // await crudAndSimpleIngestionExample.deleteTable(constants.DATABASE_NAME, constants.TABLE_NAME);
        // await crudAndSimpleIngestionExample.deleteDatabase(constants.DATABASE_NAME);
    }
  })();
