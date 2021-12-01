const constants = require('./constants');
const errorReportHelper = require("./utils/error-report-helper.js");
var localDate = require("@js-joda/core").LocalDate.now();

const HOSTNAME = "host-24Gju";
const SQ_NAME = "daily-sample";
const SCHEDULE_EXPRESSION = "cron(0/1 * * * ? *)";

// Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
const VALID_QUERY = "SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, " +
"    ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization, " +
"    ROUND(APPROX_PERCENTILE(cpu_utilization, 0.9), 2) AS p90_cpu_utilization, " +
"    ROUND(APPROX_PERCENTILE(cpu_utilization, 0.95), 2) AS p95_cpu_utilization, " +
"    ROUND(APPROX_PERCENTILE(cpu_utilization, 0.99), 2) AS p99_cpu_utilization " +
"FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
"WHERE measure_name = 'metrics' " +
"   AND hostname = '" + HOSTNAME + "' " +
"    AND time > ago(2h) " +
"GROUP BY region, hostname, az, BIN(time, 15s) " +
"ORDER BY binned_timestamp ASC " +
"LIMIT 5";

// A Query with a hardcoded column `timestamp` containing a date that in the future[2030-12-26]. The Query as such is
// valid, but when the result of the query is used as a timeColumn for ingesting into a destination Table, it would
// fail since the time-range is outside table retention window.
var date = localDate.plusDays(constants.HT_TTL_HOURS / 24 + 1);
const INVALID_QUERY = `SELECT cast('${date}' as TIMESTAMP) as timestamp, 1.0 as random_measure_value, ` +
`'1' as dim0`;

async function createScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName, databaseName, tableName) {
    console.log("Creating Valid Scheduled Query");
    const DimensionMappingList = [
        {'Name': 'region', 'DimensionValueType': 'VARCHAR'},
        {'Name': 'az', 'DimensionValueType': 'VARCHAR'},
        {'Name': 'hostname', 'DimensionValueType': 'VARCHAR'}
    ];

    const MultiMeasureMappings = {
        TargetMultiMeasureName: "multi-metrics",
        MultiMeasureAttributeMappings: [
            {'SourceColumn': 'avg_cpu_utilization', 'MeasureValueType': 'DOUBLE'},
            {'SourceColumn': 'p90_cpu_utilization', 'MeasureValueType': 'DOUBLE'},
            {'SourceColumn': 'p95_cpu_utilization', 'MeasureValueType': 'DOUBLE'},
            {'SourceColumn': 'p99_cpu_utilization', 'MeasureValueType': 'DOUBLE'},
        ]
    }

    const timestreamConfiguration = {
        DatabaseName: databaseName,
        TableName: tableName,
        TimeColumn: "binned_timestamp",
        DimensionMappings: DimensionMappingList,
        MultiMeasureMappings: MultiMeasureMappings
    }

    const createScheduledQueryRequest = {
        Name: SQ_NAME,
        QueryString: VALID_QUERY,
        ScheduleConfiguration: {
            ScheduleExpression: SCHEDULE_EXPRESSION
        },
        NotificationConfiguration: {
            SnsConfiguration: {
                TopicArn: topicArn
            }
        },
        TargetConfiguration: {
            TimestreamConfiguration: timestreamConfiguration
        },
        ScheduledQueryExecutionRoleArn: roleArn,
        ErrorReportConfiguration: {
            S3Configuration: {
                BucketName: s3ErrorReportBucketName
            }
        }
    };
    try {
        const data = await queryClient.createScheduledQuery(createScheduledQueryRequest).promise();
        console.log("Successfully created scheduled query: " + data.Arn);
        return data.Arn;
    } catch (err) {
        console.log("Scheduled Query creation failed: ", err);
        throw err;
    }
}

async function createInvalidScheduleQuery(topicArn, roleArn, s3ErrorReportBucketName, databaseName, tableName) {
    console.log("Creating Invalid Scheduled Query");
    const DimensionMappingList = [
        {'Name': 'dim0', 'DimensionValueType': 'VARCHAR'}
    ];

    const mixedMeasureMappingList = [{
        SourceColumn: "random_measure_value",
        MeasureValueType: "DOUBLE"
    }]

    const timestreamConfiguration = {
        DatabaseName: databaseName,
        TableName: tableName,
        TimeColumn: "timestamp",
        DimensionMappings: DimensionMappingList,
        MixedMeasureMappings: mixedMeasureMappingList
    }

    const createScheduleQueryRequest = {
        Name: SQ_NAME,
        QueryString: INVALID_QUERY,
        ScheduleConfiguration: {
            ScheduleExpression: SCHEDULE_EXPRESSION
        },
        NotificationConfiguration: {
            SnsConfiguration: {
                TopicArn: topicArn
            }
        },
        TargetConfiguration: {
            TimestreamConfiguration: timestreamConfiguration
        },
        ScheduledQueryExecutionRoleArn: roleArn,
        ErrorReportConfiguration: {
            S3Configuration: {
                BucketName: s3ErrorReportBucketName
            }
        }
    };

    try {
        const data = await queryClient.createScheduledQuery(createScheduleQueryRequest).promise();
        console.log("Successfully created scheduled query: " + data.Arn);
        return data.Arn;
    } catch (err) {
        console.log("Scheduled Query creation failed: ", err);
        throw err;
    }
}

async function deleteScheduleQuery(scheduledQueryArn) {
    console.log("Deleting Scheduled Query");
    const params = {
        ScheduledQueryArn: scheduledQueryArn
    }
    try {
        await queryClient.deleteScheduledQuery(params).promise();
        console.log("Successfully deleted scheduled query");
    } catch (err) {
        console.log("Scheduled Query deletion failed: ", err);
    }
}

async function listScheduledQueries() {
    console.log("Listing Scheduled Query");
    try {
        var nextToken = null; 
        do {
            var params = {
                MaxResults: 10,
                NextToken: nextToken
            }
            var data = await queryClient.listScheduledQueries(params).promise();
            var scheduledQueryList = data.ScheduledQueries;
            printScheduledQuery(scheduledQueryList);
            nextToken = data.NextToken;
        }
        while (nextToken != null);
    }  catch (err) {
        console.log("List Scheduled Query failed: ", err);
        throw err;
    }
}

async function describeScheduledQuery(scheduledQueryArn) {
    console.log("Describing Scheduled Query");
    var params = {
        ScheduledQueryArn: scheduledQueryArn
    }
    try {
        const data = await queryClient.describeScheduledQuery(params).promise();
        console.log(data.ScheduledQuery);
    } catch (err) {
        console.log("Describe Scheduled Query failed: ", err);
        throw err;
    }
}

async function executeScheduledQuery(scheduledQueryArn, invocationTime) {
    console.log("Executing Scheduled Query");
    var params = {
        ScheduledQueryArn: scheduledQueryArn,
        InvocationTime: invocationTime
    }
    try {
        await queryClient.executeScheduledQuery(params).promise();
    } catch (err) {
        console.log("Execute Scheduled Query failed: ", err);
        throw err;
    }
}

async function updateScheduledQueries(scheduledQueryArn) {
    console.log("Updating Scheduled Query");
    var params = {
        ScheduledQueryArn: scheduledQueryArn,
        State: "DISABLED"
    }
    try {
        await queryClient.updateScheduledQuery(params).promise();
        console.log("Successfully update scheduled query state");
    } catch (err) {
        console.log("Update Scheduled Query failed: ", err);
        throw err;
    }
}

async function printScheduledQuery(scheduledQueryList) {
    scheduledQueryList.forEach(element => console.log(element.Arn));
}

async function parseS3ErrorReport(s3ErrorReportBucketName, errorReportPrefix) {
    var s3Objects = await errorReportHelper.listReportFilesInS3(s3ErrorReportBucketName, errorReportPrefix);
    var errorReport = await errorReportHelper.readErrorReport(s3Objects[0], s3ErrorReportBucketName);
    console.log("Error report from S3: " + errorReport);
    // Since we expect only one error, print the first one
    errorReportJson = JSON.parse(errorReport);
    console.log("Error reason from S3: " + errorReportJson["errors"][0]["reason"]);
}

module.exports = {createScheduledQuery, createInvalidScheduleQuery, deleteScheduleQuery, listScheduledQueries,
     describeScheduledQuery, executeScheduledQuery, updateScheduledQueries, parseS3ErrorReport};