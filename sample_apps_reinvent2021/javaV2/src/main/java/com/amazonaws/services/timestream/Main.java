package com.amazonaws.services.timestream;

import java.io.IOException;
import java.time.Duration;

import org.apache.commons.lang3.RandomStringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.ScheduledQueryState;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;

import com.amazonaws.services.timestream.ScheduledQueryExample.NotificationType;
import com.google.gson.JsonObject;

import static com.amazonaws.services.timestream.QueryExample.SELECT_ALL_QUERY;
import static com.amazonaws.services.timestream.TimestreamDependencyHelper.ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX;
import static com.amazonaws.services.timestream.TimestreamDependencyHelper.ERROR_REPORT_LOCATION;
import static com.amazonaws.services.timestream.TimestreamDependencyHelper.MESSAGE;
import static com.amazonaws.services.timestream.TimestreamDependencyHelper.NOTIFICATION_TYPE;
import static com.amazonaws.services.timestream.TimestreamDependencyHelper.OBJECT_KEY;
import static com.amazonaws.services.timestream.TimestreamDependencyHelper.RECEIPT_HANDLE;
import static com.amazonaws.services.timestream.TimestreamDependencyHelper.S3_REPORT_LOCATION;

public class Main {
    public static final String REGION = "us-east-1";
    public static final String DATABASE_NAME = "devops_sample_application_multi";
    public static final String TABLE_NAME = "host_metrics_sample_application_multi";
    public static final String SQ_DATABASE_NAME = "sq_result_database_multi";
    public static final String SQ_TABLE_NAME = "sq_result_table_multi";

    // Use a FIFO queue & FIFO SNS subscription to enforce strict ordering of messages
    public static final String TOPIC = "scheduled_queries_topic.fifo";
    public static final String QUEUE_NAME = "sq_sample_app_queue.fifo";

    public static final String ROLE_NAME = "ScheduledQuerySampleApplicationRole";
    public static final String POLICY_NAME = "SampleApplicationExecutionAccess";

    public static void main(String[] args) throws IOException {
        InputArguments inputArguments = parseArguments(args);
        final String region = inputArguments.region != null ? inputArguments.region : REGION;

        TimestreamWriteClient writeClient = buildWriteClient(region);
        TimestreamQueryClient queryClient = buildQueryClient(region);
        S3Client s3Client = S3Client.builder().region(Region.of(region)).build();



        CrudAndSimpleIngestionExample crudAndSimpleIngestionExample = new CrudAndSimpleIngestionExample(writeClient, s3Client);
        QueryExample queryExample = new QueryExample(queryClient);

        try {
            createAndUpdateDatabaseExamples(crudAndSimpleIngestionExample, inputArguments);

            writeRecordsExample(crudAndSimpleIngestionExample, writeClient, inputArguments);

            queryExamples(queryExample, inputArguments);

            // Run scheduledQueryExamples only if CSV was provided
            if (inputArguments.inputFile != null) {
                scheduledQueryExamples(crudAndSimpleIngestionExample, queryClient, queryExample, s3Client, region);
            }
        } finally {
            // Uncomment the lines below to delete the database/table as a clean-up action
            //crudAndSimpleIngestionExample.deleteTable(TABLE_NAME, DATABASE_NAME);
            //crudAndSimpleIngestionExample.deleteDatabase(DATABASE_NAME);
        }

        System.exit(0);
    }

    private static void scheduledQueryExamples(CrudAndSimpleIngestionExample crudAndSimpleIngestionExample,
            TimestreamQueryClient queryClient, QueryExample queryExample, S3Client s3Client, String region) {

        TimestreamDependencyHelper timestreamDependencyHelper = new TimestreamDependencyHelper();
        ScheduledQueryExample scheduledQueryExample = new ScheduledQueryExample(queryClient);

        Region regionIAM = Region.AWS_GLOBAL;
        IamClient iamClient = IamClient.builder().region(regionIAM).build();
        SnsClient snsClient = SnsClient.builder().region(Region.of(region)).build();
        SqsClient sqsClient = SqsClient.builder().region(Region.of(region)).build();
        String policyArn = null;
        String subscriptionArn = null;
        String topicArn = null;
        String scheduledQueryArn = null;
        String s3ErrorReportBucketName = null;

        try {

            //Create database and table to store scheduled query results
            crudAndSimpleIngestionExample.createDatabase(SQ_DATABASE_NAME);
            crudAndSimpleIngestionExample.createTable(SQ_DATABASE_NAME, SQ_TABLE_NAME);

            //Create sns and sqs for scheduled query
            topicArn = timestreamDependencyHelper.createSNSTopic(snsClient, TOPIC);
            String queueUrl = timestreamDependencyHelper.createSQSQueue(sqsClient, QUEUE_NAME);

            String sqsQueueArn = timestreamDependencyHelper.getQueueArn(sqsClient, queueUrl);

            subscriptionArn = timestreamDependencyHelper.subscribeToSnsTopic(snsClient, topicArn, sqsQueueArn);
            timestreamDependencyHelper.setSqsAccessPolicy(sqsClient, queueUrl, topicArn, sqsQueueArn);

            String roleArn = timestreamDependencyHelper.createIAMRole(iamClient, ROLE_NAME, region);
            policyArn = timestreamDependencyHelper.createIAMPolicy(iamClient, POLICY_NAME);
            timestreamDependencyHelper.attachIAMRolePolicy(iamClient, ROLE_NAME, policyArn);

            // Wait at-least 15s for newly created role to be active
            System.out.println("Waiting 15 seconds for newly created role to become active");
            Thread.sleep(15000);

            // Make the bucket name unique by appending 5 random characters at the end
            s3ErrorReportBucketName =
                    timestreamDependencyHelper.createS3Bucket(s3Client, ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX +
                            RandomStringUtils.randomAlphanumeric(5).toLowerCase());

            //Scheduled Query Activities

            /* Switch between Valid and Invalid Query to test Happy-case and Failure scenarios */
            scheduledQueryArn = scheduledQueryExample.createValidScheduledQuery(topicArn, roleArn, SQ_DATABASE_NAME, SQ_TABLE_NAME, s3ErrorReportBucketName);
            //scheduledQueryArn = scheduledQueryExample.createInvalidScheduledQuery(topicArn, roleArn, SQ_DATABASE_NAME, SQ_TABLE_NAME, s3ErrorReportBucketName);

            scheduledQueryExample.listScheduledQueries();
            scheduledQueryExample.describeScheduledQueries(scheduledQueryArn);

            // Sleep for 65 seconds to let ScheduledQuery run
            System.out.println("Waiting 65 seconds for automatic ScheduledQuery executions & notifications");
            Thread.sleep(65000);

            boolean didQuerySucceedManually = false;
            boolean wasQueryTriggeredAsExpected = false;
            boolean queryFailed = false;
            for (int i = 0; i < 10; i++) {
                JsonObject response = timestreamDependencyHelper.receiveMessage(queueUrl, sqsClient);
                System.out.println(response);
                if (response == null) {
                    continue;
                }

                // Since its a FIFO queue, we need to delete the message to be able to receive further messages
                timestreamDependencyHelper.deleteMessage(queueUrl, sqsClient, response.get(RECEIPT_HANDLE).getAsString());

                switch (NotificationType.valueOf(response.get(NOTIFICATION_TYPE).getAsString())) {
                    case SCHEDULED_QUERY_CREATING:
                        // Scheduled Query is still pending to run.
                        // Fall-through
                    case SCHEDULED_QUERY_CREATED:
                        // Scheduled Query is still pending to run.
                        break;
                    case MANUAL_TRIGGER_SUCCESS:
                        System.out.println("Manual execution of Scheduled Query succeeded");
                        didQuerySucceedManually = true;
                        break;
                    case AUTO_TRIGGER_SUCCESS:
                        System.out.println("Scheduled Query was triggered as expected. Now triggering another run manually");
                        wasQueryTriggeredAsExpected = true;
                        scheduledQueryExample.executeScheduledQuery(scheduledQueryArn);
                        break;
                    case AUTO_TRIGGER_FAILURE:
                        // Fall-through
                    case MANUAL_TRIGGER_FAILURE:
                        queryFailed = true;
                        JsonObject scheduledQueryRunSummary =
                                response.get(MESSAGE).getAsJsonObject()
                                        .get("scheduledQueryRunSummary").getAsJsonObject();

                        System.out.println("Failure Reason from SQS Notification:: " +
                                scheduledQueryRunSummary.get("failureReason").getAsString());

                        if (scheduledQueryRunSummary.has(ERROR_REPORT_LOCATION)) {
                            // Error Notification has Error report associated with it. We can parse it.
                            scheduledQueryExample.parseS3ErrorReport(s3Client, s3ErrorReportBucketName,
                                    scheduledQueryRunSummary.get(ERROR_REPORT_LOCATION).getAsJsonObject()
                                            .get(S3_REPORT_LOCATION).getAsJsonObject()
                                            .get(OBJECT_KEY).getAsString());
                        }
                        break;
                    case SCHEDULED_QUERY_DELETED:
                        // Fall-through
                    case SCHEDULED_QUERY_UPDATED:
                        // Fall-through
                    default:
                        System.out.println("Received an unexpected message:: " + response);
                }

                if ((didQuerySucceedManually && wasQueryTriggeredAsExpected) || queryFailed) {
                    break;
                }
            }

            if (wasQueryTriggeredAsExpected || didQuerySucceedManually) {
                System.out.println("Fetching Scheduled Query execution results");
                queryExample.runQuery("SELECT * FROM " + SQ_DATABASE_NAME + "." + SQ_TABLE_NAME);
            }

            scheduledQueryExample.updateScheduledQuery(scheduledQueryArn, ScheduledQueryState.DISABLED);
        } catch (AwsServiceException e) {
            System.out.println(e.awsErrorDetails().errorMessage());
            throw e;
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            //Clean up for scheduled query
            if (scheduledQueryArn != null) {
                scheduledQueryExample.deleteScheduledQuery(scheduledQueryArn);
            }

            if (policyArn != null) {
                timestreamDependencyHelper.detachPolicy(iamClient, ROLE_NAME, policyArn);
                timestreamDependencyHelper.deleteIAMPolicy(iamClient, policyArn);
                timestreamDependencyHelper.deleteIAMRole(iamClient, ROLE_NAME);
            }

            if (subscriptionArn != null) {
                timestreamDependencyHelper.unsubscribeFromSnsTopic(subscriptionArn, snsClient);
                timestreamDependencyHelper.deleteSQSQueue(sqsClient, QUEUE_NAME);
            }


            if (topicArn != null) {
                timestreamDependencyHelper.deleteSNSTopic(snsClient, topicArn);
            }

            if (s3ErrorReportBucketName != null) {
                timestreamDependencyHelper.deleteS3Bucket(s3Client, s3ErrorReportBucketName);
            }

            crudAndSimpleIngestionExample.deleteTable(SQ_TABLE_NAME, SQ_DATABASE_NAME);
            crudAndSimpleIngestionExample.deleteDatabase(SQ_DATABASE_NAME);
        }
    }

    private static void queryExamples(QueryExample queryExample, InputArguments inputArguments) {
        if (inputArguments.inputFile != null) {
            // Query samples
            queryExample.runAllQueries();
            // Try cancelling a query
            queryExample.cancelQuery();
            // Run a query with Multiple pages
            queryExample.runQueryWithMultiplePages(20000);
        } else {
            System.out.println("Running the SELECT ALL query");
            queryExample.runQuery(SELECT_ALL_QUERY + " LIMIT 10");
        }
    }

    private static void writeRecordsExample(CrudAndSimpleIngestionExample crudAndSimpleIngestionExample,
            TimestreamWriteClient writeClient, InputArguments inputArguments) throws IOException {

        MultiValueAttributeExample multiValueAttributeExample = new MultiValueAttributeExample(writeClient);
        CsvIngestionExample csvIngestionExample = new CsvIngestionExample(writeClient);
        // multi value attribute
        multiValueAttributeExample.writeRecordsMultiMeasureValueSingleRecord();
        multiValueAttributeExample.writeRecordsMultiMeasureValueMultipleRecords();

        if(inputArguments.inputFile != null) {
            // Bulk record ingestion for bootstrapping a table with fresh data
            csvIngestionExample.bulkWriteRecords(inputArguments.inputFile);
        }
    }

    private static void createAndUpdateDatabaseExamples(CrudAndSimpleIngestionExample crudAndSimpleIngestionExample,
            InputArguments inputArguments) {
        crudAndSimpleIngestionExample.createDatabase();
        crudAndSimpleIngestionExample.describeDatabase();
        crudAndSimpleIngestionExample.listDatabases();
        crudAndSimpleIngestionExample.updateDatabase(inputArguments.kmsId);

        crudAndSimpleIngestionExample.createTable();
        crudAndSimpleIngestionExample.describeTable();
        crudAndSimpleIngestionExample.listTables();
        crudAndSimpleIngestionExample.updateTable();
    }

    private static InputArguments parseArguments(String[] args) {
        InputArguments inputArguments = new InputArguments();
        final CmdLineParser parser = new CmdLineParser(inputArguments);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
            System.exit(1);
        }

        return inputArguments;
    }

    /**
     * Recommended Timestream write client SDK configuration:
     *  - Set SDK retry count to 10.
     *  - Use SDK DEFAULT_BACKOFF_STRATEGY
     *  - Set RequestTimeout to 20 seconds .
     *  - Set max connections to 5000 or higher.
     */
    private static TimestreamWriteClient buildWriteClient(String region) {
        ApacheHttpClient.Builder httpClientBuilder =
                ApacheHttpClient.builder();
        httpClientBuilder.maxConnections(5000);

        RetryPolicy.Builder retryPolicy =
                RetryPolicy.builder();
        retryPolicy.numRetries(10);

        ClientOverrideConfiguration.Builder overrideConfig =
                ClientOverrideConfiguration.builder();
        overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(20));
        overrideConfig.retryPolicy(retryPolicy.build());

        return TimestreamWriteClient.builder()
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(overrideConfig.build())
                .region(Region.of(region))
                .build();
    }

    private static TimestreamQueryClient buildQueryClient(String region) {
        return TimestreamQueryClient.builder()
                .region(Region.of(region))
                .build();
    }
}
