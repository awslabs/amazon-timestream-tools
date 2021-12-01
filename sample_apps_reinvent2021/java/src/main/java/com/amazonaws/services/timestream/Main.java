package com.amazonaws.services.timestream;

import java.io.IOException;

import org.apache.commons.lang3.RandomStringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.SqsClient;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQueryClient;
import com.amazonaws.services.timestreamquery.model.ScheduledQueryState;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWrite;
import com.amazonaws.services.timestreamwrite.AmazonTimestreamWriteClientBuilder;
import com.google.gson.JsonObject;

import static com.amazonaws.services.timestream.QueryExample.SELECT_ALL_QUERY;
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
    public static final String TOPIC = "scheduled_queries_topic";
    public static final String QUEUE_NAME = "sq_sample_app_queue";
    public static final String ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX = "error-configuration-sample-s3-bucket-";
    public static final String ROLE_NAME = "ScheduledQuerySampleApplicationRole";
    public static final String POLICY_NAME = "SampleApplicationExecutionAccess";
    private static AmazonS3 s3Client;

    public static void main(String[] args) throws IOException {
        InputArguments inputArguments = parseArguments(args);
        final String region = inputArguments.region != null ? inputArguments.region : REGION;
        final AmazonTimestreamWrite writeClient = buildWriteClient(region);
        final AmazonTimestreamQuery queryClient = buildQueryClient(region);
        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(region).build();

        TimestreamDependencyHelper timestreamDependencyHelper = new TimestreamDependencyHelper();
        CrudAndSimpleIngestionExample crudAndSimpleIngestionExample = new CrudAndSimpleIngestionExample(writeClient, timestreamDependencyHelper);
        CsvIngestionExample csvIngestionExample = new CsvIngestionExample(writeClient);
        QueryExample queryExample = new QueryExample(queryClient);
        MultiValueAttributeExample mvaExample = new MultiValueAttributeExample(writeClient);
        ScheduledQueryExample scheduledQueryExample = new ScheduledQueryExample(queryClient, timestreamDependencyHelper);

        String s3ErrorReportBucketName = null;

        try {
            // Make the bucket name unique by appending 5 random characters at the end
            s3ErrorReportBucketName =
                    timestreamDependencyHelper.createS3Bucket(s3Client, ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX +
                            RandomStringUtils.randomAlphanumeric(5).toLowerCase());

            createAndUpdateDatabaseExamples(crudAndSimpleIngestionExample, inputArguments, s3ErrorReportBucketName);

            writeRecordsExample(mvaExample, csvIngestionExample, inputArguments);

            queryExamples(queryExample, inputArguments);

            // Run scheduledQueryExamples only if CSV was provided
            if (inputArguments.inputFile != null) {
                scheduledQueryExamples(crudAndSimpleIngestionExample, scheduledQueryExample, queryExample,
                        timestreamDependencyHelper, s3ErrorReportBucketName, region);
            }
        } finally {
            if (s3ErrorReportBucketName != null) {
                timestreamDependencyHelper.deleteS3Bucket(s3Client, s3ErrorReportBucketName);
            }

            // Uncomment the lines below to delete the database/table as a clean-up action
            //crudAndSimpleIngestionExample.deleteTable(TABLE_NAME, DATABASE_NAME);
            //crudAndSimpleIngestionExample.deleteDatabase(DATABASE_NAME);
        }

        System.exit(0);
    }

    private static void queryExamples(QueryExample queryExample, InputArguments inputArguments) {
        // Run all queries and cancelQuery only if CSV was provided
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

    private static void writeRecordsExample(MultiValueAttributeExample mvaExample,
            CsvIngestionExample csvIngestionExample,
            InputArguments inputArguments) throws IOException {
        mvaExample.writeRecordsMultiMeasureValueSingleRecord();
        mvaExample.writeRecordsMultiMeasureValueMultipleRecords();

        if (inputArguments.inputFile != null) {
            // CsvIngestionExample takes a MULTI_MEASURE Sample as input
            csvIngestionExample.bulkWriteRecords(inputArguments.inputFile);
        }
    }

    private static void createAndUpdateDatabaseExamples(CrudAndSimpleIngestionExample crudAndSimpleIngestionExample,
            InputArguments inputArguments, String s3ErrorReportBucketName) {
        crudAndSimpleIngestionExample.createDatabase(DATABASE_NAME);
        crudAndSimpleIngestionExample.updateDatabase(inputArguments.kmsId);
        crudAndSimpleIngestionExample.describeDatabase();
        crudAndSimpleIngestionExample.listDatabases();

        crudAndSimpleIngestionExample.createTable(DATABASE_NAME, TABLE_NAME, s3ErrorReportBucketName);
        crudAndSimpleIngestionExample.describeTable();
        crudAndSimpleIngestionExample.listTables();
        crudAndSimpleIngestionExample.updateTable();

    }

    public static void scheduledQueryExamples(CrudAndSimpleIngestionExample crudAndSimpleIngestionExample,
            ScheduledQueryExample scheduledQueryExample,
            QueryExample queryExample,
            TimestreamDependencyHelper timestreamDependencyHelper,
            String s3ErrorReportBucketName,
            String region) {
        Region regionIAM = Region.AWS_GLOBAL;
        IamClient iamClient = IamClient.builder().region(regionIAM).build();
        SnsClient snsClient = SnsClient.builder().region(Region.of(region)).build();
        SqsClient sqsClient = SqsClient.builder().region(Region.of(region)).build();
        String policyArn = null;
        String subscriptionArn = null;
        String topicArn = null;
        String scheduledQueryArn = null;

        try {
            //Create database and table to store scheduled query results
            crudAndSimpleIngestionExample.createDatabase(SQ_DATABASE_NAME);
            crudAndSimpleIngestionExample.createTable(SQ_DATABASE_NAME, SQ_TABLE_NAME, s3ErrorReportBucketName);

            //Create sns and sqs for scheduled query
            topicArn = timestreamDependencyHelper.createSNSTopic(snsClient, TOPIC);
            String queueUrl = timestreamDependencyHelper.createSQSQueue(sqsClient, QUEUE_NAME);
            //Need to wait atleast 1s after creating queue to use it
            wait(2);

            String queue_arn = timestreamDependencyHelper.getQueueArn(sqsClient, queueUrl);

            subscriptionArn = timestreamDependencyHelper.subscribeToSnsTopic(snsClient, topicArn, queue_arn);
            timestreamDependencyHelper.setSqsAccessPolicy(sqsClient, queueUrl, topicArn, queue_arn);

            String roleArn = TimestreamDependencyHelper.createIAMRole(iamClient, ROLE_NAME, region);
            policyArn = TimestreamDependencyHelper.createIAMPolicy(iamClient, POLICY_NAME);
            TimestreamDependencyHelper.attachIAMRolePolicy(iamClient, ROLE_NAME, policyArn);

            //Waiting for newly created role to be active
            System.out.println("Waiting 15 seconds for newly created role to become active");
            wait(15);

            //Scheduled Query Activities

            /* Switch between Valid and Invalid Query to test Happy-case and Failure scenarios */
            scheduledQueryArn = scheduledQueryExample.createValidScheduledQuery(topicArn, roleArn, SQ_DATABASE_NAME, SQ_TABLE_NAME, s3ErrorReportBucketName);
            // scheduledQueryArn = scheduledQueryExample.createInvalidScheduledQuery(topicArn, roleArn, SQ_DATABASE_NAME, SQ_TABLE_NAME, s3ErrorReportBucketName);

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
                System.out.println("Response: " + response);
                if (response == null) {
                    continue;
                }

                // Since its a FIFO queue, we need to delete the message to be able to receive further messages
                timestreamDependencyHelper.deleteMessage(queueUrl, sqsClient, response.get(RECEIPT_HANDLE).getAsString());

                switch (ScheduledQueryExample.NotificationType.valueOf(response.get(NOTIFICATION_TYPE).getAsString())) {
                    case SCHEDULED_QUERY_CREATING:
                        // Scheduled Query is still pending to run.
                        // Fall-through
                    case SCHEDULED_QUERY_CREATED:
                        // Scheduled Query is still pending to run.
                        // Fall-through
                    case SCHEDULED_QUERY_DELETED:
                        // Fall-through
                    case SCHEDULED_QUERY_UPDATED:
                        // Fall-through
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
        }  finally {
            //Clean up for scheduled query
            if (scheduledQueryArn != null) {
                scheduledQueryExample.deleteScheduledQuery(scheduledQueryArn);
            }

            if (policyArn != null) {
                TimestreamDependencyHelper.detachPolicy(iamClient, ROLE_NAME, policyArn);
                TimestreamDependencyHelper.deleteIAMPolicy(iamClient, policyArn);
                TimestreamDependencyHelper.deleteIAMRole(iamClient, ROLE_NAME);
            }

            if (subscriptionArn != null) {
                timestreamDependencyHelper.unsubscribeFromSnsTopic(subscriptionArn, snsClient);
                TimestreamDependencyHelper.deleteSQSQueue(sqsClient, QUEUE_NAME);
            }


            if (topicArn != null) {
                timestreamDependencyHelper.deleteSNSTopic(snsClient, topicArn);
            }

            crudAndSimpleIngestionExample.deleteTable(SQ_TABLE_NAME, SQ_DATABASE_NAME);
            crudAndSimpleIngestionExample.deleteDatabase(SQ_DATABASE_NAME);
        }
    }


    private static InputArguments parseArguments(String[] args) {
        InputArguments inputArguments = new InputArguments();
        final CmdLineParser parser = new CmdLineParser(inputArguments);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.out.println(e.getMessage());
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

    private static AmazonTimestreamWrite buildWriteClient(String region) {
        final ClientConfiguration clientConfiguration = new ClientConfiguration()
                .withMaxConnections(5000)
                .withRequestTimeout(20 * 1000)
                .withMaxErrorRetry(10);

        return AmazonTimestreamWriteClientBuilder
                .standard()
                .withRegion(region)
                .withClientConfiguration(clientConfiguration)
                .build();
    }

    private static AmazonTimestreamQuery buildQueryClient(String region) {
        return AmazonTimestreamQueryClient.builder()
                .withRegion(region)
                .build();
    }

    public static void wait(int duration) throws InterruptedException {
        java.util.concurrent.TimeUnit.SECONDS.sleep(duration);
    }
}
