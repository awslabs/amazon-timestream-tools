package com.amazonaws.services.timestream;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.model.CreateScheduledQueryRequest;
import com.amazonaws.services.timestreamquery.model.CreateScheduledQueryResult;
import com.amazonaws.services.timestreamquery.model.DeleteScheduledQueryRequest;
import com.amazonaws.services.timestreamquery.model.DescribeScheduledQueryRequest;
import com.amazonaws.services.timestreamquery.model.DescribeScheduledQueryResult;
import com.amazonaws.services.timestreamquery.model.DimensionMapping;
import com.amazonaws.services.timestreamquery.model.ErrorReportConfiguration;
import com.amazonaws.services.timestreamquery.model.ExecuteScheduledQueryRequest;
import com.amazonaws.services.timestreamquery.model.ExecuteScheduledQueryResult;
import com.amazonaws.services.timestreamquery.model.ListScheduledQueriesRequest;
import com.amazonaws.services.timestreamquery.model.ListScheduledQueriesResult;
import com.amazonaws.services.timestreamquery.model.MeasureValueType;
import com.amazonaws.services.timestreamquery.model.MixedMeasureMapping;
import com.amazonaws.services.timestreamquery.model.MultiMeasureAttributeMapping;
import com.amazonaws.services.timestreamquery.model.MultiMeasureMappings;
import com.amazonaws.services.timestreamquery.model.NotificationConfiguration;
import com.amazonaws.services.timestreamquery.model.ResourceNotFoundException;
import com.amazonaws.services.timestreamquery.model.S3Configuration;
import com.amazonaws.services.timestreamquery.model.ScheduleConfiguration;
import com.amazonaws.services.timestreamquery.model.ScheduledQuery;
import com.amazonaws.services.timestreamquery.model.ScheduledQueryState;
import com.amazonaws.services.timestreamquery.model.SnsConfiguration;
import com.amazonaws.services.timestreamquery.model.TargetConfiguration;
import com.amazonaws.services.timestreamquery.model.TimestreamConfiguration;
import com.amazonaws.services.timestreamquery.model.UpdateScheduledQueryRequest;
import com.google.gson.JsonObject;

import static com.amazonaws.services.timestream.CrudAndSimpleIngestionExample.HT_TTL_HOURS;
import static com.amazonaws.services.timestream.Main.DATABASE_NAME;
import static com.amazonaws.services.timestream.Main.TABLE_NAME;
import static com.amazonaws.services.timestreamquery.model.MeasureValueType.DOUBLE;

public class ScheduledQueryExample {
    private AmazonTimestreamQuery queryClient;
    TimestreamDependencyHelper timestreamDependencyHelper;

    public ScheduledQueryExample(AmazonTimestreamQuery queryClient, TimestreamDependencyHelper timestreamDependencyHelper) {
        this.queryClient = queryClient;
        this.timestreamDependencyHelper = timestreamDependencyHelper;
    }

    private final static String HOSTNAME = "host-24Gju";
    private final static String SQ_NAME = "daily-sample";
    private static final String SCHEDULE_EXPRESSION = "cron(0/2 * * * ? *)";

    // A Query with a column `timestamp` containing a date that is in the future.
    // The Query as such is valid, but when the result of the query is used as a timeColumn for ingesting into a
    // destination Table, it would fail if the time-range is outside table's retention window (MemoryStoreRetentionPeriodInHours).
    public static String INVALID_QUERY = String.format("SELECT cast('%s' as TIMESTAMP) as timestamp, 1.0 as random_measure_value, " +
            "'1' as dim0", LocalDate.now().plusDays(TimeUnit.HOURS.toDays(HT_TTL_HOURS) + 1));

    // Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
    public static String VALID_QUERY = "SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, " +
            "ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization, " +
            "ROUND(APPROX_PERCENTILE(cpu_utilization, 0.9), 2) AS p90_cpu_utilization, " +
            "ROUND(APPROX_PERCENTILE(cpu_utilization, 0.95), 2) AS p95_cpu_utilization, " +
            "ROUND(APPROX_PERCENTILE(cpu_utilization, 0.99), 2) AS p99_cpu_utilization " +
            "FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "WHERE measure_name = 'metrics' " +
            "AND hostname = '" + HOSTNAME + "' " +
            "AND time > ago(2h) " +
            "GROUP BY region, hostname, az, BIN(time, 15s) " +
            "ORDER BY binned_timestamp ASC " +
            "LIMIT 5";

    private String createScheduledQuery(String topicArn, String roleArn, String s3ErrorReportBucketName, String query, TargetConfiguration targetConfiguration) {
        System.out.println("Creating Scheduled Query");

        CreateScheduledQueryRequest createScheduledQueryRequest = new CreateScheduledQueryRequest()
                .withName(SQ_NAME)
                .withQueryString(query)
                .withScheduleConfiguration(new ScheduleConfiguration()
                        .withScheduleExpression(SCHEDULE_EXPRESSION))
                .withNotificationConfiguration(new NotificationConfiguration()
                        .withSnsConfiguration(new SnsConfiguration()
                                .withTopicArn(topicArn)))
                .withNotificationConfiguration(new NotificationConfiguration()
                        .withSnsConfiguration(new SnsConfiguration()
                                .withTopicArn(topicArn)))
                .withTargetConfiguration(targetConfiguration)
                .withErrorReportConfiguration(new ErrorReportConfiguration()
                        .withS3Configuration(new S3Configuration()
                                .withBucketName(s3ErrorReportBucketName)))
                .withScheduledQueryExecutionRoleArn(roleArn);

        try {
            final CreateScheduledQueryResult createScheduledQueryResult = queryClient.createScheduledQuery(createScheduledQueryRequest);
            final String scheduledQueryArn = createScheduledQueryResult.getArn();
            System.out.println("Successfully created scheduled query : " + scheduledQueryArn);
            return scheduledQueryArn;
        }
        catch (Exception e) {
            System.out.println("Scheduled Query creation failed: " + e);
            throw e;
        }
    }

    public String createValidScheduledQuery(String topicArn, String roleArn, String databaseName, String tableName, String s3ErrorReportBucketName) {
        List<Pair<String, MeasureValueType>> sourceColToMeasureValueTypes = Arrays.asList(
            Pair.of("avg_cpu_utilization", DOUBLE),
            Pair.of("p90_cpu_utilization", DOUBLE),
            Pair.of("p95_cpu_utilization", DOUBLE),
            Pair.of("p99_cpu_utilization", DOUBLE));

        TargetConfiguration targetConfiguration = new TargetConfiguration()
                .withTimestreamConfiguration(new TimestreamConfiguration()
                .withDatabaseName(databaseName)
                .withTableName(tableName)
                .withTimeColumn("binned_timestamp")
                .withDimensionMappings(Arrays.asList(
                        new DimensionMapping()
                                .withName("region")
                                .withDimensionValueType("VARCHAR"),
                        new DimensionMapping()
                                .withName("az")
                                .withDimensionValueType("VARCHAR"),
                        new DimensionMapping()
                                .withName("hostname")
                                .withDimensionValueType("VARCHAR")
                ))
                .withMultiMeasureMappings(new MultiMeasureMappings()
                        .withTargetMultiMeasureName("multi-metrics")
                        .withMultiMeasureAttributeMappings(
                                sourceColToMeasureValueTypes.stream()
                                        .map(pair -> new MultiMeasureAttributeMapping()
                                                .withMeasureValueType(pair.getValue().name())
                                                .withSourceColumn(pair.getKey()))
                                        .collect(Collectors.toList()))));
        return createScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName, VALID_QUERY, targetConfiguration);
    }

    public String createInvalidScheduledQuery(String topicArn, String roleArn,
            String databaseName, String tableName, String s3ErrorReportBucketName) {

        TargetConfiguration targetConfiguration = new TargetConfiguration()
                .withTimestreamConfiguration(new TimestreamConfiguration()
                        .withDatabaseName(databaseName)
                        .withTableName(tableName)
                        .withTimeColumn("timestamp")
                        .withDimensionMappings(Collections.singletonList(
                                new DimensionMapping()
                                        .withName("dim0")
                                        .withDimensionValueType("VARCHAR")
                        ))
                        .withMixedMeasureMappings(new MixedMeasureMapping()
                                .withSourceColumn("random_measure_value")
                                .withMeasureValueType(DOUBLE)));

        return createScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName, INVALID_QUERY, targetConfiguration);
    }

    public void deleteScheduledQuery(String scheduledQueryArn) {
        System.out.println("Deleting Scheduled Query");

        try {
            queryClient.deleteScheduledQuery(new DeleteScheduledQueryRequest().withScheduledQueryArn(scheduledQueryArn));
            System.out.println("Successfully deleted scheduled query");
        }
        catch (Exception e) {
            System.out.println("Scheduled Query deletion failed: " + e);
            //do not throw exception here, because we want the following cleanups executes
        }
    }

    public void listScheduledQueries() {
        System.out.println("Listing Scheduled Query");
        try {
            String nextToken = null;
            List<String> scheduledQueries = new ArrayList<>();

            do {
                ListScheduledQueriesResult listScheduledQueriesResult =
                        queryClient.listScheduledQueries(new ListScheduledQueriesRequest()
                                .withNextToken(nextToken).withMaxResults(10));
                List<ScheduledQuery> scheduledQueryList = listScheduledQueriesResult.getScheduledQueries();

                printScheduledQuery(scheduledQueryList);
                nextToken = listScheduledQueriesResult.getNextToken();
            } while (nextToken != null);
        }
        catch (Exception e) {
            System.out.println("List Scheduled Query failed: " + e);
            throw e;
        }
    }

    public void describeScheduledQueries(String scheduledQueryArn) {
        System.out.println("Describing Scheduled Query");
        try {
            DescribeScheduledQueryResult describeScheduledQueryResult = queryClient.describeScheduledQuery(new DescribeScheduledQueryRequest().withScheduledQueryArn(scheduledQueryArn));
            System.out.println(describeScheduledQueryResult);
        }
        catch (ResourceNotFoundException e) {
            System.out.println("Scheduled Query doesn't exist");
            throw e;
        }
        catch (Exception e) {
            System.out.println("Describe Scheduled Query failed: " + e);
            throw e;
        }
    }

    public void executeScheduledQuery(String scheduledQueryArn) {
        System.out.println("Executing Scheduled Query");
        try {
            ExecuteScheduledQueryResult executeScheduledQueryResult = queryClient.executeScheduledQuery(new ExecuteScheduledQueryRequest()
                    .withScheduledQueryArn(scheduledQueryArn)
                    .withInvocationTime(new Date())
            );

        }
        catch (ResourceNotFoundException e) {
            System.out.println("Scheduled Query doesn't exist");
            throw e;
        }
        catch (Exception e) {
            System.out.println("Execution Scheduled Query failed: " + e);
            throw e;
        }
    }

    public void updateScheduledQuery(String scheduledQueryArn, ScheduledQueryState scheduledQueryState) {
        System.out.println("Updating Scheduled Query");
        try {
            queryClient.updateScheduledQuery(new UpdateScheduledQueryRequest()
                    .withScheduledQueryArn(scheduledQueryArn)
                    .withState(scheduledQueryState));
            System.out.println("Successfully update scheduled query state");
        }
        catch (ResourceNotFoundException e) {
            System.out.println("Scheduled Query doesn't exist");
            throw e;
        }
        catch (Exception e) {
            System.out.println("Execution Scheduled Query failed: " + e);
            throw e;
        }
    }

    public void printScheduledQuery(List<ScheduledQuery> scheduledQueryList) {
        for (ScheduledQuery scheduledQuery: scheduledQueryList) {
            System.out.println(scheduledQuery.getArn());
        }
    }

    public void parseS3ErrorReport(final AmazonS3 s3Client, final String s3ErrorReportBucketName,
            final String errorReportPrefix) {
        List<S3Object> s3Objects = timestreamDependencyHelper.listObjectsInS3Bucket(s3Client, s3ErrorReportBucketName, errorReportPrefix);
        JsonObject errorReport = timestreamDependencyHelper.getS3ObjectJson(s3Client, s3Objects.get(0), s3ErrorReportBucketName);
        System.out.println("Error report from S3:: \n" + errorReport);
        // Since we expect only one error, print the first one
        System.out.println("Error reason from S3:: " +
                errorReport.get("errors").getAsJsonArray().get(0)
                        .getAsJsonObject().get("reason").getAsString());
    }

    public enum NotificationType {
        SCHEDULED_QUERY_CREATING("SCHEDULED_QUERY_CREATING"), // scheduled query is being created
        SCHEDULED_QUERY_CREATED("SCHEDULED_QUERY_CREATED"), // for successful scheduled query creation
        SCHEDULED_QUERY_UPDATED("SCHEDULED_QUERY_UPDATED"), // for successful scheduled query update
        SCHEDULED_QUERY_DELETED("SCHEDULED_QUERY_DELETED"), // for successful scheduled query deletion
        AUTO_TRIGGER_SUCCESS("AUTO_TRIGGER_SUCCESS"), // for successful completion of scheduled query
        AUTO_TRIGGER_FAILURE("AUTO_TRIGGER_FAILURE"), // for failures of scheduled query
        MANUAL_TRIGGER_SUCCESS("MANUAL_TRIGGER_SUCCESS"), // for successful completion of manual execution
        MANUAL_TRIGGER_FAILURE("MANUAL_TRIGGER_FAILURE"); // for failed manual execution

        String value;

        NotificationType(String value) {
            this.value = value;
        }
    }
}
