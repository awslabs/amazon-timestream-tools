package com.amazonaws.services.timestream;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.timestreamquery.TimestreamQueryClient;
import software.amazon.awssdk.services.timestreamquery.model.CreateScheduledQueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.CreateScheduledQueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.DeleteScheduledQueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.DescribeScheduledQueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.DescribeScheduledQueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.DimensionMapping;
import software.amazon.awssdk.services.timestreamquery.model.ErrorReportConfiguration;
import software.amazon.awssdk.services.timestreamquery.model.ExecuteScheduledQueryRequest;
import software.amazon.awssdk.services.timestreamquery.model.ExecuteScheduledQueryResponse;
import software.amazon.awssdk.services.timestreamquery.model.ListScheduledQueriesRequest;
import software.amazon.awssdk.services.timestreamquery.model.ListScheduledQueriesResponse;
import software.amazon.awssdk.services.timestreamquery.model.MeasureValueType;
import software.amazon.awssdk.services.timestreamquery.model.MixedMeasureMapping;
import software.amazon.awssdk.services.timestreamquery.model.MultiMeasureAttributeMapping;
import software.amazon.awssdk.services.timestreamquery.model.MultiMeasureMappings;
import software.amazon.awssdk.services.timestreamquery.model.NotificationConfiguration;
import software.amazon.awssdk.services.timestreamquery.model.ResourceNotFoundException;
import software.amazon.awssdk.services.timestreamquery.model.S3Configuration;
import software.amazon.awssdk.services.timestreamquery.model.ScheduleConfiguration;
import software.amazon.awssdk.services.timestreamquery.model.ScheduledQuery;
import software.amazon.awssdk.services.timestreamquery.model.ScheduledQueryState;
import software.amazon.awssdk.services.timestreamquery.model.SnsConfiguration;
import software.amazon.awssdk.services.timestreamquery.model.TargetConfiguration;
import software.amazon.awssdk.services.timestreamquery.model.TimestreamConfiguration;
import software.amazon.awssdk.services.timestreamquery.model.UpdateScheduledQueryRequest;

import com.amazonaws.services.timestream.utils.ErrorReportHelper;
import com.google.gson.JsonObject;

import static com.amazonaws.services.timestream.CrudAndSimpleIngestionExample.HT_TTL_HOURS;
import static com.amazonaws.services.timestream.Main.DATABASE_NAME;
import static com.amazonaws.services.timestream.Main.TABLE_NAME;
import static software.amazon.awssdk.services.timestreamquery.model.MeasureValueType.DOUBLE;

public class ScheduledQueryExample {
    private final TimestreamQueryClient queryClient;
    public static final String SCHEDULED_QUERY_EXAMPLE = "ScheduledQuerySample";

    public ScheduledQueryExample(TimestreamQueryClient queryClient) {
        this.queryClient = queryClient;
    }

    private final static String HOSTNAME = "host-24Gju";
    private final static String SQ_NAME = "daily-sample";
    /**
     * Timestream ScheduledQuery supports both Fixed Rate expressions and Cron Expressions.
     * 1. SUPPORTED FIXED RATE FORMAT
     *      Syntax: rate(value unit)
     *      - value : A positive number.
     *      - unit : The unit of time. Valid values: minute | minutes | hour | hours | day | days
     *      Examples: rate(1minute), rate(5 day), rate(1 hours)
     * 2. CRON EXPRESSION FORMAT
     *     Cron expressions are comprised of 5 required fields and one optional field
     *     separated by white space. More details: https://tinyurl.com/rxe5zkff
     */
    private static final String SCHEDULE_EXPRESSION = "cron(0/1 * * * ? *)";

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

    private String createScheduledQuery(String topicArn, String roleArn,
            String s3ErrorReportBucketName, String query, TargetConfiguration targetConfiguration) {
        System.out.println("Creating Scheduled Query");

        CreateScheduledQueryRequest createScheduledQueryRequest = CreateScheduledQueryRequest.builder()
                .name(SQ_NAME)
                .queryString(query)
                .scheduleConfiguration(ScheduleConfiguration.builder()
                        .scheduleExpression(SCHEDULE_EXPRESSION)
                        .build())
                .notificationConfiguration(NotificationConfiguration.builder()
                        .snsConfiguration(SnsConfiguration.builder()
                                .topicArn(topicArn)
                                .build())
                        .build())
                .targetConfiguration(targetConfiguration)
                .errorReportConfiguration(ErrorReportConfiguration.builder()
                        .s3Configuration(S3Configuration.builder()
                                .bucketName(s3ErrorReportBucketName)
                                .objectKeyPrefix(SCHEDULED_QUERY_EXAMPLE)
                                .build())
                        .build())
                .scheduledQueryExecutionRoleArn(roleArn)
                .build();

        try {
            final CreateScheduledQueryResponse response = queryClient.createScheduledQuery(createScheduledQueryRequest);
            final String scheduledQueryArn = response.arn();
            System.out.println("Successfully created scheduled query : " + scheduledQueryArn);
            return scheduledQueryArn;
        }
        catch (Exception e) {
            System.out.println("Scheduled Query creation failed: " + e);
            throw e;
        }
    }

    public String createValidScheduledQuery(String topicArn, String roleArn,
            String databaseName, String tableName, String s3ErrorReportBucketName) {
        List<Pair<String, MeasureValueType>> sourceColToMeasureValueTypes = Arrays.asList(
                Pair.of("avg_cpu_utilization", DOUBLE),
                Pair.of("p90_cpu_utilization", DOUBLE),
                Pair.of("p95_cpu_utilization", DOUBLE),
                Pair.of("p99_cpu_utilization", DOUBLE));

        TargetConfiguration targetConfiguration = TargetConfiguration.builder()
                .timestreamConfiguration(TimestreamConfiguration.builder()
                .databaseName(databaseName)
                .tableName(tableName)
                .timeColumn("binned_timestamp")
                .dimensionMappings(Arrays.asList(
                        DimensionMapping.builder()
                                .name("region")
                                .dimensionValueType("VARCHAR")
                                .build(),
                        DimensionMapping.builder()
                                .name("az")
                                .dimensionValueType("VARCHAR")
                                .build(),
                        DimensionMapping.builder()
                                .name("hostname")
                                .dimensionValueType("VARCHAR")
                                .build()
                ))
                .multiMeasureMappings(MultiMeasureMappings.builder()
                        .targetMultiMeasureName("multi-metrics")
                        .multiMeasureAttributeMappings(
                                sourceColToMeasureValueTypes.stream()
                                        .map(pair -> MultiMeasureAttributeMapping.builder()
                                                .measureValueType(pair.getValue().name())
                                                .sourceColumn(pair.getKey())
                                                .build())
                                        .collect(Collectors.toList()))
                        .build())
                .build())
                .build();

        return createScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName, VALID_QUERY, targetConfiguration);
    }

    public String createInvalidScheduledQuery(String topicArn, String roleArn,
            String databaseName, String tableName, String s3ErrorReportBucketName) {

        TargetConfiguration targetConfiguration = TargetConfiguration.builder()
                .timestreamConfiguration(TimestreamConfiguration.builder()
                        .databaseName(databaseName)
                        .tableName(tableName)
                        .timeColumn("timestamp")
                        .dimensionMappings(Collections.singletonList(
                                DimensionMapping.builder()
                                        .name("dim0")
                                        .dimensionValueType("VARCHAR")
                                        .build()
                        ))
                        .mixedMeasureMappings(MixedMeasureMapping.builder()
                                .sourceColumn("random_measure_value")
                                .measureValueType(DOUBLE)
                                .build())
                        .build())
                .build();

        return createScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName, INVALID_QUERY, targetConfiguration);
    }

    public void deleteScheduledQuery(String scheduledQueryArn) {
        System.out.println("Deleting Scheduled Query");

        try {
            queryClient.deleteScheduledQuery(DeleteScheduledQueryRequest.builder()
                    .scheduledQueryArn(scheduledQueryArn).build());
            System.out.println("Successfully deleted scheduled query");
        }
        catch (Exception e) {
            System.out.println("Scheduled Query deletion failed: " + e);
            //do not throw exception here, because we want the following cleanups executes
        }
    }

    public void listScheduledQueries() {
        System.out.println("Listing Scheduled Queries");
        try {
            String nextToken = null;

            do {
                ListScheduledQueriesResponse listScheduledQueriesResult =
                        queryClient.listScheduledQueries(ListScheduledQueriesRequest.builder()
                                .nextToken(nextToken).maxResults(10)
                                .build());
                List<ScheduledQuery> scheduledQueryList = listScheduledQueriesResult.scheduledQueries();

                printScheduledQuery(scheduledQueryList);
                nextToken = listScheduledQueriesResult.nextToken();
            } while (nextToken != null);
        }
        catch (Exception e) {
            System.out.println("List Scheduled Queries failed: " + e);
            throw e;
        }
    }

    public void describeScheduledQueries(String scheduledQueryArn) {
        System.out.println("Describing Scheduled Query");
        try {
            DescribeScheduledQueryResponse describeScheduledQueryResult =
                    queryClient.describeScheduledQuery(DescribeScheduledQueryRequest.builder()
                            .scheduledQueryArn(scheduledQueryArn)
                            .build());
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
        System.out.println("Running Scheduled Query");
        try {
            ExecuteScheduledQueryResponse executeScheduledQueryResult = queryClient.executeScheduledQuery(ExecuteScheduledQueryRequest.builder()
                    .scheduledQueryArn(scheduledQueryArn)
                    .invocationTime(Instant.now())
                    .build()
            );

            System.out.println("ScheduledQuery run response code: " + executeScheduledQueryResult.sdkHttpResponse().statusCode());

        }
        catch (ResourceNotFoundException e) {
            System.out.println("Scheduled Query doesn't exist");
            throw e;
        }
        catch (Exception e) {
            System.out.println("Scheduled Query run failed: " + e);
            throw e;
        }
    }

    public void updateScheduledQuery(String scheduledQueryArn, ScheduledQueryState state) {
        System.out.println("Updating Scheduled Query");
        try {
            queryClient.updateScheduledQuery(UpdateScheduledQueryRequest.builder()
                    .scheduledQueryArn(scheduledQueryArn)
                    .state(state)
                    .build());
            System.out.println("Successfully updated scheduled query state");
        }
        catch (ResourceNotFoundException e) {
            System.out.println("Scheduled Query doesn't exist");
            throw e;
        }
        catch (Exception e) {
            System.out.println("Scheduled Query update failed: " + e);
            throw e;
        }
    }

    public void printScheduledQuery(List<ScheduledQuery> scheduledQueryList) {
        for (ScheduledQuery scheduledQuery: scheduledQueryList) {
            System.out.println(scheduledQuery.arn());
        }
    }

    public void parseS3ErrorReport(final S3Client s3Client, final String s3ErrorReportBucketName,
            final String errorReportPrefix) {
        List<S3Object> s3Objects = ErrorReportHelper.listReportFilesInS3(s3Client, s3ErrorReportBucketName, errorReportPrefix);
        JsonObject errorReport = ErrorReportHelper.readErrorReport(s3Client, s3Objects.get(0), s3ErrorReportBucketName);
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
