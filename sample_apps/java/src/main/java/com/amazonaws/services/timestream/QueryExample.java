package com.amazonaws.services.timestream;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.timestreamquery.AmazonTimestreamQuery;
import com.amazonaws.services.timestreamquery.model.CancelQueryRequest;
import com.amazonaws.services.timestreamquery.model.ColumnInfo;
import com.amazonaws.services.timestreamquery.model.Datum;
import com.amazonaws.services.timestreamquery.model.QueryRequest;
import com.amazonaws.services.timestreamquery.model.QueryResult;
import com.amazonaws.services.timestreamquery.model.QueryStatus;
import com.amazonaws.services.timestreamquery.model.Row;
import com.amazonaws.services.timestreamquery.model.ScalarType;
import com.amazonaws.services.timestreamquery.model.TimeSeriesDataPoint;
import com.amazonaws.services.timestreamquery.model.Type;

import static com.amazonaws.services.timestream.Main.DATABASE_NAME;
import static com.amazonaws.services.timestream.Main.TABLE_NAME;

public class QueryExample {
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSSSSS");

    private static final long ONE_GB_IN_BYTES = 1073741824L;

    private AmazonTimestreamQuery queryClient;

    public QueryExample(AmazonTimestreamQuery queryClient) {
        this.queryClient = queryClient;
    }

    private final static String HOSTNAME = "host-24Gju";
    // See records ingested into this table so far
    private static String SELECT_ALL_QUERY = "SELECT * FROM " + DATABASE_NAME + "." + TABLE_NAME;
    //1. Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
    private static String QUERY_1 = "SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, " +
            "    ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization, " +
            "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_cpu_utilization, " +
            "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.95), 2) AS p95_cpu_utilization, " +
            "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_cpu_utilization " +
            "FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "WHERE measure_name = 'cpu_utilization' " +
            "   AND hostname = '" + HOSTNAME + "' " +
            "    AND time > ago(2h) " +
            "GROUP BY region, hostname, az, BIN(time, 15s) " +
            "ORDER BY binned_timestamp ASC";

    //2. Identify EC2 hosts with CPU utilization that is higher by 10%  or more compared to the average CPU utilization of the entire fleet for the past 2 hours.
    private static String QUERY_2 = "WITH avg_fleet_utilization AS ( " +
            "    SELECT COUNT(DISTINCT hostname) AS total_host_count, AVG(measure_value::double) AS fleet_avg_cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "    WHERE measure_name = 'cpu_utilization' " +
            "        AND time > ago(2h) " +
            "), avg_per_host_cpu AS ( " +
            "    SELECT region, az, hostname, AVG(measure_value::double) AS avg_cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "    WHERE measure_name = 'cpu_utilization' " +
            "        AND time > ago(2h) " +
            "    GROUP BY region, az, hostname " +
            ") " +
            "SELECT region, az, hostname, avg_cpu_utilization, fleet_avg_cpu_utilization " +
            "FROM avg_fleet_utilization, avg_per_host_cpu " +
            "WHERE avg_cpu_utilization > 1.1 * fleet_avg_cpu_utilization " +
            "ORDER BY avg_cpu_utilization DESC";

    //3. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours.
    private static String QUERY_3 = "SELECT BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization, " +
            "hostname FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "WHERE measure_name = 'cpu_utilization' " +
            "    AND hostname = '" + HOSTNAME + "' " +
            "    AND time > ago(2h) " +
            "GROUP BY hostname, BIN(time, 30s) " +
            "ORDER BY binned_timestamp ASC";

    //4. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
    private static String QUERY_4 = "WITH binned_timeseries AS ( " +
            "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "    WHERE measure_name = 'cpu_utilization' " +
            "       AND hostname = '" + HOSTNAME + "' " +
            "        AND time > ago(2h) " +
            "    GROUP BY hostname, BIN(time, 30s) " +
            "), interpolated_timeseries AS ( " +
            "    SELECT hostname, " +
            "        INTERPOLATE_LINEAR( " +
            "            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " +
            "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization " +
            "    FROM binned_timeseries " +
            "    GROUP BY hostname " +
            ") " +
            "SELECT time, ROUND(value, 2) AS interpolated_cpu " +
            "FROM interpolated_timeseries " +
            "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)";

    //5. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on the last observation carried forward.
    private static String QUERY_5 = "WITH binned_timeseries AS ( " +
            "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "    WHERE measure_name = 'cpu_utilization' " +
            "        AND hostname = '" + HOSTNAME + "' " +
            "        AND time > ago(2h) " +
            "    GROUP BY hostname, BIN(time, 30s) " +
            "), interpolated_timeseries AS ( " +
            "    SELECT hostname, " +
            "        INTERPOLATE_LOCF( " +
            "            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " +
            "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization " +
            "    FROM binned_timeseries " +
            "    GROUP BY hostname " +
            ") " +
            "SELECT time, ROUND(value, 2) AS interpolated_cpu " +
            "FROM interpolated_timeseries " +
            "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)";

    //6. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on a constant value.
    private static String QUERY_6 = "WITH binned_timeseries AS ( " +
            "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "    WHERE measure_name = 'cpu_utilization' " +
            "       AND hostname = '" + HOSTNAME + "' " +
            "        AND time > ago(2h) " +
            "    GROUP BY hostname, BIN(time, 30s) " +
            "), interpolated_timeseries AS ( " +
            "    SELECT hostname, " +
            "        INTERPOLATE_FILL( " +
            "            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " +
            "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s), 10.0) AS interpolated_avg_cpu_utilization " +
            "    FROM binned_timeseries " +
            "    GROUP BY hostname " +
            ") " +
            "SELECT time, ROUND(value, 2) AS interpolated_cpu " +
            "FROM interpolated_timeseries " +
            "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)";

    //7. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using cubic spline interpolation.
    private static String QUERY_7 = "WITH binned_timeseries AS ( " +
            "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "    WHERE measure_name = 'cpu_utilization' " +
            "        AND hostname = '" + HOSTNAME + "' " +
            "        AND time > ago(2h) " +
            "    GROUP BY hostname, BIN(time, 30s) " +
            "), interpolated_timeseries AS ( " +
            "    SELECT hostname, " +
            "        INTERPOLATE_SPLINE_CUBIC( " +
            "            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " +
            "                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization " +
            "    FROM binned_timeseries " +
            "    GROUP BY hostname " +
            ") " +
            "SELECT time, ROUND(value, 2) AS interpolated_cpu " +
            "FROM interpolated_timeseries " +
            "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)";

    //8. Find the average CPU utilization binned at 30 second intervals for all EC2 hosts over the past 2 hours, filling in the missing values using linear interpolation.
    private static String QUERY_8 = "WITH per_host_min_max_timestamp AS ( " +
            "    SELECT hostname, min(time) as min_timestamp, max(time) as max_timestamp " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "    WHERE measure_name = 'cpu_utilization' " +
            "        AND time > ago(2h) " +
            "    GROUP BY hostname " +
            "), interpolated_timeseries AS ( " +
            "    SELECT m.hostname, " +
            "        INTERPOLATE_LOCF( " +
            "            CREATE_TIME_SERIES(time, measure_value::double), " +
            "                SEQUENCE(MIN(ph.min_timestamp), MAX(ph.max_timestamp), 1s)) as interpolated_avg_cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " m " +
            "        INNER JOIN per_host_min_max_timestamp ph ON m.hostname = ph.hostname " +
            "    WHERE measure_name = 'cpu_utilization' " +
            "        AND time > ago(2h) " +
            "    GROUP BY m.hostname " +
            ") " +
            "SELECT hostname, AVG(cpu_utilization) AS avg_cpu_utilization " +
            "FROM interpolated_timeseries " +
            "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization) AS t (time, cpu_utilization) " +
            "GROUP BY hostname " +
            "ORDER BY avg_cpu_utilization DESC";

    //9. Find the percentage of measurements with CPU utilization above 70% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
    private static String QUERY_9 = "WITH time_series_view AS ( " +
            "    SELECT INTERPOLATE_LINEAR( " +
            "        CREATE_TIME_SERIES(time, ROUND(measure_value::double,2)), " +
            "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "    WHERE hostname = '" + HOSTNAME + "' " +
            "        AND  " +
            "measure_name = 'cpu_utilization' " +
            "        AND time > ago(2h) " +
            "    GROUP BY hostname " +
            ") " +
            "SELECT FILTER(cpu_utilization, x -> x.value > 70.0) AS cpu_above_threshold, " +
            "    REDUCE(FILTER(cpu_utilization, x -> x.value > 70.0), 0, (s, x) -> s + 1, s -> s) AS count_cpu_above_threshold, " +
            "    ROUND(REDUCE(cpu_utilization, CAST(ROW(0, 0) AS ROW(count_high BIGINT, count_total BIGINT)), " +
            "        (s, x) -> CAST(ROW(s.count_high + IF(x.value > 70.0, 1, 0), s.count_total + 1) AS ROW(count_high BIGINT, count_total BIGINT)), " +
            "        s -> IF(s.count_total = 0, NULL, CAST(s.count_high AS DOUBLE) / s.count_total)), 4) AS fraction_cpu_above_threshold " +
            "FROM time_series_view";

    //10. List the measurements with CPU utilization lower than 75% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
    private static String QUERY_10 = "WITH time_series_view AS ( " +
            "    SELECT min(time) AS oldest_time, INTERPOLATE_LINEAR( " +
            "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
            "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "    WHERE  " +
            " hostname = '" + HOSTNAME + "' " +
            "        AND  " +
            "measure_name = 'cpu_utilization' " +
            "        AND time > ago(2h) " +
            "    GROUP BY hostname " +
            ") " +
            "SELECT FILTER(cpu_utilization, x -> x.value < 75 AND x.time > oldest_time + 1m) " +
            "FROM time_series_view";

    //11. Find the total number of measurements with of CPU utilization of 0% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
    private static String QUERY_11 = "WITH time_series_view AS ( " +
            "    SELECT INTERPOLATE_LINEAR( " +
            "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
            "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "     WHERE  " +
            " hostname = '" + HOSTNAME + "' " +
            "        AND  " +
            "measure_name = 'cpu_utilization' " +
            "        AND time > ago(2h) " +
            "    GROUP BY hostname " +
            ") " +
            "SELECT REDUCE(cpu_utilization, " +
            "    DOUBLE '0.0', " +
            "    (s, x) -> s + 1, " +
            "    s -> s) AS count_cpu " +
            "FROM time_series_view";

    //12. Find the average CPU utilization for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
    private static String QUERY_12 = "WITH time_series_view AS ( " +
            "    SELECT INTERPOLATE_LINEAR( " +
            "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
            "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
            "    FROM " +  DATABASE_NAME + "." +  TABLE_NAME + " " +
            "     WHERE  " +
            " hostname = '" + HOSTNAME + "' " +
            "     AND  " +
            "measure_name = 'cpu_utilization' " +
            "        AND time > ago(2h) " +
            "    GROUP BY hostname " +
            ") " +
            "SELECT REDUCE(cpu_utilization, " +
            "    CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), " +
            "    (s, x) -> CAST(ROW(x.value + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)), " +
            "     s -> IF(s.count = 0, NULL, s.sum / s.count)) AS avg_cpu " +
            "FROM time_series_view";

    private static final String[] queries =
            {QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6, QUERY_7, QUERY_8, QUERY_9, QUERY_10, QUERY_11, QUERY_12};

    public void runAllQueries() {
        for (int i = 0; i < queries.length; i++) {
            System.out.println("Running query" + (i + 1) + ": " + queries[i]);
            runQuery(queries[i]);
        }
    }

    private void runQuery(String queryString) {
        try {
            QueryRequest queryRequest = new QueryRequest();
            queryRequest.setQueryString(queryString);
            QueryResult queryResult = queryClient.query(queryRequest);
            while (true) {
                parseQueryResult(queryResult);
                if (queryResult.getNextToken() == null) {
                    break;
                }
                queryRequest.setNextToken(queryResult.getNextToken());
                queryResult = queryClient.query(queryRequest);
            }
        } catch (Exception e) {
            // Some queries might fail with 500 if the result of a sequence function has more than 10000 entries
            e.printStackTrace();
        }
    }

    private void parseQueryResult(QueryResult response) {
        final QueryStatus queryStatus = response.getQueryStatus();

        System.out.println("Query progress so far: " + queryStatus.getProgressPercentage() + "%");

        double bytesScannedSoFar = (double) queryStatus.getCumulativeBytesScanned() / ONE_GB_IN_BYTES;
        System.out.println("Bytes scanned so far: " + bytesScannedSoFar + " GB");

        double bytesMeteredSoFar = (double) queryStatus.getCumulativeBytesMetered() / ONE_GB_IN_BYTES;
        System.out.println("Bytes metered so far: " + bytesMeteredSoFar + " GB");

        List<ColumnInfo> columnInfo = response.getColumnInfo();
        List<Row> rows = response.getRows();

        System.out.println("Metadata: " + columnInfo);
        System.out.println("Data: ");

        // iterate every row
        for (Row row : rows) {
            System.out.println(parseRow(columnInfo, row));
        }
    }

    private String parseRow(List<ColumnInfo> columnInfo, Row row) {
        List<Datum> data = row.getData();
        List<String> rowOutput = new ArrayList<>();
        // iterate every column per row
        for (int j = 0; j < data.size(); j++) {
            ColumnInfo info = columnInfo.get(j);
            Datum datum = data.get(j);
            rowOutput.add(parseDatum(info, datum));
        }
        return String.format("{%s}", rowOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    private String parseDatum(ColumnInfo info, Datum datum) {
        if (datum.isNullValue() != null && datum.isNullValue()) {
            return info.getName() + "=" + "NULL";
        }
        Type columnType = info.getType();
        // If the column is of TimeSeries Type
        if (columnType.getTimeSeriesMeasureValueColumnInfo() != null) {
            return parseTimeSeries(info, datum);
        }
        // If the column is of Array Type
        else if (columnType.getArrayColumnInfo() != null) {
            List<Datum> arrayValues = datum.getArrayValue();
            return info.getName() + "=" + parseArray(info.getType().getArrayColumnInfo(), arrayValues);
        }
        // If the column is of Row Type
        else if (columnType.getRowColumnInfo() != null) {
            List<ColumnInfo> rowColumnInfo = info.getType().getRowColumnInfo();
            Row rowValues = datum.getRowValue();
            return parseRow(rowColumnInfo, rowValues);
        }
        // If the column is of Scalar Type
        else {
            return parseScalarType(info, datum);
        }
    }

    private String parseTimeSeries(ColumnInfo info, Datum datum) {
        List<String> timeSeriesOutput = new ArrayList<>();
        for (TimeSeriesDataPoint dataPoint : datum.getTimeSeriesValue()) {
            timeSeriesOutput.add("{time=" + dataPoint.getTime() + ", value=" +
                    parseDatum(info.getType().getTimeSeriesMeasureValueColumnInfo(), dataPoint.getValue()) + "}");
        }
        return String.format("[%s]", timeSeriesOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    private String parseScalarType(ColumnInfo info, Datum datum) {
        switch (ScalarType.fromValue(info.getType().getScalarType())) {
            case VARCHAR:
                return parseColumnName(info) + datum.getScalarValue();
            case BIGINT:
                Long longValue = Long.valueOf(datum.getScalarValue());
                return parseColumnName(info) + longValue;
            case INTEGER:
                Integer intValue = Integer.valueOf(datum.getScalarValue());
                return parseColumnName(info) + intValue;
            case BOOLEAN:
                Boolean booleanValue = Boolean.valueOf(datum.getScalarValue());
                return parseColumnName(info) + booleanValue;
            case DOUBLE:
                Double doubleValue = Double.valueOf(datum.getScalarValue());
                return parseColumnName(info) + doubleValue;
            case TIMESTAMP:
                return parseColumnName(info) + LocalDateTime.parse(datum.getScalarValue(), TIMESTAMP_FORMATTER);
            case DATE:
                return parseColumnName(info) + LocalDate.parse(datum.getScalarValue(), DATE_FORMATTER);
            case TIME:
                return parseColumnName(info) + LocalTime.parse(datum.getScalarValue(), TIME_FORMATTER);
            case INTERVAL_DAY_TO_SECOND:
            case INTERVAL_YEAR_TO_MONTH:
                return parseColumnName(info) + datum.getScalarValue();
            case UNKNOWN:
                return parseColumnName(info) + datum.getScalarValue();
            default:
                throw new IllegalArgumentException("Given type is not valid: " + info.getType().getScalarType());
        }
    }

    private String parseColumnName(ColumnInfo info) {
        return info.getName() == null ? "" : info.getName() + "=";
    }

    private String parseArray(ColumnInfo arrayColumnInfo, List<Datum> arrayValues) {
        List<String> arrayOutput = new ArrayList<>();
        for (Datum datum : arrayValues) {
            arrayOutput.add(parseDatum(arrayColumnInfo, datum));
        }
        return String.format("[%s]", arrayOutput.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    public void runQueryWithMultiplePages(int limit) {
        String queryWithLimit = SELECT_ALL_QUERY + " LIMIT " + limit;
        System.out.println("Starting query with multiple pages: " + queryWithLimit);
        runQuery(queryWithLimit);
    }

    public void cancelQuery() {
        System.out.println("Starting query: " + SELECT_ALL_QUERY);
        QueryRequest queryRequest = new QueryRequest();
        queryRequest.setQueryString(SELECT_ALL_QUERY);
        QueryResult queryResult = queryClient.query(queryRequest);

        System.out.println("Cancelling the query: " + SELECT_ALL_QUERY);
        final CancelQueryRequest cancelQueryRequest = new CancelQueryRequest();
        cancelQueryRequest.setQueryId(queryResult.getQueryId());
        try {
            queryClient.cancelQuery(cancelQueryRequest);
            System.out.println("Query has been successfully cancelled");
        } catch (Exception e) {
            System.out.println("Could not cancel the query: " + SELECT_ALL_QUERY + " = " + e);
        }
    }
}
