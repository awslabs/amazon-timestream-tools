package com.amazonaws.services.timestream;

import javax.sql.PooledConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.StringJoiner;

public class QueryExample {

    public static final String TABLE_NAME = "DevOps";

    private String databaseName;
    private String hostName;
    private ArrayList<String> queries;

    public QueryExample(final String databaseName, final String hostName) {
        this.databaseName = databaseName;
        this.hostName = hostName;
        this.queries = new ArrayList<>();

        //1. Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
        queries.add("SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, " +
                "    ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization, " +
                "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_cpu_utilization, " +
                "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.95), 2) AS p95_cpu_utilization, " +
                "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_cpu_utilization " +
                "FROM " + databaseName + "." + TABLE_NAME + " " +
                "WHERE measure_name = 'cpu_utilization' " +
                "   AND hostname = '" + hostName + "' " +
                "   AND time > ago(2h) " +
                "GROUP BY region, hostname, az, BIN(time, 15s) " +
                "ORDER BY binned_timestamp ASC");

        //2. Identify EC2 hosts with CPU utilization that is higher by 10%  or more compared to the average CPU utilization of the entire fleet for the past 2 hours.
        queries.add("WITH avg_fleet_utilization AS ( " +
                "    SELECT COUNT(DISTINCT hostname) AS total_host_count, AVG(measure_value::double) AS fleet_avg_cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "    WHERE measure_name = 'cpu_utilization' " +
                "        AND time > ago(2h) " +
                "), avg_per_host_cpu AS ( " +
                "    SELECT region, az, hostname, AVG(measure_value::double) AS avg_cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "    WHERE measure_name = 'cpu_utilization' " +
                "        AND time > ago(2h) " +
                "    GROUP BY region, az, hostname " +
                ") " +
                "SELECT region, az, hostname, avg_cpu_utilization, fleet_avg_cpu_utilization " +
                "FROM avg_fleet_utilization, avg_per_host_cpu " +
                "WHERE avg_cpu_utilization > 1.1 * fleet_avg_cpu_utilization " +
                "ORDER BY avg_cpu_utilization DESC");

        //3. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours.
        queries.add("SELECT BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization, " +
                "hostname FROM " + databaseName + "." + TABLE_NAME + " " +
                "WHERE measure_name = 'cpu_utilization' " +
                "    AND hostname = '" + hostName + "' " +
                "    AND time > ago(2h) " +
                "GROUP BY hostname, BIN(time, 30s) " +
                "ORDER BY binned_timestamp ASC");

        //4. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        queries.add("WITH binned_timeseries AS ( " +
                "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "    WHERE measure_name = 'cpu_utilization' " +
                "       AND hostname = '" + hostName + "' " +
                "       AND time > ago(2h) " +
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
                "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)");

        //5. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on the last observation carried forward.
        queries.add("WITH binned_timeseries AS ( " +
                "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "    WHERE measure_name = 'cpu_utilization' " +
                "        AND hostname = '" + hostName + "' " +
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
                "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)");

        //6. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on a constant value.
        queries.add("WITH binned_timeseries AS ( " +
                "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "    WHERE measure_name = 'cpu_utilization' " +
                "       AND hostname = '" + hostName + "' " +
                "       AND time > ago(2h) " +
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
                "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)");

        //7. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using cubic spline interpolation.
        queries.add("WITH binned_timeseries AS ( " +
                "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "    WHERE measure_name = 'cpu_utilization' " +
                "        AND hostname = '" + hostName + "' " +
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
                "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)");

        //8. Find the average CPU utilization binned at 30 second intervals for all EC2 hosts over the past 2 hours, filling in the missing values using linear interpolation.
        queries.add("WITH per_host_min_max_timestamp AS ( " +
                "    SELECT hostname, min(time) as min_timestamp, max(time) as max_timestamp " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "    WHERE measure_name = 'cpu_utilization' " +
                "        AND time > ago(2h) " +
                "    GROUP BY hostname " +
                "), interpolated_timeseries AS ( " +
                "    SELECT m.hostname, " +
                "        INTERPOLATE_LOCF( " +
                "            CREATE_TIME_SERIES(time, measure_value::double), " +
                "                SEQUENCE(MIN(ph.min_timestamp), MAX(ph.max_timestamp), 1s)) as interpolated_avg_cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " m " +
                "        INNER JOIN per_host_min_max_timestamp ph ON m.hostname = ph.hostname " +
                "    WHERE measure_name = 'cpu_utilization' " +
                "        AND time > ago(2h) " +
                "    GROUP BY m.hostname " +
                ") " +
                "SELECT hostname, AVG(cpu_utilization) AS avg_cpu_utilization " +
                "FROM interpolated_timeseries " +
                "CROSS JOIN UNNEST(interpolated_avg_cpu_utilization) AS t (time, cpu_utilization) " +
                "GROUP BY hostname " +
                "ORDER BY avg_cpu_utilization DESC");

        //9. Find the percentage of measurements with CPU utilization above 70% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        queries.add("WITH time_series_view AS ( " +
                "    SELECT INTERPOLATE_LINEAR( " +
                "        CREATE_TIME_SERIES(time, ROUND(measure_value::double,2)), " +
                "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "    WHERE hostname = '" + hostName + "' " +
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
                "FROM time_series_view");

        //10. List the measurements with CPU utilization lower than 75% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        queries.add("WITH time_series_view AS ( " +
                "    SELECT min(time) AS oldest_time, INTERPOLATE_LINEAR( " +
                "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
                "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "    WHERE  " +
                " hostname = '" + hostName + "' " +
                "        AND  " +
                "measure_name = 'cpu_utilization' " +
                "        AND time > ago(2h) " +
                "    GROUP BY hostname " +
                ") " +
                "SELECT FILTER(cpu_utilization, x -> x.value < 75 AND x.time > oldest_time + 1m) " +
                "FROM time_series_view");

        //11. Find the total number of measurements with of CPU utilization of 0% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        queries.add("WITH time_series_view AS ( " +
                "    SELECT INTERPOLATE_LINEAR( " +
                "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
                "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "     WHERE  " +
                " hostname = '" + hostName + "' " +
                "        AND  " +
                " measure_name = 'cpu_utilization' " +
                "        AND time > ago(2h) " +
                "    GROUP BY hostname " +
                ") " +
                "SELECT REDUCE(cpu_utilization, " +
                "    DOUBLE '0.0', " +
                "    (s, x) -> s + 1, " +
                "    s -> s) AS count_cpu " +
                "FROM time_series_view");

        //12. Find the average CPU utilization for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
        queries.add("WITH time_series_view AS ( " +
                "    SELECT INTERPOLATE_LINEAR( " +
                "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
                "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
                "    FROM " + databaseName + "." + TABLE_NAME + " " +
                "     WHERE  " +
                " hostname = '" + hostName + "' " +
                "     AND  " +
                " measure_name = 'cpu_utilization' " +
                "        AND time > ago(2h) " +
                "    GROUP BY hostname " +
                ") " +
                "SELECT REDUCE(cpu_utilization, " +
                "    CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), " +
                "    (s, x) -> CAST(ROW(x.value + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)), " +
                "     s -> IF(s.count = 0, NULL, s.sum / s.count)) AS avg_cpu " +
                "FROM time_series_view");
    }

    /**
     * Creates a pooled connection, use the underlying connection to execute the sample queries then
     * recycle the pooled connection.
     * <p>
     * A pooled connection needs to be created using the {@link software.amazon.timestream.jdbc.TimestreamDataSource}.
     * To create statements, use the underlying connection through {@code
     * pooledConnection.getConnection()}.
     * <p>
     * To recycle a connection, call close on the pooledConnection: {@code pooledConnection.close()}.
     * Using {@code pooledConnection.getConnection().close()} will close the underlying connection.
     */
    public void runAllQueriesWithPooledConnection() {
        try {
            final PooledConnection pooledConnection = JdbcConnectionExample
              .createPooledConnectionWithDataSource();
            final Connection connection = pooledConnection.getConnection();
            runAllQueries(connection);

            // Call close on PooledConnection to recycle the connection.
            pooledConnection.close();
        } catch (SQLException e) {
            // Some queries might fail with 500 if the result of a sequence function has more than 10000 entries
            e.printStackTrace();
        }
    }

    /**
     * Runs all sample queries with a non-pooled Timestream connection.
     */
    public void runAllQueriesWithTimestreamConnection() {
        try (Connection connection = JdbcConnectionExample.createConnectionWithLocalCredentials()) {
            runAllQueries(connection);
        } catch (SQLException e) {
            // Some queries might fail with 500 if the result of a sequence function has more than 10000 entries
            e.printStackTrace();
        }
    }

    /**
     * Runs all the sample queries using the Timestream JDBC driver.
     * <p>
     * The connection is created with the default credentials provider chain. For all the different
     * connection methods, see the methods and documentation in {@link JdbcConnectionExample}.
     *
     * @param connection The {@link Connection} to a Timestream service.
     *
     * @throws SQLException if an error occurred running the queries.
     */
    public void runAllQueries(final Connection connection) throws SQLException {
        for (int i = 0; i < queries.size(); i++) {
            try (Statement statement = connection.createStatement()) {
                System.out.println("\n=================================================");
                System.out.println("Running query " + (i + 1) + ": " + queries.get(i));
                try (ResultSet result = statement.executeQuery(queries.get(i))) {
                    final StringJoiner dataBuilder = new StringJoiner("\n");
                    final StringJoiner columnMetaDataBuilder = new StringJoiner(", ", "[", "]");
                    final ResultSetMetaData resultSetMetaData = result.getMetaData();
                    final int columnCount = result.getMetaData().getColumnCount();
                    final String[] columnNames = new String[columnCount];

                    for (int j = 1; j <= columnCount; j++) {
                        final StringJoiner columnBuilder = new StringJoiner(", ", "{", "}");
                        final String columnName = resultSetMetaData.getColumnLabel(j);
                        columnNames[j - 1] = columnName;
                        columnBuilder.add("Name: " + columnName);
                        columnBuilder.add("Type: " + resultSetMetaData.getColumnTypeName(j));
                        columnMetaDataBuilder.add(columnBuilder.toString());
                    }

                    System.out.println("Metadata: ");
                    System.out.println(columnMetaDataBuilder.toString());

                    while (result.next()) {
                        final StringJoiner resultSetBuilder = new StringJoiner(", ", "{", "}");

                        for (int j = 1; j <= columnCount; j++) {
                            resultSetBuilder.add(columnNames[j - 1] + "=" + result.getString(j));
                        }
                        dataBuilder.add(resultSetBuilder.toString());
                    }

                    System.out.println("Data: ");
                    System.out.println(dataBuilder.toString());
                    System.out.println("=================================================\n");
                }
            }
        }
    }
}
