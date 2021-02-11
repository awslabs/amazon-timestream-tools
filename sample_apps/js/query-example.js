
const constants = require('./constants');

const HOSTNAME = "host1";

// See records ingested into this table so far
const SELECT_ALL_QUERY = "SELECT * FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME;

//1. Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
const QUERY_1 = "SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, " +
    "    ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization, " +
    "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_cpu_utilization, " +
    "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.95), 2) AS p95_cpu_utilization, " +
    "    ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_cpu_utilization " +
    "FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "WHERE measure_name = 'cpu_utilization' " +
    "   AND hostname = '" + HOSTNAME + "' " +
    "    AND time > ago(2h) " +
    "GROUP BY region, hostname, az, BIN(time, 15s) " +
    "ORDER BY binned_timestamp ASC";

//2. Identify EC2 hosts with CPU utilization that is higher by 10%  or more compared to the average CPU utilization of the entire fleet for the past 2 hours.
const QUERY_2 = "WITH avg_fleet_utilization AS ( " +
    "    SELECT COUNT(DISTINCT hostname) AS total_host_count, AVG(measure_value::double) AS fleet_avg_cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE measure_name = 'cpu_utilization' " +
    "        AND time > ago(2h) " +
    "), avg_per_host_cpu AS ( " +
    "    SELECT region, az, hostname, AVG(measure_value::double) AS avg_cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE measure_name = 'cpu_utilization' " +
    "        AND time > ago(2h) " +
    "    GROUP BY region, az, hostname " +
    ") " +
    "SELECT region, az, hostname, avg_cpu_utilization, fleet_avg_cpu_utilization " +
    "FROM avg_fleet_utilization, avg_per_host_cpu " +
    "WHERE avg_cpu_utilization > 1.1 * fleet_avg_cpu_utilization " +
    "ORDER BY avg_cpu_utilization DESC";

//3. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours.
const QUERY_3 = "SELECT BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization, " +
    "hostname FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "WHERE measure_name = 'cpu_utilization' " +
    "    AND hostname = '" + HOSTNAME + "' " +
    "    AND time > ago(2h) " +
    "GROUP BY hostname, BIN(time, 30s) " +
    "ORDER BY binned_timestamp ASC";

//4. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
const QUERY_4 = "WITH binned_timeseries AS ( " +
    "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
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
const QUERY_5 = "WITH binned_timeseries AS ( " +
    "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
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
const QUERY_6 = "WITH binned_timeseries AS ( " +
    "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
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
const QUERY_7 = "WITH binned_timeseries AS ( " +
    "    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
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
const QUERY_8 = "WITH per_host_min_max_timestamp AS ( " +
    "    SELECT hostname, min(time) as min_timestamp, max(time) as max_timestamp " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
    "    WHERE measure_name = 'cpu_utilization' " +
    "        AND time > ago(2h) " +
    "    GROUP BY hostname " +
    "), interpolated_timeseries AS ( " +
    "    SELECT m.hostname, " +
    "        INTERPOLATE_LINEAR( " +
    "            CREATE_TIME_SERIES(time, measure_value::double), " +
    "                SEQUENCE(MIN(ph.min_timestamp), MAX(ph.max_timestamp), 1s)) as interpolated_avg_cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " m " +
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
const QUERY_9 = "WITH time_series_view AS ( " +
    "    SELECT INTERPOLATE_LINEAR( " +
    "        CREATE_TIME_SERIES(time, ROUND(measure_value::double,2)), " +
    "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
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
const QUERY_10 = "WITH time_series_view AS ( " +
    "    SELECT min(time) AS oldest_time, INTERPOLATE_LINEAR( " +
    "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
    "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
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
const QUERY_11 = "WITH time_series_view AS ( " +
    "    SELECT INTERPOLATE_LINEAR( " +
    "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
    "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
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
const QUERY_12 = "WITH time_series_view AS ( " +
    "    SELECT INTERPOLATE_LINEAR( " +
    "        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)), " +
    "        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization " +
    "    FROM " +  constants.DATABASE_NAME + "." +  constants.TABLE_NAME + " " +
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

async function runAllQueries() {
    const queries = [QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6, QUERY_7, QUERY_8, QUERY_9, QUERY_10, QUERY_11, QUERY_12];

    for (let i = 0; i < queries.length; i++) {
        console.log(`Running query ${i+1} : ${queries[i]}`);
        await getAllRows(queries[i], null);
    }
}

async function getAllRows(query, nextToken = undefined) {
    let response;
    try {
        response = await queryClient.query(params = {
            QueryString: query,
            NextToken: nextToken,
        }).promise();
    } catch (err) {
        console.error("Error while querying:", err);
        throw err;
    }

    parseQueryResult(response);
    if (response.NextToken) {
        await getAllRows(query, response.NextToken);
    }
}

async function tryQueryWithMultiplePages(limit) {
    const queryWithLimits = SELECT_ALL_QUERY + " LIMIT " + limit;
    console.log(`Running query with multiple pages: ${queryWithLimits}`);
    await getAllRows(queryWithLimits)
}

async function tryCancelQuery() {
    const params = {
        QueryString: SELECT_ALL_QUERY
    };
    console.log(`Running query: ${SELECT_ALL_QUERY}`);

    const response = await queryClient.query(params).promise();

    console.log(`Sending cancellation for query: ${SELECT_ALL_QUERY}`);

    await cancelQuery(response.QueryId);
}

async function cancelQuery(queryId) {
    try {
        await queryClient.cancelQuery({
            QueryId: queryId
        }).promise();
    } catch (err) {
        console.error("Error while cancelling query:", err);
        throw err;
    }

    console.log("Query has been cancelled successfully");
}

function parseQueryResult(response) {
    const queryStatus = response.QueryStatus;
    console.log("Current query status: " + JSON.stringify(queryStatus));

    const columnInfo = response.ColumnInfo;
    const rows = response.Rows;

    console.log("Metadata: " + JSON.stringify(columnInfo));
    console.log("Data: ");

    rows.forEach(function (row) {
        console.log(parseRow(columnInfo, row));
    });
}

function parseRow(columnInfo, row) {
    const data = row.Data;
    const rowOutput = [];

    var i;
    for ( i = 0; i < data.length; i++ ) {
        info = columnInfo[i];
        datum = data[i];
        rowOutput.push(parseDatum(info, datum));
    }

    return `{${rowOutput.join(", ")}}`
}

function parseDatum(info, datum) {
    if (datum.NullValue != null && datum.NullValue === true) {
        return `${info.Name}=NULL`;
    }

    const columnType = info.Type;

    // If the column is of TimeSeries Type
    if (columnType.TimeSeriesMeasureValueColumnInfo != null) {
        return parseTimeSeries(info, datum);
    }
    // If the column is of Array Type
    else if (columnType.ArrayColumnInfo != null) {
        const arrayValues = datum.ArrayValue;
        return `${info.Name}=${parseArray(info.Type.ArrayColumnInfo, arrayValues)}`;
    }
    // If the column is of Row Type
    else if (columnType.RowColumnInfo != null) {
        const rowColumnInfo = info.Type.RowColumnInfo;
        const rowValues = datum.RowValue;
        return parseRow(rowColumnInfo, rowValues);
    }
    // If the column is of Scalar Type
    else {
        return parseScalarType(info, datum);
    }
}

function parseTimeSeries(info, datum) {
    const timeSeriesOutput = [];
    datum.TimeSeriesValue.forEach(function (dataPoint) {
        timeSeriesOutput.push(`{time=${dataPoint.Time}, value=${parseDatum(info.Type.TimeSeriesMeasureValueColumnInfo, dataPoint.Value)}}`)
    });

    return `[${timeSeriesOutput.join(", ")}]`
}

function parseScalarType(info, datum) {
    return parseColumnName(info) + datum.ScalarValue;
}

function parseColumnName(info) {
    return info.Name == null ? "" : `${info.Name}=`;
}

function parseArray(arrayColumnInfo, arrayValues) {
    const arrayOutput = [];
    arrayValues.forEach(function (datum) {
        arrayOutput.push(parseDatum(arrayColumnInfo, datum));
    });
    return `[${arrayOutput.join(", ")}]`
}

module.exports = {runAllQueries, tryCancelQuery, tryQueryWithMultiplePages, getAllRows};
