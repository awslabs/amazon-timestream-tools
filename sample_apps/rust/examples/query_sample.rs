use anyhow::{Error, Result};
use clap::Parser;
use sample_timestream_app::utils::query_common::{get_connection, run_query, write, Args};
use std::fs;

async fn execute_sample_queries(
    region: &str,
    database_name: &str,
    table_name: &str,
    f: &fs::File,
) -> Result<(), Error> {
    let client = get_connection(region)
        .await
        .expect("Failed to get connection to Timestream");

    const HOSTNAME: &str = "host-24Gju";
    const QUERY_LIMIT: i32 = 200;
    const MAX_ROWS: i32 = 200;

    // 1. Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host.
    let query_1 = format!(
        "SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, \
        ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization, \
        ROUND(APPROX_PERCENTILE(cpu_utilization, 0.9), 2) AS p90_cpu_utilization, \
        ROUND(APPROX_PERCENTILE(cpu_utilization, 0.95), 2) AS p95_cpu_utilization, \
        ROUND(APPROX_PERCENTILE(cpu_utilization, 0.99), 2) AS p99_cpu_utilization \
        FROM {database_name}.{table_name} \
        WHERE measure_name = 'metrics' AND hostname = '{HOSTNAME}' \
        GROUP BY region, hostname, az, BIN(time, 15s) \
        ORDER BY binned_timestamp ASC"
    );

    // 2. Identify EC2 hosts with CPU utilization that is higher by 10% or more compared to the average CPU utilization of the entire fleet.
    let query_2 = format!("WITH avg_fleet_utilization AS ( \
        SELECT COUNT(DISTINCT hostname) AS total_host_count, AVG(cpu_utilization) AS fleet_avg_cpu_utilization \
        FROM {database_name}.{table_name} WHERE measure_name = 'metrics' \
       ), avg_per_host_cpu AS ( \
        SELECT region, az, hostname, AVG(cpu_utilization) AS avg_cpu_utilization \
        FROM {database_name}.{table_name} \
        WHERE measure_name = 'metrics' \
        GROUP BY region, az, hostname) \
        SELECT region, az, hostname, avg_cpu_utilization, fleet_avg_cpu_utilization \
        FROM avg_fleet_utilization, avg_per_host_cpu \
        WHERE avg_cpu_utilization > 1.03 * fleet_avg_cpu_utilization \
        ORDER BY avg_cpu_utilization DESC");

    // 3. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host.
    let query_3 = format!("SELECT BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization, \
        hostname FROM {database_name}.{table_name} \
        WHERE measure_name = 'metrics' \
        AND hostname = '{HOSTNAME}' \
        GROUP BY hostname, BIN(time, 30s) \
        ORDER BY binned_timestamp ASC");

    // 4. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host, filling in the missing values using linear interpolation.
    let query_4 = format!("WITH binned_timeseries AS (\
        SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization \
        FROM {database_name}.{table_name} WHERE measure_name = 'metrics' \
        AND hostname = '{HOSTNAME}' \
        GROUP BY hostname, BIN(time, 30s)), interpolated_timeseries AS ( \
        SELECT hostname, INTERPOLATE_LINEAR( \
        CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), \
        SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization \
        FROM binned_timeseries \
        GROUP BY hostname) \
        SELECT time, ROUND(value, 2) AS interpolated_cpu \
        FROM interpolated_timeseries \
        CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)");

    // 5. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host, filling in the missing values using interpolation based on the last observation carried forward.
    let query_5 = format!("WITH binned_timeseries AS ( \
        SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization \
        FROM {database_name}.{table_name} WHERE measure_name = 'metrics' AND hostname = '{HOSTNAME}' \
        GROUP BY hostname, BIN(time, 30s)), interpolated_timeseries AS (\
        SELECT hostname, INTERPOLATE_LOCF(CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), \
        SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization \
        FROM binned_timeseries \
        GROUP BY hostname) \
        SELECT time, ROUND(value, 2) AS interpolated_cpu FROM interpolated_timeseries \
        CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)");

    // 6. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host, filling in the missing values using interpolation based on a constant value.
    let query_6 = format!("WITH binned_timeseries AS ( \
        SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization \
        FROM {database_name}.{table_name} \
        WHERE measure_name = 'metrics' \
        AND hostname = '{HOSTNAME}' \
        GROUP BY hostname, BIN(time, 30s)), interpolated_timeseries AS ( \
        SELECT hostname, \
        INTERPOLATE_FILL(CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), \
        SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s), 10.0) AS interpolated_avg_cpu_utilization \
        FROM binned_timeseries \
        GROUP BY hostname) \
        SELECT time, ROUND(value, 2) AS interpolated_cpu \
        FROM interpolated_timeseries \
        CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)");

    // 7. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host, filling in the missing values using cubic spline interpolation.
    let query_7 = format!("WITH binned_timeseries AS (\
        SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization \
        FROM {database_name}.{table_name} \
        WHERE measure_name = 'metrics' AND hostname = '{HOSTNAME}' \
        GROUP BY hostname, BIN(time, 30s)), interpolated_timeseries AS (\
        SELECT hostname, INTERPOLATE_SPLINE_CUBIC(\
        CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), \
        SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization \
        FROM binned_timeseries \
        GROUP BY hostname) \
        SELECT time, ROUND(value, 2) AS interpolated_cpu \
        FROM interpolated_timeseries CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)");

    // 8. Find the average CPU utilization binned at 30 second intervals for all EC2 hosts, filling in the missing values using linear interpolation.
    let query_8 = format!("WITH per_host_min_max_timestamp AS (\
        SELECT hostname, min(time) as min_timestamp, max(time) as max_timestamp \
        FROM {database_name}.{table_name} \
        WHERE measure_name = 'metrics' \
        GROUP BY hostname), interpolated_timeseries AS (\
        SELECT m.hostname,\
        INTERPOLATE_LOCF(\
        CREATE_TIME_SERIES(time, cpu_utilization), \
        SEQUENCE(MIN(ph.min_timestamp), MAX(ph.max_timestamp), 30s)) as interpolated_avg_cpu_utilization \
        FROM {database_name}.{table_name} m \
        INNER JOIN per_host_min_max_timestamp ph ON m.hostname = ph.hostname \
        WHERE measure_name = 'metrics'\
        GROUP BY m.hostname) \
        SELECT hostname, AVG(cpu_utilization) AS avg_cpu_utilization \
        FROM interpolated_timeseries \
        CROSS JOIN UNNEST(interpolated_avg_cpu_utilization) AS t (time, cpu_utilization) \
        GROUP BY hostname \
        ORDER BY avg_cpu_utilization DESC");

    // 9. Find the percentage of measurements with CPU utilization above 70% for a specific EC2 host, filling in the missing values using linear interpolation.
    let query_9 = format!("WITH time_series_view AS (\
        SELECT INTERPOLATE_LINEAR(\
        CREATE_TIME_SERIES(time, ROUND(cpu_utilization,2)), \
        SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization \
        FROM {database_name}.{table_name} \
        WHERE hostname = '{HOSTNAME}' AND measure_name = 'metrics' \
        GROUP BY hostname) \
        SELECT FILTER(interpolated_cpu_utilization, x -> x.value > 70.0) AS cpu_above_threshold, \
        REDUCE(FILTER(interpolated_cpu_utilization, x -> x.value > 70.0), 0, (s, x) -> s + 1, s -> s) AS count_cpu_above_threshold, \
        ROUND(REDUCE(interpolated_cpu_utilization, CAST(ROW(0, 0) AS ROW(count_high BIGINT, count_total BIGINT)), \
        (s, x) -> CAST(ROW(s.count_high + IF(x.value > 70.0, 1, 0), s.count_total + 1) AS ROW(count_high BIGINT, count_total BIGINT)), \
        s -> IF(s.count_total = 0, NULL, CAST(s.count_high AS DOUBLE) / s.count_total)), 4) AS fraction_cpu_above_threshold \
        FROM time_series_view");

    // 10. List the measurements with CPU utilization lower than 75% for a specific EC2 host, filling in the missing values using linear interpolation.
    let query_10 = format!("WITH time_series_view AS (\
        SELECT min(time) AS oldest_time, INTERPOLATE_LINEAR(\
        CREATE_TIME_SERIES(time, ROUND(cpu_utilization, 2)),\
        SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization \
        FROM {database_name}.{table_name} \
        WHERE hostname = '{HOSTNAME}' AND measure_name = 'metrics' \
        GROUP BY hostname) \
        SELECT FILTER(interpolated_cpu_utilization, x -> x.value < 75 AND x.time > oldest_time + 1m) \
        FROM time_series_view");

    // 11. Find the total number of measurements with of CPU utilization of 0% for a specific EC2 host, filling in the missing values using linear interpolation.
    let query_11 = format!(
        "WITH time_series_view AS (\
        SELECT INTERPOLATE_LINEAR(\
        CREATE_TIME_SERIES(time, ROUND(cpu_utilization, 2)), \
        SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization \
        FROM {database_name}.{table_name} \
        WHERE hostname = '{HOSTNAME}' AND measure_name = 'metrics' \
        GROUP BY hostname \
        )\
        SELECT REDUCE(interpolated_cpu_utilization, \
        DOUBLE '0.0', \
        (s, x) -> s + 1, \
        s -> s) AS count_cpu \
        FROM time_series_view"
    );

    // 12. Find the average CPU utilization for a specific EC2 host, filling in the missing values using linear interpolation.
    let query_12 = format!(
        "WITH time_series_view AS (\
        SELECT INTERPOLATE_LINEAR(CREATE_TIME_SERIES(time, ROUND(cpu_utilization, 2)), \
        SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization \
        FROM {database_name}.{table_name} \
        WHERE hostname = '{HOSTNAME}' AND measure_name = 'metrics' \
        GROUP BY hostname) \
        SELECT REDUCE(interpolated_cpu_utilization, \
        CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), \
        (s, x) -> CAST(ROW(x.value + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)), \
        s -> IF(s.count = 0, NULL, s.sum / s.count)) AS avg_cpu \
        FROM time_series_view"
    );

    // 13. Running a query with multiple pages
    let query_13 = format!("SELECT * FROM {database_name}.{table_name} LIMIT {QUERY_LIMIT}");

    let queries = [
        query_1, query_2, query_3, query_4, query_5, query_6, query_7, query_8, query_9, query_10,
        query_11, query_12, query_13,
    ];

    for (i, query) in queries.iter().enumerate() {
        let msg = format!("Running Query_{} : {}", i + 1, query);
        println!("{}", msg);
        write(f, msg.to_string())?;
        run_query(query.to_string(), &client, f, MAX_ROWS).await?;
        let divider = "--------------------------------------------";
        println!("{}", divider);
        write(f, divider.to_string())?;
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Processing command-line arguments
    let args = Args::parse();

    let output_file = fs::File::create(args.output_file).expect("Error creating log file");

    execute_sample_queries(
        &args.region,
        &args.database_name,
        &args.table_name,
        &output_file,
    )
    .await?;
    Ok(())
}
