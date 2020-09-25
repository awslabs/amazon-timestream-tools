#!/bin/sh

if test $# -ne 3; then
    echo "Usage:"
    echo "./$2.sh <database name> <table name> <hostname predicate value>"
    echo "e.g. ./$2.sh DATABASE_NAME TABLE_NAME host-ewIqi"
    exit -1
fi

# 1. Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
go run query-common.go query-sample.go -query "SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp,
    ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.9), 2) AS p90_cpu_utilization,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.95), 2) AS p95_cpu_utilization,
    ROUND(APPROX_PERCENTILE(measure_value::double, 0.99), 2) AS p99_cpu_utilization
FROM $1.$2
WHERE measure_name = 'cpu_utilization'
   AND hostname = '$3'
    AND time > ago(2h)
GROUP BY region, hostname, az, BIN(time, 15s)
ORDER BY binned_timestamp ASC"
 
#2. Identify EC2 hosts with CPU utilization that is higher by 10%  or more compared to the average CPU utilization of the entire fleet for the past 2 hours.
go run query-common.go query-sample.go -query "WITH avg_fleet_utilization AS (
    SELECT COUNT(DISTINCT hostname) AS total_host_count, AVG(measure_value::double) AS fleet_avg_cpu_utilization
    FROM $1.$2
    WHERE measure_name = 'cpu_utilization'
        AND time > ago(2h)
), avg_per_host_cpu AS (
    SELECT region, az, hostname, AVG(measure_value::double) AS avg_cpu_utilization
    FROM $1.$2
    WHERE measure_name = 'cpu_utilization'
        AND time > ago(2h)
    GROUP BY region, az, hostname
)
SELECT region, az, hostname, avg_cpu_utilization, fleet_avg_cpu_utilization
FROM avg_fleet_utilization, avg_per_host_cpu
WHERE avg_cpu_utilization > 1.1 * fleet_avg_cpu_utilization
ORDER BY avg_cpu_utilization DESC"
 
#3. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours.
go run query-common.go query-sample.go -query "SELECT BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization,
hostname FROM $1.$2
WHERE measure_name = 'cpu_utilization'
    AND hostname = '$3'
    AND time > ago(2h)
GROUP BY hostname, BIN(time, 30s)
ORDER BY binned_timestamp ASC"
 
#4. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
go run query-common.go query-sample.go -query "WITH binned_timeseries AS (
    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization
    FROM $1.$2
    WHERE measure_name = 'cpu_utilization'
       AND hostname = '$3'
        AND time > ago(2h)
    GROUP BY hostname, BIN(time, 30s)
), interpolated_timeseries AS (
    SELECT hostname,
        INTERPOLATE_LINEAR(
            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization),
                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization
    FROM binned_timeseries
    GROUP BY hostname
)
SELECT time, ROUND(value, 2) AS interpolated_cpu
FROM interpolated_timeseries
CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)"
 
#5. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on the last observation carried forward.
go run query-common.go query-sample.go -query "WITH binned_timeseries AS (
    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization
    FROM $1.$2
    WHERE measure_name = 'cpu_utilization'
        AND hostname = '$3'
        AND time > ago(2h)
    GROUP BY hostname, BIN(time, 30s)
), interpolated_timeseries AS (
    SELECT hostname,
        INTERPOLATE_LOCF(
            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization),
                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization
    FROM binned_timeseries
    GROUP BY hostname
)
SELECT time, ROUND(value, 2) AS interpolated_cpu
FROM interpolated_timeseries
CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)"
 
#6. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on a constant value.
go run query-common.go query-sample.go -query "WITH binned_timeseries AS (
    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization
    FROM $1.$2
    WHERE measure_name = 'cpu_utilization'
       AND hostname = '$3'
        AND time > ago(2h)
    GROUP BY hostname, BIN(time, 30s)
), interpolated_timeseries AS (
    SELECT hostname,
        INTERPOLATE_FILL(
            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization),
                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s), 10.0) AS interpolated_avg_cpu_utilization
    FROM binned_timeseries
    GROUP BY hostname
)
SELECT time, ROUND(value, 2) AS interpolated_cpu
FROM interpolated_timeseries
CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)"
 
#7. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using cubic spline interpolation.
go run query-common.go query-sample.go -query "WITH binned_timeseries AS (
    SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(measure_value::double), 2) AS avg_cpu_utilization
    FROM $1.$2
    WHERE measure_name = 'cpu_utilization'
        AND hostname = '$3'
        AND time > ago(2h)
    GROUP BY hostname, BIN(time, 30s)
), interpolated_timeseries AS (
    SELECT hostname,
        INTERPOLATE_SPLINE_CUBIC(
            CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization),
                SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization
    FROM binned_timeseries
    GROUP BY hostname
)
SELECT time, ROUND(value, 2) AS interpolated_cpu
FROM interpolated_timeseries
CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)"
 
#8. Find the average CPU utilization binned at 30 second intervals for all EC2 hosts over the past 2 hours, filling in the missing values using linear interpolation.
go run query-common.go query-sample.go -query "WITH per_host_min_max_timestamp AS (
    SELECT hostname, min(time) as min_timestamp, max(time) as max_timestamp
    FROM $1.$2
    WHERE measure_name = 'cpu_utilization'
        AND time > ago(2h)
    GROUP BY hostname
), interpolated_timeseries AS (
    SELECT m.hostname,
        INTERPOLATE_LOCF(
            CREATE_TIME_SERIES(time, measure_value::double),
                SEQUENCE(MIN(ph.min_timestamp), MAX(ph.max_timestamp), 30s)) as interpolated_avg_cpu_utilization
    FROM $1.$2 m
        INNER JOIN per_host_min_max_timestamp ph ON m.hostname = ph.hostname
    WHERE measure_name = 'cpu_utilization'
        AND time > ago(2h)
    GROUP BY m.hostname
)
SELECT hostname, AVG(cpu_utilization) AS avg_cpu_utilization
FROM interpolated_timeseries
CROSS JOIN UNNEST(interpolated_avg_cpu_utilization) AS t (time, cpu_utilization)
GROUP BY hostname
ORDER BY avg_cpu_utilization DESC"
 
#9. Find the percentage of measurements with CPU utilization above 70% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
go run query-common.go query-sample.go -query "WITH time_series_view AS (
    SELECT INTERPOLATE_LINEAR(
        CREATE_TIME_SERIES(time, ROUND(measure_value::double,2)),
        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization
    FROM $1.$2
    WHERE hostname = '$3'
        AND 
measure_name = 'cpu_utilization'
        AND time > ago(2h)
    GROUP BY hostname
)
SELECT FILTER(cpu_utilization, x -> x.value > 70.0) AS cpu_above_threshold,
    REDUCE(FILTER(cpu_utilization, x -> x.value > 70.0), 0, (s, x) -> s + 1, s -> s) AS count_cpu_above_threshold,
    ROUND(REDUCE(cpu_utilization, CAST(ROW(0, 0) AS ROW(count_high BIGINT, count_total BIGINT)),
        (s, x) -> CAST(ROW(s.count_high + IF(x.value > 70.0, 1, 0), s.count_total + 1) AS ROW(count_high BIGINT, count_total BIGINT)),
        s -> IF(s.count_total = 0, NULL, CAST(s.count_high AS DOUBLE) / s.count_total)), 4) AS fraction_cpu_above_threshold
FROM time_series_view"
 
#10. List the measurements with CPU utilization lower than 75% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
go run query-common.go query-sample.go -query "WITH time_series_view AS (
    SELECT min(time) AS oldest_time, INTERPOLATE_LINEAR(
        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)),
        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization
    FROM $1.$2
    WHERE 
 hostname = '$3'
        AND 
measure_name = 'cpu_utilization'
        AND time > ago(2h)
    GROUP BY hostname
)
SELECT FILTER(cpu_utilization, x -> x.value < 75 AND x.time > oldest_time + 1m)
FROM time_series_view"
 
#11. Find the total number of measurements with of CPU utilization of 0% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
go run query-common.go query-sample.go -query "WITH time_series_view AS (
    SELECT INTERPOLATE_LINEAR(
        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)),
        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization
    FROM $1.$2
     WHERE 
 hostname = '$3'
        AND 
measure_name = 'cpu_utilization'
        AND time > ago(2h)
    GROUP BY hostname
)
SELECT REDUCE(cpu_utilization,
    DOUBLE '0.0',
    (s, x) -> s + 1,
    s -> s) AS count_cpu
FROM time_series_view"

#12. Find the average CPU utilization for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
go run query-common.go query-sample.go -query "WITH time_series_view AS (
    SELECT INTERPOLATE_LINEAR(
        CREATE_TIME_SERIES(time, ROUND(measure_value::double, 2)),
        SEQUENCE(min(time), max(time), 10s)) AS cpu_utilization
    FROM $1.$2
     WHERE 
 hostname = '$3'
     AND 
measure_name = 'cpu_utilization'
        AND time > ago(2h)
    GROUP BY hostname
)
SELECT REDUCE(cpu_utilization,
    CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)),
    (s, x) -> CAST(ROW(x.value + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
     s -> IF(s.count = 0, NULL, s.sum / s.count)) AS avg_cpu
FROM time_series_view"

# Try other functionality in query
go run query-common.go query-operations.go -query "SELECT * FROM $1.$2"
