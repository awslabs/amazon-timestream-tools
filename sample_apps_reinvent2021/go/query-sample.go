package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"go_sample/utils"
	"os"

	"flag"
	"fmt"
)

func ExecuteSampleQueries(queryClient *timestreamquery.TimestreamQuery,databaseName string, tableName string, f *os.File){
	HOSTNAME := "host-24Gju"
	QUERY_LIMIT := 200

	// 1. Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
	QUERY_1  := fmt.Sprintf("SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp," +
		"ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization," +
		"ROUND(APPROX_PERCENTILE(cpu_utilization, 0.9), 2) AS p90_cpu_utilization," +
		"ROUND(APPROX_PERCENTILE(cpu_utilization, 0.95), 2) AS p95_cpu_utilization," +
		"ROUND(APPROX_PERCENTILE(cpu_utilization, 0.99), 2) AS p99_cpu_utilization " +
		"FROM %s.%s " +
		"WHERE measure_name = 'metrics' AND hostname = '%s' AND time > ago(2h) " +
		"GROUP BY region, hostname, az, BIN(time, 15s) " +
		"ORDER BY binned_timestamp ASC", databaseName, tableName, HOSTNAME)

	//2. Identify EC2 hosts with CPU utilization that is higher by 10%  or more compared to the average CPU utilization of the entire fleet for the past 2 hours.
	QUERY_2  := fmt.Sprintf("WITH avg_fleet_utilization AS ( " +
		"SELECT COUNT(DISTINCT hostname) AS total_host_count, AVG(cpu_utilization) AS fleet_avg_cpu_utilization " +
		"FROM %s.%s WHERE measure_name = 'metrics' " +
		"AND time > ago(2h)), avg_per_host_cpu AS ( " +
		"SELECT region, az, hostname, AVG(cpu_utilization) AS avg_cpu_utilization " +
		"FROM %s.%s " +
		"WHERE measure_name = 'metrics' " +
		"AND time > ago(2h) " +
		"GROUP BY region, az, hostname) " +
		"SELECT region, az, hostname, avg_cpu_utilization, fleet_avg_cpu_utilization " +
		"FROM avg_fleet_utilization, avg_per_host_cpu " +
		"WHERE avg_cpu_utilization > 1.1 * fleet_avg_cpu_utilization " +
		"ORDER BY avg_cpu_utilization DESC", databaseName, tableName, databaseName, tableName)

	//3. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours.
	QUERY_3  := fmt.Sprintf("SELECT BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization, " +
		"hostname FROM %s.%s " +
		"WHERE measure_name = 'metrics' " +
		"AND hostname = '%s' AND time > ago(2h) " +
		"GROUP BY hostname, BIN(time, 30s) " +
		"ORDER BY binned_timestamp ASC", databaseName, tableName, HOSTNAME)

	//4. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
	QUERY_4  := fmt.Sprintf("WITH binned_timeseries AS (" +
		"SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization " +
		"FROM %s.%s WHERE measure_name = 'metrics' " +
		"AND hostname = '%s' AND time > ago(2h) " +
		"GROUP BY hostname, BIN(time, 30s)), interpolated_timeseries AS ( " +
		"SELECT hostname, INTERPOLATE_LINEAR( " +
		"CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " +
		"SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization " +
		"FROM binned_timeseries " +
		"GROUP BY hostname) " +
		"SELECT time, ROUND(value, 2) AS interpolated_cpu " +
		"FROM interpolated_timeseries " +
		"CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)", databaseName, tableName, HOSTNAME)

	//5. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on the last observation carried forward.
	QUERY_5  := fmt.Sprintf("WITH binned_timeseries AS ( " +
		"SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization " +
		"FROM %s.%s WHERE measure_name = 'metrics' AND hostname = '%s' AND time > ago(2h) " +
		"GROUP BY hostname, BIN(time, 30s)), interpolated_timeseries AS (" +
		"SELECT hostname, INTERPOLATE_LOCF(CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " +
		"SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization " +
		"FROM binned_timeseries " +
		"GROUP BY hostname) " +
		"SELECT time, ROUND(value, 2) AS interpolated_cpu FROM interpolated_timeseries " +
		"CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)", databaseName, tableName, HOSTNAME)

	//6. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using interpolation based on a constant value.
	QUERY_6  := fmt.Sprintf("WITH binned_timeseries AS ( " +
		"SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization " +
		"FROM %s.%s " +
		"WHERE measure_name = 'metrics' " +
		"AND hostname = '%s' AND time > ago(2h) " +
		"GROUP BY hostname, BIN(time, 30s)), interpolated_timeseries AS ( " +
		"SELECT hostname, " +
		"INTERPOLATE_FILL(CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " +
		"SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s), 10.0) AS interpolated_avg_cpu_utilization " +
		"FROM binned_timeseries " +
		"GROUP BY hostname) " +
		"SELECT time, ROUND(value, 2) AS interpolated_cpu " +
		"FROM interpolated_timeseries " +
		"CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)", databaseName, tableName, HOSTNAME)

	//7. Find the average CPU utilization binned at 30 second intervals for a specific EC2 host over the past 2 hours, filling in the missing values using cubic spline interpolation.
	QUERY_7  := fmt.Sprintf("WITH binned_timeseries AS (" +
		"SELECT hostname, BIN(time, 30s) AS binned_timestamp, ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization " +
		"FROM %s.%s " +
		"WHERE measure_name = 'metrics' AND hostname = '%s' AND time > ago(2h) " +
		"GROUP BY hostname, BIN(time, 30s)), interpolated_timeseries AS (" +
		"SELECT hostname, INTERPOLATE_SPLINE_CUBIC(" +
		"CREATE_TIME_SERIES(binned_timestamp, avg_cpu_utilization), " +
		"SEQUENCE(min(binned_timestamp), max(binned_timestamp), 15s)) AS interpolated_avg_cpu_utilization " +
		"FROM binned_timeseries " +
		"GROUP BY hostname) " +
		"SELECT time, ROUND(value, 2) AS interpolated_cpu " +
		"FROM interpolated_timeseries CROSS JOIN UNNEST(interpolated_avg_cpu_utilization)", databaseName, tableName, HOSTNAME)

	//8. Find the average CPU utilization binned at 30 second intervals for all EC2 hosts over the past 2 hours, filling in the missing values using linear interpolation.
	QUERY_8  := fmt.Sprintf("WITH per_host_min_max_timestamp AS (" +
		"SELECT hostname, min(time) as min_timestamp, max(time) as max_timestamp " +
		"FROM %s.%s " +
		"WHERE measure_name = 'metrics' AND time > ago(2h) " +
		"GROUP BY hostname), interpolated_timeseries AS (" +
		"SELECT m.hostname," +
		"INTERPOLATE_LOCF(" +
		 	"CREATE_TIME_SERIES(time, cpu_utilization), " +
		"SEQUENCE(MIN(ph.min_timestamp), MAX(ph.max_timestamp), 30s)) as interpolated_avg_cpu_utilization " +
		"FROM %s.%s m " +
		"INNER JOIN per_host_min_max_timestamp ph ON m.hostname = ph.hostname " +
		"WHERE measure_name = 'metrics' AND time > ago(2h)" +
		"GROUP BY m.hostname) " +
		"SELECT hostname, AVG(cpu_utilization) AS avg_cpu_utilization " +
		"FROM interpolated_timeseries " +
		"CROSS JOIN UNNEST(interpolated_avg_cpu_utilization) AS t (time, cpu_utilization) " +
		"GROUP BY hostname " +
		"ORDER BY avg_cpu_utilization DESC", databaseName, tableName, databaseName, tableName)

	//9. Find the percentage of measurements with CPU utilization above 70% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
	QUERY_9  := fmt.Sprintf("WITH time_series_view AS (" +
		"SELECT INTERPOLATE_LINEAR(" +
		"CREATE_TIME_SERIES(time, ROUND(cpu_utilization,2)), " +
		"SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization " +
		"FROM %s.%s " +
		"WHERE hostname = '%s' AND measure_name = 'metrics' AND time > ago(2h) " +
		"GROUP BY hostname) " +
		"SELECT FILTER(interpolated_cpu_utilization, x -> x.value > 70.0) AS cpu_above_threshold, " +
		"REDUCE(FILTER(interpolated_cpu_utilization, x -> x.value > 70.0), 0, (s, x) -> s + 1, s -> s) AS count_cpu_above_threshold, " +
		"ROUND(REDUCE(interpolated_cpu_utilization, CAST(ROW(0, 0) AS ROW(count_high BIGINT, count_total BIGINT)), " +
		"(s, x) -> CAST(ROW(s.count_high + IF(x.value > 70.0, 1, 0), s.count_total + 1) AS ROW(count_high BIGINT, count_total BIGINT)), " +
		"s -> IF(s.count_total = 0, NULL, CAST(s.count_high AS DOUBLE) / s.count_total)), 4) AS fraction_cpu_above_threshold " +
		"FROM time_series_view", databaseName, tableName, HOSTNAME)

	//10. List the measurements with CPU utilization lower than 75% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
	QUERY_10  := fmt.Sprintf("WITH time_series_view AS (" +
		"SELECT min(time) AS oldest_time, INTERPOLATE_LINEAR(" +
			"CREATE_TIME_SERIES(time, ROUND(cpu_utilization, 2))," +
			"SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization " +
		"FROM %s.%s " +
		"WHERE hostname = '%s' AND measure_name = 'metrics' AND time > ago(2h) " +
		"GROUP BY hostname) " +
		"SELECT FILTER(interpolated_cpu_utilization, x -> x.value < 75 AND x.time > oldest_time + 1m) " +
		"FROM time_series_view", databaseName, tableName, HOSTNAME)

	//11. Find the total number of measurements with of CPU utilization of 0% for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
	QUERY_11  := fmt.Sprintf("WITH time_series_view AS (" +
		"SELECT INTERPOLATE_LINEAR(" +
		"CREATE_TIME_SERIES(time, ROUND(cpu_utilization, 2)), " +
		"SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization " +
		"FROM %s.%s " +
		"WHERE hostname = '%s' AND measure_name = 'metrics' AND time > ago(2h) " +
		"GROUP BY hostname " +
		")" +
	"SELECT REDUCE(interpolated_cpu_utilization, " +
		"DOUBLE '0.0', " +
		"(s, x) -> s + 1, " +
		"s -> s) AS count_cpu " +
		"FROM time_series_view", databaseName, tableName, HOSTNAME)

	//12. Find the average CPU utilization for a specific EC2 host over the past 2 hours, filling in the missing values using linear interpolation.
	QUERY_12  := fmt.Sprintf("WITH time_series_view AS (" +
		"SELECT INTERPOLATE_LINEAR(CREATE_TIME_SERIES(time, ROUND(cpu_utilization, 2)), " +
		"SEQUENCE(min(time), max(time), 10s)) AS interpolated_cpu_utilization " +
		"FROM %s.%s " +
		"WHERE hostname = '%s' AND measure_name = 'metrics' AND time > ago(2h) " +
		"GROUP BY hostname) " +
		"SELECT REDUCE(interpolated_cpu_utilization, " +
		"CAST(ROW(0.0, 0) AS ROW(sum DOUBLE, count INTEGER)), " +
		"(s, x) -> CAST(ROW(x.value + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)), " +
		"s -> IF(s.count = 0, NULL, s.sum / s.count)) AS avg_cpu " +
		"FROM time_series_view", databaseName, tableName, HOSTNAME)

	//13. Running a query with multiple pages
	QUERY_13  := fmt.Sprintf("SELECT * FROM %s.%s LIMIT %d", databaseName, tableName,
		 QUERY_LIMIT)

	queries := []string{QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6,
		QUERY_7, QUERY_8, QUERY_9, QUERY_10, QUERY_11, QUERY_12, QUERY_13}

	for index, query := range queries {
		fmt.Printf("Running Query_%d : %s\n", index + 1, query)
		utils.RunQuery(&query, queryClient, f)
	}
}

func main() {
	// process command line arguments
	region := flag.String("region", "us-east-1", "region")
	filePtr := flag.String("outputfile", "query_results.log", "output results file in the current folder")
	flag.Parse()

	// setup the query client
	sess, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	utils.HandleError(err, "Failed to start a new session", true)


	querySvc := timestreamquery.New(sess)

	var f *os.File
	if *filePtr != "" {
		var ferr error
		f, ferr = os.Create(*filePtr)
		utils.Check(ferr)
		defer f.Close()
	}

	ExecuteSampleQueries(querySvc, "devops_multi_sample_application",
		"host_metrics_sample_application", f)
}