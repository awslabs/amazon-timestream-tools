package utils

const (
	SQ_ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX = "sq-error-configuration-sample-s3-bucket-"
	SQ_DATABASE_NAME                             = "sq_result_database_multi"
	SQ_TABLE_NAME                                = "sq_result_table"
	DATABASE_NAME                                = "devops_multi_sample_application"
	TABLE_NAME                                   = "host_metrics_sample_application"

	ROLE_NAME           = "ScheduledQuerySampleApplicationRole"
	POLICY_NAME         = "SampleApplicationExecutionAccess"
	HOSTNAME            = "host-24Gju"
	SQ_NAME             = "daily-sample"
	SCHEDULE_EXPRESSION = "cron(0/1 * * * ? *)"
	VALID_QUERY         = "SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, " +
		"ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization, " +
		"ROUND(APPROX_PERCENTILE(cpu_utilization, 0.9), 2) AS p90_cpu_utilization, " +
		"ROUND(APPROX_PERCENTILE(cpu_utilization, 0.95), 2) AS p95_cpu_utilization, " +
		"ROUND(APPROX_PERCENTILE(cpu_utilization, 0.99), 2) AS p99_cpu_utilization " +
		"FROM %s.%s " +
		"WHERE measure_name = 'metrics' " +
		"AND hostname = '" + HOSTNAME + "' " +
		"AND time > ago(2h) " +
		"GROUP BY region, hostname, az, BIN(time, 15s) " +
		"ORDER BY binned_timestamp ASC " +
		"LIMIT 5"
	INVALID_QUERY = "SELECT cast('2030-12-16' as TIMESTAMP) as timestamp, 1.0 as random_measure_value, '1' as dim0"

	SCHEDULED_QUERY_CREATING = "SCHEDULED_QUERY_CREATING" // scheduled query is being created
	SCHEDULED_QUERY_CREATED  = "SCHEDULED_QUERY_CREATED"  // for successful scheduled query creation
	SCHEDULED_QUERY_UPDATED  = "SCHEDULED_QUERY_UPDATED"  // for successful scheduled query update
	SCHEDULED_QUERY_DELETED  = "SCHEDULED_QUERY_DELETED"  // for successful scheduled query deletion
	AUTO_TRIGGER_SUCCESS     = "AUTO_TRIGGER_SUCCESS"     // for successful completion of scheduled query
	AUTO_TRIGGER_FAILURE     = "AUTO_TRIGGER_FAILURE"     // for failures of scheduled query
	MANUAL_TRIGGER_SUCCESS   = "MANUAL_TRIGGER_SUCCESS"   // for successful completion of manual execution
	MANUAL_TRIGGER_FAILURE   = "MANUAL_TRIGGER_FAILURE"   // for failed manual execution

)
