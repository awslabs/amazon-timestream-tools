// Constants
const DATABASE_NAME = 'devops_multi_sample_application';
const TABLE_NAME = 'host_metrics_sample_application';
const SQ_DATABASE_NAME = 'sq_result_database_multi';
const SQ_TABLE_NAME = 'sq_result_table';
const MEASURE_VALUE_SAMPLE_DB = "measureValueSampleDb";
const MEASURE_VALUE_SAMPLE_TABLE = "measureValueSampleTable";
const HT_TTL_HOURS = 24;
const CT_TTL_DAYS = 7;
const TOPIC_NAME = 'scheduled_queries_topic.fifo';
const ROLE_NAME = 'ScheduledQuerySampleApplicationRole';
const QUEUE_NAME = "sq_sample_app_queue.fifo";
const POLICY_NAME = "SampleApplicationExecutionAccess";
const SQ_ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX = "error-configuration-sample-s3-bucket-";

module.exports = {DATABASE_NAME, TABLE_NAME, HT_TTL_HOURS, CT_TTL_DAYS, TOPIC_NAME, ROLE_NAME, QUEUE_NAME,
     POLICY_NAME, SQ_ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX, SQ_DATABASE_NAME, SQ_TABLE_NAME,
     MEASURE_VALUE_SAMPLE_DB, MEASURE_VALUE_SAMPLE_TABLE};