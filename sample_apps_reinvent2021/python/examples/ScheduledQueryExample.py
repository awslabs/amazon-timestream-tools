#!/usr/bin/python

import json
import time
from string import ascii_lowercase
from random import choice

from utils.WriteUtil import WriteUtil
from utils.QueryUtil import QueryUtil
from utils.TimestreamDependencyHelper import TimestreamDependencyHelper


class ScheduledQueryExample:
    HOSTNAME = "host-24Gju"
    SQ_NAME = "daily-sample"
    TOPIC = "sq_sample_app_topic"
    QUEUE_NAME = "sq_sample_app_queue"
    ROLE_NAME = "ScheduledQuerySampleApplicationRole"
    POLICY_NAME = "ScheduledQuerySampleApplicationAccess"
    ERROR_BUCKET_NAME = "scheduledquerysamplerrorbucket" + ''.join([choice(ascii_lowercase) for _ in range(5)])
    SQ_ENABLED_STATE = "ENABLED"
    SQ_DISABLED_STATE = "DISABLED"

    policy_arn = ""
    subscription_arn = ""
    queue_url = ""
    scheduled_query_arn = ""
    s3_error_bucket = ""

    def __init__(
            self,
            stage,
            region,
            database_name,
            table_name,
            sq_database_name,
            sq_table_name,
            write_client,
            query_client,
            skip_deletion,
            fail_on_execution=False
    ):
        self.write_client = write_client
        self.stage = stage
        self.region = region
        self.query_client = query_client
        self.table_name = table_name
        self.database_name = database_name
        self.sq_database_name = sq_database_name
        self.sq_table_name = sq_table_name
        self.skip_deletion = skip_deletion

        # Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
        self.valid_query = \
            "SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, " \
            "    ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization, " \
            "    ROUND(APPROX_PERCENTILE(cpu_utilization, 0.9), 2) AS p90_cpu_utilization, " \
            "    ROUND(APPROX_PERCENTILE(cpu_utilization, 0.95), 2) AS p95_cpu_utilization, " \
            "    ROUND(APPROX_PERCENTILE(cpu_utilization, 0.99), 2) AS p99_cpu_utilization " \
            "FROM " + database_name + "." + table_name + " " \
                                                         "WHERE measure_name = 'metrics' " \
                                                         "AND hostname = '" + self.HOSTNAME + "' " \
                                                                                              "AND time > ago(2h) " \
                                                                                              "GROUP BY region, hostname, az, BIN(time, 15s) " \
                                                                                              "ORDER BY binned_timestamp ASC " \
                                                                                              "LIMIT 5"
        self.invalid_query = "SELECT cast('2030-12-16' as TIMESTAMP) as timestamp, 1.0 as random_measure_value, '1' as dim0"
        self.fail_on_execution = fail_on_execution

    def create_scheduled_query_helper(self, topic_arn, role_arn, query, target_configuration):
        print("\nCreating Scheduled Query")
        schedule_configuration = {
            'ScheduleExpression': 'cron(0/2 * * * ? *)'
        }
        notification_configuration = {
            'SnsConfiguration': {
                'TopicArn': topic_arn
            }
        }
        error_report_configuration = {
            'S3Configuration': {
                'BucketName': self.s3_error_bucket
            }
        }

        try:
            create_scheduled_query_response = \
                self.query_client.create_scheduled_query(Name=self.SQ_NAME,
                                                         QueryString=query,
                                                         ScheduleConfiguration=schedule_configuration,
                                                         NotificationConfiguration=notification_configuration,
                                                         TargetConfiguration=target_configuration,
                                                         ScheduledQueryExecutionRoleArn=role_arn,
                                                         ErrorReportConfiguration=error_report_configuration
                                                         )
            print("Successfully created scheduled query : ", create_scheduled_query_response['Arn'])
            return create_scheduled_query_response['Arn']
        except Exception as err:
            print("Scheduled Query creation failed:", err)
            raise err

    def create_valid_scheduled_query(self, topic_arn, role_arn):
        target_configuration = {
            'TimestreamConfiguration': {
                'DatabaseName': self.sq_database_name,
                'TableName': self.sq_table_name,
                'TimeColumn': 'binned_timestamp',
                'DimensionMappings': [
                    {'Name': 'region', 'DimensionValueType': 'VARCHAR'},
                    {'Name': 'az', 'DimensionValueType': 'VARCHAR'},
                    {'Name': 'hostname', 'DimensionValueType': 'VARCHAR'}
                ],
                'MultiMeasureMappings': {
                    'TargetMultiMeasureName': 'target_name',
                    'MultiMeasureAttributeMappings': [
                        {'SourceColumn': 'avg_cpu_utilization', 'MeasureValueType': 'DOUBLE',
                         'TargetMultiMeasureAttributeName': 'avg_cpu_utilization'},
                        {'SourceColumn': 'p90_cpu_utilization', 'MeasureValueType': 'DOUBLE',
                         'TargetMultiMeasureAttributeName': 'p90_cpu_utilization'},
                        {'SourceColumn': 'p95_cpu_utilization', 'MeasureValueType': 'DOUBLE',
                         'TargetMultiMeasureAttributeName': 'p95_cpu_utilization'},
                        {'SourceColumn': 'p99_cpu_utilization', 'MeasureValueType': 'DOUBLE',
                         'TargetMultiMeasureAttributeName': 'p99_cpu_utilization'},
                    ]
                }
            }
        }

        return self.create_scheduled_query_helper(topic_arn, role_arn, self.valid_query, target_configuration)

    def create_invalid_scheduled_query(self, topic_arn, role_arn):
        target_configuration = {
            'TimestreamConfiguration': {
                'DatabaseName': self.sq_database_name,
                'TableName': self.sq_table_name,
                'TimeColumn': 'timestamp',
                'DimensionMappings': [
                    {'Name': 'dim0', 'DimensionValueType': 'VARCHAR'}
                ],
                'MultiMeasureMappings': {
                    'TargetMultiMeasureName': 'target_name',
                    'MultiMeasureAttributeMappings': [
                        {'SourceColumn': 'random_measure_value', 'MeasureValueType': 'DOUBLE'},
                    ]
                }
            }
        }

        return self.create_scheduled_query_helper(topic_arn, role_arn, self.invalid_query, target_configuration)

    def list_scheduled_queries(self):
        print("\nListing Scheduled Queries")
        try:
            response = self.query_client.list_scheduled_queries(MaxResults=10)
            self.print_scheduled_queries(response['ScheduledQueries'])
            next_token = response.get('NextToken', None)
            while next_token:
                response = self.query_client.list_scheduled_queries(NextToken=next_token, MaxResults=10)
                self.print_scheduled_queries(response['ScheduledQueries'])
                next_token = response.get('NextToken', None)
        except Exception as err:
            print("List scheduled queries failed:", err)
            raise err

    def execute_scheduled_query(self, scheduled_query_arn, invocation_time):
        print("\nExecuting Scheduled Query")
        try:
            self.query_client.execute_scheduled_query(ScheduledQueryArn=scheduled_query_arn, InvocationTime=invocation_time)
            print("Successfully started executing scheduled query")
        except self.query_client.exceptions.ResourceNotFoundException as err:
            print("Scheduled Query doesn't exist")
            raise err
        except Exception as err:
            print("Scheduled Query execution failed:", err)
            raise err

    def delete_scheduled_query(self, scheduled_query_arn):
        print("\nDeleting Scheduled Query")
        try:
            self.query_client.delete_scheduled_query(ScheduledQueryArn=scheduled_query_arn)
            print("Successfully deleted scheduled query :", scheduled_query_arn)
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Scheduled Query deletion failed:", err)

    def describe_scheduled_query(self, scheduled_query_arn):
        print("\nDescribing Scheduled Query")
        try:
            response = self.query_client.describe_scheduled_query(ScheduledQueryArn=scheduled_query_arn)
            if 'ScheduledQuery' in response:
                response = response['ScheduledQuery']
                for key in response:
                    print("{} :{}".format(key, response[key]))
        except self.query_client.exceptions.ResourceNotFoundException as err:
            print("Scheduled Query doesn't exist")
            raise err
        except Exception as err:
            print("Scheduled Query describe failed:", err)
            raise err

    def update_scheduled_query(self, scheduled_query_arn, state):
        print("\nUpdating Scheduled Query")
        try:
            self.query_client.update_scheduled_query(ScheduledQueryArn=scheduled_query_arn,
                                                     State=state)
            print("Successfully update scheduled query state to", state)
        except self.query_client.exceptions.ResourceNotFoundException as err:
            print("Scheduled Query doesn't exist")
            raise err
        except Exception as err:
            print("Scheduled Query deletion failed:", err)
            raise err

    def write_records(self):
        print("Writing records")
        current_time = WriteUtil.current_milli_time()

        dimensions = [
            {'Name': 'region', 'Value': 'us-east-1'},
            {'Name': 'az', 'Value': 'az1'},
            {'Name': 'hostname', 'Value': 'host-24Gju'}
        ]

        common_attributes = {
            'Dimensions': dimensions,
            'Time': current_time
        }

        cpu_utilization = {
            'Name': 'cpu_utilization',
            'Value': '13.6',
            'Type': 'DOUBLE',
        }

        memory_utilization = {
            'Name': 'memory_utilization',
            'Value': '40',
            'Type': 'DOUBLE',
        }

        records = [{
            'MeasureName': 'metrics',
            'MeasureValues': [cpu_utilization, memory_utilization],
            'MeasureValueType': 'MULTI'
        }]

        try:
            result = self.write_client.write_records(DatabaseName=self.database_name, TableName=self.table_name,
                                                     Records=records, CommonAttributes=common_attributes)
            if result and result['ResponseMetadata']:
                print("WriteRecords Status: [%s]" % result['ResponseMetadata']['HTTPStatusCode'])
        except self.write_client.exceptions.RejectedRecordsException as err:
            WriteUtil.print_rejected_records_exceptions(err)
        except Exception as err:
            print("Error:", err)

    @staticmethod
    def print_scheduled_queries(scheduled_queries):
        for scheduled_query in scheduled_queries:
            print(scheduled_query['Arn'])

    def parse_s3_error_report(self, bucket, prefix):
        objects = TimestreamDependencyHelper().list_s3_objects(bucket, prefix)

        for o in objects:
            body = o.get()['Body'].read().decode('utf-8')
            print(f"error: {body}")

    def run(self):
        write_util = WriteUtil(self.write_client)
        query_util = QueryUtil(self.query_client, self.database_name, self.table_name)
        timestream_dependency_helper = TimestreamDependencyHelper(self.region)

        try:
            # Create base table and ingest records
            write_util.create_database(self.database_name)
            write_util.create_table(self.database_name, self.table_name)
            self.write_records()

            # Create database and table to store scheduled query results
            write_util.create_database(self.sq_database_name)
            write_util.create_table(self.sq_database_name, self.sq_table_name)

            # Create sns, sqs for scheduled query
            self.topic_arn = timestream_dependency_helper.create_sns(self.TOPIC)
            self.queue_url = timestream_dependency_helper.create_sqs_queue(self.QUEUE_NAME)
            # Need to wait at least 1s after creating queue to use it
            time.sleep(2)
            self.queue_arn = timestream_dependency_helper.get_queue_arn(self.queue_url)
            self.subscription_arn = timestream_dependency_helper.subscribe_to_sns_topic(self.topic_arn, self.queue_arn)
            timestream_dependency_helper.set_sqs_access_policy(self.queue_url, self.queue_arn, self.topic_arn)

            # Create role for scheduled query access
            self.role_arn = timestream_dependency_helper.create_role(self.ROLE_NAME, self.stage, self.region)
            self.policy_arn = timestream_dependency_helper.create_policy(self.POLICY_NAME)
            timestream_dependency_helper.attach_policy_to_role(self.ROLE_NAME, self.policy_arn)

            # Waiting for newly created role to be active
            print("\nWaiting for newly created role to become active")
            time.sleep(15)

            # Create S3 bucket to hold error reports
            self.s3_error_bucket = timestream_dependency_helper.create_s3_bucket(self.ERROR_BUCKET_NAME)

            # Scheduled Query Activities
            if self.fail_on_execution:
                self.scheduled_query_arn = self.create_invalid_scheduled_query(self.topic_arn, self.role_arn)
            else:
                self.scheduled_query_arn = self.create_valid_scheduled_query(self.topic_arn, self.role_arn)

            self.describe_scheduled_query(self.scheduled_query_arn)
            self.execute_scheduled_query(self.scheduled_query_arn, time.time())
            time.sleep(15)
            print("Waiting for scheduled query to complete execution")

            # polling sqs queue for notification
            message = {}
            response = ''
            for _ in range(10):
                message = timestream_dependency_helper.receive_message(self.queue_url)
                response = message['MessageAttributes']['notificationType']['Value']
                if not (response == 'SCHEDULED_QUERY_CREATING' or response == 'SCHEDULED_QUERY_CREATED'):
                    break

            if response == 'MANUAL_TRIGGER_SUCCESS':
                print("Received notification regarding scheduled query execution")
                print("Fetching Scheduled Query execution results")
                query_util.run_query("SELECT * FROM " + self.sq_database_name + "." + self.sq_table_name)
            elif response == "MANUAL_TRIGGER_FAILURE":
                # print error report in bucket
                message_json = json.loads(message['Message'])
                schedule_query_run_summary = message_json['scheduledQueryRunSummary']
                print(f"Failure Reason from SQS Notification:: {schedule_query_run_summary['failureReason']}")

                if 'errorReportLocation' in schedule_query_run_summary:
                    error_report_location = schedule_query_run_summary['errorReportLocation']
                    object_key = error_report_location['s3ReportLocation']['objectKey']
                    self.parse_s3_error_report(self.s3_error_bucket, object_key)
            else:
                print("UNEXPECTED ERROR: Expected notification MANUAL_TRIGGER_SUCCESS, Received ", response)

            self.list_scheduled_queries()
            self.update_scheduled_query(self.scheduled_query_arn, self.SQ_DISABLED_STATE)
        finally:
            if not self.skip_deletion:
                self.cleanup(write_util, timestream_dependency_helper)

    def cleanup(self, write_util, scheduled_query_util):
        print("cleaning up resources created by sample")
        if self.scheduled_query_arn:
            self.delete_scheduled_query(self.scheduled_query_arn)

        # Clean up for scheduled query
        scheduled_query_util.detach_policy_from_role(self.ROLE_NAME, self.policy_arn)
        scheduled_query_util.delete_policy(self.policy_arn)
        scheduled_query_util.delete_role(self.ROLE_NAME)
        scheduled_query_util.unsubcribe_from_sns(self.subscription_arn)
        scheduled_query_util.delete_sqs_queue(self.queue_url)
        scheduled_query_util.delete_sns(self.topic_arn)
        scheduled_query_util.delete_s3_bucket(self.ERROR_BUCKET_NAME)

        write_util.delete_table(self.sq_database_name, self.sq_table_name)
        write_util.delete_database(self.sq_database_name)

        write_util.delete_table(self.database_name, self.table_name)
        write_util.delete_database(self.database_name)
