using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.IdentityManagement;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SimpleNotificationService;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.TimestreamQuery;
using Amazon.TimestreamQuery.Model;
using Amazon.TimestreamWrite;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using MeasureValueType = Amazon.TimestreamQuery.MeasureValueType;
using NotificationConfiguration = Amazon.TimestreamQuery.Model.NotificationConfiguration;

namespace TimestreamDotNetSample
{
    static class ScheduledQueryConstants
    {
        public const string Hostname = "host-24Gju";
        public const string SqName = "timestream-sample";
        public const string SqDatabaseName = "sq_result_database_multi";

        public const string SqTableName = "sq_result_table_multi";

        // Use a FIFO queue & FIFO SNS subscription to enforce strict ordering of messages
        public const string TopicName = "scheduled_queries_topic.fifo";
        public const string FifoQueueName = "sq_sample_app_queue.fifo";

        public const string IamRoleName = "ScheduledQuerySampleApplicationRole";
        public const string PolicyName = "SampleApplicationExecutionAccess";

        public const string TopicArn = "topicArn";
        public const string QueueUrl = "queueUrl";
        public const string SubscriptionArn = "subscriptionArn";
        public const string RoleArn = "roleArn";
        public const string RoleName = "roleName";
        public const string PolicyArn = "policyArn";
        public const string ScheduledQueryArn = "scheduledQueryArn";

        //Timestream ScheduledQuery supports both Fixed Rate expressions and Cron Expressions.
        //1. SUPPORTED FIXED RATE FORMAT
        //     Syntax: rate(value unit)
        //     - value : A positive number.
        //     - unit : The unit of time. Valid values: minute | minutes | hour | hours | day | days
        //     Examples: rate(1minute), rate(5 day), rate(1 hours)
        //2. CRON EXPRESSION FORMAT
        //    Cron expressions are comprised of 5 required fields and one optional field
        //    separated by white space. More details: https://tinyurl.com/rxe5zkff
        //
        public const string ScheduleExpression = "cron(0/2 * * * ? *)";

        // A Query with a hardcoded column `timestamp` containing a date that in the future. The Query as such is
        // valid, but when the result of the query is used as a timeColumn for ingesting into a destination Table, it would
        // fail since the time-range is outside table retention window.
        public const string InvalidQueryFormat =
            "SELECT cast('{0}' as TIMESTAMP) as timestamp, 1.0 as random_measure_value, " +
            "'1' as dim0";

        // Find the average, p90, p95, and p99 CPU utilization for a specific EC2 host over the past 2 hours.
        public const string ValidQuery = "SELECT region, az, hostname, BIN(time, 15s) AS binned_timestamp, " +
                                         "ROUND(AVG(cpu_utilization), 2) AS avg_cpu_utilization, " +
                                         "ROUND(APPROX_PERCENTILE(cpu_utilization, 0.9), 2) AS p90_cpu_utilization, " +
                                         "ROUND(APPROX_PERCENTILE(cpu_utilization, 0.95), 2) AS p95_cpu_utilization, " +
                                         "ROUND(APPROX_PERCENTILE(cpu_utilization, 0.99), 2) AS p99_cpu_utilization " +
                                         "FROM " + Constants.DATABASE_NAME + "." + Constants.TABLE_NAME + " " +
                                         "WHERE measure_name = 'metrics' " +
                                         "AND hostname = '" + Hostname + "' " +
                                         "AND time > ago(2h) " +
                                         "GROUP BY region, hostname, az, BIN(time, 15s) " +
                                         "ORDER BY binned_timestamp ASC " +
                                         "LIMIT 5";
    }

    public class ScheduledQueryExample
    {
        private readonly IAmazonTimestreamWrite _amazonTimestreamWrite;
        private readonly IAmazonTimestreamQuery _amazonTimestreamQuery;
        private readonly TimestreamDependencyHelper _timestreamDependencyHelper;
        private readonly TimestreamCrudHelper _timestreamCrudHelper;
        private readonly QueryExample _queryRunner;
        private IAmazonS3 s3Client;
        private IAmazonIdentityManagementService iamClient;
        private IAmazonSimpleNotificationService snsClient;
        private IAmazonSQS sqsClient;
        private RegionEndpoint regionEndpoint;

        public ScheduledQueryExample(IAmazonTimestreamWrite amazonTimestreamWrite,
            IAmazonTimestreamQuery amazonTimestreamQuery,
            TimestreamDependencyHelper timestreamDependencyHelper,
            TimestreamCrudHelper timestreamCrudHelper, QueryExample queryRunner, String region, IAmazonS3 s3ClientEndPoint )
        {
            _amazonTimestreamWrite = amazonTimestreamWrite;
            _amazonTimestreamQuery = amazonTimestreamQuery;
            _timestreamDependencyHelper = timestreamDependencyHelper;
            _timestreamCrudHelper = timestreamCrudHelper;
            _queryRunner = queryRunner;
            regionEndpoint = RegionEndpoint.GetBySystemName(region);
            s3Client = s3ClientEndPoint;
            iamClient = new AmazonIdentityManagementServiceClient(regionEndpoint);
            snsClient = new AmazonSimpleNotificationServiceClient(regionEndpoint);
            sqsClient = new AmazonSQSClient(regionEndpoint);
        }

        public async Task RunScheduledQueryExample(bool skipDeletion, Dictionary<string, string> resourcesCreated)
        {
            try
            {
                resourcesCreated = await CreateResourcesForScheduledQuery(resourcesCreated);
                //Scheduled Query Activities

                /* Switch between Valid and Invalid Query to test Happy-case and Failure scenarios */
                string scheduledQueryArn = await CreateValidScheduledQuery(
                    resourcesCreated[ScheduledQueryConstants.TopicArn],
                    resourcesCreated[ScheduledQueryConstants.RoleArn], ScheduledQueryConstants.SqDatabaseName,
                    ScheduledQueryConstants.SqTableName, resourcesCreated[AwsResourceConstant.BucketName]);
                resourcesCreated.Add(ScheduledQueryConstants.ScheduledQueryArn, scheduledQueryArn);

                // string scheduledQueryArn = await CreateInvalidScheduledQuery(
                //     resourcesCreated[ScheduledQueryConstants.TopicArn],
                //     resourcesCreated[ScheduledQueryConstants.RoleArn], ScheduledQueryConstants.SqDatabaseName,
                //     ScheduledQueryConstants.SqTableName, resourcesCreated[ScheduledQueryConstants.BucketName]);

                await ListScheduledQueries();
                await DescribeScheduledQuery(scheduledQueryArn);

                // Sleep for 65 seconds to let ScheduledQuery run
                Console.WriteLine("Waiting 65 seconds for automatic ScheduledQuery runs & notifications");
                Thread.Sleep(65000);

                await ProcessScheduledQueryNotifications(scheduledQueryArn);

                await UpdateScheduledQuery(scheduledQueryArn, ScheduledQueryState.DISABLED);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Failed to run Scheduled Query example: {e}");
            }
            finally
            {
                if (skipDeletion)
                {
                    Console.WriteLine("The following resources created as part of sample application will have to be deleted manually");
                    foreach (KeyValuePair<string, string> kvp in resourcesCreated)
                    {
                        Console.WriteLine("{0}: {1}", kvp.Key, kvp.Value);
                    }
                }
                else
                {
                    await CleanupResources();
                }
            }

            async Task ProcessScheduledQueryNotifications(string scheduledQueryArn)
            {
                var didQuerySucceedManually = false;
                var wasQueryTriggeredAsExpected = false;
                var queryFailed = false;
                // Reading up to 10 messages in the SQS queue
                for (int i = 0; i < 10; i++)
                {
                    ReceiveMessageResponse response =
                        await _timestreamDependencyHelper.GetMessage(sqsClient,
                            resourcesCreated[ScheduledQueryConstants.QueueUrl]);
                    if (!response.Messages.Any())
                    {
                        continue;
                    }

                    Console.WriteLine($"\nMessage body of {response.Messages[0].MessageId}:");
                    string messageBody = response.Messages[0].Body;
                    Console.WriteLine($"{messageBody}");

                    if (messageBody == null)
                    {
                        continue;
                    }

                    JObject sqsMessage = JObject.Parse(messageBody);
                    JsonSerializerSettings jsonSerializerSettings = new JsonSerializerSettings
                    {
                        NullValueHandling = NullValueHandling.Ignore
                    };
                    string notificationType =
                        sqsMessage["MessageAttributes"][AwsResourceConstant.NotificationType]["Value"].Value<string>();

                    switch (notificationType)
                    {
                        case "SCHEDULED_QUERY_CREATING":
                            // Scheduled Query is still pending to run.
                            Console.WriteLine("SCHEDULED_QUERY_CREATING notification is received");
                            break;
                        case "SCHEDULED_QUERY_CREATED":
                            // Scheduled Query is still pending to run.
                            Console.WriteLine("SCHEDULED_QUERY_CREATED notification is received");
                            break;
                        case "MANUAL_TRIGGER_SUCCESS":
                            Console.WriteLine("Manual run of Scheduled Query succeeded");
                            didQuerySucceedManually = true;
                            break;
                        case "AUTO_TRIGGER_SUCCESS":
                            Console.WriteLine(
                                "Scheduled Query was triggered as expected. Now triggering another run manually");
                            wasQueryTriggeredAsExpected = true;
                            ExecutionNotificationMessage successExecutionNotificationMessage =
                                JsonConvert.DeserializeObject<ExecutionNotificationMessage>(sqsMessage["Message"]
                                    .Value<string>(), jsonSerializerSettings);
                            DateTimeOffset dateTimeOffset =
                                DateTimeOffset.FromUnixTimeSeconds(successExecutionNotificationMessage
                                    .NextInvocationEpochSecond);
                            await ExecuteScheduledQuery(scheduledQueryArn, dateTimeOffset.DateTime);
                            break;
                        case "AUTO_TRIGGER_FAILURE":
                        // Fall-through
                        case "MANUAL_TRIGGER_FAILURE":
                            queryFailed = true;
                            ExecutionNotificationMessage failureExecutionNotificationMessage =
                                JsonConvert.DeserializeObject<ExecutionNotificationMessage>(
                                    sqsMessage["Message"].Value<string>(), jsonSerializerSettings);
                            string failureReason = failureExecutionNotificationMessage.ScheduledQueryRunSummary
                                .FailureReason;

                            Console.WriteLine($"Failure Reason from SQS Notification:: {failureReason}");

                            ErrorReportLocation errorReportLocation = failureExecutionNotificationMessage
                                .ScheduledQueryRunSummary.ErrorReportLocation;

                            if (errorReportLocation != null)
                            {
                                // Error Notification has Error report associated with it. We can parse it.
                                string s3ObjectKey = errorReportLocation.S3ReportLocation.ObjectKey;
                                await ParseS3ErrorReport(resourcesCreated[AwsResourceConstant.BucketName],
                                    s3ObjectKey);
                            }

                            break;
                        case "SCHEDULED_QUERY_DELETED":
                            // Fall-through
                            Console.WriteLine("SCHEDULED_QUERY_DELETED notification is received");
                            break;
                        case "SCHEDULED_QUERY_UPDATED":
                            // Fall-through
                            Console.WriteLine("SCHEDULED_QUERY_UPDATED notification is received");
                            break;
                        default:
                            Console.WriteLine("Received an unexpected message:: " + sqsMessage);
                            break;
                    }

                    // Since its a FIFO queue, we need to delete the message to be able to receive further messages
                    await _timestreamDependencyHelper.DeleteMessage(sqsClient, response.Messages[0],
                        resourcesCreated[ScheduledQueryConstants.QueueUrl]);

                    if ((didQuerySucceedManually && wasQueryTriggeredAsExpected) || queryFailed)
                    {
                        break;
                    }
                }

                if (wasQueryTriggeredAsExpected || didQuerySucceedManually)
                {
                    Console.WriteLine("Fetching Scheduled Query run results");
                    await _queryRunner.RunQueryAsync("SELECT * FROM " + ScheduledQueryConstants.SqDatabaseName + "." +
                                                     ScheduledQueryConstants.SqTableName);
                }
            }

            async Task CleanupResources()
            {
                //Clean up for scheduled query
                if (resourcesCreated.ContainsKey(ScheduledQueryConstants.ScheduledQueryArn))
                {
                    await DeleteScheduledQuery(resourcesCreated[ScheduledQueryConstants.ScheduledQueryArn]);
                }

                if (resourcesCreated.ContainsKey(ScheduledQueryConstants.PolicyArn))
                {
                    await _timestreamDependencyHelper.DetachIamRolePolicy(iamClient,
                        ScheduledQueryConstants.IamRoleName,
                        resourcesCreated[ScheduledQueryConstants.PolicyArn]);
                    await _timestreamDependencyHelper.DeleteIamPolicy(iamClient,
                        resourcesCreated[ScheduledQueryConstants.PolicyArn]);
                }

                if (resourcesCreated.ContainsKey(ScheduledQueryConstants.RoleName))
                {
                    await _timestreamDependencyHelper.DeleteIamRole(iamClient, ScheduledQueryConstants.IamRoleName);
                }

                if (resourcesCreated.ContainsKey(ScheduledQueryConstants.SubscriptionArn))
                {
                    await _timestreamDependencyHelper.UnsubscribeFromSnsTopic(snsClient,
                        resourcesCreated[ScheduledQueryConstants.SubscriptionArn]);
                }

                if (resourcesCreated.ContainsKey(ScheduledQueryConstants.QueueUrl))
                {
                    await _timestreamDependencyHelper.DeleteQueue(sqsClient, ScheduledQueryConstants.FifoQueueName);
                }

                if (resourcesCreated.ContainsKey(ScheduledQueryConstants.TopicArn))
                {
                    await _timestreamDependencyHelper.DeleteSnsTopic(snsClient,
                        resourcesCreated[ScheduledQueryConstants.TopicArn]);
                }

                if (resourcesCreated.ContainsKey(AwsResourceConstant.BucketName))
                {
                    await _timestreamDependencyHelper.DeleteS3Bucket(s3Client,
                        resourcesCreated[AwsResourceConstant.BucketName]);
                }

                await _timestreamCrudHelper.DeleteTable(_amazonTimestreamWrite, ScheduledQueryConstants.SqDatabaseName,
                    ScheduledQueryConstants.SqTableName);
                await _timestreamCrudHelper.DeleteDatabase(_amazonTimestreamWrite,
                    ScheduledQueryConstants.SqDatabaseName);
            }
        }

        private async Task<Dictionary<String, String>> CreateResourcesForScheduledQuery(
            Dictionary<String, String> resourcesDict)
        {
            //Create database and table to store scheduled query results
            await _timestreamCrudHelper.CreateDatabase(_amazonTimestreamWrite, ScheduledQueryConstants.SqDatabaseName);
            await _timestreamCrudHelper.CreateTable(_amazonTimestreamWrite, ScheduledQueryConstants.SqDatabaseName,
                ScheduledQueryConstants.SqTableName, resourcesDict[AwsResourceConstant.BucketName]);

            //Create sns and sqs for scheduled query
            string topicArn =
                await _timestreamDependencyHelper.CreateSnsTopic(snsClient, ScheduledQueryConstants.TopicName);
            resourcesDict.Add(ScheduledQueryConstants.TopicArn, topicArn);

            string sqsQueueUrl =
                await _timestreamDependencyHelper.CreateQueue(sqsClient, ScheduledQueryConstants.FifoQueueName);
            resourcesDict.Add(ScheduledQueryConstants.QueueUrl, sqsQueueUrl);

            string sqsQueueArn =
                await _timestreamDependencyHelper.GetQueueArn(sqsClient, sqsQueueUrl);

            string subscriptionArn =
                await _timestreamDependencyHelper.SubscribeToSnsTopic(snsClient, topicArn, sqsQueueArn);
            resourcesDict.Add(ScheduledQueryConstants.SubscriptionArn, subscriptionArn);

            await _timestreamDependencyHelper.SetSqsAccessPolicy(sqsClient, sqsQueueUrl, topicArn, sqsQueueArn);

            string roleArn = await _timestreamDependencyHelper.CreateIamRole(iamClient,
                ScheduledQueryConstants.IamRoleName, regionEndpoint.SystemName);
            resourcesDict.Add(ScheduledQueryConstants.RoleName, ScheduledQueryConstants.IamRoleName);
            resourcesDict.Add(ScheduledQueryConstants.RoleArn, roleArn);

            string policyArn =
                await _timestreamDependencyHelper.CreateIamPolicy(iamClient, ScheduledQueryConstants.PolicyName);
            resourcesDict.Add(ScheduledQueryConstants.PolicyArn, policyArn);

            await _timestreamDependencyHelper.AttachIamRolePolicy(iamClient, ScheduledQueryConstants.IamRoleName,
                policyArn);

            // Wait at-least 15s for policy to be ready
            Console.WriteLine("Waiting 15 seconds for policy to be ready");
            Thread.Sleep(15000);

            return resourcesDict;
        }

        private async Task<String> CreateValidScheduledQuery(string topicArn, string roleArn,
            string databaseName, string tableName, string s3ErrorReportBucketName)
        {
            List<MultiMeasureAttributeMapping> sourceColToMeasureValueTypes =
                new List<MultiMeasureAttributeMapping>()
                {
                    new()
                    {
                        SourceColumn = "avg_cpu_utilization",
                        MeasureValueType = MeasureValueType.DOUBLE.Value
                    },
                    new()
                    {
                        SourceColumn = "p90_cpu_utilization",
                        MeasureValueType = MeasureValueType.DOUBLE.Value
                    },
                    new()
                    {
                        SourceColumn = "p95_cpu_utilization",
                        MeasureValueType = MeasureValueType.DOUBLE.Value
                    },
                    new()
                    {
                        SourceColumn = "p99_cpu_utilization",
                        MeasureValueType = MeasureValueType.DOUBLE.Value
                    }
                };

            TargetConfiguration targetConfiguration = new TargetConfiguration()
            {
                TimestreamConfiguration = new TimestreamConfiguration()
                {
                    DatabaseName = databaseName,
                    TableName = tableName,
                    TimeColumn = "binned_timestamp",
                    DimensionMappings = new List<DimensionMapping>()
                    {
                        new()
                        {
                            Name = "region",
                            DimensionValueType = "VARCHAR"
                        },
                        new()
                        {
                            Name = "az",
                            DimensionValueType = "VARCHAR"
                        },
                        new()
                        {
                            Name = "hostname",
                            DimensionValueType = "VARCHAR"
                        }
                    },
                    MultiMeasureMappings = new MultiMeasureMappings()
                    {
                        TargetMultiMeasureName = "multi-metrics",
                        MultiMeasureAttributeMappings = sourceColToMeasureValueTypes
                    }
                }
            };
            return await CreateScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName,
                ScheduledQueryConstants.ValidQuery, targetConfiguration);
        }

        private async Task<String> CreateInvalidScheduledQuery(string topicArn, string roleArn,
            string databaseName, string tableName, string s3ErrorReportBucketName)
        {
            TargetConfiguration targetConfiguration = new TargetConfiguration()
            {
                TimestreamConfiguration = new TimestreamConfiguration()
                {
                    DatabaseName = databaseName,
                    TableName = tableName,
                    TimeColumn = "timestamp",
                    DimensionMappings = new List<DimensionMapping>()
                    {
                        new()
                        {
                            Name = "dim0",
                            DimensionValueType = "VARCHAR"
                        }
                    },
                    MixedMeasureMappings = new List<MixedMeasureMapping>()
                    {
                        new()
                        {
                            SourceColumn = "random_measure_value",
                            MeasureValueType = MeasureValueType.DOUBLE.Value
                        }
                    }
                }
            };
            DateTime futureTime = DateTime.Now.AddHours(Constants.HT_TTL_HOURS + 1);
            string futureTimestamp = futureTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
            string invalidQuery = string.Format(ScheduledQueryConstants.InvalidQueryFormat, futureTimestamp);

            return await CreateScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName,
                invalidQuery, targetConfiguration);
        }

        private async Task<String> CreateScheduledQuery(string topicArn, string roleArn,
            string s3ErrorReportBucketName, string query, TargetConfiguration targetConfiguration)
        {
            try
            {
                Console.WriteLine("Creating Scheduled Query");
                CreateScheduledQueryResponse response = await _amazonTimestreamQuery.CreateScheduledQueryAsync(
                    new CreateScheduledQueryRequest()
                    {
                        Name = ScheduledQueryConstants.SqName,
                        QueryString = query,
                        ScheduleConfiguration = new ScheduleConfiguration()
                        {
                            ScheduleExpression = ScheduledQueryConstants.ScheduleExpression
                        },
                        NotificationConfiguration = new NotificationConfiguration()
                        {
                            SnsConfiguration = new SnsConfiguration()
                            {
                                TopicArn = topicArn
                            }
                        },
                        TargetConfiguration = targetConfiguration,
                        ErrorReportConfiguration = new ErrorReportConfiguration()
                        {
                            S3Configuration = new S3Configuration()
                            {
                                BucketName = s3ErrorReportBucketName
                            }
                        },
                        ScheduledQueryExecutionRoleArn = roleArn
                    });
                Console.WriteLine($"Successfully created scheduled query : {response.Arn}");
                return response.Arn;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Scheduled Query creation failed: {e}");
                throw;
            }
        }

        private async Task DeleteScheduledQuery(string scheduledQueryArn)
        {
            try
            {
                Console.WriteLine("Deleting Scheduled Query");
                await _amazonTimestreamQuery.DeleteScheduledQueryAsync(new DeleteScheduledQueryRequest()
                {
                    ScheduledQueryArn = scheduledQueryArn
                });
                Console.WriteLine($"Successfully deleted scheduled query : {scheduledQueryArn}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Scheduled Query deletion failed: {e}");
                throw;
            }
        }

        private async Task ListScheduledQueries()
        {
            try
            {
                Console.WriteLine("Listing Scheduled Query");
                string nextToken;
                do
                {
                    ListScheduledQueriesResponse response =
                        await _amazonTimestreamQuery.ListScheduledQueriesAsync(new ListScheduledQueriesRequest());
                    foreach (var scheduledQuery in response.ScheduledQueries)
                    {
                        Console.WriteLine($"{scheduledQuery.Arn}");
                    }

                    nextToken = response.NextToken;
                } while (nextToken != null);
            }
            catch (Exception e)
            {
                Console.WriteLine($"List Scheduled Query failed: {e}");
                throw;
            }
        }

        private async Task DescribeScheduledQuery(string scheduledQueryArn)
        {
            try
            {
                Console.WriteLine("Describing Scheduled Query");
                DescribeScheduledQueryResponse response = await _amazonTimestreamQuery.DescribeScheduledQueryAsync(
                    new DescribeScheduledQueryRequest()
                    {
                        ScheduledQueryArn = scheduledQueryArn
                    });
                Console.WriteLine($"{JsonConvert.SerializeObject(response.ScheduledQuery)}");
            }
            catch (ResourceNotFoundException e)
            {
                Console.WriteLine($"Scheduled Query doesn't exist: {e}");
                throw;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Describe Scheduled Query failed: {e}");
                throw;
            }
        }

        private async Task UpdateScheduledQuery(string scheduledQueryArn, ScheduledQueryState state)
        {
            try
            {
                Console.WriteLine("Updating Scheduled Query");
                await _amazonTimestreamQuery.UpdateScheduledQueryAsync(new UpdateScheduledQueryRequest()
                {
                    ScheduledQueryArn = scheduledQueryArn,
                    State = state
                });
                Console.WriteLine("Successfully update scheduled query state");
            }
            catch (ResourceNotFoundException e)
            {
                Console.WriteLine($"Scheduled Query doesn't exist: {e}");
                throw;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Update Scheduled Query failed: {e}");
                throw;
            }
        }

        private async Task ExecuteScheduledQuery(string scheduledQueryArn, DateTime invocationTime)
        {
            try
            {
                Console.WriteLine("Running Scheduled Query");
                await _amazonTimestreamQuery.ExecuteScheduledQueryAsync(new ExecuteScheduledQueryRequest()
                {
                    ScheduledQueryArn = scheduledQueryArn,
                    InvocationTime = invocationTime
                });
                Console.WriteLine("Successfully started manual run of scheduled query");
            }
            catch (ResourceNotFoundException e)
            {
                Console.WriteLine($"Scheduled Query doesn't exist: {e}");
                throw;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Execute Scheduled Query failed: {e}");
                throw;
            }
        }

        // Method that parses ErrorReport from S3. A sample error report looks like:
        // {"reportId":"8A729E3CA0755E13EFE4346FB24875AA",
        // "errors":[{"reason":"The record timestamp is outside the time range [2021-11-19T00:07:04.593Z, 2021-11-20T00:47:04.593Z) of the memory store.",
        // "records":[{"dimensions":[{"name":"dim0","value":"1","dimensionValueType":null}],
        // "measureName":"random_measure_value","measureValue":"1.0","measureValues":null,
        // "measureValueType":"DOUBLE","time":"1797393807000000000",
        // "timeUnit":"NANOSECONDS","version":null}]}]}
        private async Task ParseS3ErrorReport(string s3ErrorReportBucketName,
            string errorReportPrefix)
        {
            List<S3Object> s3Objects =
                await this._timestreamDependencyHelper.ListFilesInS3(s3Client, s3ErrorReportBucketName,
                    errorReportPrefix);
            foreach (var s3Object in s3Objects)
            {
                await this._timestreamDependencyHelper.PrintS3ObjectContent(s3Client, s3ErrorReportBucketName,
                    s3Object.Key);
            }
        }
    }

    class ExecutionNotificationMessage
    {
        [JsonProperty("type")]
        public string Type { get; set; }
        [JsonProperty("arn")]
        public string Arn { get; set; }
        [JsonProperty("nextInvocationEpochSecond")]
        public long NextInvocationEpochSecond { get; set; }
        [JsonProperty("scheduledQueryRunSummary")]
        public ScheduledQueryRunSummary ScheduledQueryRunSummary { get; set; }
    }

    class ScheduledQueryRunSummary
    {
        [JsonProperty("invocationEpochSecond")]
        public long InvocationEpochSecond { get; set; }
        [JsonProperty("triggerTimeMillis")]
        public long TriggerTimeMillis { get; set; }
        [JsonProperty("runStatus")]
        public string RunStatus { get; set; }
        [JsonProperty("executionStats")]
        public ExecutionStats ExecutionStats { get; set; }
        [JsonProperty("errorReportLocation")]
        public ErrorReportLocation ErrorReportLocation { get; set; }
        [JsonProperty("failureReason")]
        public string FailureReason { get; set; }
    }
}
