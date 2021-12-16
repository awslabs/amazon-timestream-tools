using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.IdentityManagement;
using Amazon.IdentityManagement.Model;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;

namespace TimestreamDotNetSample
{
    static class AwsResourceConstant
    {
        public const string PolicyDocument =
            @"{" +
            @"  ""Version"": ""2012-10-17""," +
            @"  ""Statement"": [" +
            @"    {" +
            @"        ""Action"": [" +
            @"            ""kms:Decrypt""," +
            @"            ""sns:Publish""," +
            @"            ""timestream:describeEndpoints""," +
            @"            ""timestream:Select""," +
            @"            ""timestream:SelectValues""," +
            @"            ""timestream:WriteRecords""," +
            @"            ""s3:GetObject""," +
            @"            ""s3:List*""," +
            @"            ""s3:Put*""" +
            @"       ]," +
            @"       ""Resource"": ""*""," +
            @"       ""Effect"": ""Allow""" +
            @"    }" +
            @"   ]" +
            @"}";

        public const string SqsPolicyFormat =
            @"{{" +
            @"""Version"":""2012-10-17""," +
            @"""Statement"": [" +
            @"   {{" +
            @"       ""Sid"":""topic-subscription-{0}"", " +
            @"       ""Effect"": ""Allow"", " +
            @"       ""Principal"":{{""AWS"":""*""}}, " +
            @"       ""Action"": ""sqs:SendMessage"", " +
            @"       ""Resource"": ""{1}"", " +
            @"       ""Condition"":{{" +
            @"           ""ArnEquals"":{{" +
            @"               ""aws:SourceArn"":""{2}""" +
            @"           }}" +
            @"       }}" +
            @"   }}" +
            @"  ]" +
            @"}}";

        public const string RolePolicyFormat =
            @"{{" +
            @"""Version"":""2012-10-17""," +
            @"""Statement"":[" +
            @"   {{" +
            @"       ""Effect"":""Allow""," +
            @"       ""Principal"":{{" +
            @"           {0}" +
            @"       }}," +
            @"       ""Action"":""sts:AssumeRole""" +
            @"   }}" +
            @"  ]" +
            @"}}";

        public const string NotificationType = "notificationType";
        public const string Message = "Message";
        public const string ErrorConfigurationS3BucketNamePrefix = "error-configuration-sample-s3-bucket-";
        public const string BucketName = "bucketName";
    }

    public class TimestreamDependencyHelper
    {
        public async Task<string> CreateSnsTopic(IAmazonSimpleNotificationService snsClient,
            string topicName)
        {
            try
            {
                Console.WriteLine("Creating SNS Topic");
                var attrs = new Dictionary<string, string>
                {
                    {"FifoTopic", "true"},
                    {"ContentBasedDeduplication", "false"}
                };
                CreateTopicResponse responseCreate = await snsClient.CreateTopicAsync(
                    new CreateTopicRequest() {Name = topicName, Attributes = attrs});
                Console.WriteLine($"Topic Arn: {responseCreate.TopicArn}");
                return responseCreate.TopicArn;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Creating SNS Topic failed: {e}");
                throw;
            }
        }

        public async Task DeleteSnsTopic(IAmazonSimpleNotificationService snsClient,
            string topicArn)
        {
            try
            {
                Console.WriteLine("Deleting SNS Topic");
                await snsClient.DeleteTopicAsync(
                    new DeleteTopicRequest() {TopicArn = topicArn});
                Console.WriteLine($"SNS Topic deletion success: {topicArn}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"SNS Topic deletion failed: {e}");
                //do not throw exception here, because we want subsequent cleanups to run
            }
        }

        public async Task<string> CreateQueue(IAmazonSQS sqsClient, string queueName)
        {
            try
            {
                Console.WriteLine("Creating SQS Queue");
                var attrs = new Dictionary<string, string>
                {
                    {QueueAttributeName.FifoQueue, "true"},
                    {QueueAttributeName.ContentBasedDeduplication, "false"}
                };
                CreateQueueResponse responseCreate = await sqsClient.CreateQueueAsync(
                    new CreateQueueRequest {QueueName = queueName, Attributes = attrs});
                return responseCreate.QueueUrl;
            }
            catch (Exception e)
            {
                Console.WriteLine($"SQS Queue creation failed: {e}");
                throw;
            }
        }

        public async Task DeleteQueue(IAmazonSQS sqsClient, string queueName)
        {
            try
            {
                var queueUrl = (await sqsClient.GetQueueUrlAsync(queueName)).QueueUrl;
                Console.WriteLine($"Deleting queue {queueUrl}...");
                await sqsClient.DeleteQueueAsync(queueUrl);
                Console.WriteLine($"Queue {queueUrl} has been deleted.");
            }
            catch (Exception e)
            {
                Console.WriteLine($"SQS Queue deletion failed: {e}");
                //do not throw exception here, because we want subsequent cleanups to run
            }
        }

        public async Task<string> GetQueueArn(IAmazonSQS sqsClient, string queueUrl)
        {
            try
            {
                GetQueueAttributesResponse responseGetAtt = await sqsClient.GetQueueAttributesAsync(
                    queueUrl, new List<string> {QueueAttributeName.QueueArn});
                return responseGetAtt.QueueARN;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Get Queue Arn failed: {e}");
                throw;
            }
        }

        public async Task<string> SubscribeToSnsTopic(IAmazonSimpleNotificationService snsClient,
            string topicArn, string queueArn)
        {
            try
            {
                SubscribeRequest subscribeRequest = new SubscribeRequest();
                subscribeRequest.TopicArn = topicArn;
                subscribeRequest.Protocol = "sqs";
                subscribeRequest.Endpoint = queueArn;
                SubscribeResponse response = await snsClient.SubscribeAsync(subscribeRequest);
                Console.WriteLine($"Subscription ARN: {response.SubscriptionArn} Status is {response.HttpStatusCode}");
                return response.SubscriptionArn;
            }
            catch (Exception e)
            {
                Console.WriteLine($"Subscribe to SNS failed: {e}");
                throw;
            }
        }

        public async Task UnsubscribeFromSnsTopic(IAmazonSimpleNotificationService snsClient,
            string subscriptionArn)
        {
            try
            {
                UnsubscribeRequest unsubscribeRequest = new UnsubscribeRequest
                {
                    SubscriptionArn = subscriptionArn
                };
                await snsClient.UnsubscribeAsync(unsubscribeRequest);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Unsubscribing to Sns Topic failed: {e}");
                //do not throw exception here, because we want subsequent cleanups to run
            }
        }

        public async Task SetSqsAccessPolicy(IAmazonSQS sqsClient, string queueUrl, string topicArn,
            string queueArn)
        {
            try
            {
                Console.WriteLine("Setting SQS access policy");
                string accessPolicy = string.Format(AwsResourceConstant.SqsPolicyFormat, topicArn, queueArn, topicArn);
                var attrs = new Dictionary<string, string>
                {
                    {
                        QueueAttributeName.Policy, accessPolicy
                    }
                };
                await sqsClient.SetQueueAttributesAsync(queueUrl, attrs);
            }
            catch (Exception e)
            {
                Console.WriteLine($"Setting SQS access policy failed: {e}");
                throw;
            }
        }

        public async Task<ReceiveMessageResponse> GetMessage(IAmazonSQS sqsClient, string queueUrl,
            int waitTime = 20, int maxMessages = 1)
        {
            try
            {
                return await sqsClient.ReceiveMessageAsync(new ReceiveMessageRequest
                {
                    QueueUrl = queueUrl,
                    MaxNumberOfMessages = maxMessages,
                    WaitTimeSeconds = waitTime
                });
            }
            catch (Exception e)
            {
                Console.WriteLine($"Receiving messages failed: {e}");
                throw;
            }
        }

        public async Task DeleteMessage(IAmazonSQS sqsClient, Message message, string queueUrl)
        {
            try
            {
                Console.WriteLine($"\nDeleting message {message.MessageId} from queue...");
                DeleteMessageResponse response = await sqsClient.DeleteMessageAsync(queueUrl, message.ReceiptHandle);
                Console.WriteLine($"Delete message request status code :  {response.HttpStatusCode}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Deleting SQS message failed: {e}");
                throw;
            }
        }

        public string GetServiceName(string region)
        {
            return $@"""Service"":""timestream.{region}.amazonaws.com""";
        }

        public async Task<string> CreateIamRole(IAmazonIdentityManagementService iamClient, string roleName,
            string region)
        {
            try
            {
                Console.WriteLine("Creating IAM Role");
                CreateRoleResponse response = await iamClient.CreateRoleAsync(new CreateRoleRequest()
                {
                    RoleName = roleName,
                    AssumeRolePolicyDocument =
                        string.Format(AwsResourceConstant.RolePolicyFormat, GetServiceName(region)),
                    Description = "Created via AWS Timestream sample"
                });
                Console.WriteLine($"The ARN of the role is :  {response.Role.Arn}");

                // Wait for the role to be available
                Thread.Sleep(2000);

                return response.Role.Arn;
            }
            catch (EntityAlreadyExistsException)
            {
                Console.WriteLine("Role already exists");
                GetRoleResponse getRoleResponse = await iamClient.GetRoleAsync(new GetRoleRequest()
                {
                    RoleName = roleName
                });
                return getRoleResponse.Role.Arn;
            }
            catch (Exception e)
            {
                Console.WriteLine($"IAM role creation failed: {e}");
                throw;
            }
        }

        public async Task DeleteIamRole(IAmazonIdentityManagementService iamClient, string roleName)
        {
            try
            {
                Console.WriteLine("Deleting IAM Role");
                await iamClient.DeleteRoleAsync(new DeleteRoleRequest()
                {
                    RoleName = roleName
                });
                Console.WriteLine($"Deleted role :  {roleName}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"IAM role creation failed: {e}");
                //do not throw exception here, because we want subsequent cleanups to run
            }
        }


        public async Task<string> CreateIamPolicy(IAmazonIdentityManagementService iamClient, string policyName)
        {
            try
            {
                Console.WriteLine("Creating IAM Policy");
                CreatePolicyResponse response = await iamClient.CreatePolicyAsync(new CreatePolicyRequest()
                {
                    PolicyName = policyName,
                    PolicyDocument = AwsResourceConstant.PolicyDocument,
                    Description = "Created via AWS Timestream sample"
                });

                Console.WriteLine($"The ARN of the policy is :  {response.Policy.Arn}");

                // Wait for the policy to be available
                Thread.Sleep(2000);
                return response.Policy.Arn;
            }
            catch (EntityAlreadyExistsException)
            {
                string accountId = new AmazonSecurityTokenServiceClient().GetCallerIdentityAsync(new GetCallerIdentityRequest()).Result.Account;
                return String.Format("arn:aws:iam::{0}:policy/{1}", accountId, policyName);
            }
            catch (Exception e)
            {
                Console.WriteLine($"IAM policy creation failed: {e}");
                throw;
            }
        }

        public async Task DeleteIamPolicy(IAmazonIdentityManagementService iamClient, string policyArn)
        {
            try
            {
                Console.WriteLine("Deleting IAM Policy");
                await iamClient.DeletePolicyAsync(new DeletePolicyRequest()
                {
                    PolicyArn = policyArn
                });
                Console.WriteLine($"Deleted policy :  {policyArn}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"IAM policy deletion failed: {e}");
                //do not throw exception here, because we want subsequent cleanups to run
            }
        }

        public async Task AttachIamRolePolicy(IAmazonIdentityManagementService iamClient, string roleName,
            string policyArn)
        {
            try
            {
                Console.WriteLine("Attaching IAM Role Policy");
                ListAttachedRolePoliciesResponse response = await iamClient.ListAttachedRolePoliciesAsync(
                    new ListAttachedRolePoliciesRequest()
                    {
                        RoleName = roleName
                    });

                List<AttachedPolicyType> attachedPolicies = response.AttachedPolicies;

                foreach (var attachedPolicyType in attachedPolicies)
                {
                    if (policyArn.Equals(attachedPolicyType.PolicyArn))
                    {
                        Console.WriteLine($"Policy : {policyArn} is already attached to role {roleName}");
                        return;
                    }
                }

                await iamClient.AttachRolePolicyAsync(new AttachRolePolicyRequest()
                {
                    RoleName = roleName,
                    PolicyArn = policyArn
                });

                Console.WriteLine($"Successfully attached policy : {policyArn} to role {roleName}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"IAM policy attach failed: {e}");
                throw;
            }
        }

        public async Task DetachIamRolePolicy(IAmazonIdentityManagementService iamClient, string roleName,
            string policyArn)
        {
            try
            {
                Console.WriteLine("Detaching IAM Role Policy");

                await iamClient.DetachRolePolicyAsync(new DetachRolePolicyRequest()
                {
                    RoleName = roleName,
                    PolicyArn = policyArn
                });

                Console.WriteLine($"Successfully detached policy : {policyArn} from role {roleName}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"IAM policy detach failed: {e}");
                //do not throw exception here, because we want subsequent cleanups to run
            }
        }

        public async Task CreateS3Bucket(IAmazonS3 s3Client, string bucketName)
        {
            try
            {
                Console.WriteLine("Creating S3 bucket for Scheduled Query error reporting");

                await s3Client.PutBucketAsync(new PutBucketRequest()
                {
                    BucketName = bucketName
                });

                Console.WriteLine($"Successfully created S3 bucket : {bucketName}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"S3 Bucket creation failed: {e}");
                throw;
            }
        }

        public async Task DeleteS3Bucket(IAmazonS3 s3Client, string bucketName)
        {
            try
            {
                Console.WriteLine("Deleting Scheduled Query error Reporting S3 bucket");
                // To delete a bucket, all the objects in the bucket must be deleted first

                ListObjectsV2Request request = new ListObjectsV2Request()
                {
                    BucketName = bucketName
                };
                ListObjectsV2Response response;
                do
                {
                    response = await s3Client.ListObjectsV2Async(request);
                    foreach (var responseS3Object in response.S3Objects)
                    {
                        await s3Client.DeleteObjectAsync(new DeleteObjectRequest()
                        {
                            BucketName = bucketName,
                            Key = responseS3Object.Key
                        });
                    }

                    request = new ListObjectsV2Request()
                    {
                        BucketName = bucketName,
                        ContinuationToken = response.ContinuationToken
                    };
                } while (response.IsTruncated);

                await s3Client.DeleteBucketAsync(new DeleteBucketRequest()
                {
                    BucketName = bucketName
                });
            }
            catch (Exception e)
            {
                Console.WriteLine($"S3 Bucket Deletion failed: {e}");
                //do not throw exception here, because we want subsequent cleanups to run
            }
        }

        public async Task<List<S3Object>> ListFilesInS3(IAmazonS3 s3Client, string bucketName, string keyPrefix)
        {
            ListObjectsV2Response response = await s3Client.ListObjectsV2Async(new ListObjectsV2Request()
            {
                BucketName = bucketName,
                Prefix = keyPrefix
            });
            return response.S3Objects;
        }

        public async Task PrintS3ObjectContent(IAmazonS3 s3Client, string bucketName, string key)
        {
            Task<GetObjectResponse> res = s3Client.GetObjectAsync(new GetObjectRequest()
            {
                BucketName = bucketName,
                Key = key
            });
            Task.WaitAll(res);

            if (res.IsCompletedSuccessfully)
            {
                using (var reader = new StreamReader(res.Result.ResponseStream))
                {
                    Console.WriteLine("Retrieved contents of object '{0}' in bucket '{1}'", key, bucketName);
                    Console.WriteLine(await reader.ReadToEndAsync());
                }
            }
        }
    }
}