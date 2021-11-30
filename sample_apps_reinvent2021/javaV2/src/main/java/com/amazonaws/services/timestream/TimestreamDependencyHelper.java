package com.amazonaws.services.timestream;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.core.waiters.WaiterResponse;
import software.amazon.awssdk.services.iam.IamClient;
import software.amazon.awssdk.services.iam.model.AttachRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.AttachedPolicy;
import software.amazon.awssdk.services.iam.model.CreatePolicyRequest;
import software.amazon.awssdk.services.iam.model.CreatePolicyResponse;
import software.amazon.awssdk.services.iam.model.CreateRoleRequest;
import software.amazon.awssdk.services.iam.model.CreateRoleResponse;
import software.amazon.awssdk.services.iam.model.DeletePolicyRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleRequest;
import software.amazon.awssdk.services.iam.model.DeleteRoleResponse;
import software.amazon.awssdk.services.iam.model.DetachRolePolicyRequest;
import software.amazon.awssdk.services.iam.model.GetPolicyRequest;
import software.amazon.awssdk.services.iam.model.GetPolicyResponse;
import software.amazon.awssdk.services.iam.model.GetRoleRequest;
import software.amazon.awssdk.services.iam.model.GetRoleResponse;
import software.amazon.awssdk.services.iam.model.IamException;
import software.amazon.awssdk.services.iam.model.ListAttachedRolePoliciesRequest;
import software.amazon.awssdk.services.iam.model.ListAttachedRolePoliciesResponse;
import software.amazon.awssdk.services.iam.waiters.IamWaiter;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.waiters.S3Waiter;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.CreateTopicRequest;
import software.amazon.awssdk.services.sns.model.CreateTopicResponse;
import software.amazon.awssdk.services.sns.model.DeleteTopicRequest;
import software.amazon.awssdk.services.sns.model.DeleteTopicResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sns.model.SubscribeRequest;
import software.amazon.awssdk.services.sns.model.SubscribeResponse;
import software.amazon.awssdk.services.sns.model.UnsubscribeRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.CreateQueueResponse;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageResponse;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import static com.amazonaws.services.timestream.ScheduledQueryExample.NotificationType;


public class TimestreamDependencyHelper {
    public static final JsonParser JSON_PARSER = new JsonParser();
    public static final String POLICY_DOCUMENT =
            "{" +
                    "  \"Version\": \"2012-10-17\"," +
                    "  \"Statement\": [" +
                    "    {" +
                    "        \"Action\": [" +
                    "            \"kms:Decrypt\"," +
                    "            \"sns:Publish\"," +
                    "            \"timestream:describeEndpoints\"," +
                    "            \"timestream:Select\"," +
                    "            \"timestream:SelectValues\"," +
                    "            \"timestream:WriteRecords\"," +
                    "            \"s3:PutObject\"" +
                    "       ]," +
                    "       \"Resource\": \"*\"," +
                    "       \"Effect\": \"Allow\"" +
                    "    }" +
                    "   ]" +
                    "}";

    public static final String SQS_POLICY_FORMAT =
            "{" +
                    "\"Version\":\"2012-10-17\"," +
                    "\"Statement\": [" +
                    "   {" +
                    "       \"Sid\":\"topic-subscription-%s\", " +
                    "       \"Effect\": \"Allow\", " +
                    "       \"Principal\":{\"AWS\":\"*\"}, " +
                    "       \"Action\": \"sqs:SendMessage\", " +
                    "       \"Resource\": \"%s\", " +
                    "       \"Condition\":{" +
                    "           \"ArnEquals\":{" +
                    "               \"aws:SourceArn\":\"%s\"" +
                    "           }" +
                    "       }" +
                    "   }" +
                    "  ]" +
                    "}";

    public static final String ROLE_POLICY_FORMAT =
            "{" +
                    "\"Version\":\"2012-10-17\"," +
                    "\"Statement\":[" +
                    "   {" +
                    "       \"Effect\":\"Allow\"," +
                    "       \"Principal\":{" +
                    "           %s" +
                    "       }," +
                    "       \"Action\":\"sts:AssumeRole\"" +
                    "   }" +
                    "  ]" +
                    "}";
    public static final String RECEIPT_HANDLE = "receiptHandle";
    public static final String NOTIFICATION_TYPE = "notificationType";
    public static final String INVOCATION_EPOCH_SECOND = "invocationEpochSecond";
    public static final String MESSAGE = "Message";
    public static final String ERROR_REPORT_LOCATION = "errorReportLocation";
    public static final String OBJECT_KEY = "objectKey";
    public static final String S3_REPORT_LOCATION = "s3ReportLocation";
    public static final String ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX = "error-configuration-sample-s3-bucket-";


    public String createSQSQueue(SqsClient sqsClient, String queueName) {
        System.out.println("Creating SQS Queue");
        CreateQueueResponse result = null;
        try {
            Map<QueueAttributeName, String> queueAttributes = new HashMap<>();
            queueAttributes.put(QueueAttributeName.FIFO_QUEUE, Boolean.TRUE.toString());
            queueAttributes.put(QueueAttributeName.CONTENT_BASED_DEDUPLICATION, Boolean.FALSE.toString());
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(queueAttributes)
                    .build();

            result = sqsClient.createQueue(request);
            return result.queueUrl();
        } catch (Exception e) {
            System.out.println("SQS Queue creation failed: " + e);
            throw e;
        }
    }

    public static void deleteSQSQueue(SqsClient sqsClient, String queueName) {
        System.out.println("Deleting SQS Queue");
        try {

            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();

            String queueUrl = sqsClient.getQueueUrl(getQueueRequest).queueUrl();
            DeleteQueueRequest deleteQueueRequest = DeleteQueueRequest.builder()
                    .queueUrl(queueUrl)
                    .build();

            sqsClient.deleteQueue(deleteQueueRequest);

        } catch (SqsException e) {
            System.out.println("Delete SQS Queue failed: " + e);
            //do not throw exception here, because we want the following cleanups executes
        }
    }

    public String getQueueArn(SqsClient sqsClient, String queueUrl) {
        System.out.println("Getting Queue Arn");
        try {
            List<QueueAttributeName> atts = new ArrayList();
            atts.add(QueueAttributeName.QUEUE_ARN);
            GetQueueAttributesResponse response = sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .attributeNames(atts).queueUrl(queueUrl).build());
            String queueArn = response.attributes().get(QueueAttributeName.QUEUE_ARN);
            System.out.println("queueArn: " + queueArn);
            return queueArn;
        } catch (Exception e) {
            System.out.println("Get Queue Arn failed: " + e);
            throw e;
        }
    }

    public String subscribeToSnsTopic(SnsClient snsClient, String topicArn, String queueArn) {
        System.out.println("Subscribing to Sns Topic");
        try {
            SubscribeRequest request = SubscribeRequest.builder()
                    .topicArn(topicArn)
                    .protocol("sqs")
                    .endpoint(queueArn)
                    .build();
            SubscribeResponse response = snsClient.subscribe(request);
            System.out.println("Subscription ARN: " + response.subscriptionArn() + "Status is " + response.sdkHttpResponse().statusCode());
            return response.subscriptionArn();
        } catch (Exception e) {
            System.out.println("Subscribe to SNS failed: " + e);
            throw e;
        }
    }

    public void unsubscribeFromSnsTopic(String subscriptionArn, SnsClient snsClient) {
        System.out.println("UnSubscribing to Sns Topic");
        try {
            snsClient.unsubscribe(UnsubscribeRequest.builder().subscriptionArn(subscriptionArn).build());
        } catch (SnsException e) {
            System.out.println("UnSubscribing to Sns Topic failed: " + e);
            //do not throw exception here, because we want the following cleanups executes
        }
    }

    public void setSqsAccessPolicy(SqsClient sqsClient, String queueUrl, String topicArn, String queueArn) {
        System.out.println("Setting SQS access policy");
        try {
            Map<QueueAttributeName, String> attributes = new HashMap<>();
            QueueAttributeName policy = QueueAttributeName.POLICY;
            attributes.put(policy, String.format(SQS_POLICY_FORMAT, topicArn, queueArn, topicArn));
            SetQueueAttributesRequest request = SetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributes(attributes).build();
            sqsClient.setQueueAttributes(request);
        } catch (Exception e) {
            System.out.println("Setting SQS access policy failed: " + e);
            throw e;
        }
    }

    public JsonObject receiveMessage(String queueUrl, SqsClient sqsClient) throws UnsupportedEncodingException {
        System.out.println("Receiving messages");
        try {
            ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .waitTimeSeconds(20)
                    .maxNumberOfMessages(1)
                    .build();
            List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
            if (messages != null && messages.size() > 0) {
                String messageBody = messages.get(0).body();
                System.out.println(messageBody);
                JsonObject messageJSON = (JsonObject) JSON_PARSER.parse(messageBody);
                String notificationType =
                        messageJSON.get("MessageAttributes").getAsJsonObject().get("notificationType").getAsJsonObject().get("Value").getAsString();
                JsonObject result = new JsonObject();
                result.addProperty(RECEIPT_HANDLE, messages.get(0).receiptHandle());
                result.addProperty(NOTIFICATION_TYPE, notificationType);
                if (NotificationType.valueOf(notificationType).equals(NotificationType.AUTO_TRIGGER_FAILURE) ||
                        NotificationType.valueOf(notificationType).equals(NotificationType.MANUAL_TRIGGER_FAILURE)) {
                    result.add(MESSAGE,
                            JSON_PARSER.parse(messageJSON.get("Message").getAsString().replace("\\\"", "\"")).getAsJsonObject());
                    String messageObject =
                            messageJSON.get("Message").getAsString().replace("\\\"", "\"");
                    result.addProperty(INVOCATION_EPOCH_SECOND, JSON_PARSER.parse(messageObject).getAsJsonObject()
                                    .get("scheduledQueryRunSummary").getAsJsonObject()
                                    .get("invocationEpochSecond").getAsLong());
                }
                return result;
            }
        } catch (Exception e) {
            System.out.println("Receiving messages failed: " + e.getMessage());
            throw e;
        }
        return null;
    }

    public void deleteMessage(String queueUrl, SqsClient sqsClient, String receiptHandle) {
        System.out.println("Deleting message");
        try {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .receiptHandle(receiptHandle)
                    .queueUrl(queueUrl)
                    .build();

            DeleteMessageResponse deleteMessageResponse = sqsClient.deleteMessage(deleteMessageRequest);
            System.out.println("Delete message request status code:: " + deleteMessageResponse.sdkHttpResponse().statusCode());
        } catch (Exception e) {
            System.err.println("Deleting SQS message failed:: " + e.getMessage());
            throw e;
        }
    }

    public String createSNSTopic(SnsClient snsClient, String topicName ) {
        System.out.println("Creating SNS Topic");
        try {
            CreateTopicResponse result = null;
            Map<String, String> topicAttributes = new HashMap<>();

            topicAttributes.put("FifoTopic", "true");
            topicAttributes.put("ContentBasedDeduplication", "false");
            CreateTopicRequest request = CreateTopicRequest.builder()
                    .name(topicName)
                    .attributes(topicAttributes)
                    .build();

            result = snsClient.createTopic(request);
            System.out.println("topic_arn: " + result.topicArn());
            return result.topicArn();
        } catch (Exception e) {
            System.out.println("Creating SNS Topic failed: " + e);
            throw e;
        }
    }

    public void deleteSNSTopic(SnsClient snsClient, String topicArn ) {
        System.out.println("Deleting SNS Topic");
        try {
            DeleteTopicRequest request = DeleteTopicRequest.builder()
                    .topicArn(topicArn)
                    .build();

            DeleteTopicResponse result = snsClient.deleteTopic(request);
            System.out.println("Status was " + result.sdkHttpResponse().statusCode());
        } catch (SnsException e) {
            System.out.println("SNS Topic deletion failed: " + e);
            //do not throw exception here, because we want the following cleanups executes
        }
    }

    public static String createIAMRole(IamClient iam, String rolename, String region) {
        System.out.println("Creating Role");
        try {
            IamWaiter iamWaiter = iam.waiter();
            CreateRoleRequest request = CreateRoleRequest.builder()
                    .roleName(rolename)
                    .assumeRolePolicyDocument(String.format(ROLE_POLICY_FORMAT, getServiceName(region)))
                    .description("Created using the AWS SDK for Java")
                    .build();

            CreateRoleResponse response = iam.createRole(request);

            // Wait until the role is created
            GetRoleRequest roleRequest = GetRoleRequest.builder()
                    .roleName(response.role().roleName())
                    .build();

            WaiterResponse<GetRoleResponse> waitUntilRoleExists = iamWaiter.waitUntilRoleExists(roleRequest);
            waitUntilRoleExists.matched().response().ifPresent(System.out::println);

            System.out.println("The ARN of the role is "+response.role().arn());
            return response.role().arn();
        } catch (Exception e) {
            System.out.println("IAM role creation failed: " + e);
            throw e;
        }
    }

    public static String deleteIAMRole(IamClient iam, String roleName) {
        System.out.println("Deleting Role");
        try {
            DeleteRoleRequest request = DeleteRoleRequest.builder()
                    .roleName(roleName)
                    .build();

            DeleteRoleResponse response = iam.deleteRole(request);
            System.out.println("Deleting Role: "+roleName);
        } catch (IamException e) {
            System.out.println(e.awsErrorDetails().errorMessage());
            //do not throw exception here, because we want subsequent cleanups to execute
        } catch (Exception e) {
            System.out.println("IAM role deletion failed: " + e);
            //do not throw exception here, because we want subsequent cleanups to execute
        }
        return null;
    }

    public static String createIAMPolicy(IamClient iam, String policyName ) {
        System.out.println("Creating IAM Policy");
        try {
            // Create an IamWaiter object
            IamWaiter iamWaiter = iam.waiter();

            CreatePolicyRequest request = CreatePolicyRequest.builder()
                    .policyName(policyName)
                    .policyDocument(POLICY_DOCUMENT).build();

            CreatePolicyResponse response = iam.createPolicy(request);

            // Wait until the policy is created
            GetPolicyRequest polRequest = GetPolicyRequest.builder()
                    .policyArn(response.policy().arn())
                    .build();

            WaiterResponse<GetPolicyResponse> waitUntilPolicyExists = iamWaiter.waitUntilPolicyExists(polRequest);
            waitUntilPolicyExists.matched().response().ifPresent(System.out::println);
            return response.policy().arn();
        } catch (Exception e) {
            System.out.println("IAM policy creation failed: " + e);
            throw e;
        }
    }

    public static void deleteIAMPolicy(IamClient iam,String policyARN) {
        System.out.println("Deleting IAM Policy");
        try {
            DeletePolicyRequest request = DeletePolicyRequest.builder()
                    .policyArn(policyARN)
                    .build();

            iam.deletePolicy(request);
            System.out.println("Successfully deleted the policy");
        } catch (Exception e) {
            System.out.println("IAM policy deletion failed: " + e);
            //do not throw exception here, because we want the following cleanups executes
        }
    }

    public static void attachIAMRolePolicy(IamClient iam, String roleName, String policyArn) {
        System.out.println("Attaching IAM Role Policy");
        try {
            ListAttachedRolePoliciesRequest request = ListAttachedRolePoliciesRequest.builder()
                    .roleName(roleName)
                    .build();

            ListAttachedRolePoliciesResponse response = iam.listAttachedRolePolicies(request);
            List<AttachedPolicy> attachedPolicies = response.attachedPolicies();

            for (AttachedPolicy policy : attachedPolicies) {
                String arn = policy.policyArn();
                if (arn.compareTo(policyArn) == 0) {
                    System.out.println(roleName +
                            "policy is already attached to this role.");
                    return;
                }
            }

            AttachRolePolicyRequest attachRequest =
                    AttachRolePolicyRequest.builder()
                            .roleName(roleName)
                            .policyArn(policyArn)
                            .build();

            iam.attachRolePolicy(attachRequest);

            System.out.println("Successfully attached policy " + policyArn +
                    " to role " + roleName);
        } catch (Exception e) {
            System.out.println("IAM policy attach failed: " + e);
            throw e;
        }
    }

    public static void detachPolicy(IamClient iam, String roleName, String policyArn ) {
        System.out.println("Detaching IAM Role Policy");
        try {
            DetachRolePolicyRequest request = DetachRolePolicyRequest.builder()
                    .roleName(roleName)
                    .policyArn(policyArn)
                    .build();

            iam.detachRolePolicy(request);
            System.out.println("Successfully detached policy " + policyArn +
                    " from role " + roleName);

        } catch (IamException e) {
            System.out.println("IAM policy detach failed: " + e);
            //do not throw exception here, because we want subsequent cleanups to execute
        }
    }

    public static String getServiceName(String region) {
        String rolePolicy;
        rolePolicy = "\"Service\":\"timestream." + region + ".amazonaws.com\"";
        return rolePolicy;
    }



    public String createS3Bucket(S3Client s3Client, final String bucketName) {
        System.out.println("Creating S3 bucket");
        try {
            S3Waiter s3Waiter = s3Client.waiter();
            CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3Client.createBucket(bucketRequest);

            // Wait until the bucket is created and print out the response
            HeadBucketRequest bucketRequestWait = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            WaiterResponse<HeadBucketResponse> waiterResponse = s3Waiter.waitUntilBucketExists(bucketRequestWait);
            waiterResponse.matched().response().ifPresent(System.out::println);
        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
        return bucketName;
    }

    public void deleteS3Bucket(S3Client s3Client, String bucket) {
        System.out.println("Deleting S3 Error Reporting sample bucket");
        try {
            // To delete a bucket, all the objects in the bucket must be deleted first
            ListObjectsV2Request listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket).build();
            ListObjectsV2Response listObjectsV2Response;

            do {
                listObjectsV2Response = s3Client.listObjectsV2(listObjectsV2Request);
                for (S3Object s3Object : listObjectsV2Response.contents()) {
                    s3Client.deleteObject(DeleteObjectRequest.builder()
                            .bucket(bucket)
                            .key(s3Object.key())
                            .build());
                }

                listObjectsV2Request = ListObjectsV2Request.builder().bucket(bucket)
                        .continuationToken(listObjectsV2Response.nextContinuationToken())
                        .build();

            } while(listObjectsV2Response.isTruncated());
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket).build();
            s3Client.deleteBucket(deleteBucketRequest);

        } catch (S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
