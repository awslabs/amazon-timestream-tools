package com.amazonaws.services.timestream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
                    "            \"s3:GetObject\"," +
                    "            \"s3:List*\"," +
                    "            \"s3:Put*\"" +
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

    public String createSQSQueue(SqsClient sqsClient, String queueName) {
        System.out.println("Creating SQS Queue " + queueName);
        CreateQueueResponse result = null;
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();

            result = sqsClient.createQueue(request);
            return result.queueUrl();
        } catch (Exception e) {
            System.out.println("SQS Queue creation failed: " + e);
            throw e;
        }
    }

    public static void deleteSQSQueue(SqsClient sqsClient, String queueName) {
        System.out.println("Deleting SQS Queue " + queueName);
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
        System.out.println("Getting Queue Arn for " + queueUrl);
        try {
            List<QueueAttributeName> atts = new ArrayList();
            atts.add(QueueAttributeName.QUEUE_ARN);
            GetQueueAttributesResponse response = sqsClient.getQueueAttributes(GetQueueAttributesRequest.builder()
                    .attributeNames(atts).queueUrl(queueUrl).build());
            String queue_arn = response.attributes().get(QueueAttributeName.QUEUE_ARN);
            System.out.println("Queue Arn: " + queue_arn);
            return queue_arn;
        } catch (Exception e) {
            System.out.println("Get Queue Arn failed: " + e);
            throw e;
        }
    }

    public String subscribeToSnsTopic(SnsClient snsClient, String topicArn, String queueArn) {
        System.out.println("Subscribing to Sns Topic " + topicArn);
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
        System.out.println("Unsubscribing to Sns Arn " + subscriptionArn);
        try {
            snsClient.unsubscribe(UnsubscribeRequest.builder().subscriptionArn(subscriptionArn).build());
        } catch (SnsException e) {
            System.out.println("Unsubscribe to Sns Arn failed: " + e);
            //do not throw exception here, because we want the following cleanups executes
        }
    }

    public void setSqsAccessPolicy(SqsClient sqsClient, String queueUrl, String topicArn, String queueArn) {
        System.out.println("Setting access policy for SQS queue " + queueUrl);
        try {
            Map<QueueAttributeName, String> attributes = new HashMap<>();
            QueueAttributeName policy = QueueAttributeName.POLICY;
            attributes.put(policy, String.format(SQS_POLICY_FORMAT, topicArn, queueArn, topicArn));
            SetQueueAttributesRequest request = SetQueueAttributesRequest.builder()
                    .queueUrl(queueUrl)
                    .attributes(attributes).build();
            sqsClient.setQueueAttributes(request);
        } catch (Exception e) {
            System.out.println("Setting access policy for SQS queue failed: " + e);
            throw e;
        }
    }

    public JsonObject receiveMessage(String queueUrl, SqsClient sqsClient) {
        System.out.println("Receiving messages from " + queueUrl);
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
        System.out.println("Deleting message from " + queueUrl);
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

    public String createSNSTopic(SnsClient snsClient, String topicName) {
        System.out.println("Creating SNS Topic " + topicName);
        try {
            CreateTopicResponse result = null;
            CreateTopicRequest request = CreateTopicRequest.builder()
                    .name(topicName)
                    .build();

            result = snsClient.createTopic(request);
            System.out.println("Topic Arn: " + result.topicArn());
            return result.topicArn();
        } catch (Exception e) {
            System.out.println("Creating SNS Topic failed: " + e);
            throw e;
        }
    }

    public void deleteSNSTopic(SnsClient snsClient, String topicArn) {
        System.out.println("Deleting SNS Topic " + topicArn);
        try {
            DeleteTopicRequest request = DeleteTopicRequest.builder()
                    .topicArn(topicArn)
                    .build();

            DeleteTopicResponse result = snsClient.deleteTopic(request);
            System.out.println("Successfully deleted SNS Topic, Status was " + result.sdkHttpResponse().statusCode());
        } catch (SnsException e) {
            System.out.println("SNS Topic deletion failed: " + e);
            //do not throw exception here, because we want the following cleanups executes
        }
    }

    public static String createIAMRole(IamClient iam, String rolename, String region) {
        System.out.println("Creating IAM role " + rolename );
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

            System.out.println("Successfully created IAM role: " + response.role().arn());
            return response.role().arn();
        } catch (Exception e) {
            System.out.println("IAM role creation failed: " + e);
            throw e;
        }
    }

    public static String deleteIAMRole(IamClient iam, String roleName) {
        System.out.println("Deleting IAM Role " + roleName);
        try {
            DeleteRoleRequest request = DeleteRoleRequest.builder()
                    .roleName(roleName)
                    .build();

            DeleteRoleResponse response = iam.deleteRole(request);
            System.out.println("Successfully deleted Role: " + roleName);
        } catch (IamException e) {
            System.out.println(e.awsErrorDetails().errorMessage());
            //do not throw exception here, because we want the following cleanups executes
        } catch (Exception e) {
            System.out.println("IAM role deletion failed: " + e);
            //do not throw exception here, because we want the following cleanups executes
        }
        return null;
    }

    public static String createIAMPolicy(IamClient iam, String policyName ) {
        System.out.println("Creating IAM Policy " + policyName);
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
        System.out.println("Deleting IAM Policy " + policyARN);
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

    public static void attachIAMRolePolicy(IamClient iam, String roleName, String policyArn ) {
        System.out.println("Attaching IAM Role Policy");
        try {
            ListAttachedRolePoliciesRequest request = ListAttachedRolePoliciesRequest.builder()
                    .roleName(roleName)
                    .build();

            ListAttachedRolePoliciesResponse response = iam.listAttachedRolePolicies(request);
            List<AttachedPolicy> attachedPolicies = response.attachedPolicies();

            // Ensure that the policy is not attached to this role
            String polArn = "";
            for (AttachedPolicy policy : attachedPolicies) {
                polArn = policy.policyArn();
                if (polArn.compareTo(policyArn) == 0) {
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
            //do not throw exception here, because we want the following cleanups executes
        }
    }

    public static String getServiceName(String region) {
        String rolePolicy;
        rolePolicy = "\"Service\":\"timestream." + region + ".amazonaws.com\"";
        return rolePolicy;
    }

    public String createS3Bucket(AmazonS3 s3Client, String bucketName) {
        System.out.println("Creating S3 bucket " + bucketName);
        if (s3Client.doesBucketExistV2(bucketName)) {
            System.out.format("Bucket %s already exists.\n", bucketName);
        } else {
            try {
                s3Client.createBucket(bucketName);
                System.out.println("Successfully created S3 bucket: " + bucketName);
            } catch (Exception e) {
                System.out.println("Failed to create S3 bucket " + e);
                return null;
            }
        }
        return bucketName;
    }

    public void deleteS3Bucket(AmazonS3 s3Client, String bucketName) {
        System.out.println("Deleting S3 bucket " + bucketName);
        ObjectListing objectListing;
        try {
            // Delete all objects from the bucket before deleting the bucket.
            objectListing = s3Client.listObjects(bucketName);
            while (true) {
                Iterator<S3ObjectSummary> objIter = objectListing.getObjectSummaries().iterator();
                while (objIter.hasNext()) {
                    s3Client.deleteObject(bucketName, objIter.next().getKey());
                }

                // If the bucket contains many objects, the listObjects() call
                // might not return all of the objects in the first listing. Check to
                // see whether the listing was truncated. If so, retrieve the next page of objects
                // and delete them.
                if (objectListing.isTruncated()) {
                    objectListing = s3Client.listNextBatchOfObjects(objectListing);
                } else {
                    break;
                }
            }

            // Delete the bucket
            s3Client.deleteBucket(bucketName);

        } catch (Exception e) {
            System.out.println("Deletion of S3 bucket failed: " + e.getMessage());
            // Do not throw from here since we need further clean-up actions
        }
    }

    public Bucket getBucket(AmazonS3 s3Client, String bucketName) {
        Bucket namedBucket = null;
        List<Bucket> buckets = s3Client.listBuckets();
        for (Bucket b : buckets) {
            if (b.getName().equals(bucketName)) {
                namedBucket = b;
            }
        }
        return namedBucket;
    }

    public List<S3Object> listObjectsInS3Bucket(AmazonS3 s3Client, String bucketName, String prefix) {
        System.out.println("Listing objects in S3 bucket " + bucketName);
        List<S3Object> s3Objects = new ArrayList<>();

        ListObjectsV2Result listObjectsV2Result;
        try {
            // Delete all objects from the bucket before deleting the bucket.
            listObjectsV2Result = s3Client.listObjectsV2(bucketName, prefix);
            while (true) {
                Iterator<S3ObjectSummary> objIter = listObjectsV2Result.getObjectSummaries().iterator();
                while (objIter.hasNext()) {
                    s3Objects.add(s3Client.getObject(bucketName, objIter.next().getKey()));
                }

                // If the bucket contains many objects, the listObjects() call
                // might not return all of the objects in the first listing. Check to
                // see whether the listing was truncated. If so, retrieve the next page of objects
                // and delete them.
                if (listObjectsV2Result.isTruncated()) {
                    ListObjectsV2Request listObjectsV2Request = new ListObjectsV2Request()
                            .withBucketName(bucketName)
                            .withPrefix(prefix)
                            .withContinuationToken(listObjectsV2Result.getNextContinuationToken());
                    listObjectsV2Result = s3Client.listObjectsV2(listObjectsV2Request);
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Listing and getting S3 objects in bucket failed: " + e.getMessage());
        }
        return s3Objects;
    }

    public JsonObject getS3ObjectJson(AmazonS3 s3Client, S3Object s3Object, String bucketName) {
        return JSON_PARSER.parse(s3Client.getObjectAsString(bucketName, s3Object.getKey())).getAsJsonObject();
    }

}
