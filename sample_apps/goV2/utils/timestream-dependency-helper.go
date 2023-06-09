package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/iam/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"

	"math/rand"
	"os"
	"runtime"
	"time"
)

/**

 */

const (
	POLICY_DOCUMENT = "{" +
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
		"}"

	SQS_POLICY_FORMAT = "{" +
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
		"}"

	ROLE_POLICY_FORMAT = "{" +
		"\"Version\":\"2012-10-17\"," +
		"\"Statement\":[" +
		"   {" +
		"       \"Effect\":\"Allow\"," +
		"       \"Principal\":{" +
		"            \"Service\": \"timestream.amazonaws.com\"" +
		"       }," +
		"       \"Action\":\"sts:AssumeRole\"" +
		"   }" +
		"  ]" +
		"}"
)

type TimestreamDependencyHelper struct {
	SnsSvc *sns.Client
	SqsSvc *sqs.Client
	S3Svc  *s3.Client
	IamSvc *iam.Client
	StsSvc *sts.Client
}

type NotificationMessage struct {
	ParseFlag         bool
	TopicArn          string `json:"TopicArn"`
	ReceiptHandle     string
	MessageAttributes struct {
		QueryArn struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"queryArn"`
		NotificationType struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"notificationType"`
	} `json:"MessageAttributes"`
	Type          string `json:"Type"`
	MessageString string `json:"Message"`
	Message       struct {
		Type                      string `json:"type"`
		Arn                       string `json:"arn"`
		NextInvocationEpochSecond int64  `json:"NextInvocationEpochSecond"`
		ScheduledQueryRunSummary  struct {
			FailureReason         string `json:"failureReason"`
			RunStatus             string `json:"runStatus"`
			InvocationEpochSecond int64  `json:"invocationEpochSecond"`
			TriggerTimeMillis     int64  `json:"triggerTimeMillis"`
			ErrorReportLocation   struct {
				S3ReportLocation struct {
					BucketName string `json:"bucketName"`
					ObjectKey  string `json:"objectKey"`
				} `json:"s3ReportLocation"`
				ExecutionStats map[string]interface{} `json:"executionStats"`
			} `json:"errorReportLocation"`
		} `json:"scheduledQueryRunSummary"`
	}
}

type Resource struct {
	Type              string
	Identifier        string
	AdditionalDetails string
}

func CleanUp(timestreamBuilder TimestreamBuilder, timestreamDependencyHelper TimestreamDependencyHelper,
	databaseName string, tableName string, s3BucketName string) {
	fmt.Println("Cleaning created resources as part of this sample..")
	if s3BucketName != "" {
		err := timestreamDependencyHelper.DeleteS3Bucket(s3BucketName)
		if err != nil {
			fmt.Printf("\nFailed to delete s3BucketName='%s', please delete it manually\n", s3BucketName)
		}
	}

	if tableName != "" {
		fmt.Printf("\nDeleting table %s\n", tableName)
		timestreamBuilder.DeleteTable(databaseName, tableName)
	}

	if databaseName != "" {
		fmt.Printf("\nDeleting database %s\n", databaseName)
		timestreamBuilder.DeleteDatabase(databaseName)
	}
}

func JsonMarshalIgnoreError(input interface{}) string {
	jsonString, _ := json.Marshal(input)
	return fmt.Sprintf("%s\n", string(jsonString))
}

func HandleError(err error, errorMessage string, exitFlag bool) {
	if err != nil {
		_, fn, line, _ := runtime.Caller(1)
		fmt.Printf("[error] %s:%d %s with error %v\n", fn, line, errorMessage, err)
		if exitFlag == true {
			panic(err)
		}
	}

}

func FileExists(name string) (bool, error) {
	_, err := os.Stat(name)
	if err == nil {
		return true, nil
	}
	return false, err
}

func GenerateRandomStringWithSize(size int) string {
	rand.Seed(time.Now().UnixNano())
	alphaNumericList := []rune("abcdefghijklmnopqrstuvwxyz0123456789")
	randomPrefix := make([]rune, size)
	for i := range randomPrefix {
		randomPrefix[i] = alphaNumericList[rand.Intn(len(alphaNumericList))]
	}
	return string(randomPrefix)
}

func parseNotificationMessage(message string) (NotificationMessage, error) {
	var notificationMessage NotificationMessage
	err := json.Unmarshal([]byte(message), &notificationMessage)
	if err != nil {
		HandleError(err, "", false)
		return notificationMessage, err
	}
	err = json.Unmarshal([]byte(notificationMessage.MessageString), &notificationMessage.Message)
	if err != nil {
		HandleError(err, "", false)
		return notificationMessage, err
	}
	notificationMessage.MessageString = ""
	return notificationMessage, err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) CreateSnsTopic(topicName string) (string, error) {
	createTopicInput := &sns.CreateTopicInput{
		Name:       aws.String(topicName),
		Attributes: map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "false"},
	}

	createTopicOutput, err := timestreamDependencyHelper.SnsSvc.CreateTopic(context.TODO(), createTopicInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("Creating SNS topic is successful")
		return *createTopicOutput.TopicArn, nil
	}
	return "", err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) CreateSqsQueue(queueName string) (string, error) {

	createQueueInput := &sqs.CreateQueueInput{
		QueueName:  aws.String(queueName),
		Attributes: map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "false"},
	}

	createQueueOutput, err := timestreamDependencyHelper.SqsSvc.CreateQueue(context.TODO(), createQueueInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("Create SQS Queue with name %s is successful\n", queueName)
		return *createQueueOutput.QueueUrl, nil
	}

	return "", err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) GetSqsQueryUrl(queueName string) (string, error) {

	getQueueUrlInput := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName),
	}

	getQueueUrlOutput, err := timestreamDependencyHelper.SqsSvc.GetQueueUrl(context.TODO(), getQueueUrlInput)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "ConflictException":
				fmt.Println("ConflictException", apiErr.Error())
				return "", nil
			default:
				fmt.Println(apiErr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
		return "", err
	} else {
		return *getQueueUrlOutput.QueueUrl, nil
	}
}

func (timestreamDependencyHelper TimestreamDependencyHelper) DeleteSqsQueue(queueUrl string) error {

	deleteQueueInput := &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueUrl),
	}

	_, err := timestreamDependencyHelper.SqsSvc.DeleteQueue(context.TODO(), deleteQueueInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("delete_sqs_queue is successful")
	}

	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) ReceiveMessage(queueUrl string) (NotificationMessage, error) {
	receiveMessageInput := &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueUrl),
		WaitTimeSeconds:     20,
		MaxNumberOfMessages: 1,
	}
	receiveMessageOutput, err := timestreamDependencyHelper.SqsSvc.ReceiveMessage(context.TODO(), receiveMessageInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("received Message successfully")
		messages := receiveMessageOutput.Messages
		if len(messages) > 0 {
			messageBody := messages[0].Body
			response, err := parseNotificationMessage(*messageBody)
			if err == nil {
				response.ReceiptHandle = *messages[0].ReceiptHandle
				response.ParseFlag = true
				return response, nil
			}

		}
	}
	return NotificationMessage{}, err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) DeleteMessage(queueUrl string, receiptHandle string) error {
	deleteMessageInput := &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(queueUrl),
		ReceiptHandle: aws.String(receiptHandle),
	}
	_, err := timestreamDependencyHelper.SqsSvc.DeleteMessage(context.TODO(), deleteMessageInput)
	if err != nil {
		fmt.Printf("sqs DeleteMessage is failed with error: %s\n", err.Error())
	} else {
		fmt.Println("sqs DeleteMessage is successful")
	}
	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) DeleteSnsTopic(topicArn string) error {

	deleteTopicInput := &sns.DeleteTopicInput{
		TopicArn: aws.String(topicArn),
	}

	_, err := timestreamDependencyHelper.SnsSvc.DeleteTopic(context.TODO(), deleteTopicInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("DeleteTopic is successful")
	}
	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) GetSqsQueueArn(queueUrl string) (string, error) {

	createGetQueueAttributesInput := &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueUrl),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeName("QueueArn")},
	}

	createGetQueueAttributesOutput, err := timestreamDependencyHelper.SqsSvc.GetQueueAttributes(context.TODO(), createGetQueueAttributesInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("GetQueueAttributes is successful : %s", JsonMarshalIgnoreError(&createGetQueueAttributesOutput.Attributes))
		return createGetQueueAttributesOutput.Attributes["QueueArn"], nil
	}

	return "", err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) SetSqsAccessPolicy(queueUrl string, topicArn string, queueArn string) error {

	setQueueAttributesInput := &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(queueUrl),
		Attributes: map[string]string{"Policy": fmt.Sprintf(SQS_POLICY_FORMAT, topicArn, queueArn, topicArn)},
	}

	setQueueAttributesOutput, err := timestreamDependencyHelper.SqsSvc.SetQueueAttributes(context.TODO(), setQueueAttributesInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("setQueueAttributesOutput is successful : %s",
			JsonMarshalIgnoreError(setQueueAttributesOutput.ResultMetadata))
	}
	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) getIamRole(roleName string) (string, error) {

	getRoleInput := &iam.GetRoleInput{
		RoleName: aws.String(roleName),
	}

	getRoleOutput, err := timestreamDependencyHelper.IamSvc.GetRole(context.TODO(), getRoleInput)
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NoSuchEntityException":
				fmt.Println("NoSuchEntityException", apiErr.Error())
				return "", nil
			default:
				fmt.Println(apiErr.Error())
			}
		} else {
			fmt.Println(err.Error())
		}
		return "", err
	}

	fmt.Printf("getIamRole is successful : %s\n", JsonMarshalIgnoreError(&getRoleOutput.Role))
	return *getRoleOutput.Role.Arn, nil
}

func (timestreamDependencyHelper TimestreamDependencyHelper) CreateIamRole(roleName string) (string, error) {

	roleArn, err := timestreamDependencyHelper.getIamRole(roleName)
	if err == nil && roleArn != "" {
		return roleArn, err
	}
	createRoleInput := &iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		Description:              aws.String("Created using the AWS SDK for Go"),
		AssumeRolePolicyDocument: aws.String(ROLE_POLICY_FORMAT),
	}

	createRoleOutput, err := timestreamDependencyHelper.IamSvc.CreateRole(context.TODO(), createRoleInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("createRoleOutput is successful : %s\n", JsonMarshalIgnoreError(&createRoleOutput.Role))
		return *createRoleOutput.Role.Arn, nil
	}

	return "", err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) AttachIamPolicy(roleName string, policyArn string) error {

	attachRolePolicyInput := &iam.AttachRolePolicyInput{
		RoleName:  aws.String(roleName),
		PolicyArn: aws.String(policyArn),
	}

	_, err := timestreamDependencyHelper.IamSvc.AttachRolePolicy(context.TODO(), attachRolePolicyInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("attaching RolePolicy is successful\n")
	}

	return err

}

func (timestreamDependencyHelper TimestreamDependencyHelper) DetachIamPolicy(roleName string, policyArn string) error {

	detachRolePolicyInput := &iam.DetachRolePolicyInput{
		RoleName:  aws.String(roleName),
		PolicyArn: aws.String(policyArn),
	}

	_, err := timestreamDependencyHelper.IamSvc.DetachRolePolicy(context.TODO(), detachRolePolicyInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("DetachRolePolicy is successful")
	}

	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) DeleteIamPolicy(policyArn string) error {

	deletePolicyInput := &iam.DeletePolicyInput{
		PolicyArn: aws.String(policyArn),
	}

	_, err := timestreamDependencyHelper.IamSvc.DeletePolicy(context.TODO(), deletePolicyInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("DeletePolicy is successful")
	}

	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) DeleteIamRole(roleName string) error {

	deleteRoleInput := &iam.DeleteRoleInput{
		RoleName: aws.String(roleName),
	}

	_, err := timestreamDependencyHelper.IamSvc.DeleteRole(context.TODO(), deleteRoleInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("delete_IAM_role is successful")
	}

	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) CreateIamPolicy(policyName string) (string, error) {

	createPolicyInput := &iam.CreatePolicyInput{
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(POLICY_DOCUMENT),
	}

	createPolicyOutput, err := timestreamDependencyHelper.IamSvc.CreatePolicy(context.TODO(), createPolicyInput)
	if err != nil {
		var ENTITY_ALREADY_EXISTS *types.EntityAlreadyExistsException
		if errors.As(err, &ENTITY_ALREADY_EXISTS) {
			fmt.Printf("EntityAlreadyExistsException : %s\n", err.Error())
			return timestreamDependencyHelper.getPolicyArn(policyName), nil
		} else {
			fmt.Printf("Error while creating policy : %s\n", err.Error())
			return "", err
		}
	} else {
		fmt.Printf("CreatePolicy is successful : %v\n", createPolicyOutput.ResultMetadata)
		return *createPolicyOutput.Policy.Arn, nil
	}
}

func (timestreamDependencyHelper TimestreamDependencyHelper) SubscribeToSnsTopic(topicArn string, queueArn string) (string, error) {

	createSubscribeInput := &sns.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
	}

	createQueueOutput, err := timestreamDependencyHelper.SnsSvc.Subscribe(context.TODO(), createSubscribeInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("Subscribe to SNS Topic %s using SQS Queue %s is successful\n", topicArn, queueArn)
		return *createQueueOutput.SubscriptionArn, nil
	}

	return "", err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) UnsubscribeToSnsTopic(subscriptionArn string) error {

	unsubscribeInput := &sns.UnsubscribeInput{
		SubscriptionArn: aws.String(subscriptionArn),
	}

	_, err := timestreamDependencyHelper.SnsSvc.Unsubscribe(context.TODO(), unsubscribeInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("unsubscribe_to_sns_topic is successful")
	}

	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) CreateS3Bucket(bucketName string, region string) error {

	fmt.Printf("Creating S3 Bucket with name %s in region %s\n", bucketName, region)

	createBucketInput := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}
	if region != "us-east-1" {
		createBucketInput.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}
	createBucketOutput, err := timestreamDependencyHelper.S3Svc.CreateBucket(context.TODO(), createBucketInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("Create S3 Bucket is successful : %s", *createBucketOutput.Location)
	}

	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) S3BucketExists(bucketName string) (bool, error) {
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	}
	var RESOURCE_NOT_FOUND *s3types.NotFound
	_, err := timestreamDependencyHelper.S3Svc.HeadBucket(context.TODO(), input)
	if err != nil {
		if errors.As(err, &RESOURCE_NOT_FOUND) {
			fmt.Printf("Bucket %s does not exists\n", bucketName)
			return false,nil
		}
		fmt.Printf("Failed to locate Bucket %s with error %+v\n", bucketName, err)
		return false,err
	}
	return true,nil
}

func (timestreamDependencyHelper TimestreamDependencyHelper) DeleteS3Bucket(bucketName string) error {

	bucketExistsFlag, err := timestreamDependencyHelper.S3BucketExists(bucketName)
	if bucketExistsFlag == false {
		return err
	}
	// Setup BatchDeleteIterator to iterate through a list of objects.
	deleteObject := func(bucket, key, versionId *string) {
		fmt.Printf("Object: %s/%s\n", *key, aws.ToString(versionId))
		_, err := timestreamDependencyHelper.S3Svc.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket:    bucket,
			Key:       key,
			VersionId: versionId,
		})
		if err != nil {
			fmt.Printf("Failed to delete object: %v", err)
		}
	}

	in := &s3.ListObjectsV2Input{Bucket: aws.String(bucketName)}
	for {
		out, err := timestreamDependencyHelper.S3Svc.ListObjectsV2(context.TODO(), in)
		if err != nil {
			fmt.Printf("Failed to list objects: %v", err)
		}
		for _, item := range out.Contents {
			deleteObject(&bucketName, item.Key, nil)
		}
		if out.IsTruncated {
			in.ContinuationToken = out.NextContinuationToken
		} else {
			break
		}
	}

	inVer := &s3.ListObjectVersionsInput{Bucket: aws.String(bucketName)}
	for {
		out, err := timestreamDependencyHelper.S3Svc.ListObjectVersions(context.TODO(), inVer)
		if err != nil {
			fmt.Printf("Failed to list version objects: %v", err)
		}

		for _, item := range out.DeleteMarkers {
			deleteObject(&bucketName, item.Key, item.VersionId)
		}

		for _, item := range out.Versions {
			deleteObject(&bucketName, item.Key, item.VersionId)
		}

		if out.IsTruncated {
			inVer.VersionIdMarker = out.NextVersionIdMarker
			inVer.KeyMarker = out.NextKeyMarker
		} else {
			break
		}
	}

	fmt.Printf("Deleted object(s) from bucket: %s\n", bucketName)
	deleteBucketInput := &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	}
	_, err = timestreamDependencyHelper.S3Svc.DeleteBucket(context.TODO(), deleteBucketInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("delete S3 Bucket is successful")
	}
	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) getAccountId() (string, error) {

	getCallerIdentityInput := &sts.GetCallerIdentityInput{}
	getCallerIdentityOutput, err := timestreamDependencyHelper.StsSvc.GetCallerIdentity(context.TODO(), getCallerIdentityInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return "", nil
	} else {
		fmt.Printf("get_account_id is successful : %s\n", JsonMarshalIgnoreError(getCallerIdentityOutput))
		return *getCallerIdentityOutput.Account, nil
	}
}

func (timestreamDependencyHelper TimestreamDependencyHelper) getPolicyArn(policyName string) string {
	accountId, _ := timestreamDependencyHelper.getAccountId()
	return fmt.Sprintf("arn:aws:iam::%s:policy/%s", accountId, policyName)
}

func (timestreamDependencyHelper TimestreamDependencyHelper) listReportFilesInS3(s3ErrorReportBucketName string,
	errorReportPrefix string) ([]s3types.Object, error) {
	listObjectsInput := &s3.ListObjectsV2Input{
		Bucket: aws.String(s3ErrorReportBucketName),
		Prefix: aws.String(errorReportPrefix),
	}
	var reports []s3types.Object
	for {
		listObjectsOutput, err := timestreamDependencyHelper.S3Svc.ListObjectsV2(context.TODO(), listObjectsInput)
		if err != nil {
			fmt.Printf("Failed to listObjects Error: %s\n", err.Error())
			return reports, err
		}
		reports = append(reports, listObjectsOutput.Contents...)
		if listObjectsOutput.IsTruncated {
			listObjectsInput.ContinuationToken = listObjectsOutput.NextContinuationToken
		} else {
			break
		}
	}
	fmt.Printf("listObjectsOutput is successful : %+v\n", reports)
	return reports, nil

}

func (timestreamDependencyHelper TimestreamDependencyHelper) ParseS3ErrorReport(s3ErrorReportBucketName string,
	errorReportPrefix string) error {
	s3Objects, err := timestreamDependencyHelper.listReportFilesInS3(s3ErrorReportBucketName, errorReportPrefix)
	if err != nil {
		return err
	}
	fmt.Printf("s3Objects first object contents : %+v\n", s3Objects[0])
	err = timestreamDependencyHelper.readErrorReport(&s3Objects[0], s3ErrorReportBucketName)
	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) readErrorReport(s3Object *s3types.Object,
	s3ErrorReportBucketName string) error {
	getObjectInput := &s3.GetObjectInput{
		Key:    s3Object.Key,
		Bucket: aws.String(s3ErrorReportBucketName),
	}
	getObjectOutput, err := timestreamDependencyHelper.S3Svc.GetObject(context.TODO(), getObjectInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return err
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(getObjectOutput.Body)
	objectContents := buf.String()
	fmt.Printf("S3 Error Report Contents : %+v\n", objectContents)
	return nil
}
