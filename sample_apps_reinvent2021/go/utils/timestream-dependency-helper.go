package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
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
	SnsSvc *sns.SNS
	SqsSvc *sqs.SQS
	S3Svc  *s3.S3
	IamSvc *iam.IAM
	StsSvc *sts.STS
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
	Type string
	Identifier  string
	AdditionalDetails string
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
		Attributes: map[string]*string{"FifoTopic": aws.String("true"), "ContentBasedDeduplication": aws.String("false")},
	}

	createTopicOutput, err := timestreamDependencyHelper.SnsSvc.CreateTopic(createTopicInput)

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
		Attributes: map[string]*string{"FifoQueue": aws.String("true"), "ContentBasedDeduplication": aws.String("false")},
	}

	createQueueOutput, err := timestreamDependencyHelper.SqsSvc.CreateQueue(createQueueInput)
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

	getQueueUrlOutput, err := timestreamDependencyHelper.SqsSvc.GetQueueUrl(getQueueUrlInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case timestreamquery.ErrCodeConflictException:
				fmt.Println(timestreamquery.ErrCodeConflictException, aerr.Error())
				return "", nil
			default:
				fmt.Println(aerr.Error())
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

	_, err := timestreamDependencyHelper.SqsSvc.DeleteQueue(deleteQueueInput)
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
		WaitTimeSeconds:     aws.Int64(20),
		MaxNumberOfMessages: aws.Int64(1),
	}
	receiveMessageOutput, err := timestreamDependencyHelper.SqsSvc.ReceiveMessage(receiveMessageInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("received Message successfully")
		messages := receiveMessageOutput.Messages
		if messages != nil && len(messages) > 0 {
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
	_, err := timestreamDependencyHelper.SqsSvc.DeleteMessage(deleteMessageInput)
	if err != nil {
		fmt.Printf("sqs DeleteMessage is failed with error: %s", err.Error())
	} else {
		fmt.Println("sqs DeleteMessage is successful")
	}
	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) DeleteSnsTopic(topicArn string) error {

	deleteTopicInput := &sns.DeleteTopicInput{
		TopicArn: aws.String(topicArn),
	}

	_, err := timestreamDependencyHelper.SnsSvc.DeleteTopic(deleteTopicInput)
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
		AttributeNames: []*string{aws.String("QueueArn")},
	}

	createGetQueueAttributesOutput, err := timestreamDependencyHelper.SqsSvc.GetQueueAttributes(createGetQueueAttributesInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("GetQueueAttributes is successful : %s", createGetQueueAttributesOutput.String())
		return *createGetQueueAttributesOutput.Attributes["QueueArn"], nil
	}

	return "", err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) SetSqsAccessPolicy(queueUrl string, topicArn string, queueArn string) error {

	setQueueAttributesInput := &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(queueUrl),
		Attributes: map[string]*string{"Policy": aws.String(fmt.Sprintf(SQS_POLICY_FORMAT, topicArn, queueArn, topicArn))},
	}

	setQueueAttributesOutput, err := timestreamDependencyHelper.SqsSvc.SetQueueAttributes(setQueueAttributesInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("setQueueAttributesOutput is successful : %s", setQueueAttributesOutput.GoString())
	}
	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) GetIamRole(roleName string) (string, error) {

	getRoleInput := &iam.GetRoleInput{
		RoleName: aws.String(roleName),
	}

	getRoleOutput, err := timestreamDependencyHelper.IamSvc.GetRole(getRoleInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case iam.ErrCodeNoSuchEntityException:
				fmt.Println(iam.ErrCodeNoSuchEntityException, aerr.Error())
				return "", nil
			default:
				fmt.Printf("Error: %s\n", aerr.Error())
			}
		} else {
			fmt.Printf("Error: %s\n", err.Error())
		}
		return "", err
	}

	fmt.Println("getIamRole is successful : %s\n", getRoleOutput.GoString())
	return *getRoleOutput.Role.Arn, nil
}

func (timestreamDependencyHelper TimestreamDependencyHelper) CreateIamRole(roleName string) (string, error) {

	roleArn, err := timestreamDependencyHelper.GetIamRole(roleName)
	if err == nil && roleArn != "" {
		return roleArn, err
	}
	createRoleInput := &iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		Description:              aws.String("Created using the AWS SDK for Go"),
		AssumeRolePolicyDocument: aws.String(ROLE_POLICY_FORMAT),
	}

	createRoleOutput, err := timestreamDependencyHelper.IamSvc.CreateRole(createRoleInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("createRoleOutput is successful : %s\n", createRoleOutput.GoString())
		getRoleInput := &iam.GetRoleInput{RoleName: createRoleOutput.Role.RoleName}
		err = timestreamDependencyHelper.IamSvc.WaitUntilRoleExists(getRoleInput)
		if err != nil {
			return "", err
		}
		return *createRoleOutput.Role.Arn, nil
	}

	return "", err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) AttachIamPolicy(roleName string, policyArn string) error {

	attachRolePolicyInput := &iam.AttachRolePolicyInput{
		RoleName:  aws.String(roleName),
		PolicyArn: aws.String(policyArn),
	}

	_, err := timestreamDependencyHelper.IamSvc.AttachRolePolicy(attachRolePolicyInput)
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

	_, err := timestreamDependencyHelper.IamSvc.DetachRolePolicy(detachRolePolicyInput)
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

	_, err := timestreamDependencyHelper.IamSvc.DeletePolicy(deletePolicyInput)
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

	_, err := timestreamDependencyHelper.IamSvc.DeleteRole(deleteRoleInput)
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

	createPolicyOutput, err := timestreamDependencyHelper.IamSvc.CreatePolicy(createPolicyInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case iam.ErrCodeEntityAlreadyExistsException:
				fmt.Println(iam.ErrCodeEntityAlreadyExistsException, aerr.Error())
				return timestreamDependencyHelper.getPolicyArn(policyName), nil
			default:
				fmt.Printf("Error: %s\n", aerr.Error())
			}
		} else {
			fmt.Printf("Error: %s\n", err.Error())
		}
		return "", err
	} else {
		fmt.Printf("CreatePolicy is successful : %s\n", createPolicyOutput.GoString())
		return *createPolicyOutput.Policy.Arn, nil
	}
}

func (timestreamDependencyHelper TimestreamDependencyHelper) SubscribeToSnsTopic(topicArn string, queueArn string) (string, error) {

	createSubscribeInput := &sns.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
	}

	createQueueOutput, err := timestreamDependencyHelper.SnsSvc.Subscribe(createSubscribeInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("Create SQS Queue is successful")
		return *createQueueOutput.SubscriptionArn, nil
	}

	return "", err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) UnsubscribeToSnsTopic(subscriptionArn string) error {

	unsubscribeInput := &sns.UnsubscribeInput{
		SubscriptionArn: aws.String(subscriptionArn),
	}

	_, err := timestreamDependencyHelper.SnsSvc.Unsubscribe(unsubscribeInput)
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
		createBucketInput.CreateBucketConfiguration = &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(region),
		}
	}
	createBucketOutput, err := timestreamDependencyHelper.S3Svc.CreateBucket(createBucketInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("Create S3 Bucket is successful : %v\n", *createBucketOutput)
	}

	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) DeleteS3Bucket(bucketName string) error {

	// Setup BatchDeleteIterator to iterate through a list of objects.
	iter := s3manager.NewDeleteListIterator(timestreamDependencyHelper.S3Svc, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})

	// Traverse iterator deleting each object
	if err := s3manager.NewBatchDeleteWithClient(timestreamDependencyHelper.S3Svc).Delete(aws.BackgroundContext(), iter); err != nil {
		fmt.Printf("Unable to delete objects from bucket %q, %v\n", bucketName, err)
		return err
	}
	fmt.Printf("Deleted object(s) from bucket: %s\n", bucketName)
	deleteBucketInput := &s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	}
	_, err := timestreamDependencyHelper.S3Svc.DeleteBucket(deleteBucketInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("delete S3 Bucket is successful")
	}
	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) getAccountId() (string, error) {

	getCallerIdentityInput := &sts.GetCallerIdentityInput{}
	getCallerIdentityOutput, err := timestreamDependencyHelper.StsSvc.GetCallerIdentity(getCallerIdentityInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return "", nil
	} else {
		fmt.Printf("get_account_id is successful : %s\n", getCallerIdentityOutput)
		return *getCallerIdentityOutput.Account, nil
	}
}

func (timestreamDependencyHelper TimestreamDependencyHelper) getPolicyArn(policyName string) string {
	accountId, _ := timestreamDependencyHelper.getAccountId()
	return fmt.Sprintf("arn:aws:iam::%s:policy/%s", accountId, policyName)
}

func (timestreamDependencyHelper TimestreamDependencyHelper) listReportFilesInS3(s3ErrorReportBucketName string,
	errorReportPrefix string) ([]*s3.Object, error) {
	listObjectsInput := &s3.ListObjectsInput{
		Bucket: aws.String(s3ErrorReportBucketName),
		Prefix: aws.String(errorReportPrefix),
	}
	listObjectsOutput, err := timestreamDependencyHelper.S3Svc.ListObjects(listObjectsInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		return nil, nil
	} else {
		fmt.Printf("listObjectsOutput is successful : %s\n", listObjectsOutput)
		return listObjectsOutput.Contents, nil
	}
}

func (timestreamDependencyHelper TimestreamDependencyHelper) ParseS3ErrorReport(s3ErrorReportBucketName string,
	errorReportPrefix string) error {
	s3Objects, err := timestreamDependencyHelper.listReportFilesInS3(s3ErrorReportBucketName, errorReportPrefix)
	if err != nil {
		return err
	}
	fmt.Printf("s3Objects first object contents : %+v\n", s3Objects[0])
	err = timestreamDependencyHelper.readErrorReport(s3Objects[0], s3ErrorReportBucketName)
	return err
}

func (timestreamDependencyHelper TimestreamDependencyHelper) readErrorReport(s3Object *s3.Object,
	s3ErrorReportBucketName string) error {
	getObjectInput := &s3.GetObjectInput{
		Key:    s3Object.Key,
		Bucket: aws.String(s3ErrorReportBucketName),
	}
	getObjectOutput, err := timestreamDependencyHelper.S3Svc.GetObject(getObjectInput)
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
