package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"go_sample/utils"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
)

func main() {

	databaseName := utils.DATABASE_NAME
	tableName := utils.TABLE_NAME
	sqSampleAppTopicName := "sq_sample_app_topic.fifo"
	sqSampleAppQueueName := "sq_sample_app_queue.fifo"
	sqResultsDatabaseName := utils.SQ_DATABASE_NAME
	sqResultsTableName := utils.SQ_TABLE_NAME
	region := flag.String("region", "us-east-1", "region")
	csvFilePath := flag.String("csv_file_path", "../data/sample.csv", "sample csv file path to"+
		" ingest records into the table used for scheduled query")
	deleteResourcesFlag := flag.Bool("delete_resources_after_execution_flag", true, "true/false to"+
		" delete created resources after execution of this code")
	runInvalidScheduledQueryFlag := flag.Bool("run_invalid_scheduled_query_flag", false, "false/true to"+
		" run valid/invalid scheduledQuery E2E flow")
	flag.Parse()
	ok, err := utils.FileExists(*csvFilePath)
	if !ok {
		if errors.Is(err, os.ErrNotExist) {
			utils.HandleError(err, fmt.Sprintf("csv file path provided=%s doesn't exist. Please "+
				"provide with a valid csv file path\n", *csvFilePath), true)
		}
		utils.HandleError(err, fmt.Sprintf("csv file path provided=%s is invalid. Please "+
			"provide with a valid csv file path\n", *csvFilePath), true)
	}

	sess, err := utils.GetSession(*region)
	utils.HandleError(err, "Failed to start a new session", true)

	writeSvc := timestreamwrite.New(sess)
	querySvc := timestreamquery.New(sess)

	snsSvc := sns.New(sess)
	sqsSvc := sqs.New(sess)
	s3Svc := s3.New(sess, aws.NewConfig().WithRegion(*region))
	iamSvc := iam.New(sess)
	stsSvc := sts.New(sess)

	timestreamBuilder := utils.TimestreamBuilder{WriteSvc: writeSvc, QuerySvc: querySvc}
	timestreamDependencyHelper := utils.TimestreamDependencyHelper{
		SnsSvc: snsSvc, SqsSvc: sqsSvc, S3Svc: s3Svc, IamSvc: iamSvc, StsSvc: stsSvc}

	var topicArn, s3BucketName, queueUrl, queueArn, subscriptionArn, scheduledQueryArn string
	createdResourcesList := []utils.Resource{}


	// Make the bucket name unique by appending 5 random characters at the end
	s3BucketName = utils.SQ_ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX + utils.GenerateRandomStringWithSize(5)

	err = timestreamDependencyHelper.CreateS3Bucket(s3BucketName, *region)
	utils.HandleError(err, fmt.Sprintf("Failed to create S3Bucket %s ", s3BucketName), true)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "S3", Identifier: s3BucketName})
	//Create sns topic and sqs queue for scheduled query
	topicArn, err = timestreamDependencyHelper.CreateSnsTopic(sqSampleAppTopicName)
	utils.HandleError(err, fmt.Sprintf("Failed to create sns topic %s ", sqSampleAppTopicName), true)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "SNS_TOPIC", Identifier: topicArn})

	queueUrl, err = timestreamDependencyHelper.CreateSqsQueue(sqSampleAppQueueName)
	utils.HandleError(err, fmt.Sprintf("Failed to create sqs queue %s ", sqSampleAppQueueName), true)

	//Need to wait atleast 1s after creating queue to use it
	time.Sleep(2 * time.Second)

	queueArn, err = timestreamDependencyHelper.GetSqsQueueArn(queueUrl)
	utils.HandleError(err, "Failed to get sqs queue Arn ", true)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "SQS_QUEUE", Identifier: queueArn})

	subscriptionArn, _ = timestreamDependencyHelper.SubscribeToSnsTopic(topicArn, queueArn)
	utils.HandleError(err, "Failed to subscribe sqs queue to sns topic ", true)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "SNS_SUBSCRIPTION", AdditionalDetails:
		fmt.Sprintf("TOPIC_ARN='%s' QUEUE_ARN='%s'",topicArn, queueArn), Identifier: subscriptionArn})


	err = timestreamDependencyHelper.SetSqsAccessPolicy(queueUrl, topicArn, queueArn)
	utils.HandleError(err, "Failed to set sqs policy ", true)

	roleArn, err := timestreamDependencyHelper.CreateIamRole(utils.ROLE_NAME)
	utils.HandleError(err, fmt.Sprintf("Failed to create iam role with name %s ", utils.ROLE_NAME), true)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "IAM_ROLE", Identifier: roleArn})

	policyArn, _ := timestreamDependencyHelper.CreateIamPolicy(utils.POLICY_NAME)
	utils.HandleError(err, fmt.Sprintf("Failed to create iam policy with name %s ", utils.POLICY_NAME), true)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "IAM_POLICY", Identifier: policyArn})

	timestreamDependencyHelper.AttachIamPolicy(utils.ROLE_NAME, policyArn)
	utils.HandleError(err, fmt.Sprintf("Failed to attach iam role with name %s to the policy with name %s ",
		utils.ROLE_NAME, policyArn), true)

	fmt.Println("Waiting for 15 seconds for newly created role to become active")
	time.Sleep(15 * time.Second)

	//Scheduled Query Activities

	// Create database.
	err = timestreamBuilder.CreateDatabase(databaseName)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "TIMESTREAM_DATABASE", Identifier: databaseName})

	err = timestreamBuilder.CreateTable(databaseName, tableName, s3BucketName)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "TIMESTREAM_TABLE", Identifier: tableName})

	//Create database and table to store scheduled query results
	err = timestreamBuilder.CreateDatabase(sqResultsDatabaseName)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "TIMESTREAM_DATABASE", Identifier: sqResultsDatabaseName})

	err = timestreamBuilder.CreateTable(sqResultsDatabaseName, sqResultsTableName, s3BucketName)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "TIMESTREAM_TABLE", Identifier: sqResultsTableName})

	//err = timestreamBuilder.write_sample_records(*databaseName, *tableName)
	timestreamBuilder.IngestRecordsFromCsv(*csvFilePath, databaseName, tableName)

	/* Switch between Valid and Invalid Query to test Happy-case and Failure scenarios */
	if *runInvalidScheduledQueryFlag == false {
		fmt.Println("Creating a valid Scheduled Query Flow")
		scheduledQueryArn, err = timestreamBuilder.CreateValidScheduledQuery(topicArn, roleArn, s3BucketName, sqResultsDatabaseName, sqResultsTableName, databaseName, tableName)
	} else {
		fmt.Println("Creating a in-valid Scheduled Query Flow")
		scheduledQueryArn, err = timestreamBuilder.CreateInvalidScheduledQuery(topicArn, roleArn, s3BucketName, sqResultsDatabaseName, sqResultsTableName)
	}
	utils.HandleError(err, "Failed to create scheduled query ", true)

	scheduledQueries, err := timestreamBuilder.ListScheduledQueries()
	if err == nil {
		fmt.Printf("Total scheduledQueries size : %d\n", len(scheduledQueries))
	}

	err = timestreamBuilder.DescribeScheduledQuery(scheduledQueryArn)
	utils.HandleError(err, fmt.Sprintf("Failed to describe scheduled query with scheduledQueryArn %s ", scheduledQueryArn), false)

	err = timestreamBuilder.ExecuteScheduledQuery(scheduledQueryArn, time.Now())
	utils.HandleError(err, fmt.Sprintf("Failed to execute scheduled query with scheduledQueryArn %s ", scheduledQueryArn), false)

	err = timestreamBuilder.DescribeScheduledQuery(scheduledQueryArn)
	utils.HandleError(err, fmt.Sprintf("Failed to describe scheduled query with scheduledQueryArn %s ", scheduledQueryArn), false)

	// Sleep for 65 seconds to let ScheduledQuery run
	fmt.Printf("Waiting for 65 seconds for automatic ScheduledQuery executions & notifications from queueUrl %s", queueUrl)
	time.Sleep(65 * time.Second)

	didQuerySucceedManually := false
	wasQueryTriggeredAsExpected := false
	queryFailed := false

	for i := 0; i < 10; i++ {
		response, err := timestreamDependencyHelper.ReceiveMessage(queueUrl)
		fmt.Printf("response %+v, err %s", response, err)

		if response.ParseFlag != true {
			continue
		}
		timestreamDependencyHelper.DeleteMessage(queueUrl, response.ReceiptHandle)

		switch response.MessageAttributes.NotificationType.Value {
		case utils.SCHEDULED_QUERY_CREATING:
			fallthrough
		case utils.SCHEDULED_QUERY_CREATED:
			break
		case utils.MANUAL_TRIGGER_SUCCESS:
			fmt.Println("Manual execution of Scheduled Query succeeded")
			didQuerySucceedManually = true
			break
		case utils.AUTO_TRIGGER_SUCCESS:
			fmt.Println("Scheduled Query was triggered as expected. Now triggering another run manually")
			wasQueryTriggeredAsExpected = true
			break
		case utils.AUTO_TRIGGER_FAILURE:
			fallthrough
		case utils.MANUAL_TRIGGER_FAILURE:
			queryFailed = true
			fmt.Printf("Failure Reason from SQS Notification:: %s\n", response.Message.ScheduledQueryRunSummary.FailureReason)
			fmt.Printf("ErrorReportLocation Details: %+v\n", response.Message.ScheduledQueryRunSummary.ErrorReportLocation)
			s3BucketName := response.Message.ScheduledQueryRunSummary.ErrorReportLocation.S3ReportLocation.BucketName
			objectKey := response.Message.ScheduledQueryRunSummary.ErrorReportLocation.S3ReportLocation.ObjectKey
			if s3BucketName != "" && objectKey != "" {
				timestreamDependencyHelper.ParseS3ErrorReport(s3BucketName, objectKey)
			}
		case utils.SCHEDULED_QUERY_DELETED:
			fallthrough
		case utils.SCHEDULED_QUERY_UPDATED:
			fallthrough
		default:
			fmt.Printf("response = %+v\n", response)
		}

		if (didQuerySucceedManually && wasQueryTriggeredAsExpected) || queryFailed {
			break
		}

	}

	OUTPUT_FILE := "output.log"
	var f *os.File
	if OUTPUT_FILE != "" {
		var ferr error
		f, ferr = os.Create(OUTPUT_FILE)
		utils.HandleError(ferr, "", true)
		defer f.Close()
	}

	if wasQueryTriggeredAsExpected || didQuerySucceedManually {
		fmt.Println("Fetching Scheduled Query execution results")
		queryString := fmt.Sprintf("SELECT * FROM %s.%s", sqResultsDatabaseName, sqResultsTableName)
		utils.RunQuery(&queryString, timestreamBuilder.QuerySvc, f)
	}

	err = timestreamBuilder.UpdateScheduledQuery(scheduledQueryArn)
	utils.HandleError(err, fmt.Sprintf("Failed to update scheduled query with scheduledQueryArn %s ", scheduledQueryArn), false)

	if *deleteResourcesFlag {

		reader := bufio.NewReader(os.Stdin)
		fmt.Println("deleteResourcesFlag is set to true, Deleting all the resources that got created " +
			" as part of this sample, hit any key to continue")
		reader.ReadString('\n')

		if scheduledQueryArn != "" {
			timestreamBuilder.DeleteScheduledQuery(scheduledQueryArn)
		}

		scheduledQueries, err = timestreamBuilder.ListScheduledQueries()
		if err == nil {
			fmt.Printf("scheduledQueries Size after deleting current ScheduledQuery: %d\n", len(scheduledQueries))
		}

		if s3BucketName != "" {
			timestreamDependencyHelper.DeleteS3Bucket(s3BucketName)
		}

		if policyArn != "" {
			timestreamDependencyHelper.DetachIamPolicy(utils.ROLE_NAME, policyArn)
			timestreamDependencyHelper.DeleteIamPolicy(policyArn)
			timestreamDependencyHelper.DeleteIamRole(utils.ROLE_NAME)
		}

		if subscriptionArn != "" {
			timestreamDependencyHelper.UnsubscribeToSnsTopic(subscriptionArn)
			queueUrl, err := timestreamDependencyHelper.GetSqsQueryUrl(sqSampleAppQueueName)
			if err == nil && queueUrl != "" {
				timestreamDependencyHelper.DeleteSqsQueue(queueUrl)
			}
		}

		if topicArn != "" {
			timestreamDependencyHelper.DeleteSnsTopic(topicArn)
		}

		timestreamBuilder.DeleteTable(sqResultsDatabaseName, sqResultsTableName)
		timestreamBuilder.DeleteDatabase(sqResultsDatabaseName)
		timestreamBuilder.DeleteTable(databaseName, tableName)
		timestreamBuilder.DeleteDatabase(databaseName)
	} else if (len(createdResourcesList) > 0 ) {
		fmt.Println("Following Resources are created and not cleaned")
		for _, resource := range createdResourcesList {
			fmt.Printf("\tResource Type : %s, Identifier (Arn/Name) : %s\n", resource.Type, resource.Identifier)
		}
	}
}
