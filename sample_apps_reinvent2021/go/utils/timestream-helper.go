package utils

import (
	"encoding/csv"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"golang.org/x/net/http2"
	"io"
	"net"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"time"
)

func GetSession(region string) (*session.Session, error) {
	/**
	* Recommended Timestream write client SDK configuration:
	*  - Set SDK retry count to 10.
	*  - Use SDK DEFAULT_BACKOFF_STRATEGY
	*  - Request timeout of 20 seconds
	 */

	// Setting 20 seconds for timeout
	tr := &http.Transport{
		ResponseHeaderTimeout: 20 * time.Second,
		// Using DefaultTransport values for other parameters: https://golang.org/pkg/net/http/#RoundTripper
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: 30 * time.Second,
			DualStack: true,
			Timeout:   30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// So client makes HTTP/2 requests
	http2.ConfigureTransport(tr)

	sess, err := session.NewSession(&aws.Config{Region: aws.String(region), MaxRetries: aws.Int(10), HTTPClient: &http.Client{Transport: tr}})
	return sess, err
}

type TimestreamBuilder struct {
	WriteSvc *timestreamwrite.TimestreamWrite
	QuerySvc *timestreamquery.TimestreamQuery
}

func getTimestreamEndpoint(stage string, cell string, region string, serviceType string) string {
	if stage == "prod" {
		return fmt.Sprintf("https://%s-%s.timestream.%s.amazonaws.com", cell, serviceType, region)
	}

	return fmt.Sprintf("https://%s-%s-%s.timestream.%s.amazonaws.com", stage, serviceType, cell, region)
}

func (timestreamBuilder TimestreamBuilder) CreateDatabase(databaseName string) error {
	createDatabaseInput := &timestreamwrite.CreateDatabaseInput{
		DatabaseName: aws.String(databaseName),
	}
	createDatabaseOutput, err := timestreamBuilder.WriteSvc.CreateDatabase(createDatabaseInput)

	fmt.Printf("timestreamBuilder: %s\n", reflect.TypeOf(timestreamBuilder.WriteSvc))

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case timestreamquery.ErrCodeResourceNotFoundException:
				fmt.Println(timestreamquery.ErrCodeResourceNotFoundException, aerr.Error())
			default:
				fmt.Printf("Error: %s", aerr.Error())
			}
		} else {
			fmt.Printf("Error: %s", err.Error())
		}
		return err
	} else {
		fmt.Printf("Database with name %s successfully created", databaseName)
		fmt.Println(createDatabaseOutput)

	}
	return err
}

func (timestreamBuilder TimestreamBuilder) DescribeDatabase(databaseName string) error {

	describeDatabaseInput := &timestreamwrite.DescribeDatabaseInput{
		DatabaseName: aws.String(databaseName),
	}
	describeDatabaseOutput, err := timestreamBuilder.WriteSvc.DescribeDatabase(describeDatabaseInput)

	if err != nil {
		fmt.Printf("Failed to describe database with Error: %s", err.Error())
	} else {
		fmt.Printf("Describe database is successful : %s", *describeDatabaseOutput)
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) UpdateDatabase(databaseName string, kmsKeyId string) error {

	updateDatabaseInput := &timestreamwrite.UpdateDatabaseInput{
		DatabaseName: aws.String(databaseName),
		KmsKeyId:     aws.String(kmsKeyId),
	}

	updateDatabaseOutput, err := timestreamBuilder.WriteSvc.UpdateDatabase(updateDatabaseInput)

	if err != nil {
		fmt.Printf("Error: %s", err.Error())
	} else {
		fmt.Printf("Update database is successful : %s\n", updateDatabaseOutput)
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) ListDatabases(maxResults int64) error {

	listDatabasesMaxResult := maxResults

	listDatabasesInput := &timestreamwrite.ListDatabasesInput{
		MaxResults: &listDatabasesMaxResult,
	}

	listDatabasesOutput, err := timestreamBuilder.WriteSvc.ListDatabases(listDatabasesInput)

	if err != nil {
		fmt.Printf("Error: %s", err.Error())
	} else {
		fmt.Println("List databases is successful : %s\n", *listDatabasesOutput)
		fmt.Println()
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) CreateTable(databaseName string, tableName string, s3BucketName string) error {

	createTableInput := &timestreamwrite.CreateTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
		// Enable MagneticStoreWrite for Table
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: aws.Bool(true),
			// Persist MagneticStoreWrite rejected records in S3
			MagneticStoreRejectedDataLocation: &timestreamwrite.MagneticStoreRejectedDataLocation{
				S3Configuration: &timestreamwrite.S3Configuration{
					BucketName:       aws.String(s3BucketName),
					EncryptionOption: aws.String("SSE_S3"),
				},
			},
		},
	}
	createTableOutput, err := timestreamBuilder.WriteSvc.CreateTable(createTableInput)

	if err != nil {
		fmt.Printf("Error while creating table: %s", err.Error())
	} else {
		fmt.Printf("Create table is successful : %s", createTableOutput)
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) DescribeTable(databaseName string, tableName string) (*timestreamwrite.DescribeTableOutput, error) {

	describeTableInput := &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
	}
	describeTableOutput, err := timestreamBuilder.WriteSvc.DescribeTable(describeTableInput)

	if err != nil {
		fmt.Printf("Failed to describe table with Error: %s", err.Error())
	} else {
		fmt.Printf("Describe table is successful, %s\n", *describeTableOutput)
	}

	return describeTableOutput, err
}

func (timestreamBuilder TimestreamBuilder) ListTables(databaseName string, maxResults int64) error {

	listTablesMaxResult := int64(15)

	listTablesInput := &timestreamwrite.ListTablesInput{
		DatabaseName: aws.String(databaseName),
		MaxResults:   &listTablesMaxResult,
	}
	listTablesOutput, err := timestreamBuilder.WriteSvc.ListTables(listTablesInput)

	if err != nil {
		fmt.Printf("Error: %s", err.Error())
	} else {
		fmt.Println("List tables is successful, below is the output:")
		fmt.Println(listTablesOutput)
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) UpdateTable(databaseName string, tableName string,
	magneticStoreRetentionPeriodInDays int64, memoryStoreRetentionPeriodInHours int64) error {

	updateTableInput := &timestreamwrite.UpdateTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MagneticStoreRetentionPeriodInDays: &magneticStoreRetentionPeriodInDays,
			MemoryStoreRetentionPeriodInHours:  &memoryStoreRetentionPeriodInHours,
		},
	}
	updateTableOutput, err := timestreamBuilder.WriteSvc.UpdateTable(updateTableInput)

	if err != nil {
		fmt.Printf("Error: %s", err.Error())
	} else {
		fmt.Println("Update table is successful, below is the output:")
		fmt.Println(updateTableOutput)
	}

	return err
}

func (timestreamBuilder TimestreamBuilder) DeleteTable(databaseName string, tableName string) error {

	deleteTableInput := &timestreamwrite.DeleteTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
	}
	_, err := timestreamBuilder.WriteSvc.DeleteTable(deleteTableInput)

	if err != nil {
		fmt.Printf("Error: %s", err.Error())
	} else {
		fmt.Printf("Table %s deleted", tableName)
	}

	return err
}

func (timestreamBuilder TimestreamBuilder) DeleteDatabase(databaseName string) error {

	deleteDatabaseInput := &timestreamwrite.DeleteDatabaseInput{
		DatabaseName: aws.String(databaseName),
	}
	_, err := timestreamBuilder.WriteSvc.DeleteDatabase(deleteDatabaseInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("database %s deleted\n", databaseName)
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) QueryWithQueryString(queryString string) (*timestreamquery.QueryOutput, error) {

	queryInput := &timestreamquery.QueryInput{
		QueryString: aws.String(queryString),
	}
	queryOutput, err := timestreamBuilder.QuerySvc.Query(queryInput)

	if err != nil {
		fmt.Printf("Failed to query with Error: %s\n", err.Error())
	}
	return queryOutput, err
}

func (timestreamBuilder TimestreamBuilder) writeSampleRecords(databaseName string, tableName string) error {

	now := time.Now()
	currentTimeInSeconds := now.Unix()

	writeRecordsInput := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
		Records: []*timestreamwrite.Record{
			{
				Dimensions: []*timestreamwrite.Dimension{
					{
						Name:  aws.String("region"),
						Value: aws.String("us-east-1"),
					},
					{
						Name:  aws.String("az"),
						Value: aws.String("az1"),
					},
					{
						Name:  aws.String("hostname"),
						Value: aws.String("host1"),
					},
				},
				MeasureName:      aws.String("cpu_utilization"),
				MeasureValue:     aws.String("13.5"),
				MeasureValueType: aws.String("DOUBLE"),
				Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
				TimeUnit:         aws.String("SECONDS"),
			},
			{
				Dimensions: []*timestreamwrite.Dimension{
					{
						Name:  aws.String("region"),
						Value: aws.String("us-east-1"),
					},
					{
						Name:  aws.String("az"),
						Value: aws.String("az1"),
					},
					{
						Name:  aws.String("hostname"),
						Value: aws.String("host1"),
					},
				},
				MeasureName:      aws.String("memory_utilization"),
				MeasureValue:     aws.String("40"),
				MeasureValueType: aws.String("DOUBLE"),
				Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
				TimeUnit:         aws.String("SECONDS"),
			},
		},
	}

	_, err := timestreamBuilder.WriteSvc.WriteRecords(writeRecordsInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("Write records is successful\n")
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) ListScheduledQueries() ([]*timestreamquery.ScheduledQuery, error) {

	var nextToken *string = nil
	var scheduledQueries []*timestreamquery.ScheduledQuery
	for ok := true; ok; ok = nextToken != nil {
		listScheduledQueriesInput := &timestreamquery.ListScheduledQueriesInput{
			MaxResults: aws.Int64(15),
		}
		if nextToken != nil {
			listScheduledQueriesInput.NextToken = aws.String(*nextToken)
		}

		listScheduledQueriesOutput, err := timestreamBuilder.QuerySvc.ListScheduledQueries(listScheduledQueriesInput)
		if err != nil {
			fmt.Printf("Error: %s\n", err.Error())
			return nil, err
		}
		scheduledQueries = append(scheduledQueries, listScheduledQueriesOutput.ScheduledQueries...)
		nextToken = listScheduledQueriesOutput.NextToken
	}
	return scheduledQueries, nil
}

func (timestreamBuilder TimestreamBuilder) DescribeScheduledQuery(scheduledQueryArn string) error {

	describeScheduledQueryInput := &timestreamquery.DescribeScheduledQueryInput{
		ScheduledQueryArn: aws.String(scheduledQueryArn),
	}
	describeScheduledQueryOutput, err := timestreamBuilder.QuerySvc.DescribeScheduledQuery(describeScheduledQueryInput)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case timestreamquery.ErrCodeResourceNotFoundException:
				fmt.Println(timestreamquery.ErrCodeResourceNotFoundException, aerr.Error())
			default:
				fmt.Printf("Error: %s\n", err.Error())
			}
		} else {
			fmt.Printf("Error: %s\n", aerr.Error())
		}
		return err
	} else {
		fmt.Println("DescribeScheduledQuery is successful, below is the output:")
		fmt.Println(describeScheduledQueryOutput.ScheduledQuery)
		return nil
	}
}

func (timestreamBuilder TimestreamBuilder) ExecuteScheduledQuery(scheduledQueryArn string, invocationTime time.Time) error {

	executeScheduledQueryInput := &timestreamquery.ExecuteScheduledQueryInput{
		ScheduledQueryArn: aws.String(scheduledQueryArn),
		InvocationTime:    aws.Time(invocationTime),
	}
	executeScheduledQueryOutput, err := timestreamBuilder.QuerySvc.ExecuteScheduledQuery(executeScheduledQueryInput)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case timestreamquery.ErrCodeResourceNotFoundException:
				fmt.Println(timestreamquery.ErrCodeResourceNotFoundException, aerr.Error())
			default:
				fmt.Printf("Error: %s\n", aerr.Error())
			}
		} else {
			fmt.Printf("Error: %s\n", err.Error())
		}
		return err
	} else {
		fmt.Println("ExecuteScheduledQuery is successful, below is the output:")
		fmt.Println(executeScheduledQueryOutput.GoString())
		return nil
	}
}

func (timestreamBuilder TimestreamBuilder) UpdateScheduledQuery(scheduledQueryArn string) error {

	updateScheduledQueryInput := &timestreamquery.UpdateScheduledQueryInput{
		ScheduledQueryArn: aws.String(scheduledQueryArn),
		State:             aws.String(timestreamquery.ScheduledQueryStateDisabled),
	}
	_, err := timestreamBuilder.QuerySvc.UpdateScheduledQuery(updateScheduledQueryInput)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case timestreamquery.ErrCodeResourceNotFoundException:
				fmt.Println(timestreamquery.ErrCodeResourceNotFoundException, aerr.Error())
			default:
				fmt.Printf("Error: %s\n", aerr.Error())
			}
		} else {
			fmt.Printf("Error: %s\n", err.Error())
		}
		return err
	} else {
		fmt.Println("UpdateScheduledQuery is successful")
		return nil
	}
}

func (timestreamBuilder TimestreamBuilder) DeleteScheduledQuery(scheduledQueryArn string) error {

	deleteScheduledQueryInput := &timestreamquery.DeleteScheduledQueryInput{
		ScheduledQueryArn: aws.String(scheduledQueryArn),
	}
	_, err := timestreamBuilder.QuerySvc.DeleteScheduledQuery(deleteScheduledQueryInput)

	if err != nil {
		fmt.Println("Error:")
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case timestreamquery.ErrCodeResourceNotFoundException:
				fmt.Println(timestreamquery.ErrCodeResourceNotFoundException, aerr.Error())
			default:
				fmt.Printf("Error: %s\n", aerr.Error())
			}
		} else {
			fmt.Printf("Error: %s\n", err.Error())
		}
		return err
	} else {
		fmt.Println("DeleteScheduledQuery is successful")
		return nil
	}
}

func (timestreamBuilder TimestreamBuilder) createScheduledQuery(topicArn string, roleArn string, s3ErrorReportBucketName string,
	query string, targetConfiguration timestreamquery.TargetConfiguration) (string, error) {

	createScheduledQueryInput := &timestreamquery.CreateScheduledQueryInput{
		Name:        aws.String(SQ_NAME),
		QueryString: aws.String(query),
		ScheduleConfiguration: &timestreamquery.ScheduleConfiguration{
			ScheduleExpression: aws.String(SCHEDULE_EXPRESSION),
		},
		NotificationConfiguration: &timestreamquery.NotificationConfiguration{
			SnsConfiguration: &timestreamquery.SnsConfiguration{
				TopicArn: aws.String(topicArn),
			},
		},
		TargetConfiguration: &targetConfiguration,
		ErrorReportConfiguration: &timestreamquery.ErrorReportConfiguration{
			S3Configuration: &timestreamquery.S3Configuration{
				BucketName: aws.String(s3ErrorReportBucketName),
			},
		},
		ScheduledQueryExecutionRoleArn: aws.String(roleArn),
	}

	createScheduledQueryOutput, err := timestreamBuilder.QuerySvc.CreateScheduledQuery(createScheduledQueryInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("createScheduledQueryResult is successful")
		return *createScheduledQueryOutput.Arn, nil
	}
	return "", err
}

func (timestreamBuilder TimestreamBuilder) CreateValidScheduledQuery(topicArn string, roleArn string, s3ErrorReportBucketName string,
	sqDatabaseName string, sqTableName string, databaseName string, tableName string) (string, error) {

	targetConfiguration := timestreamquery.TargetConfiguration{
		TimestreamConfiguration: &timestreamquery.TimestreamConfiguration{
			DatabaseName: aws.String(sqDatabaseName),
			TableName:    aws.String(sqTableName),
			TimeColumn:   aws.String("binned_timestamp"),
			DimensionMappings: []*timestreamquery.DimensionMapping{
				{
					Name:               aws.String("region"),
					DimensionValueType: aws.String("VARCHAR"),
				},
				{
					Name:               aws.String("az"),
					DimensionValueType: aws.String("VARCHAR"),
				},
				{
					Name:               aws.String("hostname"),
					DimensionValueType: aws.String("VARCHAR"),
				},
			},
			MultiMeasureMappings: &timestreamquery.MultiMeasureMappings{
				TargetMultiMeasureName: aws.String("multi-metrics"),
				MultiMeasureAttributeMappings: []*timestreamquery.MultiMeasureAttributeMapping{
					{
						SourceColumn:     aws.String("avg_cpu_utilization"),
						MeasureValueType: aws.String(timestreamquery.MeasureValueTypeDouble),
					},
					{
						SourceColumn:     aws.String("p90_cpu_utilization"),
						MeasureValueType: aws.String(timestreamquery.MeasureValueTypeDouble),
					},
					{
						SourceColumn:     aws.String("p95_cpu_utilization"),
						MeasureValueType: aws.String(timestreamquery.MeasureValueTypeDouble),
					},
					{
						SourceColumn:     aws.String("p99_cpu_utilization"),
						MeasureValueType: aws.String(timestreamquery.MeasureValueTypeDouble),
					},
				},
			},
		},
	}
	return timestreamBuilder.createScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName,
		fmt.Sprintf(VALID_QUERY, databaseName, tableName), targetConfiguration)
}

func (timestreamBuilder TimestreamBuilder) CreateInvalidScheduledQuery(topicArn string, roleArn string,
	s3ErrorReportBucketName string, sqDatabaseName string, sqTableName string) (string, error) {

	targetConfiguration := timestreamquery.TargetConfiguration{
		TimestreamConfiguration: &timestreamquery.TimestreamConfiguration{
			DatabaseName: aws.String(sqDatabaseName),
			TableName:    aws.String(sqTableName),
			TimeColumn:   aws.String("timestamp"),
			DimensionMappings: []*timestreamquery.DimensionMapping{
				{
					Name:               aws.String("dim0"),
					DimensionValueType: aws.String("VARCHAR"),
				},
			},
			MixedMeasureMappings: []*timestreamquery.MixedMeasureMapping{
				{
					SourceColumn:     aws.String("random_measure_value"),
					MeasureValueType: aws.String(timestreamquery.MeasureValueTypeDouble),
				},
			},
		},
	}
	return timestreamBuilder.createScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName, INVALID_QUERY, targetConfiguration)
}

func (timestreamBuilder TimestreamBuilder) IngestRecords(databaseName string, tableName string,
	records []*timestreamwrite.Record) error {
	writeRecordsInput := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
		Records:      records,
	}
	_, err := timestreamBuilder.WriteSvc.WriteRecords(writeRecordsInput)
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	}
	return err
}
func (timestreamBuilder TimestreamBuilder) IngestRecordsFromCsv(testFileName string, databaseName string, tableName string) {

	csvFile, err := os.Open(testFileName)
	records := make([]*timestreamwrite.Record, 0)
	if err != nil {
		fmt.Println("Couldn't open the csv file", err)
		return
	}

	// Get current time in nano seconds.
	currentTimeInMilliSeconds := time.Now().UnixNano() / int64(time.Millisecond)
	// Counter for number of records.
	counter := int64(0)
	reader := csv.NewReader(csvFile)
	// Iterate through the records
	for {
		// Read each record from csv
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println(err)
		}
		var multiMeasures []*timestreamwrite.MeasureValue
		multiMeasures = append(multiMeasures, &timestreamwrite.MeasureValue{
			Name:  aws.String(record[8]),
			Value: aws.String(record[9]),
			Type:  aws.String(record[10]),
		}, &timestreamwrite.MeasureValue{
			Name:  aws.String(record[11]),
			Value: aws.String(record[12]),
			Type:  aws.String(record[13]),
		})
		records = append(records, &timestreamwrite.Record{
			Dimensions: []*timestreamwrite.Dimension{
				&timestreamwrite.Dimension{
					Name:  aws.String(record[0]),
					Value: aws.String(record[1]),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String(record[2]),
					Value: aws.String(record[3]),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String(record[4]),
					Value: aws.String(record[5]),
				},
			},
			MeasureName:      aws.String("metrics"),
			MeasureValues:    multiMeasures,
			MeasureValueType: aws.String("MULTI"),
			Time:             aws.String(strconv.FormatInt(currentTimeInMilliSeconds-counter*int64(50), 10)),
			TimeUnit:         aws.String("MILLISECONDS"),
		})

		counter++
		// WriteRecordsRequest has 100 records limit per request.
		if counter%100 == 0 {
			err = timestreamBuilder.IngestRecords(databaseName, tableName, records)
			if err == nil {
				fmt.Printf("Ingested %d records to the table.\n", counter)
			}
			records = make([]*timestreamwrite.Record, 0)
		}
	}
	// Ingest rest of the records (if any)
	if len(records) > 0 {

		err = timestreamBuilder.IngestRecords(databaseName, tableName, records)
		if err == nil {
			fmt.Printf("Ingested %d records to the table.\n", counter)
		}
	}
}
