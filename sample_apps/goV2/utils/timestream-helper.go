package utils

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/aws/smithy-go"
	"golang.org/x/net/http2"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	qtypes "github.com/aws/aws-sdk-go-v2/service/timestreamquery/types"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"github.com/aws/aws-sdk-go/aws/awserr"
)

type TimestreamBuilder struct {
	WriteSvc *timestreamwrite.Client
	QuerySvc *timestreamquery.Client
}

func (timestreamBuilder TimestreamBuilder) CreateDatabase(databaseName string) error {
	createDatabaseInput := &timestreamwrite.CreateDatabaseInput{
		DatabaseName: aws.String(databaseName),
	}
	createDatabaseOutput, err := timestreamBuilder.WriteSvc.CreateDatabase(context.TODO(), createDatabaseInput)

	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "ResourceNotFoundException":
				fmt.Println("ResourceNotFoundException", apiErr.Error())
			default:
				fmt.Printf("Error: %s", err.Error())
			}
		} else {
			fmt.Printf("Error: %s", err.Error())
		}
	} else {
		fmt.Printf("Database with name %s successfully created : %s\n", databaseName, JsonMarshalIgnoreError(createDatabaseOutput))
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) DescribeDatabase(databaseName string) error {
	describeDatabaseInput := &timestreamwrite.DescribeDatabaseInput{
		DatabaseName: aws.String(databaseName),
	}
	describeDatabaseOutput, err := timestreamBuilder.WriteSvc.DescribeDatabase(context.TODO(), describeDatabaseInput)

	if err != nil {
		fmt.Printf("Failed to describe database with Error: %s\n", err.Error())
	} else {
		fmt.Printf("Describe database is successful : %s\n", JsonMarshalIgnoreError(describeDatabaseOutput))
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) UpdateDatabase(databaseName *string, kmsKeyId *string) error {
	updateDatabaseInput := &timestreamwrite.UpdateDatabaseInput{
		DatabaseName: aws.String(*databaseName),
		KmsKeyId:     aws.String(*kmsKeyId),
	}

	updateDatabaseOutput, err := timestreamBuilder.WriteSvc.UpdateDatabase(context.TODO(), updateDatabaseInput)

	if err != nil {
		fmt.Printf("Error: %s", err.Error())
	} else {
		fmt.Printf("Update database is successful")
		printDatabaseDetails(*updateDatabaseOutput.Database)
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) ListDatabases(maxResultCount int32) error {
	listDatabasesMaxResult := maxResultCount
	var nextToken *string = nil

	for ok := true; ok; ok = nextToken != nil {
		listDatabasesInput := &timestreamwrite.ListDatabasesInput{
			MaxResults: &listDatabasesMaxResult,
		}
		if nextToken != nil {
			listDatabasesInput.NextToken = aws.String(*nextToken)
		}

		listDatabasesOutput, err := timestreamBuilder.WriteSvc.ListDatabases(context.TODO(), listDatabasesInput)

		if err != nil {
			fmt.Println("Error:")
			fmt.Println(err)
			return err
		} else {
			fmt.Println("List databases is successful, below is the output:")
			for _, database := range listDatabasesOutput.Databases {
				printDatabaseDetails(database)
			}
		}
		nextToken = listDatabasesOutput.NextToken
	}
	return nil
}

func (timestreamBuilder TimestreamBuilder) CreateTable(databaseName string, tableName string, s3BucketName string) error {
	_, err := timestreamBuilder.WriteSvc.CreateTable(context.TODO(), &timestreamwrite.CreateTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
		MagneticStoreWriteProperties: &types.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: aws.Bool(true),
			// Persist MagneticStoreWrite rejected records in S3
			MagneticStoreRejectedDataLocation: &types.MagneticStoreRejectedDataLocation{
				S3Configuration: &types.S3Configuration{
					BucketName:       aws.String(s3BucketName),
					EncryptionOption: "SSE_S3",
				},
			},
		},
	})

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Create table is successful")
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) CreateTableWithPartitionKeys(databaseName string, tableName string,
	s3BucketName string, compositePartitionKey []types.PartitionKey) error {
	_, err := timestreamBuilder.WriteSvc.CreateTable(context.TODO(), &timestreamwrite.CreateTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
		MagneticStoreWriteProperties: &types.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: aws.Bool(true),
			// Persist MagneticStoreWrite rejected records in S3
			MagneticStoreRejectedDataLocation: &types.MagneticStoreRejectedDataLocation{
				S3Configuration: &types.S3Configuration{
					BucketName:       aws.String(s3BucketName),
					EncryptionOption: "SSE_S3",
				},
			},
		},
		Schema: &types.Schema{
			CompositePartitionKey: compositePartitionKey,
		},
	})

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Create table is successful")
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) DescribeTable(databaseName string, tableName string) (*timestreamwrite.DescribeTableOutput, error) {
	describeTableInput := &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
	}
	describeTableOutput, err := timestreamBuilder.WriteSvc.DescribeTable(context.TODO(), describeTableInput)

	if err != nil {
		fmt.Printf("Failed to describe table with Error: %s", err.Error())
	} else {
		fmt.Printf("Describe table is successful : %s\n", JsonMarshalIgnoreError(*describeTableOutput))
	}

	return describeTableOutput, err
}

func (timestreamBuilder TimestreamBuilder) ListTables(databaseName string, maxResultCount int32) error {
	listTablesMaxResult := maxResultCount
	var nextToken *string = nil

	for ok := true; ok; ok = nextToken != nil {
		listTablesInput := &timestreamwrite.ListTablesInput{
			DatabaseName: aws.String(databaseName),
			MaxResults:   &listTablesMaxResult,
		}
		if nextToken != nil {
			listTablesInput.NextToken = aws.String(*nextToken)
		}
		listTablesOutput, err := timestreamBuilder.WriteSvc.ListTables(context.TODO(), listTablesInput)

		if err != nil {
			fmt.Println("Error:")
			fmt.Println(err)
			return err
		} else {
			fmt.Println("List tables is successful, below is the output:")
			for _, table := range listTablesOutput.Tables {
				printTableDetails(table)
			}
		}
		nextToken = listTablesOutput.NextToken
	}
	return nil
}

func (timestreamBuilder TimestreamBuilder) UpdateTable(databaseName string, tableName string) error {
	magneticStoreRetentionPeriodInDays := int64(7 * 365)
	memoryStoreRetentionPeriodInHours := int64(24)

	updateTableInput := &timestreamwrite.UpdateTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
		RetentionProperties: &types.RetentionProperties{
			MagneticStoreRetentionPeriodInDays: magneticStoreRetentionPeriodInDays,
			MemoryStoreRetentionPeriodInHours:  memoryStoreRetentionPeriodInHours,
		},
	}
	updateTableOutput, err := timestreamBuilder.WriteSvc.UpdateTable(context.TODO(), updateTableInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Update table is successful, below is the output:")
		printTableDetails(*updateTableOutput.Table)
	}
	return err
}

func (timestreamBuilder TimestreamBuilder) DeleteTable(databaseName string, tableName string) error {

	deleteTableInput := &timestreamwrite.DeleteTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
	}
	_, err := timestreamBuilder.WriteSvc.DeleteTable(context.TODO(), deleteTableInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("Table %s deleted\n", tableName)
	}

	return err
}

func (timestreamBuilder TimestreamBuilder) DeleteDatabase(databaseName string) error {
	deleteDatabaseInput := &timestreamwrite.DeleteDatabaseInput{
		DatabaseName: aws.String(databaseName),
	}

	_, err := timestreamBuilder.WriteSvc.DeleteDatabase(context.TODO(), deleteDatabaseInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Printf("Database %s deleted\n", databaseName)
	}
	return err
}

func printDatabaseDetails(database types.Database) {
	fmt.Println("'DatabaseName':", *database.DatabaseName,
		", 'Arn':", *database.Arn, ", 'CreationTime':", *database.CreationTime,
		", 'KmsKeyId':", *database.KmsKeyId, ", 'LastUpdatedTime':", *database.LastUpdatedTime,
		", 'TableCount':", database.TableCount)
}

func printTableDetails(table types.Table) {
	fmt.Println("'TableName':", *table.TableName,
		", 'Arn':", *table.Arn, ", 'CreationTime':", *table.CreationTime,
		", 'DatabaseName':", *table.DatabaseName, ", 'LastUpdatedTime':", *table.LastUpdatedTime,
		", 'RetentionProperties':{ 'MagneticStoreRetentionPeriodInDays':",
		table.RetentionProperties.MagneticStoreRetentionPeriodInDays, ", 'MemoryStoreRetentionPeriodInHours':",
		table.RetentionProperties.MemoryStoreRetentionPeriodInHours, "}")
}

func IngestToTimestream(writeSvc *timestreamwrite.Client,
	writeRecordsInputMulti *timestreamwrite.WriteRecordsInput,
	message string) {
	_, err := writeSvc.WriteRecords(context.TODO(), writeRecordsInputMulti)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println(message)
	}
}

func GetRecordsWithMultiMeasures(dimensions []types.Dimension) []types.Record {
	currentTimeInSeconds := time.Now().Unix()
	multiMeasures := []types.MeasureValue{{
		Name:  aws.String("cpu_utilization"),
		Value: aws.String("13.5"),
		Type:  "DOUBLE",
	}, {
		Name:  aws.String("memory_utilization"),
		Value: aws.String("40"),
		Type:  "DOUBLE",
	}}
	records := []types.Record{{
		Dimensions:       dimensions,
		MeasureName:      aws.String("cpu_memory"),
		MeasureValueType: "MULTI",
		MeasureValues:    multiMeasures,
		Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
		TimeUnit:         "SECONDS",
	}}
	return records
}

func GetRecordsWithMultiMeasuresMultipleRecords(dimensions []types.Dimension) []types.Record {
	currentTimeInSeconds := time.Now().Unix()
	multiMeasures := []types.MeasureValue{{
		Name:  aws.String("cpu_utilization"),
		Value: aws.String("13.5"),
		Type:  "DOUBLE",
	}, {
		Name:  aws.String("memory_utilization"),
		Value: aws.String("40"),
		Type:  "DOUBLE",
	}, {
		Name:  aws.String("active_cores"),
		Value: aws.String("4"),
		Type:  "BIGINT",
	}}
	records := []types.Record{{
		Dimensions:       dimensions,
		MeasureName:      aws.String("computational_utilization"),
		MeasureValueType: "MULTI",
		MeasureValues:    multiMeasures,
		Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
		TimeUnit:         "SECONDS",
	}, {
		Dimensions:       dimensions,
		MeasureName:      aws.String("is_healthy"),
		MeasureValueType: "BOOLEAN",
		MeasureValue:     aws.String("true"),
		Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
		TimeUnit:         "SECONDS",
	}}
	return records
}

func (timestreamBuilder TimestreamBuilder) QueryWithQueryString(queryString string) (*timestreamquery.QueryOutput, error) {

	queryInput := &timestreamquery.QueryInput{
		QueryString: aws.String(queryString),
	}
	queryOutput, err := timestreamBuilder.QuerySvc.Query(context.TODO(), queryInput)

	if err != nil {
		fmt.Printf("Failed to query with Error: %s\n", err.Error())
	}
	return queryOutput, err
}

func (timestreamBuilder TimestreamBuilder) ListScheduledQueries() ([]qtypes.ScheduledQuery, error) {

	var nextToken *string = nil
	var scheduledQueries []qtypes.ScheduledQuery
	for ok := true; ok; ok = nextToken != nil {
		listScheduledQueriesInput := &timestreamquery.ListScheduledQueriesInput{
			MaxResults: aws.Int32(15),
		}
		if nextToken != nil {
			listScheduledQueriesInput.NextToken = aws.String(*nextToken)
		}

		listScheduledQueriesOutput, err := timestreamBuilder.QuerySvc.ListScheduledQueries(context.TODO(), listScheduledQueriesInput)
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
	describeScheduledQueryOutput, err := timestreamBuilder.QuerySvc.DescribeScheduledQuery(context.TODO(), describeScheduledQueryInput)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "ResourceNotFoundException":
				fmt.Println("ResourceNotFoundException", aerr.Error())
			default:
				fmt.Printf("Error: %s\n", err.Error())
			}
		} else {
			fmt.Printf("Error: %s\n", aerr.Error())
		}
		return err
	} else {
		fmt.Printf("DescribeScheduledQuery is successful, output: %s",
			JsonMarshalIgnoreError(describeScheduledQueryOutput.ScheduledQuery))
		return nil
	}
}

func (timestreamBuilder TimestreamBuilder) ExecuteScheduledQuery(scheduledQueryArn string, invocationTime time.Time) error {

	executeScheduledQueryInput := &timestreamquery.ExecuteScheduledQueryInput{
		ScheduledQueryArn: aws.String(scheduledQueryArn),
		InvocationTime:    aws.Time(invocationTime),
	}
	executeScheduledQueryOutput, err := timestreamBuilder.QuerySvc.ExecuteScheduledQuery(context.TODO(), executeScheduledQueryInput)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "ResourceNotFoundException":
				fmt.Println("ResourceNotFoundException", aerr.Error())
			default:
				fmt.Printf("Error: %s\n", aerr.Error())
			}
		} else {
			fmt.Printf("Error: %s\n", err.Error())
		}
		return err
	} else {
		fmt.Println("ExecuteScheduledQuery is successful, below is the output:")
		fmt.Println(executeScheduledQueryOutput.ResultMetadata)
		return nil
	}
}

func (timestreamBuilder TimestreamBuilder) UpdateScheduledQuery(scheduledQueryArn string) error {

	updateScheduledQueryInput := &timestreamquery.UpdateScheduledQueryInput{
		ScheduledQueryArn: aws.String(scheduledQueryArn),
		State:             qtypes.ScheduledQueryStateDisabled,
	}
	_, err := timestreamBuilder.QuerySvc.UpdateScheduledQuery(context.TODO(), updateScheduledQueryInput)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "ResourceNotFoundException":
				fmt.Println("ResourceNotFoundException", aerr.Error())
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
	_, err := timestreamBuilder.QuerySvc.DeleteScheduledQuery(context.TODO(), deleteScheduledQueryInput)

	if err != nil {
		fmt.Println("Error:")
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case "ResourceNotFoundException":
				fmt.Println("ResourceNotFoundException", aerr.Error())
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

func (timestreamBuilder TimestreamBuilder) CreateScheduledQuery(topicArn string, roleArn string, s3ErrorReportBucketName string,
	query string, targetConfiguration qtypes.TargetConfiguration) (string, error) {

	createScheduledQueryInput := &timestreamquery.CreateScheduledQueryInput{
		Name:        aws.String(SQ_NAME),
		QueryString: aws.String(query),
		ScheduleConfiguration: &qtypes.ScheduleConfiguration{
			ScheduleExpression: aws.String(SCHEDULE_EXPRESSION),
		},
		NotificationConfiguration: &qtypes.NotificationConfiguration{
			SnsConfiguration: &qtypes.SnsConfiguration{
				TopicArn: aws.String(topicArn),
			},
		},
		TargetConfiguration: &targetConfiguration,
		ErrorReportConfiguration: &qtypes.ErrorReportConfiguration{
			S3Configuration: &qtypes.S3Configuration{
				BucketName: aws.String(s3ErrorReportBucketName),
			},
		},
		ScheduledQueryExecutionRoleArn: aws.String(roleArn),
	}

	createScheduledQueryOutput, err := timestreamBuilder.QuerySvc.CreateScheduledQuery(context.TODO(), createScheduledQueryInput)

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

	targetConfiguration := qtypes.TargetConfiguration{
		TimestreamConfiguration: &qtypes.TimestreamConfiguration{
			DatabaseName: aws.String(sqDatabaseName),
			TableName:    aws.String(sqTableName),
			TimeColumn:   aws.String("binned_timestamp"),
			DimensionMappings: []qtypes.DimensionMapping{
				{
					Name:               aws.String("region"),
					DimensionValueType: "VARCHAR",
				},
				{
					Name:               aws.String("az"),
					DimensionValueType: "VARCHAR",
				},
				{
					Name:               aws.String("hostname"),
					DimensionValueType: "VARCHAR",
				},
			},
			MultiMeasureMappings: &qtypes.MultiMeasureMappings{
				TargetMultiMeasureName: aws.String("multi-metrics"),
				MultiMeasureAttributeMappings: []qtypes.MultiMeasureAttributeMapping{
					{
						SourceColumn:     aws.String("avg_cpu_utilization"),
						MeasureValueType: qtypes.ScalarMeasureValueTypeDouble,
					},
					{
						SourceColumn:     aws.String("p90_cpu_utilization"),
						MeasureValueType: qtypes.ScalarMeasureValueTypeDouble,
					},
					{
						SourceColumn:     aws.String("p95_cpu_utilization"),
						MeasureValueType: qtypes.ScalarMeasureValueTypeDouble,
					},
					{
						SourceColumn:     aws.String("p99_cpu_utilization"),
						MeasureValueType: qtypes.ScalarMeasureValueTypeDouble,
					},
				},
			},
		},
	}
	return timestreamBuilder.CreateScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName,
		fmt.Sprintf(VALID_QUERY, databaseName, tableName), targetConfiguration)
}

func (timestreamBuilder TimestreamBuilder) CreateInvalidScheduledQuery(topicArn string, roleArn string,
	s3ErrorReportBucketName string, sqDatabaseName string, sqTableName string) (string, error) {

	targetConfiguration := qtypes.TargetConfiguration{
		TimestreamConfiguration: &qtypes.TimestreamConfiguration{
			DatabaseName: aws.String(sqDatabaseName),
			TableName:    aws.String(sqTableName),
			TimeColumn:   aws.String("timestamp"),
			DimensionMappings: []qtypes.DimensionMapping{
				{
					Name:               aws.String("dim0"),
					DimensionValueType: "VARCHAR",
				},
			},
			MixedMeasureMappings: []qtypes.MixedMeasureMapping{
				{
					SourceColumn:     aws.String("random_measure_value"),
					MeasureValueType: qtypes.MeasureValueTypeDouble,
				},
			},
		},
	}
	return timestreamBuilder.CreateScheduledQuery(topicArn, roleArn, s3ErrorReportBucketName, INVALID_QUERY, targetConfiguration)
}

func (timestreamBuilder TimestreamBuilder) IngestRecordsFromCsv(testFileName string, databaseName string,
	tableName string) error {
	csvFile, err := os.Open(testFileName)
	records := make([]types.Record, 0)
	if err != nil {
		fmt.Println("Couldn't open the csv file", err)
		return err
	}

	// Get current time in nano-seconds.
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
		multiMeasures := []types.MeasureValue{{
			Name:  aws.String(record[8]),
			Value: aws.String(record[9]),
			Type:  types.MeasureValueType(record[10]),
		}, {
			Name:  aws.String(record[11]),
			Value: aws.String(record[12]),
			Type:  types.MeasureValueType(record[13]),
		}}

		dimensions := []types.Dimension{{
			Name:  aws.String(record[0]),
			Value: aws.String(record[1]),
		}, {
			Name:  aws.String(record[2]),
			Value: aws.String(record[3]),
		}, {
			Name:  aws.String(record[4]),
			Value: aws.String(record[5]),
		}}

		records = append(records, types.Record{
			Dimensions:       dimensions,
			MeasureName:      aws.String("metrics"),
			MeasureValues:    multiMeasures,
			MeasureValueType: "MULTI",
			Time:             aws.String(strconv.FormatInt(currentTimeInMilliSeconds-counter*int64(50), 10)),
			TimeUnit:         "MILLISECONDS",
		})

		counter++
		// WriteRecordsRequest has 100 records limit per request.
		if counter%100 == 0 {
			writeRecordsInput := &timestreamwrite.WriteRecordsInput{
				DatabaseName: aws.String(databaseName),
				TableName:    aws.String(tableName),
				Records:      records,
			}
			ingestRecords(writeRecordsInput, timestreamBuilder.WriteSvc, counter)
			records = make([]types.Record, 0)
		}
	}
	// Ingest rest of the records (if any)
	if len(records) > 0 {
		writeRecordsInput := &timestreamwrite.WriteRecordsInput{
			DatabaseName: aws.String(databaseName),
			TableName:    aws.String(tableName),
			Records:      records,
		}
		ingestRecords(writeRecordsInput, timestreamBuilder.WriteSvc, counter)
	}
	return nil
}

func ingestRecords(writeRecordsInput *timestreamwrite.WriteRecordsInput, writeSvc *timestreamwrite.Client,
	counter int64) error {
	_, err := writeSvc.WriteRecords(context.TODO(), writeRecordsInput)

	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Print("Ingested ", counter)
		fmt.Println(" records to the table.")
	}
	return err
}

func LoadHttpSettings() *http.Transport {
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
			Timeout:   30 * time.Second,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	// So client makes HTTP/2 requests
	http2.ConfigureTransport(tr)
	return tr
}

func ParseQueryResult(queryResponse *timestreamquery.QueryOutput, f *os.File) {
	columnInfo := queryResponse.ColumnInfo
	metaDataString := fmt.Sprintf("Metadata : %s\n", JsonMarshalIgnoreError(columnInfo))
	fmt.Print(metaDataString)
	Write(f, metaDataString)
	fmt.Println("Data:")
	Write(f, "Data :\n")
	for _, row := range queryResponse.Rows {
		rowString := fmt.Sprintf("%s\n", parseRow(columnInfo, row))
		fmt.Print(rowString)
		Write(f, rowString)
	}
}

func parseRow(columnInfo []qtypes.ColumnInfo, row qtypes.Row) string {
	data := row.Data
	rowOutput := []string{}
	for i := 0; i < len(data); i++ {
		info := columnInfo[i]
		datum := data[i]
		rowOutput = append(rowOutput, parseDatum(info, datum))
	}
	return fmt.Sprintf("{%s}", strings.Join(rowOutput[:], ","))
}

func parseDatum(info qtypes.ColumnInfo, datum qtypes.Datum) string {
	if datum.NullValue != nil && *datum.NullValue {
		return *info.Name + "=NULL"
	}
	columnType := info.Type
	// If the column is of TimeSeries Type
	if columnType.TimeSeriesMeasureValueColumnInfo != nil {
		return parseTimeSeries(info, datum)
	}
	// If the column is of Array Type
	if columnType.ArrayColumnInfo != nil {
		arrayValues := datum.ArrayValue
		return fmt.Sprintf("%s=%s", *info.Type.ArrayColumnInfo, parseArray(info, arrayValues))
	}
	// If the column is of Row Type
	if columnType.RowColumnInfo != nil && len(columnType.RowColumnInfo) > 0 {
		rowColumnInfo := info.Type.RowColumnInfo
		rowValues := datum.RowValue
		return parseRow(rowColumnInfo, *rowValues)
	}
	// If the column is of Scalar Type
	return parseScalarType(info, datum)
}

func parseTimeSeries(info qtypes.ColumnInfo, datum qtypes.Datum) string {
	var timeSeriesOutput []string
	for _, dataPoint := range datum.TimeSeriesValue {
		timeSeriesOutput = append(timeSeriesOutput,
			fmt.Sprintf("{time=%s, value=%s}", *dataPoint.Time, parseDatum(*info.Type.TimeSeriesMeasureValueColumnInfo, *dataPoint.Value)))
	}
	return fmt.Sprintf("[%s]", strings.Join(timeSeriesOutput[:], ","))
}

func parseArray(arrayColumnInfo qtypes.ColumnInfo, arrayValues []qtypes.Datum) interface{} {
	var arrayOutput []string
	for _, datum := range arrayValues {
		arrayOutput = append(arrayOutput, parseDatum(arrayColumnInfo, datum))
	}
	return fmt.Sprintf("[%s]", strings.Join(arrayOutput[:], ","))
}

func parseScalarType(info qtypes.ColumnInfo, datum qtypes.Datum) string {
	return parseColumnName(info) + *datum.ScalarValue
}

func parseColumnName(info qtypes.ColumnInfo) string {
	if info.Name == nil {
		return ""
	}
	return *info.Name + "="
}
