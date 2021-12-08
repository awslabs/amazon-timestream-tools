package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"go_sample/utils"
	"strconv"
	"time"
)

/**
  This code sample is to run the CRUD APIs and WriteRecords API in a logical order.
*/

func main() {

	databaseName := flag.String("database_name", utils.DATABASE_NAME, "database name string")
	tableName := flag.String("table_name", utils.TABLE_NAME, "table name string")
	kmsKeyId := flag.String("kms_key_id", "", "kms key id for update database string")
	region := flag.String("region", "us-east-1", "region")

	flag.Parse()

	// common set of dimensions used for ingestion
	var dimensions []*timestreamwrite.Dimension
	dimensions = append(dimensions, &timestreamwrite.Dimension{
		Name:  aws.String("region"),
		Value: aws.String("us-east-1"),
	}, &timestreamwrite.Dimension{
		Name:  aws.String("az"),
		Value: aws.String("az1"),
	}, &timestreamwrite.Dimension{
		Name:  aws.String("hostname"),
		Value: aws.String("host1"),
	})

	sess, err := utils.GetSession(*region)
	utils.HandleError(err, "Failed to start a new session", true)

	writeSvc := timestreamwrite.New(sess)
	querySvc := timestreamquery.New(sess)
	s3Svc := s3.New(sess, aws.NewConfig().WithRegion(*region))

	fmt.Println("Creating a database")

	timestreamBuilder := utils.TimestreamBuilder{WriteSvc: writeSvc, QuerySvc: querySvc}
	timestreamDependencyHelper := utils.TimestreamDependencyHelper{S3Svc: s3Svc}

	// Create database.
	err = timestreamBuilder.CreateDatabase(*databaseName)
	if err == nil {
		fmt.Println("Database successfully created")
	}

	fmt.Println("Describing the database")

	// Describe database.
	err = timestreamBuilder.DescribeDatabase(*databaseName)

	if *kmsKeyId == "" {
		fmt.Println("Skipping update database because kmsKeyId was not provided")
	} else {
		fmt.Println("Updating the database")

		// Update Database.
		err = timestreamBuilder.UpdateDatabase(*databaseName, *kmsKeyId)
	}

	fmt.Println("Listing databases")

	// List databases.
	LIST_DATABASE_MAX_RESULTS_COUNT := int64(15)
	err = timestreamBuilder.ListDatabases(LIST_DATABASE_MAX_RESULTS_COUNT)

	fmt.Println("Creating a table")

	// Create table.

	// Make the bucket name unique by appending 5 random characters at the end
	s3BucketName := utils.SQ_ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX + utils.GenerateRandomStringWithSize(5)
	err = timestreamDependencyHelper.CreateS3Bucket(s3BucketName, *region)
	utils.HandleError(err, fmt.Sprintf("Failed to create S3Bucket %s ", s3BucketName), true)

	err = timestreamBuilder.CreateTable(*databaseName, *tableName, s3BucketName)

	fmt.Println("Describing the table")

	// Describe table.
	_, err = timestreamBuilder.DescribeTable(*databaseName, *tableName)

	fmt.Println("Listing tables")

	// List tables.
	LIST_TABLES_MAX_RESULTS_COUNT := int64(15)

	err = timestreamBuilder.ListTables(*databaseName, LIST_TABLES_MAX_RESULTS_COUNT)

	fmt.Println("Updating the table")

	// Update table.
	magneticStoreRetentionPeriodInDays := int64(7 * 365)
	memoryStoreRetentionPeriodInHours := int64(24)

	err = timestreamBuilder.UpdateTable(*databaseName, *tableName,
		magneticStoreRetentionPeriodInDays, memoryStoreRetentionPeriodInHours)

	fmt.Println("Ingesting records")

	// Below code will create a table and ingest multi-measure records into created table
	fmt.Println("Ingesting records with multi measures to table", *tableName, "hit enter to continue")

	err = timestreamBuilder.IngestRecords(*databaseName, *tableName,
		getRecordsWithMultiMeasures(dimensions))

	if err == nil {
		fmt.Println("Write records with multi measures is successful")
	}

	fmt.Println("Ingesting records with multi measures with mixture type to table", *tableName, "hit enter to continue")

	err = timestreamBuilder.IngestRecords(*databaseName, *tableName,
		getRecordsWithMultiMeasuresMultipleRecords(dimensions))

	if err == nil {
		fmt.Println("Write records with multi measures with mixture type is successful")
	}

	// sample query
	queryString := fmt.Sprintf("select count(*) from %s.%s", *databaseName, *tableName)
	// execute the query
	queryOutput, err := timestreamBuilder.QueryWithQueryString(queryString)
	if err == nil {
		fmt.Println(queryOutput)
	}

	fmt.Println("Deleting tables.")

	err = timestreamBuilder.DeleteTable(*databaseName, *tableName)

	fmt.Println("Deleting database.")

	err = timestreamBuilder.DeleteDatabase(*databaseName)

}

func getRecordsWithMultiMeasures(dimensions []*timestreamwrite.Dimension) []*timestreamwrite.Record {
	var records []*timestreamwrite.Record
	var multiMeasures []*timestreamwrite.MeasureValue
	currentTimeInSeconds := time.Now().Unix()

	multiMeasures = append(multiMeasures, &timestreamwrite.MeasureValue{
		Name:  aws.String("cpu_utilization"),
		Value: aws.String("13.5"),
		Type:  aws.String("DOUBLE"),
	}, &timestreamwrite.MeasureValue{
		Name:  aws.String("memory_utilization"),
		Value: aws.String("40"),
		Type:  aws.String("DOUBLE"),
	})
	records = append(records, &timestreamwrite.Record{
		Dimensions:       dimensions,
		MeasureName:      aws.String("metrics"),
		MeasureValueType: aws.String("MULTI"),
		MeasureValues:    multiMeasures,
		Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
		TimeUnit:         aws.String("SECONDS"),
	})
	return records
}
func getRecordsWithMultiMeasuresMultipleRecords(dimensions []*timestreamwrite.Dimension) []*timestreamwrite.Record {
	var records []*timestreamwrite.Record
	var multiMeasures []*timestreamwrite.MeasureValue
	currentTimeInSeconds := time.Now().Unix()

	multiMeasures = append(multiMeasures, &timestreamwrite.MeasureValue{
		Name:  aws.String("cpu_utilization"),
		Value: aws.String("13.5"),
		Type:  aws.String("DOUBLE"),
	}, &timestreamwrite.MeasureValue{
		Name:  aws.String("memory_utilization"),
		Value: aws.String("40"),
		Type:  aws.String("DOUBLE"),
	}, &timestreamwrite.MeasureValue{
		Name:  aws.String("active_cores"),
		Value: aws.String("4"),
		Type:  aws.String("BIGINT"),
	})
	records = append(records, &timestreamwrite.Record{
		Dimensions:       dimensions,
		MeasureName:      aws.String("computational_utilization"),
		MeasureValueType: aws.String("MULTI"),
		MeasureValues:    multiMeasures,
		Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
		TimeUnit:         aws.String("SECONDS"),
	})
	return records
}
