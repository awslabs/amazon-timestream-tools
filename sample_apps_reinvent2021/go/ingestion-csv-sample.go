package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"go_sample/utils"
)

/**
  This code sample is to read data from a CSV file and ingest data into a Timestream table. Each line of the CSV file is a record to ingest.
  The record schema is fixed, the format is [dimension_name_1, dimension_value_1, dimension_name_2, dimension_value_2, dimension_name_2, dimension_value_2, measure_name, measure_value, measure_data_type, time, time_unit].
  The code will replace the time in the record with a time in the range [current_epoch_in_seconds - number_of_records * 10, current_epoch_in_seconds].
*/
func main() {

	databaseName := flag.String("database_name", utils.DATABASE_NAME, "database name string")
	tableName := flag.String("table_name", utils.TABLE_NAME, "table name string")
	testFileName := flag.String("test_file", "../data/sample-multi.csv", "CSV file containing the data to ingest")
	region := flag.String("region", "us-east-1", "region")

	flag.Parse()

	sess, err := utils.GetSession(*region)
	utils.HandleError(err, "Failed to start a new session", true)

	writeSvc := timestreamwrite.New(sess)
	querySvc := timestreamquery.New(sess)
	s3Svc := s3.New(sess, aws.NewConfig().WithRegion(*region))

	timestreamBuilder := utils.TimestreamBuilder{WriteSvc: writeSvc, QuerySvc: querySvc}
	timestreamDependencyHelper := utils.TimestreamDependencyHelper{S3Svc: s3Svc}

	createdResourcesList := []utils.Resource{}
	// Make the bucket name unique by appending 5 random characters at the end
	s3BucketName := utils.SQ_ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX + utils.GenerateRandomStringWithSize(5)
	err = timestreamDependencyHelper.CreateS3Bucket(s3BucketName, *region)
	utils.HandleError(err, fmt.Sprintf("Failed to create S3Bucket %s ", s3BucketName), true)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "S3", Identifier: s3BucketName})

	// Describe database.
	err = timestreamBuilder.DescribeDatabase(*databaseName)
	if err != nil {
		// Create database if database doesn't exist.
		serr, ok := err.(*timestreamwrite.ResourceNotFoundException)
		fmt.Println(serr)
		if ok {
			fmt.Println("Creating database")
			createDatabaseInput := &timestreamwrite.CreateDatabaseInput{
				DatabaseName: aws.String(*databaseName),
			}
			_, err = writeSvc.CreateDatabase(createDatabaseInput)
			if err != nil {
				utils.HandleError(err, fmt.Sprintf("Failed to create table %s in database %s ",
					*tableName, *databaseName), true)
			}
			createdResourcesList = append(createdResourcesList, utils.Resource{Type: "TIMESTREAM_DATABASE", Identifier: *databaseName})
		}
	} else {
		fmt.Println("Database exists")
	}

	// Describe table.
	_, err = timestreamBuilder.DescribeTable(*databaseName, *tableName)
	if err != nil {
		serr, ok := err.(*timestreamwrite.ResourceNotFoundException)
		fmt.Println(serr)
		if ok {
			// Create table if table doesn't exist.
			fmt.Println("Creating the table now")
			err = timestreamBuilder.CreateTable(*databaseName, *tableName, s3BucketName)
			if err != nil {
				utils.HandleError(err, fmt.Sprintf("Failed to create table %s in database %s ",
					*tableName, *databaseName), true)
			}
			createdResourcesList = append(createdResourcesList, utils.Resource{Type: "TIMESTREAM_TABLE", Identifier: *tableName})
		}
	} else {
		fmt.Println("Table already exists")
		fmt.Printf("Deleting created s3Bucket=%s as it was not used for creating the table.", s3BucketName)
		timestreamDependencyHelper.DeleteS3Bucket(s3BucketName)
		if len(createdResourcesList) > 0 && createdResourcesList[0].Identifier == s3BucketName {
			createdResourcesList = createdResourcesList[1:]
		}
	}

	//Ingest records from csv file
	timestreamBuilder.IngestRecordsFromCsv(*testFileName, *databaseName, *tableName)

	// sample query
	queryString := fmt.Sprintf("select count(*) from %s.%s", *databaseName, *tableName)
	// execute the query
	queryOutput, err := timestreamBuilder.QueryWithQueryString(queryString)
	if err == nil {
		fmt.Println(queryOutput)
	}

	fmt.Println("Ingesting Records Complete")
	if len(createdResourcesList) > 0 {
		fmt.Println("Following Resources are created and not cleaned")
		for _, resource := range createdResourcesList {
			fmt.Printf("\tResource Type : %s, Identifier/Name : %s\n", resource.Type, resource.Identifier)
		}
	} else {
		fmt.Println("Used existing resources to ingest data")
	}
}
