package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"go_sample/utils"
	"net/http"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
)

/**
  This code sample is to run the CRUD APIs and WriteRecords API in a logical order.
*/

func main() {

	databaseName := flag.String("database_name", utils.DATABASE_NAME, "database name string")
	tableName := flag.String("table_name", utils.TABLE_NAME, "table name string")
	testFileName := flag.String("test_file", utils.SAMPLE_DATA_CSV_FILE_PATH, "CSV file containing the data to ingest")
	region := flag.String("region", utils.REGION, "region")

	flag.Parse()

	// Use the SDK's default configuration.
	cfg, err := config.LoadDefaultConfig(context.TODO())
	tr := utils.LoadHttpSettings()

	writeSvc := timestreamwrite.NewFromConfig(cfg, func(o *timestreamwrite.Options) {
		o.Region = *region
		o.HTTPClient = &http.Client{Transport: tr}
	})
	querySvc := timestreamquery.NewFromConfig(cfg, func(o *timestreamquery.Options) {
		o.Region = *region
		o.HTTPClient = &http.Client{Transport: tr}
	})
	timestreamBuilder := utils.TimestreamBuilder{WriteSvc: writeSvc, QuerySvc: querySvc}

	s3Svc := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.Region = *region
	})
	timestreamDependencyHelper := utils.TimestreamDependencyHelper{S3Svc: s3Svc}

	createdResourcesList := []utils.Resource{}
	// Make the bucket name unique by appending 5 random characters at the end
	s3BucketName := utils.ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX + utils.GenerateRandomStringWithSize(5)
	err = timestreamDependencyHelper.CreateS3Bucket(s3BucketName, *region)
	utils.HandleError(err, fmt.Sprintf("Failed to create S3Bucket %s ", s3BucketName), true)
	createdResourcesList = append(createdResourcesList, utils.Resource{Type: "S3", Identifier: s3BucketName})
	var RESOURCE_NOT_FOUND *types.ResourceNotFoundException
	// Describe database.
	err = timestreamBuilder.DescribeDatabase(*databaseName)
	if err != nil {
		if errors.As(err, &RESOURCE_NOT_FOUND) {
			err = timestreamBuilder.CreateDatabase(*databaseName)
			if err != nil {
				utils.CleanUp(timestreamBuilder, timestreamDependencyHelper, "", "", s3BucketName)
				utils.HandleError(err, fmt.Sprintf("Failed to create Database %s\n", *databaseName), true)
			}
			createdResourcesList = append(createdResourcesList, utils.Resource{Type: "TIMESTREAM_DATABASE", Identifier: *databaseName})
		}
	}

	// Describe table.
	_, err = timestreamBuilder.DescribeTable(*databaseName, *tableName)
	if err != nil {
		if errors.As(err, &RESOURCE_NOT_FOUND) {
			err = timestreamBuilder.CreateTable(*databaseName, *tableName, s3BucketName)
			if err != nil {
				utils.CleanUp(timestreamBuilder, timestreamDependencyHelper, *databaseName, "", s3BucketName)
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

	total_records := int64(0)
	// sample query
	queryString := fmt.Sprintf("select count(*) as total_records from %s.%s", *databaseName, *tableName)
	// execute the query
	queryOutput, err := timestreamBuilder.QueryWithQueryString(queryString)
	if err != nil {
		fmt.Printf("Error while querying: %s", err.Error())
		utils.HandleError(err, fmt.Sprintf("Failed to query from table %s in database %s ",
			*tableName, *databaseName), true)
	} else {
		utils.ParseQueryResult(queryOutput, nil)
		if len(queryOutput.Rows) > 0 {
			total_records, _ = strconv.ParseInt(*queryOutput.Rows[0].Data[0].ScalarValue, 10, 64)
		}
	}

	if total_records >= 63000 {
		fmt.Println("Records are already ingested into database, Skipping Ingestion from csv provided")
	} else {
		//Ingest records from csv file
		err = timestreamBuilder.IngestRecordsFromCsv(*testFileName, *databaseName, *tableName)
		if err != nil {
			utils.CleanUp(timestreamBuilder, timestreamDependencyHelper, *databaseName, *tableName, s3BucketName)
			utils.HandleError(err, fmt.Sprintf("Failed to ingest data from csv path `%s` table %s in database %s ", *testFileName),
				true)
		} else {
			fmt.Println("Ingesting Records Complete")
		}

		// sample query
		queryString := fmt.Sprintf("select count(*) as total_records from %s.%s", *databaseName, *tableName)
		// execute the query
		queryOutput, err := timestreamBuilder.QueryWithQueryString(queryString)
		if err != nil {
			fmt.Printf("Error while querying: %s", err.Error())
		} else {
			utils.ParseQueryResult(queryOutput, nil)
			if len(queryOutput.Rows) > 0 {
				total_records, _ := strconv.ParseInt(*queryOutput.Rows[0].Data[0].ScalarValue, 10, 64)
				fmt.Println("Total ingested records count : ", total_records)
			}
		}
	}

	if len(createdResourcesList) > 0 {
		fmt.Println("Following Resources are created and not cleaned")
		for _, resource := range createdResourcesList {
			fmt.Printf("\tResource Type : %s, Identifier/Name : %s\n", resource.Type, resource.Identifier)
		}
	} else {
		fmt.Println("Used existing resources to ingest data")
	}
}
