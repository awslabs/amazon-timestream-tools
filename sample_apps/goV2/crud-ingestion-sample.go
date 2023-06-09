package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"go_sample/utils"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
)

/**
  This code sample is to run the CRUD APIs and WriteRecords API in a logical order.
*/
func main() {
	tr := utils.LoadHttpSettings()
	// Use the SDK's default configuration.
	cfg, _ := config.LoadDefaultConfig(context.TODO())

	databaseName := flag.String("database_name", utils.DATABASE_NAME, "database name string")
	tableName := flag.String("table_name", utils.TABLE_NAME, "table name string")
	kmsKeyId := flag.String("kms_key_id", "", "kms key id for update database string")
	region := flag.String("region", utils.REGION, "region")

	flag.Parse()

	// common set of dimensions used for ingestion
	dimensions := []types.Dimension{{
		Name:  aws.String("region"),
		Value: aws.String("us-east-1"),
	}, {
		Name:  aws.String("az"),
		Value: aws.String("az1"),
	}, {
		Name:  aws.String("hostname"),
		Value: aws.String("host1"),
	}}

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
	s3BucketName := utils.ERROR_CONFIGURATION_S3_BUCKET_NAME_PREFIX + utils.GenerateRandomStringWithSize(5)
	var RESOURCE_ALREADY_EXISTS *types.ConflictException


	//Cleaning up resources at the end
	defer utils.CleanUp(timestreamBuilder, timestreamDependencyHelper, *databaseName, *tableName, s3BucketName)

	fmt.Printf("Creating a database with name %s\n", *databaseName)

	// Create database.
	err := timestreamBuilder.CreateDatabase(*databaseName)
	if err != nil {
		if errors.As(err, &RESOURCE_ALREADY_EXISTS) {
			fmt.Printf("Creating database with name %s failed as it already exists\n", *databaseName)
		} else {
			utils.HandleError(err, fmt.Sprintf("Failed to create database %s ", *databaseName), true)
		}
	}

	fmt.Printf("\n\nDescribing the database %s", *databaseName)
	// Describe database.
	timestreamBuilder.DescribeDatabase(*databaseName)

	if *kmsKeyId == "" {
		fmt.Println("\n\nSkipping update database because kmsKeyId was not provided")
	} else {
		fmt.Println("\n\nUpdating the database")
		// Update Database.
		timestreamBuilder.UpdateDatabase(databaseName, kmsKeyId)
	}

	fmt.Println("\n\nListing databases")

	// List databases.
	listDatabasesMaxResult := int32(15)
	timestreamBuilder.ListDatabases(listDatabasesMaxResult)

	// Create table and S3 bucket.
	err = timestreamDependencyHelper.CreateS3Bucket(s3BucketName, *region)
	utils.HandleError(err, fmt.Sprintf("Failed to create S3Bucket %s ", s3BucketName), true)

	fmt.Printf("\n\nCreating a table with name %s\n", *tableName)
	err = timestreamBuilder.CreateTable(*databaseName, *tableName, s3BucketName)
	if err != nil {
		if errors.As(err, &RESOURCE_ALREADY_EXISTS) {
			fmt.Printf("\n\nCreating table with name %s failed as it already exists\n", *tableName)
		} else {
			utils.HandleError(err, fmt.Sprintf("Failed to create table %s ", *tableName), true)
		}
	}
	fmt.Printf("\n\nDescribing a table with name %s\n", *tableName)

	// Describe table.
	timestreamBuilder.DescribeTable(*databaseName, *tableName)

	fmt.Printf("\n\nListing a table with name %s\n", *tableName)
	// List tables.
	listTablesMaxResult := int32(15)
	timestreamBuilder.ListTables(*databaseName, listTablesMaxResult)

	fmt.Printf("\n\nUpdating a table with name %s\n", *tableName)
	// Update table.
	timestreamBuilder.UpdateTable(*databaseName, *tableName)

	// Below code will create a table and ingest multi records into created table
	fmt.Printf("\n\nIngesting records with multi measures to table %s\n", *tableName)

	// Ingest multi-measure record
	writeRecordsInputMulti := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		Records:      utils.GetRecordsWithMultiMeasures(dimensions),
	}
	utils.IngestToTimestream(writeSvc, writeRecordsInputMulti, "Ingesting records with multi measures successful")

	fmt.Printf("\n\nIngesting records with multi measures to table %s\n", *tableName)

	// Ingest multi-measure record with multiple records or mixture type
	writeRecordsInputMultiMixture := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		Records:      utils.GetRecordsWithMultiMeasuresMultipleRecords(dimensions),
	}
	utils.IngestToTimestream(writeSvc, writeRecordsInputMultiMixture, "Ingesting records with multi measures mixture type successful")

	queryString := fmt.Sprintf("select * from %s.%s", *databaseName, *tableName)
	fmt.Printf("\n\nQueryOutput for the query `%s` is as follows...\n", queryString)
	queryOutput, err := timestreamBuilder.QueryWithQueryString(queryString)

	if err != nil {
		fmt.Printf("Error while querying: %s", err.Error())
	} else {
		utils.ParseQueryResult(queryOutput, nil)
	}
}
