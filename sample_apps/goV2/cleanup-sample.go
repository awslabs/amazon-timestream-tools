package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
	"go_sample/utils"
	"net/http"
)

func main() {
	// process command line arguments
	region := flag.String("region", "us-east-1", "region")
	flag.Parse()

	databaseName := utils.DATABASE_NAME
	tableName := utils.TABLE_NAME

	tr := utils.LoadHttpSettings()
	// Use the SDK's default configuration.
	cfg, err := config.LoadDefaultConfig(context.TODO())
	cfg.Region = *region
	cfg.HTTPClient = &http.Client{Transport: tr}

	utils.HandleError(err, "Failed to load config ", true)

	writeSvc := timestreamwrite.NewFromConfig(cfg)
	querySvc := timestreamquery.NewFromConfig(cfg)

	s3Svc := s3.NewFromConfig(cfg)

	timestreamBuilder := utils.TimestreamBuilder{WriteSvc: writeSvc, QuerySvc: querySvc}
	timestreamDependencyHelper := utils.TimestreamDependencyHelper{S3Svc: s3Svc}
	var RESOURCE_NOT_FOUND *types.ResourceNotFoundException

	fmt.Printf("Describing a table with name %s\n", tableName)
	// Describe table.
	describeTableOutput, err := timestreamBuilder.DescribeTable(databaseName, tableName)
	if err != nil {
		if errors.As(err, &RESOURCE_NOT_FOUND) {
			fmt.Printf("Table %s was already deleted/does not exist\n\n", tableName)
		} else {
			utils.HandleError(err, fmt.Sprintf("Failed to describe table='%s'\n", tableName), true)
		}
	} else {
		if *describeTableOutput.Table.MagneticStoreWriteProperties.EnableMagneticStoreWrites == true &&
			describeTableOutput.Table.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation != nil &&
			describeTableOutput.Table.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation.S3Configuration != nil {
			s3BucketName := *describeTableOutput.Table.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation.S3Configuration.BucketName
			fmt.Printf("Deleting S3 bucket associated with the table. s3BucketName='%s'\n", s3BucketName)
			err = timestreamDependencyHelper.DeleteS3Bucket(s3BucketName)
			if err != nil {
				fmt.Printf("Failed to delete s3BucketName='%s', please delete it manually\n", s3BucketName)
			}
		}

		fmt.Printf("Deleting table='%s'\n", tableName)
		err = timestreamBuilder.DeleteTable(databaseName, tableName)
		utils.HandleError(err, fmt.Sprintf("Failed to delete table='%s'\n\n", tableName), true)
	}

	fmt.Printf("Deleting database='%s'\n", databaseName)
	err = timestreamBuilder.DeleteDatabase(databaseName)
	if err != nil && errors.As(err, &RESOURCE_NOT_FOUND) {
		fmt.Printf("Database %s was already deleted/does not exist\n", databaseName)
	}
}
