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

func main() {
	// process command line arguments
	region := flag.String("region", "us-east-1", "region")
	flag.Parse()

	databaseName := utils.DATABASE_NAME
	tableName := utils.TABLE_NAME

	sess, err := utils.GetSession(*region)
	utils.HandleError(err, "Failed to start a new session", true)

	writeSvc := timestreamwrite.New(sess)
	querySvc := timestreamquery.New(sess)

	s3Svc := s3.New(sess, aws.NewConfig().WithRegion(*region))

	timestreamBuilder := utils.TimestreamBuilder{WriteSvc: writeSvc, QuerySvc: querySvc}
	timestreamDependencyHelper := utils.TimestreamDependencyHelper{S3Svc: s3Svc}

	fmt.Println("Describing the table")
	// Describe table.
	describeTableOutput, err := timestreamBuilder.DescribeTable(databaseName, tableName)
	if *describeTableOutput.Table.MagneticStoreWriteProperties.EnableMagneticStoreWrites == true &&
		describeTableOutput.Table.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation != nil &&
			describeTableOutput.Table.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation.S3Configuration != nil {
		s3BucketName := *describeTableOutput.Table.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation.S3Configuration.BucketName
		fmt.Printf("Deleting S3 bucket associated with the table. s3BucketName='%s'\n",s3BucketName)
		err = timestreamDependencyHelper.DeleteS3Bucket(s3BucketName)
		if err != nil {
			fmt.Printf("Failed to delete s3BucketName='%s', please delete it manually\n",s3BucketName)
		}
	}

	fmt.Printf("Deleting table='%s'\n", tableName)
	err = timestreamBuilder.DeleteTable(databaseName, tableName)
	utils.HandleError(err, fmt.Sprintf("Failed to delete table='%s'\n", tableName), true)

	fmt.Printf("Deleting database='%s'\n", databaseName)
	err = timestreamBuilder.DeleteDatabase(databaseName)
	utils.HandleError(err, fmt.Sprintf("Failed to delete database='%s'\n", databaseName), true)

}
