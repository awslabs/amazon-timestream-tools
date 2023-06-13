package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"go_sample/utils"
	"net/http"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/timestreamquery"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite"
	"github.com/aws/aws-sdk-go-v2/service/timestreamwrite/types"
)

/**
  This code sample is to demonstrate
			1. Creating a table with composite partition key (dimension key) with optional enforcement
			2. Creating a table with composite partition key (measure key)
			3. Describe table with partition key
			4. Ingest records without dimension partition key and succeed
	        5. Update dimension partition key Optional enforcement to Required enforcement
	        6. Ingest records with dimension partition key to table with enforced dimension partition key
			7. Ingest records without dimension partition key to table with enforced dimension partition key (rejected records)
			8. Ingest records table created with measure partition key
	    	9. Execute query on tables with partition keys
			10. Clean up resources
*/

const (
	PartitionKeyDimensionTableName = "host_metrics_dim_pk"
	PartitionKeyMeasureTableName   = "host_metrics_measure_pk"
	CompositePartitionKeyDimName   = "hostId"
	CompositePartitionKeyDimValue  = "host1"
	CompositePartitionKeyDiffName  = "hostIdDiff"
)

var ResourceAlreadyExists *types.ConflictException

func main() {
	tr := utils.LoadHttpSettings()
	// Use the SDK's default configuration.
	cfg, _ := config.LoadDefaultConfig(context.TODO())

	databaseName := flag.String("database_name", utils.DATABASE_NAME, "database name string")
	tableForDimensionPartitionKey := flag.String("table_with_dimension_partition_key", PartitionKeyDimensionTableName, "table name string")
	tableForMeasurePartitionKey := flag.String("table_with_measure_partition_key", PartitionKeyMeasureTableName, "table name string")
	region := flag.String("region", utils.REGION, "region")

	flag.Parse()
	reader := bufio.NewReader(os.Stdin)

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

	//Cleaning up resources at the end
	defer utils.CleanUp(timestreamBuilder, timestreamDependencyHelper, *databaseName, *tableForDimensionPartitionKey, s3BucketName)
	defer utils.CleanUp(timestreamBuilder, timestreamDependencyHelper, *databaseName, *tableForMeasurePartitionKey, s3BucketName)

	fmt.Printf("Creating a database with name %s\n", *databaseName)

	// Create database.
	err := timestreamBuilder.CreateDatabase(*databaseName)
	if err != nil {
		if errors.As(err, &ResourceAlreadyExists) {
			fmt.Printf("Creating database with name %s failed as it already exists\n", *databaseName)
		} else {
			utils.HandleError(err, fmt.Sprintf("Failed to create database %s ", *databaseName), true)
		}
	}

	// Create table and S3 bucket.
	err = timestreamDependencyHelper.CreateS3Bucket(s3BucketName, *region)
	utils.HandleError(err, fmt.Sprintf("Failed to create S3Bucket %s ", s3BucketName), true)

	/* This sample demonstrates workflow using dimension type partition key
	 * 1. Create table with dimension type partition key, with optional enforcement.
	 * 2. Ingest records with missing partition key. Records will be accepted as enforcement level is optional
	 * 3. Update table with required enforcement.
	 * 4. Ingest records with same partition key.
	 * 5. Ingest records with missing partition key. This will return rejected records as they do not
	 *    contain required partition key.
	 * 6. Query records with same partition key.
	 */
	runSampleWithDimensionPartitionKey(timestreamBuilder, s3BucketName, databaseName, tableForDimensionPartitionKey, reader)

	/* This sample demonstrates workflow using measure name type partition key
	 * 1. Create table with measure name type partition key
	 * 2. Ingest records and query
	 */
	runSampleWithMeasureNamePartitionKey(timestreamBuilder, s3BucketName, databaseName, tableForMeasurePartitionKey, reader)
}

func runSampleWithMeasureNamePartitionKey(timestreamBuilder utils.TimestreamBuilder, s3BucketName string,
	databaseName *string, tableName *string, reader *bufio.Reader) {

	fmt.Println("Starting example for measure name type partition key:")
	fmt.Printf("Creating table %s with measure partition key, hit enter to continue", *tableName)
	reader.ReadString('\n')
	partitionKeyWithMeasure := []types.PartitionKey{
		{
			Type: types.PartitionKeyTypeMeasure,
		},
	}
	// Create table with measure partition key and OPTIONAL enforcement
	fmt.Printf("\n\nCreating a table with name %s\n", *tableName)
	err := timestreamBuilder.CreateTableWithPartitionKeys(*databaseName, *tableName, s3BucketName, partitionKeyWithMeasure)
	if err != nil {
		if errors.As(err, &ResourceAlreadyExists) {
			fmt.Printf("\n\nCreating table with name %s failed as it already exists\n", *tableName)
		} else {
			utils.HandleError(err, fmt.Sprintf("Failed to create table %s ", *tableName), true)
		}
	}

	// Describe table.
	fmt.Printf("Describing the table %s with dimension partition key, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	timestreamBuilder.DescribeTable(*databaseName, *tableName)

	fmt.Printf("Ingesting records for table %s, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	writeRecords(timestreamBuilder.WriteSvc, databaseName, tableName, CompositePartitionKeyDimName, reader)

	// Run Query with partitionKey
	fmt.Printf("Run query for table %s, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	runSingleQuery(databaseName, tableName, aws.String(CompositePartitionKeyDimName), aws.String("'host1'"), timestreamBuilder)
}

func runSampleWithDimensionPartitionKey(timestreamBuilder utils.TimestreamBuilder, s3BucketName string,
	databaseName *string, tableName *string, reader *bufio.Reader) {
	fmt.Println("Starting example for dimension type partition key:")

	// Composite Partition Keys are most effective when dimension has high cardinality
	// and are frequently accessed in queries
	// Using dimension name with high cardinality, "hostId"
	fmt.Printf("Creating table %s with dimension partition key(OPTIONAL ENFORCEMENT), hit enter to continue", *tableName)
	reader.ReadString('\n')

	partitionKeyWithDimensionAndOptionalEnforcement := []types.PartitionKey{
		{
			Name:                aws.String(CompositePartitionKeyDimName),
			EnforcementInRecord: types.PartitionKeyEnforcementLevelOptional,
			Type:                types.PartitionKeyTypeDimension,
		},
	}

	// Create table with dimension type partition key and OPTIONAL enforcement
	fmt.Printf("\n\nCreating a table with name %s\n", *tableName)
	err := timestreamBuilder.CreateTableWithPartitionKeys(*databaseName, *tableName, s3BucketName, partitionKeyWithDimensionAndOptionalEnforcement)
	if err != nil {
		if errors.As(err, &ResourceAlreadyExists) {
			fmt.Printf("\n\nCreating table with name %s failed as it already exists\n", *tableName)
		} else {
			utils.HandleError(err, fmt.Sprintf("Failed to create table %s ", *tableName), true)
		}
	}

	// Describe table.
	fmt.Printf("Describing the table %s with dimension partition key, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	timestreamBuilder.DescribeTable(*databaseName, *tableName)

	fmt.Printf("Ingest records without dimension type partition key for table  %s, Since the enforcement level is OPTIONAL the records will be ingested"+
		", hit enter to continue", *tableName)
	reader.ReadString('\n')
	writeRecords(timestreamBuilder.WriteSvc, databaseName, tableName, CompositePartitionKeyDiffName, reader)

	fmt.Printf("Updating the dimension partition key enforcement from optional to required for table %s, hit enter to continue", *tableName)
	reader.ReadString('\n')

	// Update table partition key enforcement attribute
	updateTableInput := &timestreamwrite.UpdateTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		Schema: &types.Schema{
			CompositePartitionKey: []types.PartitionKey{
				{
					Name:                aws.String(CompositePartitionKeyDimName),
					EnforcementInRecord: types.PartitionKeyEnforcementLevelRequired,
					Type:                types.PartitionKeyTypeDimension,
				},
			}},
	}
	updateTableOutput, err := timestreamBuilder.WriteSvc.UpdateTable(context.TODO(), updateTableInput)
	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Update table is successful, below is the output:")
		fmt.Println(updateTableOutput)
	}

	// Describe table.
	fmt.Printf("Describing the table %s with dimension partition key, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	timestreamBuilder.DescribeTable(*databaseName, *tableName)

	fmt.Printf("Ingesting records for table %s, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	writeRecords(timestreamBuilder.WriteSvc, databaseName, tableName, CompositePartitionKeyDimName, reader)

	fmt.Printf("Ingesting records for table %s - Records will be rejected as ingestion done without enforced partition key"+
		", hit enter to continue", *tableName)
	reader.ReadString('\n')
	writeRecords(timestreamBuilder.WriteSvc, databaseName, tableName, CompositePartitionKeyDiffName, reader)

	// Run Query with partitionKey
	fmt.Printf("Run query for table %s, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	runSingleQuery(databaseName, tableName, aws.String(CompositePartitionKeyDimName), aws.String("'host1'"), timestreamBuilder)
}

func runSingleQuery(databaseName *string, tableName *string, partitionKeyName *string,
	partitionKeyValue *string, timestreamBuilder utils.TimestreamBuilder) {

	// sample query
	queryString := fmt.Sprintf("select * from \"%s\".\"%s\" where \"%s\"=%s",
		*databaseName, *tableName, *partitionKeyName, *partitionKeyValue)

	fmt.Println(queryString)
	queryOutput, err := timestreamBuilder.QueryWithQueryString(queryString)

	if err != nil {
		fmt.Printf("Error while querying: %s", err.Error())
	} else {
		utils.ParseQueryResult(queryOutput, nil)
	}
}

func writeRecords(writeSvc *timestreamwrite.Client, databaseName *string, tableName *string,
	compositePartitionKeyDimName string, reader *bufio.Reader) {

	// common set of dimensions used for ingestion
	dimensions := []types.Dimension{
		{
			Name:  aws.String(compositePartitionKeyDimName),
			Value: aws.String(CompositePartitionKeyDimValue),
		},
		{
			Name:  aws.String("region"),
			Value: aws.String("us-east-1"),
		},
		{
			Name:  aws.String("az"),
			Value: aws.String("az1"),
		},
	}

	fmt.Printf("\n\nIngesting records with multi measures to table %s\n", *tableName)
	reader.ReadString('\n')

	// Ingest multi-measure record with multiple records or mixture type
	writeRecordsInputMultiMixture := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		Records:      utils.GetRecordsWithMultiMeasuresMultipleRecords(dimensions),
	}
	utils.IngestToTimestream(writeSvc, writeRecordsInputMultiMixture, "Ingesting records with multi measures mixture type successful")
}
