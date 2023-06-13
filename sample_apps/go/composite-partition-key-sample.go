package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	"golang.org/x/net/http2"
)

/*
	    This code sample is to demonstrate
			1. Creating a table with composite partition key (dimension key) with optional enforcement
			2. Creating a table with composite partition key (measure key)
			3. Describe table with partition key
			4. Ingest records without dimension partition key (optional enforcement) and succeed
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

func main() {

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

	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1"),
		MaxRetries: aws.Int(10), HTTPClient: &http.Client{Transport: tr}})
	writeSvc := timestreamwrite.New(sess)

	qsess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1"),
		MaxRetries: aws.Int(10), HTTPClient: &http.Client{Transport: tr}})
	querySvc := timestreamquery.New(qsess)

	databaseName := flag.String("database_name", "devops", "database name string")
	tableForDimensionPartitionKey := flag.String("table_with_dimension_partition_key", PartitionKeyDimensionTableName, "table name string")
	tableForMeasurePartitionKey := flag.String("table_with_measure_partition_key", PartitionKeyMeasureTableName, "table name string")
	flag.Parse()

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Creating a database, hit enter to continue")
	reader.ReadString('\n')
	// Create database.
	createDatabaseInput := &timestreamwrite.CreateDatabaseInput{
		DatabaseName: aws.String(*databaseName),
	}
	_, err = writeSvc.CreateDatabase(createDatabaseInput)
	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Database successfully created")
	}

	/* This sample demonstrates workflow using dimension type partition key
	 * 1. Create table with dimension type partition key, with optional enforcement.
	 * 2. Ingest records with missing partition key. Records will be accepted as enforcement level is optional
	 * 3. Update table with required enforcement.
	 * 4. Ingest records with same partition key.
	 * 5. Ingest records with missing partition key. This will return rejected records as they do not
	 *    contain required partition key.
	 * 6. Query records with same partition key.
	 */
	runSampleWithDimensionPartitionKey(writeSvc, querySvc, databaseName, tableForDimensionPartitionKey, reader)

	/* This sample demonstrates workflow using measure name type partition key
	 * 1. Create table with measure name type partition key
	 * 2. Ingest records and query
	 */
	runSampleWithMeasureNamePartitionKey(writeSvc, querySvc, databaseName, tableForMeasurePartitionKey, reader)

	// Delete tables
	fmt.Println("Deleting tables, hit enter to continue.")
	reader.ReadString('\n')

	deleteCompositePartitionKeyTable(writeSvc, databaseName, tableForDimensionPartitionKey)
	deleteCompositePartitionKeyTable(writeSvc, databaseName, tableForMeasurePartitionKey)

	fmt.Println("Deleting database, hit enter to continue.")
	reader.ReadString('\n')
	deleteDatabaseInput := &timestreamwrite.DeleteDatabaseInput{
		DatabaseName: aws.String(*databaseName),
	}
	_, err = writeSvc.DeleteDatabase(deleteDatabaseInput)
	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Database deleted:", *databaseName)
	}
}

func runSampleWithMeasureNamePartitionKey(writeSvc *timestreamwrite.TimestreamWrite, querySvc *timestreamquery.TimestreamQuery,
	databaseName *string, tableName *string, reader *bufio.Reader) {

	fmt.Println("Starting example for measure name type partition key:")
	fmt.Printf("Creating table %s with measure partition key, hit enter to continue", *tableName)
	reader.ReadString('\n')
	partitionKeyListMeasure := []*timestreamwrite.PartitionKey{
		{
			Type: aws.String("MEASURE"),
		},
	}
	createTableWithPartitionKey(writeSvc, databaseName, tableName, partitionKeyListMeasure)

	fmt.Printf("Describing the table %s with measure partition key, hit enter to continue", *tableName)
	reader.ReadString('\n')
	// Describe table.
	describeTableInputForMeasurePartitionKey := &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
	}
	describeTableOutputForMeasurePartitionKey, err := writeSvc.DescribeTable(describeTableInputForMeasurePartitionKey)
	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Describe table is successful, below is the output:")
		fmt.Println(describeTableOutputForMeasurePartitionKey)
	}

	fmt.Printf("Ingesting records for table %s, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	writeRecords(writeSvc, reader, databaseName, tableName, CompositePartitionKeyDimName)

	// Run Query with partitionKey
	fmt.Printf("Run query for table %s, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	runSingleQuery(databaseName, tableName, aws.String("cpu_utilization"), aws.String("13.5"), querySvc)
}

func runSampleWithDimensionPartitionKey(writeSvc *timestreamwrite.TimestreamWrite, querySvc *timestreamquery.TimestreamQuery,
	databaseName *string, tableName *string, reader *bufio.Reader) {
	fmt.Println("Starting example for dimension type partition key:")

	// Composite Partition Keys are most effective when dimension has high cardinality
	// and are frequently accessed in queries
	// Using dimension name with high cardinality, "hostId"
	fmt.Printf("Creating table %s with dimension partition key(OPTIONAL ENFORCEMENT), hit enter to continue", *tableName)
	reader.ReadString('\n')
	partitionKeyWithDimensionAndOptionalEnforcement := []*timestreamwrite.PartitionKey{
		{
			Name:                aws.String(CompositePartitionKeyDimName),
			EnforcementInRecord: aws.String("OPTIONAL"),
			Type:                aws.String("DIMENSION"),
		},
	}

	// Create table with dimension type partition key and OPTIONAL enforcement
	createTableWithPartitionKey(writeSvc, databaseName, tableName, partitionKeyWithDimensionAndOptionalEnforcement)

	fmt.Printf("Describing the table %s with dimension partition key, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	// Describe table.
	describeTableInputForDimensionPartitionKey := &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
	}
	describeTableOutput, err := writeSvc.DescribeTable(describeTableInputForDimensionPartitionKey)
	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Describe table is successful, below is the output:")
		fmt.Println(describeTableOutput)
	}

	fmt.Printf("Ingest records without dimension type partition key for table  %s, Since the enforcement level is OPTIONAL the records will be ingested"+
		", hit enter to continue", *tableName)
	reader.ReadString('\n')
	writeRecords(writeSvc, reader, databaseName, tableName, CompositePartitionKeyDiffName)

	fmt.Printf("Updating the dimension partition key enforcement from optional to required for table %s, hit enter to continue", *tableName)
	reader.ReadString('\n')
	// Update table partition key enforcement attribute
	updateTableInput := &timestreamwrite.UpdateTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		Schema: &timestreamwrite.Schema{
			CompositePartitionKey: []*timestreamwrite.PartitionKey{
				{
					Name:                aws.String(CompositePartitionKeyDimName),
					EnforcementInRecord: aws.String("REQUIRED"),
					Type:                aws.String("DIMENSION"),
				},
			}},
	}
	updateTableOutput, err := writeSvc.UpdateTable(updateTableInput)
	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Update table is successful, below is the output:")
		fmt.Println(updateTableOutput)
	}

	fmt.Printf("Describing the table %s, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	// Describe table.
	describeTableInputForDimensionPartitionKeyAfterUpdate := &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
	}
	describeTableOutputForDimensionPartitionKeyAfterUpdate, err := writeSvc.DescribeTable(describeTableInputForDimensionPartitionKeyAfterUpdate)
	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Describe table is successful, below is the output:")
		fmt.Println(describeTableOutputForDimensionPartitionKeyAfterUpdate)
	}

	fmt.Printf("Ingesting records for table %s, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	writeRecords(writeSvc, reader, databaseName, tableName, CompositePartitionKeyDimName)

	fmt.Printf("Ingesting records for table %s - Records will be rejected as ingestion done without enforced partition key"+
		", hit enter to continue", *tableName)
	reader.ReadString('\n')
	writeRecords(writeSvc, reader, databaseName, tableName, CompositePartitionKeyDiffName)

	// Run Query with partitionKey
	fmt.Printf("Run query for table %s, hit enter to continue\n", *tableName)
	reader.ReadString('\n')
	runSingleQuery(databaseName, tableName, aws.String(CompositePartitionKeyDimName), aws.String("'host1'"), querySvc)
}

func deleteCompositePartitionKeyTable(writeSvc *timestreamwrite.TimestreamWrite, databaseName *string, tableName *string) {
	_, err := writeSvc.DeleteTable(&timestreamwrite.DeleteTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
	})

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Table deleted", *tableName)
	}
}

func runSingleQuery(databaseName *string, tableName *string, partitionKeyName *string,
	partitionKeyValue *string, querySvc *timestreamquery.TimestreamQuery) {

	// sample query
	queryString := fmt.Sprintf("select * from \"%s\".\"%s\" where \"%s\"=%s",
		*databaseName, *tableName, *partitionKeyName, *partitionKeyValue)

	/*queryString := "select * from cdpk-crud-db.host_metrics_dim_pk where hostId='host1'"*/
	fmt.Println(queryString)
	queryInput := &timestreamquery.QueryInput{
		QueryString: aws.String(queryString),
	}

	// execute the query
	queryOutput, err := querySvc.Query(queryInput)
	if err == nil {
		fmt.Println(queryOutput)
	} else {
		fmt.Println("Error:")
		fmt.Println(err)
	}
}

func generateRecords(dimensions []*timestreamwrite.Dimension) []*timestreamwrite.Record {
	var records []*timestreamwrite.Record
	currentTimeInSeconds := time.Now().Unix()

	records = append(records, &timestreamwrite.Record{
		Dimensions:       dimensions,
		MeasureName:      aws.String("cpu_utilization"),
		MeasureValue:     aws.String("13.5"),
		MeasureValueType: aws.String("DOUBLE"),
		Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
		TimeUnit:         aws.String("SECONDS"),
	}, &timestreamwrite.Record{
		Dimensions:       dimensions,
		MeasureName:      aws.String("memory_utilization"),
		MeasureValue:     aws.String("40"),
		MeasureValueType: aws.String("DOUBLE"),
		Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
		TimeUnit:         aws.String("SECONDS"),
	})

	return records
}

func generateMultiMeasureRecords(dimensions []*timestreamwrite.Dimension) []*timestreamwrite.Record {
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

func writeRecords(writeSvc *timestreamwrite.TimestreamWrite, reader *bufio.Reader, databaseName *string,
	tableName *string, compositePartitionKeyDimName string) {

	var dimensions []*timestreamwrite.Dimension
	dimensions = append(dimensions, &timestreamwrite.Dimension{
		Name:  aws.String(compositePartitionKeyDimName),
		Value: aws.String(CompositePartitionKeyDimValue),
	}, &timestreamwrite.Dimension{
		Name:  aws.String("region"),
		Value: aws.String("us-east-1"),
	}, &timestreamwrite.Dimension{
		Name:  aws.String("az"),
		Value: aws.String("az1"),
	})

	writeRecordsInput := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		Records:      generateRecords(dimensions),
	}

	_, err := writeSvc.WriteRecords(writeRecordsInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Write records is successful")
	}

	fmt.Println("Ingesting multi measure records, hit enter to continue")
	reader.ReadString('\n')

	writeRecordsInputMulti := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		Records:      generateMultiMeasureRecords(dimensions),
	}

	_, err = writeSvc.WriteRecords(writeRecordsInputMulti)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Write records is successful")
	}
}

func createTableWithPartitionKey(writeSvc *timestreamwrite.TimestreamWrite, databaseName *string,
	tableName *string, compositePartitionKey []*timestreamwrite.PartitionKey) {

	fmt.Printf("Creating table with table name: %s\n", *tableName)
	// Create table with partition key
	createTableInput := &timestreamwrite.CreateTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		// Enable MagneticStoreWrite for Table
		MagneticStoreWriteProperties: &timestreamwrite.MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: aws.Bool(true),
			// Persist MagneticStoreWrite rejected records in S3
			MagneticStoreRejectedDataLocation: &timestreamwrite.MagneticStoreRejectedDataLocation{
				S3Configuration: &timestreamwrite.S3Configuration{
					BucketName:       aws.String("timestream-sample-bucket"),
					ObjectKeyPrefix:  aws.String("TimeStreamCustomerSampleGo"),
					EncryptionOption: aws.String("SSE_S3"),
				},
			},
		},
		Schema: &timestreamwrite.Schema{
			CompositePartitionKey: compositePartitionKey,
		},
	}
	_, err := writeSvc.CreateTable(createTableInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Create table is successful")
	}
}