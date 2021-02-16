package main

import (
	"flag"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
	"bufio"

	"golang.org/x/net/http2"
)

/**
  This code sample is to run the CRUD APIs and WriteRecords API in a logical order.
*/
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

	sess, err := session.NewSession(&aws.Config{Region: aws.String("us-east-1"), MaxRetries: aws.Int(10), HTTPClient: &http.Client{Transport: tr}})
	writeSvc := timestreamwrite.New(sess)

	databaseName := flag.String("database_name", "devops", "database name string")
	tableName := flag.String("table_name", "host_metrics", "table name string")
	kmsKeyId := flag.String("kms_key_id", "", "kms key id for update database string")

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

	fmt.Println("Describing the database")

	// Describe database.
	describeDatabaseInput := &timestreamwrite.DescribeDatabaseInput{
		DatabaseName: aws.String(*databaseName),
	}

	describeDatabaseOutput, err := writeSvc.DescribeDatabase(describeDatabaseInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Describe database is successful, below is the output:")
		fmt.Println(describeDatabaseOutput)
	}


	if (*kmsKeyId == "") {
		fmt.Println("Skipping update database because kmsKeyId was not provided")
	} else {
		fmt.Println("Updating the database")

		// Update Database.
		updateDatabaseInput := &timestreamwrite.UpdateDatabaseInput {
			DatabaseName: aws.String(*databaseName),
			KmsKeyId: aws.String(*kmsKeyId),
		}

		updateDatabaseOutput, err := writeSvc.UpdateDatabase(updateDatabaseInput)

		if err != nil {
			fmt.Println("Error:")
			fmt.Println(err)
		} else {
			fmt.Println("Update database is successful, below is the output:")
			fmt.Println(updateDatabaseOutput)
		}
	}

	fmt.Println("Listing databases")

	// List databases.
	listDatabasesMaxResult := int64(15)

	listDatabasesInput := &timestreamwrite.ListDatabasesInput{
		MaxResults: &listDatabasesMaxResult,
	}

	listDatabasesOutput, err := writeSvc.ListDatabases(listDatabasesInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("List databases is successful, below is the output:")
		fmt.Println(listDatabasesOutput)
	}

	fmt.Println("Creating a table")

	// Create table.
	createTableInput := &timestreamwrite.CreateTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
	}
	_, err = writeSvc.CreateTable(createTableInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Create table is successful")
	}

	fmt.Println("Describing the table")

	// Describe table.
	describeTableInput := &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
	}
	describeTableOutput, err := writeSvc.DescribeTable(describeTableInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Describe table is successful, below is the output:")
		fmt.Println(describeTableOutput)
	}

	fmt.Println("Listing tables")

	// List tables.
	listTablesMaxResult := int64(15)

	listTablesInput := &timestreamwrite.ListTablesInput{
		DatabaseName: aws.String(*databaseName),
		MaxResults:   &listTablesMaxResult,
	}
	listTablesOutput, err := writeSvc.ListTables(listTablesInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("List tables is successful, below is the output:")
		fmt.Println(listTablesOutput)
	}

	fmt.Println("Updating the table")

	// Update table.
	magneticStoreRetentionPeriodInDays := int64(7 * 365)
	memoryStoreRetentionPeriodInHours := int64(24)

	updateTableInput := &timestreamwrite.UpdateTableInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MagneticStoreRetentionPeriodInDays: &magneticStoreRetentionPeriodInDays,
			MemoryStoreRetentionPeriodInHours:  &memoryStoreRetentionPeriodInHours,
		},
	}
	updateTableOutput, err := writeSvc.UpdateTable(updateTableInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Update table is successful, below is the output:")
		fmt.Println(updateTableOutput)
	}

	fmt.Println("Ingesting records")

	// Below code will ingest cpu_utilization and memory_utilization metric for a host on
	// region=us-east-1, az=az1, and hostname=host1

	// Get current time in seconds.
	now := time.Now()
	currentTimeInSeconds := now.Unix()
	writeRecordsInput := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		Records: []*timestreamwrite.Record{
			&timestreamwrite.Record{
				Dimensions: []*timestreamwrite.Dimension{
					&timestreamwrite.Dimension{
						Name:  aws.String("region"),
						Value: aws.String("us-east-1"),
					},
					&timestreamwrite.Dimension{
						Name:  aws.String("az"),
						Value: aws.String("az1"),
					},
					&timestreamwrite.Dimension{
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
			&timestreamwrite.Record{
				Dimensions: []*timestreamwrite.Dimension{
					&timestreamwrite.Dimension{
						Name:  aws.String("region"),
						Value: aws.String("us-east-1"),
					},
					&timestreamwrite.Dimension{
						Name:  aws.String("az"),
						Value: aws.String("az1"),
					},
					&timestreamwrite.Dimension{
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

	_, err = writeSvc.WriteRecords(writeRecordsInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Write records is successful")
	}

	fmt.Println("Ingesting records with common attributes method")

	// Below code will ingest the same data with common attributes approach.
	now = time.Now()
	currentTimeInSeconds = now.Unix()
	writeRecordsCommonAttributesInput := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		CommonAttributes: &timestreamwrite.Record{
			Dimensions: []*timestreamwrite.Dimension{
				&timestreamwrite.Dimension{
					Name:  aws.String("region"),
					Value: aws.String("us-east-1"),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String("az"),
					Value: aws.String("az1"),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String("hostname"),
					Value: aws.String("host1"),
				},
			},
			MeasureValueType: aws.String("DOUBLE"),
			Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
			TimeUnit:         aws.String("SECONDS"),
		},
		Records: []*timestreamwrite.Record{
			&timestreamwrite.Record{
				MeasureName:  aws.String("cpu_utilization"),
				MeasureValue: aws.String("13.5"),
			},
			&timestreamwrite.Record{
				MeasureName:  aws.String("memory_utilization"),
				MeasureValue: aws.String("40"),
			},
		},
	}

	_, err = writeSvc.WriteRecords(writeRecordsCommonAttributesInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Ingest records is successful")
	}

	// Below code will ingest and upsert cpu_utilization and memory_utilization metric for a host on
	// region=us-east-1, az=az1, and hostname=host1
	fmt.Println("Ingesting records and set version as currentTimeInMills, hit enter to continue")
	reader.ReadString('\n')

	// Get current time in seconds.
	now = time.Now()
	currentTimeInSeconds = now.Unix()
	// To achieve upsert (last writer wins) semantic, one example is to use current time as the version if you are writing directly from the data source
	version := time.Now().Round(time.Millisecond).UnixNano() / 1e6   // set version as currentTimeInMills

	writeRecordsCommonAttributesUpsertInput := &timestreamwrite.WriteRecordsInput{
		DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
		CommonAttributes: &timestreamwrite.Record{
			Dimensions: []*timestreamwrite.Dimension{
				&timestreamwrite.Dimension{
					Name:  aws.String("region"),
					Value: aws.String("us-east-1"),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String("az"),
					Value: aws.String("az1"),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String("hostname"),
					Value: aws.String("host1"),
				},
			},
			MeasureValueType: aws.String("DOUBLE"),
			Time:             aws.String(strconv.FormatInt(currentTimeInSeconds, 10)),
			TimeUnit:         aws.String("SECONDS"),
			Version:          &version,
		},
		Records: []*timestreamwrite.Record{
			&timestreamwrite.Record{
				MeasureName:  aws.String("cpu_utilization"),
				MeasureValue: aws.String("13.5"),
			},
			&timestreamwrite.Record{
				MeasureName:  aws.String("memory_utilization"),
				MeasureValue: aws.String("40"),
			},
		},
	}

	// write records for first time
	_, err = writeSvc.WriteRecords(writeRecordsCommonAttributesUpsertInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Frist-time write records is successful")
	}

	fmt.Println("Retry same writeRecordsRequest with same records and versions. Because writeRecords API is idempotent, this will success. hit enter to continue")
	reader.ReadString('\n')

	_, err = writeSvc.WriteRecords(writeRecordsCommonAttributesUpsertInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Retry write records for same request is successful")
	}

	fmt.Println("Upsert with lower version, this would fail because a higher version is required to update the measure value. hit enter to continue")
	reader.ReadString('\n')
	version -= 1
	writeRecordsCommonAttributesUpsertInput.CommonAttributes.Version = &version

	updated_cpu_utilization := &timestreamwrite.Record{
		MeasureName:      aws.String("cpu_utilization"),
		MeasureValue:     aws.String("14.5"),
	}
	updated_memory_utilization := &timestreamwrite.Record{
		MeasureName:      aws.String("memory_utilization"),
		MeasureValue:     aws.String("50"),
	}
	

	writeRecordsCommonAttributesUpsertInput.Records = []*timestreamwrite.Record{
		updated_cpu_utilization,
		updated_memory_utilization,
	}

	_, err = writeSvc.WriteRecords(writeRecordsCommonAttributesUpsertInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Write records with lower version is successful")
	}

	fmt.Println("Upsert with higher version as new data is generated, this would success. hit enter to continue")
	reader.ReadString('\n')

	version = time.Now().Round(time.Millisecond).UnixNano() / 1e6  // set version as currentTimeInMills
	writeRecordsCommonAttributesUpsertInput.CommonAttributes.Version = &version

	_, err = writeSvc.WriteRecords(writeRecordsCommonAttributesUpsertInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Write records with higher version is successful")
	}

	// Exiting from here to avoid table and database cleanup being called.
	// Comment-out/Remove the exit line to run delete table and delete database
	fmt.Println("\nExiting from here to avoid table and database cleanup being called.")
	os.Exit(0)

	fmt.Println("Deleting table")

	deleteTableInput := &timestreamwrite.DeleteTableInput{
        DatabaseName: aws.String(*databaseName),
		TableName:    aws.String(*tableName),
	}
	_, err = writeSvc.DeleteTable(deleteTableInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Table deleted", *tableName)
	}

	fmt.Println("Deleting database")

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
