package main

import (
	"encoding/json"
	"encoding/csv"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
	"net/url"
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/sts"
	"github.com/aws/aws-sdk-go/service/timestreamquery"
	"github.com/aws/aws-sdk-go/service/timestreamwrite"
	"golang.org/x/net/http2"
)

type Manifest struct {
	Author interface{}
	Query_metadata map[string]any
	Result_files  []struct {
		File_metadata interface{}
		Url string
	}
}

type Metadata struct {
	Author interface{}
	ColumnInfo []struct {
		Name string
		Type map[string]string
	}
}

type UnloadQuery struct {
	Query string
	Partitioned_by []string
	Format string
	S3Location string
	ResultPrefix string
	Compression string
}

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

	var databaseName = "timestream_unload_db"
	var tableName = "timestream_unload_table"

	testFileName := flag.String("csv_file_path", "../data/sample_unload.csv", "CSV file containing the data to ingest")
	region := flag.String("region", "us-west-2", "region to execute the Unload queries")
	skipDeletion := flag.Bool("skip_deletion", true, "skip deleting the resources after executing the query")
	flag.Parse()

	sessWrite, err := session.NewSession(&aws.Config{Region: aws.String(*region), MaxRetries: aws.Int(10), HTTPClient: &http.Client{Transport: tr}})
	writeSvc := timestreamwrite.New(sessWrite)

	// setup the query client
	sessQuery, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	querySvc := timestreamquery.New(sessQuery)

	sessS3, err := session.NewSession(&aws.Config{Region: aws.String(*region)})
	s3Svc := s3.New(sessS3)

	svc := sts.New(session.New())
	input := &sts.GetCallerIdentityInput{}
	result, err := svc.GetCallerIdentity(input)

	bucketName := "timestream-" + *region + "-" + *result.Account;
		
		// Describe database.
	describeDatabaseInput := &timestreamwrite.DescribeDatabaseInput{
		DatabaseName: aws.String(databaseName),
	}

	describeDatabaseOutput, err := writeSvc.DescribeDatabase(describeDatabaseInput)

	if err != nil {
		fmt.Println("Error while describing database:")
		fmt.Println(err)
		// Create database if database doesn't exist.
		serr, ok := err.(*timestreamwrite.ResourceNotFoundException)
		fmt.Println(serr)
		if ok {
			fmt.Println("Creating database")
			createDatabaseInput := &timestreamwrite.CreateDatabaseInput{
				DatabaseName: aws.String(databaseName),
			}

			_, err = writeSvc.CreateDatabase(createDatabaseInput)

			if err != nil {
				fmt.Println("Error while creating database:")
				fmt.Println(err)
			} else {
				fmt.Println("Database Created!")
			}
		}
	} else {
		fmt.Println("Database exists")
		fmt.Println(describeDatabaseOutput)
	}

	// Describe table.
	describeTableInput := &timestreamwrite.DescribeTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
	}
	describeTableOutput, err := writeSvc.DescribeTable(describeTableInput)

	if err != nil {
		fmt.Println("Error while describing table:")
		fmt.Println(err)
		serr, ok := err.(*timestreamwrite.ResourceNotFoundException)
		fmt.Println(serr)
		if ok {
			// Create table if table doesn't exist.
			fmt.Println("Creating the table")
			createTableInput := &timestreamwrite.CreateTableInput{
				DatabaseName: aws.String(databaseName),
				TableName:    aws.String(tableName),
			}
			_, err = writeSvc.CreateTable(createTableInput)

			if err != nil {
				fmt.Println("Error while creating table:")
				fmt.Println(err)
			} else {
				fmt.Println("Table Created!")
			}
		}
	} else {
		fmt.Println("Table exists")
		fmt.Println(describeTableOutput)
	}


	fmt.Printf("Creating S3 Bucket with name %s in region %s\n", bucketName, region)
	
	createBucketInput := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}
	if *region != "us-east-1" {
		createBucketInput.CreateBucketConfiguration = &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(*region),
		}
	}
	createBucketOutput, err := s3Svc.CreateBucket(createBucketInput)
	
	if err != nil {
		fmt.Printf("Error: %s\n", err.Error())
	} else {
		fmt.Println("Create S3 Bucket is successful : %v\n", *createBucketOutput)
	}


	csvfile, err := os.Open(*testFileName)
	records := make([]*timestreamwrite.Record, 0)
	if err != nil {
		fmt.Println("Couldn't open the csv file", err)
	}

	// Get current time in nano seconds.
	currentTimeInMilliSeconds := time.Now().UnixNano() / int64(time.Millisecond)
	// Counter for number of records.
	counter := int64(0)
	i := int(0)
	reader := csv.NewReader(csvfile)
	var header []string

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

		if i == 0 {
			header = record
			fmt.Println("Header", header)
			i++
			continue
		}
		
		var measures []*timestreamwrite.MeasureValue
		if record[7] != "" {
			measures = append(measures, &timestreamwrite.MeasureValue{
				Name: aws.String(header[7]),
				Value: aws.String(record[7]),
				Type: aws.String("VARCHAR"),
			},
		)}

		if record[8] != "" {
			measures = append(measures, &timestreamwrite.MeasureValue{
				Name: aws.String(header[8]),
				Value: aws.String(record[8]),
				Type: aws.String("VARCHAR"),
			},
		)}

		if record[9] != "" {
			measures = append(measures, &timestreamwrite.MeasureValue{
				Name: aws.String(header[9]),
				Value: aws.String(record[9]),
				Type: aws.String("VARCHAR"),
			},
		)}

		if record[10] != "" {
			measures = append(measures, &timestreamwrite.MeasureValue{
				Name: aws.String(header[10]),
				Value: aws.String(record[10]),
				Type: aws.String("DOUBLE"),
			},
		)}

		records = append(records, &timestreamwrite.Record{
			Dimensions: []*timestreamwrite.Dimension{
				&timestreamwrite.Dimension{
					Name:  aws.String(header[0]),
					Value: aws.String(record[0]),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String(header[1]),
					Value: aws.String(record[1]),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String(header[2]),
					Value: aws.String(record[2]),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String(header[3]),
					Value: aws.String(record[3]),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String(header[4]),
					Value: aws.String(record[4]),
				},
				&timestreamwrite.Dimension{
					Name:  aws.String(header[5]),
					Value: aws.String(record[5]),
				},
			},

			MeasureValues: measures,
			MeasureName:      aws.String("metrics"),
			MeasureValueType: aws.String("MULTI"),
			Time:             aws.String(strconv.FormatInt(currentTimeInMilliSeconds-counter*int64(50), 10)),
			TimeUnit:         aws.String("MILLISECONDS"),
		})

		counter++
		// WriteRecordsRequest has 100 records limit per request.
		if counter%100 == 0 {
			writeRecordsInput := &timestreamwrite.WriteRecordsInput{
				DatabaseName: aws.String(databaseName),
				TableName:    aws.String(tableName),
				Records:      records,
			}
			ingest(writeRecordsInput, writeSvc, counter)
			records = make([]*timestreamwrite.Record, 0)
		}
	}

	if len(records) > 0 {
		writeRecordsInput := &timestreamwrite.WriteRecordsInput{
			DatabaseName: aws.String(databaseName),
			TableName:    aws.String(tableName),
			Records:      records,
		}
		ingest(writeRecordsInput, writeSvc, counter)
	}

	runQueries(querySvc, s3Svc, bucketName, databaseName, tableName)

	if (*skipDeletion == false) {
		deleteTable(writeSvc, databaseName, tableName)
		deleteDatabase(writeSvc, databaseName)
		deleteBucket(s3Svc, bucketName)
	}
}

func runQueries(querySvc *timestreamquery.TimestreamQuery, s3Svc *s3.S3, bucketName string, databaseName string, tableName string) {

	queries := []UnloadQuery{
		UnloadQuery{
			Query: "SELECT user_id, ip_address, event, session_id, measure_name, time, query, quantity, product_id, channel FROM " + databaseName + "." + tableName + 
			" WHERE time BETWEEN ago(2d) AND now()",
			Partitioned_by: []string{},
			Compression: "GZIP",
			Format: "CSV",
			S3Location: bucketName,
			ResultPrefix: "without_partition",
		},
		UnloadQuery{
			Query: "SELECT user_id, ip_address, event, session_id, measure_name, time, query, quantity, product_id, channel FROM " + databaseName + "." + tableName + 
			" WHERE time BETWEEN ago(2d) AND now()",
			Partitioned_by: []string{"channel"},
			Compression: "GZIP",
			Format: "CSV",
			S3Location: bucketName,
			ResultPrefix: "partition_by_channel",
		},
		UnloadQuery{
			Query: "SELECT user_id, ip_address, session_id, measure_name, time, query, quantity, product_id, channel, event FROM " + databaseName + "." + tableName + 
			" WHERE time BETWEEN ago(2d) AND now()",
			Partitioned_by: []string{"event"},
			Compression: "GZIP",
			Format: "CSV",
			S3Location: bucketName,
			ResultPrefix: "partition_by_event",
		},
		UnloadQuery{
			Query: "SELECT user_id, ip_address, session_id, measure_name, time, query, quantity, product_id, channel, event FROM " + databaseName + "." + tableName + 
			" WHERE time BETWEEN ago(2d) AND now()",
			Partitioned_by: []string{"channel", "event"},
			Compression: "GZIP",
			Format: "CSV",
			S3Location: bucketName,
			ResultPrefix: "partition_by_channel_and_event",
		},
	}

	for _, unloadQuery := range queries {
	// execute the query
		fmt.Println("Running: ", unloadQuery.Query)
		queryInput := &timestreamquery.QueryInput{
			QueryString: build_query(unloadQuery),
		}

		err := querySvc.QueryPages(queryInput,
			func(page *timestreamquery.QueryOutput, lastPage bool) bool {
				if (lastPage) {
					var response = parseQueryResult(page)
					var manifest = getManifestFile(s3Svc, response)
					var metadata = getMetadataFile(s3Svc, response)
					displayColumns(metadata, unloadQuery.Partitioned_by)
					displayResults(s3Svc, manifest)
				}
				return true
			})

		if err != nil {
			fmt.Println("Error:")
			fmt.Println(err)
		}
	}
}

func ingest(writeRecordsInput *timestreamwrite.WriteRecordsInput, writeSvc *timestreamwrite.TimestreamWrite, counter int64) {
	_, err := writeSvc.WriteRecords(writeRecordsInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Print("Ingested ", counter)
		fmt.Println(" records to the table.")
	}
}

func parseQueryResult(queryOutput *timestreamquery.QueryOutput) map[string]string {
	var columnInfo = queryOutput.ColumnInfo;
	fmt.Println("ColumnInfo", columnInfo)
	fmt.Println("QueryId", queryOutput.QueryId)
	fmt.Println("QueryStatus", queryOutput.QueryStatus)
	return parseResponse(columnInfo, queryOutput.Rows[0])
}

func parseResponse(columnInfo []*timestreamquery.ColumnInfo, row *timestreamquery.Row) map[string]string {
	var datum = row.Data
	response := make(map[string]string)
	for i, column := range columnInfo {
		response[*column.Name] = *datum[i].ScalarValue
	}
	return response
}

func getManifestFile(s3Svc *s3.S3, response map[string]string) Manifest {
    var manifestBuf = getObject(s3Svc, response["manifestFile"])
    var manifest Manifest
    json.Unmarshal(manifestBuf.Bytes(), &manifest)
    return manifest
}

func getMetadataFile(s3Svc *s3.S3, response map[string]string) Metadata {
    var metadataBuf = getObject(s3Svc, response["metadataFile"])
    var metadata Metadata
    json.Unmarshal(metadataBuf.Bytes(), &metadata)
    return metadata
}

func displayColumns(metadata Metadata, partitioned_by []string) {
	var columns []string
	for _, column := range metadata.ColumnInfo {
		if contains(column.Name, partitioned_by) {
			continue
		}
		columns = append(columns, column.Name)
	}
	fmt.Println("Columns:", columns)
}

func displayResults(s3Svc *s3.S3, manifest Manifest) {
	var resultFiles = manifest.Result_files;
	fmt.Println("Result file count:", len(resultFiles))
	// Only reading the first file
	var dat = getObject(s3Svc, resultFiles[0].Url)
	reader := bytes.NewReader([]byte(dat.Bytes()))
	gzreader, _ := gzip.NewReader(reader)
	defer gzreader.Close()
	cr := csv.NewReader(gzreader)
	rec, err := cr.ReadAll()
	if err != nil {
        fmt.Println("Error while reading the CSV", err)
		return
    }

	fmt.Println("First 10 rows of the first file:")
	// Displaying first 10 values in the CSV
	for i, v := range rec {
		if i > 10 {
			break
		}
        fmt.Println(v)
    }
}

func getObject(s3Svc *s3.S3, s3Uri string) *bytes.Buffer {
	u,_ := url.Parse(s3Uri)
	getObjectInput := &s3.GetObjectInput{
		Key:    aws.String(u.Path),
		Bucket: aws.String(u.Host),
	}
	getObjectOutput, err := s3Svc.GetObject(getObjectInput)
	if err != nil {
		fmt.Println("Error: %s\n", err.Error())
	}
	buf := new(bytes.Buffer)
	buf.ReadFrom(getObjectOutput.Body)
	return buf
}

func deleteTable(writeSvc *timestreamwrite.TimestreamWrite, databaseName string, tableName string) {
	_, err := writeSvc.DeleteTable(&timestreamwrite.DeleteTableInput{
		DatabaseName: aws.String(databaseName),
		TableName:    aws.String(tableName),
	})

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Table deleted", tableName)
	}
}

func deleteDatabase(writeSvc *timestreamwrite.TimestreamWrite, databaseName string) {
	deleteDatabaseInput := &timestreamwrite.DeleteDatabaseInput{
		DatabaseName: aws.String(databaseName),
	}

	_, err := writeSvc.DeleteDatabase(deleteDatabaseInput)

	if err != nil {
		fmt.Println("Error:")
		fmt.Println(err)
	} else {
		fmt.Println("Database deleted:", databaseName)
	}
}

func contains(val string, arr []string) bool {
	for _, v := range arr {
		if v == val {
			return true
		}
	}
	return false
}

func build_query(unload_query UnloadQuery) *string {
	var query_results_s3_path = "'s3://" + unload_query.S3Location + "/" + unload_query.ResultPrefix + "/'"
	var query = "UNLOAD(" + unload_query.Query + ") TO " + query_results_s3_path + " WITH ( "
	if (len(unload_query.Partitioned_by) > 0) {
		query = query + "partitioned_by=ARRAY["
		for i, column := range unload_query.Partitioned_by {
			if i == 0 {
				query = query + "'" + column + "'"
			} else {
				query = query + ",'" + column + "'"
			}
		}
		query = query + "],"
	}
	query = query + " format='" + unload_query.Format + "', "
	query = query + "  compression='" + unload_query.Compression + "')"
	fmt.Println(query)
	return aws.String(query)
}

func deleteBucket(s3Svc *s3.S3, bucketName string) {

	deleteAllItems(s3Svc, bucketName)

	_, err := s3Svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		fmt.Println("Unable to delete the bucket", err)
		return
	}
	
	// Wait until bucket is deleted before finishing
	fmt.Printf("Waiting for bucket %q to be deleted...\n", bucketName)
	
	s3Svc.WaitUntilBucketNotExists(&s3.HeadBucketInput{
		Bucket: aws.String(bucketName),
	})

	fmt.Printf("Bucket deleted")
}

func deleteAllItems(s3Svc *s3.S3, bucketName string) {
	iter := s3manager.NewDeleteListIterator(s3Svc, &s3.ListObjectsInput{
		Bucket: aws.String(bucketName),
	})
	
	if err := s3manager.NewBatchDeleteWithClient(s3Svc).Delete(aws.BackgroundContext(), iter); err != nil {
		fmt.Println("Unable to delete objects from bucket", err)
	}
}