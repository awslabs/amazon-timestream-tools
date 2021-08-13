# Amazon Timestream multithreaded Java writer

This is a collection of sample Java applications which shows how to use multiple threads to write records to Timestream. While writing the records, the writer generates a dozen of metrics, which you can monitor for operational purposes.

## Sample Applications

### Local CSV file ingestion to Timestream

This sample shows how to read a local CSV file, transform each record to Timestream WriteRecordsRequest and ingest records to Timestream with multithreaded Timestream writer. Each WriteRecordsRequest (a collection of Timestream records) is sent to multithreaded writer by using `TimestreamWriter.putWriteRecordRequest` method.
To customize sample application code to your data, change **com.amazonaws.sample.csv.mapping.SampleCsvRow.java** and  **com.amazonaws.sample.csv.mapping.SampleCsvMapper** classes.

![sample-local-csv-ingestion-diagram](sample-local-csv-ingestion-diagram.png?raw=true "sample-local-csv-ingestion-diagram")

### Lambda function ingesting S3 CSV file to Timestream

This sample shows how to read a file from S3, transform each record to Timestream WriteRecordsRequest and ingest records to Timestream with multithreaded Timestream writer. The file reading is stream based and therefore you can process file sizes bigger than allocated memory to your Lambda function. For details how multithreaded writer works, see the section below.
To customize sample application code to your data, change **com.amazonaws.sample.csv.mapping.SampleCsvRow.java** and  **com.amazonaws.sample.csv.mapping.SampleCsvMapper** classes from **sample-local-csv-ingestion** module.

![sample-lambda-from-s3-csv-ingestion](sample-lambda-from-s3-csv-ingestion.png?raw=true "sample-lambda-from-s3-csv-ingestion")

## Multithreaded writer module

### How does it work?

![timestream-multithreaded-writer-diagram](timestream-multithreaded-writer-diagram.png?raw=true "timestream-multithreaded-writer-diagram")

Each WriteRecordsRequest (a collection of Timestream records) is sent to multithreaded writer by using `TimestreamWriter.putWriteRecordRequest` method. The operation is asynchronous and only puts given WriteRecordsRequest to in-memory queue. The in-memory queue is consumed by `T` number of threads. Each of them attempts to retrieve the WriteRecordsRequest from the in-memory queue and ingest it to Timestream. A single thread is processing only a single WriteRecordsRequest at a time. It's blockingly waiting for the response from Timestream and retries the ingestion if neccassary (and according to the configuration). `TimestreamWriterMetrics` are collected during the code execution and - if configured - printed to the console. 

### Multithreaded writer interface

```
interface TimestreamWriter {
    // Put the WriteRecordsRequest to a in-memory queue. The function will return false when the queue is full.
	// In rare circumstances, the function can block until there is space in the queue.
    boolean putWriteRecordRequest(WriteRecordsRequest writeRequest);

    // Shut down gracefully - wait for all writes to complete/retry.
    void shutDownGracefully();

    // Check if writer queue is empty and there are no records in flight.
    // There might be a brief moment when this method returns true, but a record is actually in flight. See note in TimestreamWriterWorker.
    boolean isWriteApproximatelyComplete();

    // Return the size of the in-memory queue. This does not include records in flight.
    int getQueueSize();
	
	// Return the number of writes currently in flight to Timestream.
	int getWritesInFlight();

    // Return aggregated metrics from the previous method invocation.
    TimestreamWriterMetrics getAndClearMetrics();
}
```

## Running the code

See individual README files for each application:
 - [Local CSV file ingestion to Timestream](SampleLocalCsvIngestion/)
 - [Lambda function ingesting S3 CSV file to Timestream](SampleLambdaFromS3CsvIngestion/)
 

## Results

`sample.csv` ingestion performance (126000 records) on t3.2xlarge EC2:

 - 1 thread, serial ingestion: 1m 19s
 - 10 threads: 11.5s
 - 100 threads: 6s
 
## Sample Metrics

```
11:11:11,111 INFO  Thread-0 LogMetricsPublisher  - Metrics over last PT5S: TimestreamWriterMetrics{insertionMetrics=TimestreamInsertionMetrics{
        recordsSuccess=100,
        recordsRejectAll=0,
        recordsRejectInvalidVersion=0,
        recordsRejectValidation=0,
        writesSuccess=1,
        recordsDrop=0,
        writesDrop=0,
        writesErrorAll=0,
        writesErrorThrottling=0,
        writesResourceNotFound=0,
        writesErrorInternalServer=0,
        writeLatencyMsSum=123,
        writeLatencyMsCount=1,
        nonSDKReties=0,
        writeLatencyMsAvg=123
}, queueSize=0, writesInFlight=0}
11:11:11,111 INFO  Thread-0 LogMetricsPublisher  - Total metrics: TimestreamInsertionMetrics{
        recordsSuccess=1000,
        recordsRejectAll=0,
        recordsRejectInvalidVersion=0,
        recordsRejectValidation=0,
        writesSuccess=10,
        recordsDrop=0,
        writesDrop=0,
        writesErrorAll=3,
        writesErrorThrottling=1,
        writesResourceNotFound=0,
        writesErrorInternalServer=2,
        writeLatencyMsSum=1230,
        writeLatencyMsCount=10,
        nonSDKReties=2,
        writeLatencyMsAvg=123
}
```