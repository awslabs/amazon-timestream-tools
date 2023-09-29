## Amazon Timestream data ingestion and query tools
To understand the performance and scale capabilities of Amazon Timestream, you can run the following workload:
* [Running large scale workloads with Amazon Timestream](python/perf-scale-workload)

The following tools can be used to continuously send data into Amazon Timestream:
* [Publishing data with Amazon Kinesis](python/kinesis_ingestor)
* [Writing data using a multi-thread Python DevOps data generator](python/continuous-ingestor)

The following tools show example to write common file formats:
* [Processing Apache Parquet files](python/parquet-writer)

The following tool shows how to use multiple threads to write to Amazon Timestream with Java, while collecting important operational metrics. It includes samples which shows:
* [Local CSV file ingestion to Amazon Timestream](java/multithreaded-writer#Local-CSV-file-ingestion-to-Timestream)
* [Lambda function ingesting S3 CSV file to Amazon Timestream](java/multithreaded-writer#Lambda-function-ingesting-S3-CSV-file-to-Timestream)