## Amazon Timestream data ingestion and query tools
To understand the performance and scale capabilities of Amazon Timestream, you can run the following workload:
* [Running large scale workloads with Amazon Timestream](perf-scale-workload)

The following tools can be used to continuously send data into Amazon Timestream:
* [Publishing data with Amazon Kinesis](kinesis_ingestor)
* [Writing data using a multi-thread Python DevOps data generator](continuous-ingestor)

The following tool shows how to use multiple threads to write to Amazon Timestream with Java, while collecting important operational metrics. It includes samples which shows:
* [Local CSV file ingestion to Amazon Timestream](multithreaded-writer#Local-CSV-file-ingestion-to-Timestream)
* [Lambda function ingesting S3 CSV file to Amazon Timestream](multithreaded-writer#Lambda-function-ingesting-S3-CSV-file-to-Timestream)