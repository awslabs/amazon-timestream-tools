# Local CSV file ingestion to Timestream

See [parent readme](../) for description.

## Running the code

### Requirements

 - [Amazon Corretto 11](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/what-is-corretto-11.html)
 - [Maven](https://docs.aws.amazon.com/corretto/latest/corretto-11-ug/what-is-corretto-11.html)
 - The Bash shell. For Linux and macOS, this is included by default. In Windows 10, you can install the [Windows Subsystem for Linux](https://docs.microsoft.com/en-us/windows/wsl/install-win10) to get a Windows-integrated version of Ubuntu and Bash.
 - [The AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-install.html).
 
### Compilation

 1. Use Maven to package the code (target: `package`).
 2. After packaging the code, the jar file will be created in `SampleLocalCsvIngestion/target/sample-local-csv-ingestion-1.0-SNAPSHOT-jar-with-dependencies.jar`.

### Run the code

```
cd SampleLocalCsvIngestion/target

# tweak the values below to your use-case
java -Xss1024K -Xms2048m -Xmx12288m -jar sample-local-csv-ingestion-1.0-SNAPSHOT-jar-with-dependencies.jar -f ./../src/main/resources/sample_withHeader.csv --database test_database --table test_table --region us-east-1 -t 2000 -q 20000
```

Parameters description:
 - `-Xss` - set each thread stack size
 - `-Xms` - set initial memory allocation pool for Java Virtual Machine (JVM)
 - `-Xmx` - set initial memory allocation pool for Java Virtual Machine (JVM)
 - `-jar` - specify the compiled jar file from previous step
 - `-f` - specify CSV file path to process
 - `-q` - specify the size of the queue buffer for Timestream WriteRequests to be consumed by multiple writers
 - `-t` - specify the theadPool size to use when inserting records to Timestream
