# Getting started with Amazon Timestream with .NET
  
This sample application shows to
1. Create table enabled with magnetic tier upsert
2. Ingest data with multi measure records
3. Create ScheduledQuery and interpret its run.

This application populates the table with ~63K rows of sample  multi measure value data (provided as part of csv) , and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

-------
## How to use it

### Using .Net Core CLI


1. Install required NuGet. Ensure package version are higher than what is mentioned in provided .csproj file.
   ```shell
   dotnet add package AWSSDK.Core
   dotnet add package AWSSDK.TimestreamWrite
   dotnet add package AWSSDK.TimestreamQuery
   dotnet add package CommandLineParser
   dotnet add package AWSSDK.IdentityManagement
   dotnet add package AWSSDK.SimpleNotificationService
   dotnet add package AWSSDK.SQS
   dotnet add package AWSSDK.S3
   dotnet add package Newtonsoft.Json
   dotnet add package AWSSDK.SecurityToken
   ```
   
   NOTE: If the older SDK has been installed, you might have to remove the older packages and clean cache before adding new SDKs.
   ```
   dotnet remove package AWSSDK.Core
   dotnet remove package AWSSDK.TimestreamWrite
   dotnet remove package AWSSDK.TimestreamQuery
   dotnet remove package AWSSDK.IdentityManagement
   dotnet remove package AWSSDK.SimpleNotificationService
   dotnet remove package AWSSDK.SQS
   dotnet remove package AWSSDK.S3
   dotnet remove package Newtonsoft.Json
   dotnet remove package AWSSDK.SecurityToken
   dotnet nuget locals all --clear
   ```   

2. Run the project
   ```shell
   dotnet run
   ```
   
3. Run with kms key id for Update database
   ```
   dotnet run -- -k ValidKmsKeyId
   ```   

4. Run with sample-multi csv data file

   Note: Only when the CSV file is provided, the sample will run all queries, and the ScheduledQuery examples.
   ```shell
   dotnet run -- -f <path to sample data file>
   ```
   
5. Run against a specific region and sample-multi csv data file
   ```shell
   dotnet run -- -f <path to sample data file> -r us-west-2
   ```
   
6. Run the sample application by skipping deletion of resources created by the application.
   
   Specifying the -skip-deletion flag will skip the resource deletion, absence of it will delete all the resources created by the sample application
   ```shell
   dotnet run -- -f <path to sample data file> -r us-west-2 -skip-deletion
   ```

