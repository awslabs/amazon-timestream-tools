# Getting started with Amazon Timestream with .NET

This sample application shows how you can create a database and table, populate the table with ~126K rows of sample data, and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

-------
## How to use it

### Using .Net Core CLI


1. Install required NuGet. Ensure AWSSDK.Core version is 3.3.107 or newer.
   ```shell
   dotnet add package AWSSDK.Core --version 3.5.1.42
   dotnet add package AWSSDK.TimestreamWrite --version 3.5.1
   dotnet add package AWSSDK.TimestreamQuery --version 3.5.1.1
   dotnet add package CommandLineParser
   ```

   NOTE: If the older SDK has been installed, you might have to remove the older packages and clean cache before adding new SDKs.
   ```
   dotnet remove package AWSSDK.Core
   dotnet remove package AWSSDK.TimestreamWrite
   dotnet remove package AWSSDK.TimestreamQuery
   dotnet nuget locals all --clear
   ```

1. Run the project
   ```shell
   dotnet run
   ```
   
1. Run with kms key id for Update database
   ```
   dotnet run -- -k ValidKmsKeyId
   ```

1. Run with sample csv data file
   ```shell
   dotnet run -- -f ../data/sample.csv
   ```

