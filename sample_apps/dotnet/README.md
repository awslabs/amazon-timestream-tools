# Getting started with Amazon Timestream with .NET

This sample application shows how you can create a database and table, populate the table with ~126K rows of sample data, and run sample queries to jumpstart your evaluation and/or proof-of-concept applications with Amazon Timestream.

-------
## How to use it

### Using .Net Core 
1. Install [.NET](https://docs.microsoft.com/en-us/dotnet/core/install/) 

2.  NOTE: If the older SDK has been installed, you might have to remove the older packages and clean cache before adding new SDKs.
   ```
   dotnet remove package AWSSDK.Core
   dotnet remove package AWSSDK.TimestreamWrite
   dotnet remove package AWSSDK.TimestreamQuery
   dotnet remove package CommandLineParser
   dotnet nuget locals all --clear
   ```

3. Install required NuGet. Ensure AWSSDK.Core version is 3.3.107 or newer.
   ```
   dotnet add package AWSSDK.Core
   dotnet add package AWSSDK.TimestreamWrite
   dotnet add package AWSSDK.TimestreamQuery 
   dotnet add package CommandLineParser
   ```

4. Run the project
   ```
   dotnet run
   ```
   
5. Run with kms key id for Update database
   ```
   dotnet run -- -k ValidKmsKeyId
   ```

6. Run with sample csv data file
   ```
   dotnet run -- -f ../data/sample.csv
   ```