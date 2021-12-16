using System;
using System.Threading.Tasks;
using System.Collections.Generic;
using Amazon.TimestreamWrite;
using Amazon.TimestreamWrite.Model;

namespace TimestreamDotNetSample
{
    public class CrudAndSimpleIngestionExample
    {

        private readonly AmazonTimestreamWriteClient writeClient;
        private readonly TimestreamCrudHelper _crudHelper;

        public CrudAndSimpleIngestionExample(AmazonTimestreamWriteClient writeClient, TimestreamCrudHelper crudHelper)
        {
            this.writeClient = writeClient;
            _crudHelper = crudHelper;
        }

        public async Task CreateDatabase()
        {
            await _crudHelper.CreateDatabase(writeClient, Constants.DATABASE_NAME);
        }

        public async Task DescribeDatabase()
        {
            Console.WriteLine("Describing Database");

            try
            {
                var describeDatabaseRequest = new DescribeDatabaseRequest
                {
                    DatabaseName = Constants.DATABASE_NAME
                };
                DescribeDatabaseResponse response = await writeClient.DescribeDatabaseAsync(describeDatabaseRequest);
                Console.WriteLine($"Database {Constants.DATABASE_NAME} has id:{response.Database.Arn}");
            }
            catch (ResourceNotFoundException)
            {
                Console.WriteLine("Database does not exist.");
            }
            catch (Exception e)
            {
                Console.WriteLine("Describe database failed:" + e.ToString());
            }

        }

        public async Task ListDatabases()
        {
            Console.WriteLine("Listing Databases");

            try
            {
                var listDatabasesRequest = new ListDatabasesRequest
                {
                    MaxResults = 5
                };
                ListDatabasesResponse response = await writeClient.ListDatabasesAsync(listDatabasesRequest);
                PrintDatabases(response.Databases);
                var nextToken = response.NextToken;
                while (nextToken != null)
                {
                    listDatabasesRequest.NextToken = nextToken;
                    response = await writeClient.ListDatabasesAsync(listDatabasesRequest);
                    PrintDatabases(response.Databases);
                    nextToken = response.NextToken;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("List database failed:" + e.ToString());
            }

        }

        public async Task UpdateDatabase(String updatedKmsKeyId)
        {
            Console.WriteLine("Updating Database");

            try
            {
                var updateDatabaseRequest = new UpdateDatabaseRequest
                {
                    DatabaseName = Constants.DATABASE_NAME,
                    KmsKeyId = updatedKmsKeyId
                };
                UpdateDatabaseResponse response = await writeClient.UpdateDatabaseAsync(updateDatabaseRequest);
                Console.WriteLine($"Database {Constants.DATABASE_NAME} updated with KmsKeyId {updatedKmsKeyId}");
            }
            catch (ResourceNotFoundException)
            {
                Console.WriteLine("Database does not exist.");
            }
            catch (Exception e)
            {
                Console.WriteLine("Update database failed: " + e.ToString());
            }

        }

        private void PrintDatabases(List<Database> databases)
        {
            foreach (Database database in databases)
                Console.WriteLine($"Database:{database.DatabaseName}");
        }

        public async Task DeleteDatabase()
        {
            await _crudHelper.DeleteDatabase(writeClient, Constants.DATABASE_NAME);
        }

        public async Task CreateTable(string s3ErrorReportBucketName)
        {
            await _crudHelper.CreateTable(writeClient, Constants.DATABASE_NAME, Constants.TABLE_NAME, s3ErrorReportBucketName);
        }

        public async Task DescribeTable()
        {
            Console.WriteLine("Describing Table");

            try
            {
                var describeTableRequest = new DescribeTableRequest
                {
                    DatabaseName = Constants.DATABASE_NAME,
                    TableName = Constants.TABLE_NAME
                };
                DescribeTableResponse response = await writeClient.DescribeTableAsync(describeTableRequest);
                Console.WriteLine($"Table {Constants.TABLE_NAME} has id:{response.Table.Arn}");
            }
            catch (ResourceNotFoundException)
            {
                Console.WriteLine("Table does not exist.");
            }
            catch (Exception e)
            {
                Console.WriteLine("Describe table failed:" + e.ToString());
            }

        }

        public async Task ListTables()
        {
            Console.WriteLine("Listing Tables");

            try
            {
                var listTablesRequest = new ListTablesRequest
                {
                    MaxResults = 5,
                    DatabaseName = Constants.DATABASE_NAME
                };
                ListTablesResponse response = await writeClient.ListTablesAsync(listTablesRequest);
                PrintTables(response.Tables);
                string nextToken = response.NextToken;
                while (nextToken != null)
                {
                    listTablesRequest.NextToken = nextToken;
                    response = await writeClient.ListTablesAsync(listTablesRequest);
                    PrintTables(response.Tables);
                    nextToken = response.NextToken;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("List table failed:" + e.ToString());
            }

        }

        private void PrintTables(List<Table> tables)
        {
            foreach (Table table in tables)
                Console.WriteLine($"Table: {table.TableName}");
        }

        public async Task UpdateTable()
        {
            Console.WriteLine("Updating Table");

            try
            {
                var updateTableRequest = new UpdateTableRequest
                {
                    DatabaseName = Constants.DATABASE_NAME,
                    TableName = Constants.TABLE_NAME,
                    RetentionProperties = new RetentionProperties
                    {
                        MagneticStoreRetentionPeriodInDays = Constants.CT_TTL_DAYS,
                        MemoryStoreRetentionPeriodInHours = Constants.HT_TTL_HOURS
                    }
                };
                UpdateTableResponse response = await writeClient.UpdateTableAsync(updateTableRequest);
                Console.WriteLine($"Table {Constants.TABLE_NAME} updated");
            }
            catch (ResourceNotFoundException)
            {
                Console.WriteLine("Table does not exist.");
            }
            catch (Exception e)
            {
                Console.WriteLine("Update table failed:" + e.ToString());
            }

        }

        public async Task DeleteTable()
        {
            await _crudHelper.DeleteTable(writeClient, Constants.DATABASE_NAME, Constants.TABLE_NAME);
        }

        public async Task WriteRecordsMultiMeasure()
        {
            Console.WriteLine("Writing records");

            DateTimeOffset now = DateTimeOffset.UtcNow;
            string currentTimeString = (now.ToUnixTimeMilliseconds()).ToString();

            List<Dimension> dimensions = new List<Dimension>{
                new Dimension { Name = "region", Value = "us-east-1" },
                new Dimension { Name = "az", Value = "az1" },
                new Dimension { Name = "hostname", Value = "host1" }
            };

            var cpuUtilization = new Record
            {
                Dimensions = dimensions,
                MeasureName = "cpu_utilization",
                MeasureValues = new List<MeasureValue>{
                                    new MeasureValue{
                                        Name= "cpu1",
                                        Value= "13.5",
                                        Type= "DOUBLE",
                                    },
                                    new MeasureValue{
                                        Name= "cpu2",
                                        Value= "15",
                                        Type= "DOUBLE",
                                    }
                                },
                MeasureValueType = MeasureValueType.MULTI,
                Time = currentTimeString
            };

            var memoryUtilization = new Record
            {
                Dimensions = dimensions,
                MeasureName = "memory_utilization",
                MeasureValues = new List<MeasureValue>{
                                    new MeasureValue{
                                        Name= "memory1",
                                        Value= "40",
                                        Type= "DOUBLE",
                                    },
                                    new MeasureValue{
                                        Name= "memory2",
                                        Value= "60",
                                        Type= "DOUBLE",
                                    }
                                },
                MeasureValueType = MeasureValueType.MULTI,
                Time = currentTimeString
            };


            List<Record> records = new List<Record> {
                cpuUtilization,
                memoryUtilization
            };

            try
            {
                var writeRecordsRequest = new WriteRecordsRequest
                {
                    DatabaseName = Constants.DATABASE_NAME,
                    TableName = Constants.TABLE_NAME,
                    Records = records
                };
                WriteRecordsResponse response = await writeClient.WriteRecordsAsync(writeRecordsRequest);
                Console.WriteLine($"Write records status code: {response.HttpStatusCode.ToString()}");
            }
            catch (RejectedRecordsException e) {
                PrintRejectedRecordsException(e);
            }
            catch (Exception e)
            {
                Console.WriteLine("Write records failure:" + e.ToString());
            }
        }

        public async Task WriteRecordsWithCommonAttributes()
        {
            Console.WriteLine("Writing records with common attributes");

            DateTimeOffset now = DateTimeOffset.UtcNow;
            string currentTimeString = (now.ToUnixTimeMilliseconds()).ToString();

            List<Dimension> dimensions = new List<Dimension>{
                new Dimension { Name = "region", Value = "us-east-1" },
                new Dimension { Name = "az", Value = "az1" },
                new Dimension { Name = "hostname", Value = "host1" }
            };

            var commonAttributes = new Record
            {
                Dimensions = dimensions,
                MeasureValueType = MeasureValueType.MULTI,
                Time = currentTimeString
            };

            var cpuUtilization = new Record
                        {
                            MeasureName = "cpu_utilization",
                            MeasureValues = new List<MeasureValue>{
                                                new MeasureValue{
                                                    Name= "cpu1",
                                                    Value= "13.5",
                                                    Type= "DOUBLE",
                                                }
                                            },
                        };

            var memoryUtilization = new Record
                        {
                            MeasureName = "memory_utilization",
                            MeasureValues = new List<MeasureValue>{
                                                new MeasureValue{
                                                    Name= "memory1",
                                                    Value= "40",
                                                    Type= "DOUBLE",
                                                }
                                            }
                        };


            List<Record> records = new List<Record>();
            records.Add(cpuUtilization);
            records.Add(memoryUtilization);

            try
            {
                var writeRecordsRequest = new WriteRecordsRequest
                {
                    DatabaseName = Constants.DATABASE_NAME,
                    TableName = Constants.TABLE_NAME,
                    Records = records,
                    CommonAttributes = commonAttributes
                };
                WriteRecordsResponse response = await writeClient.WriteRecordsAsync(writeRecordsRequest);
                Console.WriteLine($"Write records status code: {response.HttpStatusCode.ToString()}");
            }
            catch (RejectedRecordsException e) {
                PrintRejectedRecordsException(e);
            }
            catch (Exception e)
            {
                Console.WriteLine("Write records failure:" + e.ToString());
            }
        }

        public async Task WriteRecordsWithUpsert()
            {
                Console.WriteLine("Writing records with upsert");

                DateTimeOffset now = DateTimeOffset.UtcNow;
                string currentTimeString = (now.ToUnixTimeMilliseconds()).ToString();
                // To achieve upsert (last writer wins) semantic, one example is to use current time as the version if you are writing directly from the data source
                long version = now.ToUnixTimeMilliseconds();

                List<Dimension> dimensions = new List<Dimension>{
                    new Dimension { Name = "region", Value = "us-east-1" },
                    new Dimension { Name = "az", Value = "az1" },
                    new Dimension { Name = "hostname", Value = "host1" }
                };

                var commonAttributes = new Record
                {
                    Dimensions = dimensions,
                    MeasureValueType = MeasureValueType.MULTI,
                    Time = currentTimeString,
                    Version = version
                };


            var cpuUtilization = new Record
                        {
                            MeasureName = "cpu_utilization",
                            MeasureValues = new List<MeasureValue>{
                                                new MeasureValue{
                                                    Name= "cpu1",
                                                    Value= "13.5",
                                                    Type= "DOUBLE",
                                                }
                                            },
                        };

            var memoryUtilization = new Record
                        {
                            MeasureName = "memory_utilization",
                            MeasureValues = new List<MeasureValue>{
                                                new MeasureValue{
                                                    Name= "memory1",
                                                    Value= "40",
                                                    Type= "DOUBLE",
                                                }
                                            }
                        };


                List<Record> records = new List<Record>();
                records.Add(cpuUtilization);
                records.Add(memoryUtilization);

                // write records for first time
                try
                {
                    var writeRecordsRequest = new WriteRecordsRequest
                    {
                        DatabaseName = Constants.DATABASE_NAME,
                        TableName = Constants.TABLE_NAME,
                        Records = records,
                        CommonAttributes = commonAttributes
                    };
                    WriteRecordsResponse response = await writeClient.WriteRecordsAsync(writeRecordsRequest);
                    Console.WriteLine($"WriteRecords Status for first time: {response.HttpStatusCode.ToString()}");
                }
                catch (RejectedRecordsException e) {
                    PrintRejectedRecordsException(e);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Write records failure:" + e.ToString());
                }

                // Successfully retry same writeRecordsRequest with same records and versions, because writeRecords API is idempotent.
                try
                {
                    var writeRecordsRequest = new WriteRecordsRequest
                    {
                        DatabaseName = Constants.DATABASE_NAME,
                        TableName = Constants.TABLE_NAME,
                        Records = records,
                        CommonAttributes = commonAttributes
                    };
                    WriteRecordsResponse response = await writeClient.WriteRecordsAsync(writeRecordsRequest);
                    Console.WriteLine($"WriteRecords Status for retry: {response.HttpStatusCode.ToString()}");
                }
                catch (RejectedRecordsException e) {
                    PrintRejectedRecordsException(e);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Write records failure:" + e.ToString());
                }

                // upsert with lower version, this would fail because a higher version is required to update the measure value.
                version--;
                var cpuUtilizationUpsert = new Record
                                                        {
                                                            MeasureName = "cpu_utilization",
                                                            MeasureValues = new List<MeasureValue>{
                                                                                new MeasureValue{
                                                                                    Name= "cpu1",
                                                                                    Value= "13.5",
                                                                                    Type= "DOUBLE",
                                                                                }
                                                                            },
                                                        };
                var memoryUtilizationUpsert = new Record
                                        {
                                            MeasureName = "memory_utilization",
                                            MeasureValues = new List<MeasureValue>{
                                                                new MeasureValue{
                                                                    Name= "memory1",
                                                                    Value= "40",
                                                                    Type= "DOUBLE",
                                                                }
                                                            },
                                        };
                Type recordType = typeof(Record);
                recordType.GetProperty("Version").SetValue(commonAttributes, version);

                List<Record> upsertedRecords = new List<Record> {
                    cpuUtilizationUpsert,
                    memoryUtilizationUpsert
                };

                try
                {
                    var writeRecordsUpsertRequest = new WriteRecordsRequest
                    {
                        DatabaseName = Constants.DATABASE_NAME,
                        TableName = Constants.TABLE_NAME,
                        Records = upsertedRecords,
                        CommonAttributes = commonAttributes
                    };
                    WriteRecordsResponse upsertResponse = await writeClient.WriteRecordsAsync(writeRecordsUpsertRequest);
                    Console.WriteLine($"WriteRecords Status for upsert with lower version: {upsertResponse.HttpStatusCode.ToString()}");
                }
                catch (RejectedRecordsException e) {
                    PrintRejectedRecordsException(e);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Write records failure:" + e.ToString());
                }

                // upsert with higher version as new data in generated
                now = DateTimeOffset.UtcNow;
                version = now.ToUnixTimeMilliseconds();
                recordType.GetProperty("Version").SetValue(commonAttributes, version);

                try
                {
                    var writeRecordsUpsertRequest = new WriteRecordsRequest
                    {
                        DatabaseName = Constants.DATABASE_NAME,
                        TableName = Constants.TABLE_NAME,
                        Records = upsertedRecords,
                        CommonAttributes = commonAttributes
                    };
                    WriteRecordsResponse upsertResponse = await writeClient.WriteRecordsAsync(writeRecordsUpsertRequest);
                    Console.WriteLine($"WriteRecords Status for upsert with higher version:  {upsertResponse.HttpStatusCode.ToString()}");
                }
                catch (RejectedRecordsException e) {
                    PrintRejectedRecordsException(e);
                }
                catch (Exception e)
                {
                    Console.WriteLine("Write records failure:" + e.ToString());
                }
            }

        private void PrintRejectedRecordsException(RejectedRecordsException e)
        {
            Console.WriteLine("RejectedRecordsException:" + e.ToString());
            foreach (RejectedRecord rr in e.RejectedRecords) {
                Console.WriteLine("RecordIndex " + rr.RecordIndex + " : " + rr.Reason);
                long? existingVersion = rr.ExistingVersion;
                if (existingVersion != null) {
                    Console.WriteLine("Rejected record existing version: " + existingVersion);
                }
            }
        }
    }
}
