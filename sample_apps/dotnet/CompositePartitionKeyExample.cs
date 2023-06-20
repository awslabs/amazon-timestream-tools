using System;
using System.Collections.Generic;
using Amazon.TimestreamWrite;
using Amazon.S3;
using Amazon.TimestreamWrite.Model;
using System.Threading.Tasks;
using MeasureValueType = Amazon.TimestreamWrite.MeasureValueType;

namespace TimestreamDotNetSample
{
    public class CompositePartitionKeyExample
    {
        private readonly AmazonTimestreamWriteClient writeClient;
        private readonly CrudAndSimpleIngestionExample crudAndSimpleIngestionExample;
        private readonly QueryExample queryExample;
        private readonly string bucketName;
        private readonly TimestreamDependencyHelper timestreamDependencyHelper;
        private readonly AmazonS3Client s3Client;

        public CompositePartitionKeyExample(AmazonTimestreamWriteClient writeClient,
            QueryExample queryExample, string region, string accountId, AmazonS3Client s3Client)
        {
            this.writeClient = writeClient;
            this.crudAndSimpleIngestionExample = new CrudAndSimpleIngestionExample(writeClient);
            this.queryExample = queryExample;
            String randomString = Guid.NewGuid().ToString("n").Substring(0, 5);
            this.bucketName = Constants.S3_BUCKET_PREFIX + "-" + accountId + "-" + region + randomString;
            this.timestreamDependencyHelper = new TimestreamDependencyHelper();
            this.s3Client = s3Client;
        }

        public async Task CreateTableWithPartitionKeys(String databaseName,
            String tableName, List<PartitionKey> compositePartitionKey, String s3ErrorReportBucketName)
        {
            Console.WriteLine("Creating Table With CompositePartitionKey");
            try
            {
                var createTableRequest = new CreateTableRequest
                {
                    DatabaseName = databaseName,
                    TableName = tableName,
                    RetentionProperties = new RetentionProperties
                    {
                        MagneticStoreRetentionPeriodInDays = Constants.CT_TTL_DAYS,
                        MemoryStoreRetentionPeriodInHours = Constants.HT_TTL_HOURS
                    },
                    // Set compositePartitionKey
                    Schema = new Schema
                    {
                        CompositePartitionKey = compositePartitionKey,
                    },
                    // Enable MagneticStoreWrite
                    MagneticStoreWriteProperties = new MagneticStoreWriteProperties
                    {
                        EnableMagneticStoreWrites = true,
                        // Persist MagneticStoreWrite rejected records in S3
                        MagneticStoreRejectedDataLocation = new MagneticStoreRejectedDataLocation
                        {
                            S3Configuration = new S3Configuration
                            {
                                BucketName = s3ErrorReportBucketName,
                                EncryptionOption = "SSE_S3",
                            },
                        },
                    }
                };
                CreateTableResponse response = await writeClient.CreateTableAsync(createTableRequest);
                Console.WriteLine($"Table {tableName} created");
            }
            catch (ConflictException)
            {
                Console.WriteLine("Table already exists.");
            }
            catch (Exception e)
            {
                Console.WriteLine("Create table failed:" + e.ToString());
            }
        }

        public async Task UpdateTableWithPartitionKeys(String databaseName, String tableName,
            List<PartitionKey> compositePartitionKey)
        {
            Console.WriteLine("Updating Table");
            try
            {
                var updateTableRequest = new UpdateTableRequest
                {
                    DatabaseName = databaseName,
                    TableName = tableName,
                    Schema = new Schema
                    {
                        CompositePartitionKey = compositePartitionKey,
                    }
                };
                UpdateTableResponse response = await writeClient.UpdateTableAsync(updateTableRequest);
                Console.WriteLine($"Table {tableName} updated");
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

        public async Task WriteRecords(String databaseName, String tableName, String dimensionName)
        {
            Console.WriteLine("Writing records");

            DateTimeOffset now = DateTimeOffset.UtcNow;
            string currentTimeString = (now.ToUnixTimeMilliseconds()).ToString();

            List<Dimension> dimensions = new List<Dimension>
            {
                new Dimension {Name = dimensionName, Value = Constants.COMPOSITE_PARTITION_KEY_DIM_VALUE},
                new Dimension {Name = "region", Value = "us-east-1"},
                new Dimension {Name = "az", Value = "az1"},
            };

            var cpuUtilization = new Record
            {
                Dimensions = dimensions,
                MeasureName = "cpu_utilization",
                MeasureValue = "13.6",
                MeasureValueType = MeasureValueType.DOUBLE,
                Time = currentTimeString
            };

            var memoryUtilization = new Record
            {
                Dimensions = dimensions,
                MeasureName = "memory_utilization",
                MeasureValue = "40",
                MeasureValueType = MeasureValueType.DOUBLE,
                Time = currentTimeString
            };


            List<Record> records = new List<Record>
            {
                cpuUtilization,
                memoryUtilization
            };

            try
            {
                var writeRecordsRequest = new WriteRecordsRequest
                {
                    DatabaseName = databaseName,
                    TableName = tableName,
                    Records = records
                };
                WriteRecordsResponse response = await writeClient.WriteRecordsAsync(writeRecordsRequest);
                Console.WriteLine($"Write records status code: {response.HttpStatusCode.ToString()}");
            }
            catch (RejectedRecordsException e)
            {
                crudAndSimpleIngestionExample.PrintRejectedRecordsException(e);
            }
            catch (Exception e)
            {
                Console.WriteLine("Write records failure:" + e.ToString());
            }
        }

        public async Task WriteRecordsMultiMeasure(String databaseName, String tableName, String dimensionName)
        {
            Console.WriteLine("Writing records with MultiMeasure");

            DateTimeOffset now = DateTimeOffset.UtcNow;
            string currentTimeString = (now.ToUnixTimeMilliseconds()).ToString();

            List<Dimension> dimensions = new List<Dimension>
            {
                new Dimension {Name = dimensionName, Value = Constants.COMPOSITE_PARTITION_KEY_DIM_VALUE},
                new Dimension {Name = "region", Value = "us-east-1"},
                new Dimension {Name = "az", Value = "az1"}
            };

            var cpuMemory = new Record
            {
                Dimensions = dimensions,
                MeasureName = "cpu_memory",
                MeasureValues = new List<MeasureValue>
                {
                    new MeasureValue
                    {
                        Name = "cpu_utilization",
                        Value = "13.5",
                        Type = "DOUBLE",
                    },
                    new MeasureValue
                    {
                        Name = "memoryUtilization",
                        Value = "15",
                        Type = "DOUBLE",
                    }
                },
                MeasureValueType = MeasureValueType.MULTI,
                Time = currentTimeString
            };

            List<Record> records = new List<Record>
            {
                cpuMemory
            };

            try
            {
                var writeRecordsRequest = new WriteRecordsRequest
                {
                    DatabaseName = databaseName,
                    TableName = tableName,
                    Records = records
                };
                WriteRecordsResponse response = await writeClient.WriteRecordsAsync(writeRecordsRequest);
                Console.WriteLine($"Write records status code: {response.HttpStatusCode.ToString()}");
            }
            catch (RejectedRecordsException e)
            {
                crudAndSimpleIngestionExample.PrintRejectedRecordsException(e);
            }
            catch (Exception e)
            {
                Console.WriteLine("Write records failure:" + e.ToString());
            }
        }

        private async Task runSingleQuery(String databaseName, String tableName, String partitionKeyName,
            String partitionKeyValue)
        {
            String queryString =
                $"SELECT * FROM \"{databaseName}\".\"{tableName}\" where \"{partitionKeyName}\"={partitionKeyValue}";
            
            Console.WriteLine("Query String: {0}", queryString);
            await queryExample.RunQueryAsync(queryString);
        }

        /* This sample demonstrates workflow using dimension type partition key
        * 1. Create table with dimension type partition key, with optional enforcement.
        * 2. Ingest records with missing partition key. Records will be accepted as enforcement level is optional
        * 3. Update table with required enforcement.
        * 4. Ingest records with same partition key.
        * 5. Ingest records with missing partition key. This will return rejected records as they do not
        *    contain required partition key.
        * 6. Query records with same partition key.
        */
        public async Task runSampleWithDimensionPartitionKey(String databaseName, String tableName, String bucketName)
        {
            Console.WriteLine("Creating table {0} with dimension partition key(OPTIONAL ENFORCEMENT)", tableName);

            List<PartitionKey> partitionKeyListWithDimensionAndOptionalEnforcement =
                new List<PartitionKey>
                {
                    new PartitionKey
                    {
                        Name = Constants.COMPOSITE_PARTITION_KEY_DIM_NAME,
                        EnforcementInRecord = PartitionKeyEnforcementLevel.OPTIONAL,
                        Type = PartitionKeyType.DIMENSION
                    }
                };

            await CreateTableWithPartitionKeys(databaseName, tableName,
                partitionKeyListWithDimensionAndOptionalEnforcement, bucketName);

            Console.WriteLine("Describing table {0} with dimension partition key(OPTIONAL ENFORCEMENT)", tableName);
            await crudAndSimpleIngestionExample.DescribeTable(databaseName, tableName);

            Console.WriteLine(
                "Ingest records without dimension type partition key for table  {0}, Since the enforcement level is OPTIONAL the records will be ingested",
                tableName);
            await WriteRecords(databaseName, tableName, Constants.COMPOSITE_PARTITION_KEY_DIFF_NAME);
            await WriteRecordsMultiMeasure(databaseName, tableName, Constants.COMPOSITE_PARTITION_KEY_DIFF_NAME);

            Console.WriteLine(
                "Updating the dimension partition key enforcement from optional to required for table {0}", tableName);

            List<PartitionKey> partitionKeyListWithDimensionAndRequiredEnforcement =
                new List<PartitionKey>
                {
                    new PartitionKey
                    {
                        Name = Constants.COMPOSITE_PARTITION_KEY_DIM_NAME,
                        EnforcementInRecord = PartitionKeyEnforcementLevel.REQUIRED,
                        Type = PartitionKeyType.DIMENSION
                    }
                };

            await UpdateTableWithPartitionKeys(databaseName, tableName,
                partitionKeyListWithDimensionAndRequiredEnforcement);

            Console.WriteLine("Describing table {0} with dimension partition key(REQUIRED ENFORCEMENT)", tableName);
            await crudAndSimpleIngestionExample.DescribeTable(databaseName, tableName);

            Console.WriteLine("Ingesting records for table {0} with dimension partition key", tableName);
            await WriteRecords(databaseName, tableName, Constants.COMPOSITE_PARTITION_KEY_DIM_NAME);
            await WriteRecordsMultiMeasure(databaseName, tableName, Constants.COMPOSITE_PARTITION_KEY_DIM_NAME);

            Console.WriteLine(
                "Ingesting records for table {0} - Records will be rejected as ingestion done without enforced partition key",
                tableName);
            await WriteRecords(databaseName, tableName, Constants.COMPOSITE_PARTITION_KEY_DIFF_NAME);
            await WriteRecordsMultiMeasure(databaseName, tableName, Constants.COMPOSITE_PARTITION_KEY_DIFF_NAME);

            Console.WriteLine("Run Query for table {0} ", tableName);
            await runSingleQuery(databaseName, tableName, Constants.COMPOSITE_PARTITION_KEY_DIM_NAME,
                "'" + Constants.COMPOSITE_PARTITION_KEY_DIM_VALUE + "'");
        }

        /* This sample demonstrates workflow using measure name type partition key
        * 1. Create table with measure name type partition key
        * 2. Ingest records and query
        */
        public async Task runSampleWithMeasurePartitionKey(String databaseName, String tableName, String bucketName)
        {
            Console.WriteLine("Creating table {0} with measure partition key", tableName);

            List<PartitionKey> partitionKeyListWithDimensionAndOptionalEnforcement =
                new List<PartitionKey>
                {
                    new PartitionKey
                    {
                        Type = PartitionKeyType.MEASURE
                    }
                };

            await CreateTableWithPartitionKeys(databaseName, tableName,
                partitionKeyListWithDimensionAndOptionalEnforcement, bucketName);

            Console.WriteLine("Describing table {0} with measure partition key, there will not be enforcement & name as the " +
                              "table created with measure partition key", tableName);
            await crudAndSimpleIngestionExample.DescribeTable(databaseName, tableName);

            Console.WriteLine("Ingest records for table with measure partition key {0}", tableName);
            await WriteRecords(databaseName, tableName, Constants.COMPOSITE_PARTITION_KEY_DIM_NAME);
            await WriteRecordsMultiMeasure(databaseName, tableName, Constants.COMPOSITE_PARTITION_KEY_DIM_NAME);

            Console.WriteLine("Run Query for table {0} ", tableName);
            await runSingleQuery(databaseName, tableName, "cpu_utilization",
                "13.5");
        }

        public async Task Run(bool skipDeletion)
        {
            try
            {
                
                await timestreamDependencyHelper.CreateS3Bucket(this.s3Client, this.bucketName);
                await crudAndSimpleIngestionExample.CreateDatabase(Constants.DATABASE_NAME);
                
                Console.WriteLine("Starting example for dimension type partition key:");
                await runSampleWithDimensionPartitionKey(Constants.DATABASE_NAME,
                    Constants.PARTITION_KEY_DIMENSION_TABLE_NAME, this.bucketName);
                
                Console.WriteLine("Starting example for measure type partition key:");
                await runSampleWithMeasurePartitionKey(Constants.DATABASE_NAME,
                    Constants.PARTITION_KEY_MEASURE_TABLE_NAME, this.bucketName);
            }
            finally
            {
                if (!skipDeletion)
                {
                    await timestreamDependencyHelper.DeleteS3Bucket(this.s3Client, this.bucketName);
                    await crudAndSimpleIngestionExample.DeleteTable(Constants.DATABASE_NAME,
                        Constants.PARTITION_KEY_DIMENSION_TABLE_NAME);
                    await crudAndSimpleIngestionExample.DeleteTable(Constants.DATABASE_NAME,
                        Constants.PARTITION_KEY_MEASURE_TABLE_NAME);
                    await crudAndSimpleIngestionExample.DeleteDatabase(Constants.DATABASE_NAME);
                }
            }
        }
    }
}