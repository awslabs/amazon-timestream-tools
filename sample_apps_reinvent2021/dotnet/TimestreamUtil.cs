using System;
using System.Threading.Tasks;
using Amazon.TimestreamWrite;
using Amazon.TimestreamWrite.Model;

namespace TimestreamDotNetSample
{
    public class TimestreamCrudHelper
    {
        public async Task CreateDatabase(IAmazonTimestreamWrite writeClient, string databaseName)
        {
            Console.WriteLine($"Creating Database {databaseName}");

            try
            {
                var createDatabaseRequest = new CreateDatabaseRequest
                {
                    DatabaseName = databaseName
                };
                await writeClient.CreateDatabaseAsync(createDatabaseRequest);
                Console.WriteLine($"Database {databaseName} created");
            }
            catch (ConflictException)
            {
                Console.WriteLine("Database already exists.");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Create database failed: {e}");
            }
        }

        public async Task DeleteDatabase(IAmazonTimestreamWrite writeClient, string databaseName)
        {
            Console.WriteLine($"Deleting database {databaseName}");
            try
            {
                var deleteDatabaseRequest = new DeleteDatabaseRequest
                {
                    DatabaseName = databaseName
                };
                DeleteDatabaseResponse response = await writeClient.DeleteDatabaseAsync(deleteDatabaseRequest);
                Console.WriteLine($"Database {databaseName} delete request status:{response.HttpStatusCode}");
            }
            catch (ResourceNotFoundException)
            {
                Console.WriteLine($"Database {databaseName} does not exists");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception while deleting database: {e}");
            }
        }

        public async Task CreateTable(IAmazonTimestreamWrite writeClient, string databaseName, string tableName, string s3ErrorReportBucketName)
        {
            Console.WriteLine($"Creating Table {tableName}");

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
                    // Enable MagneticStoreWrite
                    MagneticStoreWriteProperties = new MagneticStoreWriteProperties 
                    {
                        EnableMagneticStoreWrites = true,
                        // Persist MagneticStoreWrite rejected records in S3
                        MagneticStoreRejectedDataLocation = new MagneticStoreRejectedDataLocation{
                                S3Configuration        =  new S3Configuration{
                                BucketName            =  s3ErrorReportBucketName,
                                EncryptionOption    =  "SSE_S3",
                            },
                        },
                    }
                };
                await writeClient.CreateTableAsync(createTableRequest);
                Console.WriteLine($"Table {tableName} created");
            }
            catch (ConflictException)
            {
                Console.WriteLine("Table already exists.");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Create table failed: {e}");
            }
        }

        public async Task DeleteTable(IAmazonTimestreamWrite writeClient, string databaseName, string tableName)
        {
            Console.WriteLine($"Deleting table {tableName}");
            try
            {
                var deleteTableRequest = new DeleteTableRequest
                {
                    DatabaseName = databaseName,
                    TableName = tableName
                };
                DeleteTableResponse response = await writeClient.DeleteTableAsync(deleteTableRequest);
                Console.WriteLine($"Table {tableName} delete request status: {response.HttpStatusCode}");
            }
            catch (ResourceNotFoundException)
            {
                Console.WriteLine($"Table {tableName} does not exists");
            }
            catch (Exception e)
            {
                Console.WriteLine($"Exception while deleting table: {e}");
            }
        }
    }
}