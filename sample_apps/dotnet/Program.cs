using Amazon.TimestreamWrite;
using Amazon;
using Amazon.TimestreamQuery;
using System.Threading.Tasks;
using System;
using CommandLine;
using Amazon.S3;

static class Constants
{
    public const string DATABASE_NAME = "testDotNetDB";
    public const string TABLE_NAME = "testDotNetTable";
    public const string UNLOAD_TABLE_NAME = "clickstream_shopping_metrics";
    public const string S3_BUCKET_PREFIX = "timestream-sample";
    public const long HT_TTL_HOURS = 24;
    public const long CT_TTL_DAYS = 7;
}

namespace TimestreamDotNetSample
{
    class MainClass
    {
        public class Options
        {
            [Option('k', "kms-key", Required = false, HelpText = "Kms Key Id for UpdateDatabase.")]
            public string KmsKey { get; set; }

            [Option('f', "csv-file", Required = false, HelpText = "Csv file path for sample data.")]
            public string CsvFile { get; set; }

            [Option('r', "region", Required = false, HelpText = "aws region to run sample application", Default = "us-east-1")]
	        public string Region { get; set; }

            [Option('s', "skip-deletion", HelpText = "skip deleting resources created as part of sample application")]
            public bool SkipDeletion { get; set; }

            [Option('t', "type", HelpText = "skip deleting resources created as part of sample application", Default = "BASIC")]
            public string Type { get; set; }
        }

        public static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed<Options>(o => {
                    MainAsync(o.KmsKey, o.CsvFile, o.Region, o.SkipDeletion, o.Type).GetAwaiter().GetResult();
                });
        }

        static async Task MainAsync(string kmsKeyId, string csvFilePath, string region, bool skipDeletion, string type)
        {
            // Recommended Timestream write client SDK configuration:
            // - Set SDK retry count to 10
            // - Set RequestTimeout to 20 seconds
            var writeClientConfig = new AmazonTimestreamWriteConfig
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(region),
                Timeout = TimeSpan.FromSeconds(20),
                MaxErrorRetry = 10
            };
            
            var writeClient = new AmazonTimestreamWriteClient(writeClientConfig);

            var queryClientConfig = new AmazonTimestreamQueryConfig
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(region)
            };
            AmazonTimestreamQueryClient queryClient = new AmazonTimestreamQueryClient(queryClientConfig);
            AmazonS3Client s3Client = new AmazonS3Client(RegionEndpoint.GetBySystemName(region));

            CrudAndSimpleIngestionExample crudAndSimpleIngestionExample = new CrudAndSimpleIngestionExample(writeClient);
            QueryExample queryExample = new QueryExample(queryClient);
            CsvIngestionExample csvIngestionExample = new CsvIngestionExample(writeClient);

            TimestreamDependencyHelper timestreamDependencyHelper = new TimestreamDependencyHelper();
            string accountId = await timestreamDependencyHelper.GetAccount();
            UnloadExample unloadExample = new UnloadExample(queryClient, writeClient, s3Client, region, accountId);

            if (type == AppType.BASIC.ToString())
            {
                await CallServices(crudAndSimpleIngestionExample, csvIngestionExample,
                queryExample, kmsKeyId, csvFilePath, skipDeletion);
            }
            else if (type == AppType.UNLOAD.ToString())
            {
                await CallUnload(unloadExample, csvFilePath, region, skipDeletion);
            }
            else if (type == AppType.CLEANUP.ToString())
            {
                await Cleanup(crudAndSimpleIngestionExample);
            }
        }

        static async Task CallServices(CrudAndSimpleIngestionExample crudAndSimpleIngestionExample,
        CsvIngestionExample csvIngestionExample, QueryExample queryExample, string kmsKeyId, string csvFilePath, bool skipDeletion)
        {
            await crudAndSimpleIngestionExample.CreateDatabase(Constants.DATABASE_NAME);
            await crudAndSimpleIngestionExample.DescribeDatabase(Constants.DATABASE_NAME);
            await crudAndSimpleIngestionExample.ListDatabases();
            if (kmsKeyId != null)
            {
                await crudAndSimpleIngestionExample.UpdateDatabase(Constants.DATABASE_NAME, "TestFakeKey");
            }

            await crudAndSimpleIngestionExample.CreateTable(Constants.DATABASE_NAME, Constants.TABLE_NAME);
            await crudAndSimpleIngestionExample.DescribeTable(Constants.DATABASE_NAME, Constants.TABLE_NAME);
            await crudAndSimpleIngestionExample.ListTables(Constants.DATABASE_NAME);
            await crudAndSimpleIngestionExample.UpdateTable(Constants.DATABASE_NAME, Constants.TABLE_NAME);

            // Simple records ingestion
            await crudAndSimpleIngestionExample.WriteRecords(Constants.DATABASE_NAME, Constants.TABLE_NAME);
            await crudAndSimpleIngestionExample.WriteRecordsWithCommonAttributes(Constants.DATABASE_NAME, Constants.TABLE_NAME);

            // upsert records
            await crudAndSimpleIngestionExample.WriteRecordsWithUpsert(Constants.DATABASE_NAME, Constants.TABLE_NAME);

            if (csvFilePath != null) {
                // Bulk record ingestion for bootstrapping a table with fresh data
                await csvIngestionExample.BulkWriteRecords(csvFilePath);
            }

            await queryExample.RunAllQueries();
            // Try cancelling query
            await queryExample.CancelQuery();

            // Run query with multiple pages
            await queryExample.RunQueryWithMultiplePages(20000);

            if (!skipDeletion)
            {
                await crudAndSimpleIngestionExample.DeleteTable(Constants.DATABASE_NAME, Constants.TABLE_NAME);
                await crudAndSimpleIngestionExample.DeleteDatabase(Constants.DATABASE_NAME);
            }
        }

        static async Task CallUnload(UnloadExample unloadExample, string csvFilePath, string region, bool skipDeletion)
        {
            await unloadExample.Run(csvFilePath, region, skipDeletion);
        }

        static async Task Cleanup(CrudAndSimpleIngestionExample crudAndSimpleIngestionExample)
        {
            await crudAndSimpleIngestionExample.DeleteTable(Constants.DATABASE_NAME, Constants.TABLE_NAME);
            await crudAndSimpleIngestionExample.DeleteTable(Constants.DATABASE_NAME, Constants.UNLOAD_TABLE_NAME);
            await crudAndSimpleIngestionExample.DeleteDatabase(Constants.DATABASE_NAME);
        }
    }

    public enum AppType
    {
         BASIC, UNLOAD, CLEANUP
    }
}
