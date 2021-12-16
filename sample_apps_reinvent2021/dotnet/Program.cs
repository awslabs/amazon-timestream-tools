using Amazon.TimestreamWrite;
using Amazon;
using Amazon.TimestreamQuery;
using System.Threading.Tasks;
using System;
using System.Collections.Generic;
using Amazon.S3;
using CommandLine;



namespace TimestreamDotNetSample
{

    static class Constants
    {
        public const string DATABASE_NAME = "devops_sample_application_multi";
        public const string TABLE_NAME = "host_metrics_sample_application_multi";
        public const long HT_TTL_HOURS = 24;
        public const long CT_TTL_DAYS = 7;
    }
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
        }

        public static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed<Options>(o => {
                    MainAsync(o.KmsKey, o.CsvFile, o.Region, o.SkipDeletion).GetAwaiter().GetResult();
                });
        }

        static async Task MainAsync(string kmsKeyId, string csvFilePath, string region, bool skipDeletion)
        {
            // Recommended Timestream write client SDK configuration:
            // - Set SDK retry count to 10
            // - Set RequestTimeout to 100 seconds
            var writeClientConfig = new AmazonTimestreamWriteConfig
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(region),
                Timeout = TimeSpan.FromSeconds(100),
                MaxErrorRetry = 10
            };

            var writeClient = new AmazonTimestreamWriteClient(writeClientConfig);
            var crudHelper = new TimestreamCrudHelper();
            var timestreamDependencyHelper = new TimestreamDependencyHelper();
            var crudAndSimpleIngestionExample = new CrudAndSimpleIngestionExample(writeClient, crudHelper);
            var csvIngestionExample = new CsvIngestionExample(writeClient);
            var multiValueIngestionExample = new MultiValueAttributesExample(writeClient);
            var s3Client = new AmazonS3Client(RegionEndpoint.GetBySystemName(region));

            var queryClientConfig = new AmazonTimestreamQueryConfig
            {
                RegionEndpoint = RegionEndpoint.GetBySystemName(region)
            };
            var queryClient = new AmazonTimestreamQueryClient(queryClientConfig);
            var queryExample = new QueryExample(queryClient);

            await crudAndSimpleIngestionExample.CreateDatabase();
            await crudAndSimpleIngestionExample.DescribeDatabase();
            await crudAndSimpleIngestionExample.ListDatabases();
            if (kmsKeyId != null) {
                await crudAndSimpleIngestionExample.UpdateDatabase("TestFakeKey");
            }

            Dictionary<string, string> resourcesCreated = new Dictionary<string, string>();
            var s3ErrorReportBucketName = AwsResourceConstant.ErrorConfigurationS3BucketNamePrefix +
                                          Guid.NewGuid().ToString("n").Substring(0, 8);
            await timestreamDependencyHelper.CreateS3Bucket(s3Client, s3ErrorReportBucketName);
            resourcesCreated.Add(AwsResourceConstant.BucketName, s3ErrorReportBucketName);

            await crudAndSimpleIngestionExample.CreateTable(s3ErrorReportBucketName);
            await crudAndSimpleIngestionExample.DescribeTable();
            await crudAndSimpleIngestionExample.ListTables();
            await crudAndSimpleIngestionExample.UpdateTable();

            // Simple records ingestion
            await crudAndSimpleIngestionExample.WriteRecordsMultiMeasure();
            await crudAndSimpleIngestionExample.WriteRecordsWithCommonAttributes();

            // upsert records
            await crudAndSimpleIngestionExample.WriteRecordsWithUpsert();

            // write multi value records
            await crudHelper.CreateDatabase(writeClient, MultiMeasureValueConstants.MultiMeasureValueSampleDb);
            await crudHelper.CreateTable(writeClient, MultiMeasureValueConstants.MultiMeasureValueSampleDb,
                MultiMeasureValueConstants.MultiMeasureValueSampleTable, s3ErrorReportBucketName);
            await multiValueIngestionExample.WriteRecordsMultiMeasureValueSingleRecord();
            await multiValueIngestionExample.WriteRecordsMultiMeasureValueMultipleRecords();

            if (csvFilePath != null)
            {
                // Bulk record ingestion for bootstrapping a table with fresh data
                await csvIngestionExample.BulkWriteRecordsMultiMeasure(csvFilePath);

                await queryExample.RunAllQueries();

                // Try cancelling query
                await queryExample.CancelQuery();

                // Run query with multiple pages
                await queryExample.RunQueryWithMultiplePages(20000);

                // Run Scheduled Query examples only if CSV was provided
                var scheduledQueryExample = new ScheduledQueryExample(writeClient, queryClient,
                    timestreamDependencyHelper,
                    crudHelper, queryExample, region, s3Client);
                await scheduledQueryExample.RunScheduledQueryExample(skipDeletion, resourcesCreated);
            }
            else
            {
                Console.WriteLine("Running the SELECT ALL query");
                await queryExample.RunQueryAsync(QueryExample.SELECT_ALL_QUERY + " LIMIT 10");
            }

            if (!skipDeletion)
            {
                await crudAndSimpleIngestionExample.DeleteTable();
                await crudAndSimpleIngestionExample.DeleteDatabase();
            }
        }
    }
}
