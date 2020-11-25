using Amazon.TimestreamWrite;
using Amazon;
using Amazon.TimestreamQuery;
using System.Threading.Tasks;
using System;
using CommandLine;

static class Constants
{
    public const string DATABASE_NAME = "testDotNetDB";
    public const string TABLE_NAME = "testDotNetTable";
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
        }

        public static void Main(string[] args)
        {
            Parser.Default.ParseArguments<Options>(args)
                .WithParsed<Options>(o => {
                    MainAsync(o.KmsKey, o.CsvFile).GetAwaiter().GetResult();
                });
        }

        static async Task MainAsync(string kmsKeyId, string csvFilePath)
        {
            // Recommended Timestream write client SDK configuration:
            // - Set SDK retry count to 10
            // - Set RequestTimeout to 20 seconds
            var writeClientConfig = new AmazonTimestreamWriteConfig
            {
                Timeout = TimeSpan.FromSeconds(20),
                MaxErrorRetry = 10
            };
            
            var writeClient = new AmazonTimestreamWriteClient(writeClientConfig);
            var crudAndSimpleIngestionExample = new CrudAndSimpleIngestionExample(writeClient);
            var csvIngestionExample = new CsvIngestionExample(writeClient);

            var queryClient = new AmazonTimestreamQueryClient();
            var queryExample = new QueryExample(queryClient);

            await crudAndSimpleIngestionExample.CreateDatabase();
            await crudAndSimpleIngestionExample.DescribeDatabase();
            await crudAndSimpleIngestionExample.ListDatabases();
            if (kmsKeyId != null) {
                await crudAndSimpleIngestionExample.UpdateDatabase(kmsKeyId);
            }

            await crudAndSimpleIngestionExample.CreateTable();
            await crudAndSimpleIngestionExample.DescribeTable();
            await crudAndSimpleIngestionExample.ListTables();
            await crudAndSimpleIngestionExample.UpdateTable();

            // Simple records ingestion
            await crudAndSimpleIngestionExample.WriteRecords();
            await crudAndSimpleIngestionExample.WriteRecordsWithCommonAttributes();

            // upsert records
            await crudAndSimpleIngestionExample.WriteRecordsWithUpsert();

            if (csvFilePath != null) {
                // Bulk record ingestion for bootstrapping a table with fresh data
                await csvIngestionExample.BulkWriteRecords(csvFilePath);
            }

            await queryExample.RunAllQueries();

            // Try cancelling query
            await queryExample.CancelQuery();

            // Run query with multiple pages
            await queryExample.RunQueryWithMultiplePages(20000);

            // Commenting out clean up.
            // await crudAndSimpleIngestionExample.DeleteTable();
            // await crudAndSimpleIngestionExample.DeleteDatabase();
        }
    }
}
