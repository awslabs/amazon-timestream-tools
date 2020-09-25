using System;
using System.IO;
using System.Collections.Generic;
using Amazon.TimestreamWrite;
using Amazon.TimestreamWrite.Model;
using System.Threading.Tasks;

namespace TimestreamDotNetSample
{
    public class CsvIngestionExample
    {
        private readonly AmazonTimestreamWriteClient writeClient;

        public CsvIngestionExample(AmazonTimestreamWriteClient writeClient)
        {
            this.writeClient = writeClient;
        }

        public async Task BulkWriteRecords(string csvFilePath)
        {
            List<Record> records = new List<Record>();
            long currentTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            int counter = 0;

            List<Task> writetasks = new List<Task>();

            foreach (string line in File.ReadLines(csvFilePath))
            {
                string[] columns = line.Split(',');

                List<Dimension> dimensions = new List<Dimension> {
                    new Dimension { Name = columns[0], Value = columns[1] },
                    new Dimension { Name = columns[2], Value = columns[3] },
                    new Dimension { Name = columns[4], Value = columns[5] }
                };

                long recordTime = currentTime - counter * 50;

                var record = new Record {
                    Dimensions = dimensions,
                    MeasureName = columns[6],
                    MeasureValue = columns[7],
                    MeasureValueType = columns[8],
                    Time = recordTime.ToString()
                };

                records.Add(record);
                counter++;

                // when the batch hits the max size, submit the batch
                if (records.Count == 100)
                {
                    writetasks.Add(SubmitBatchAsync(records, counter));
                    records.Clear();
                }
            }

            if(records.Count != 0)
            {
                writetasks.Add(SubmitBatchAsync(records, counter));
            }

            await Task.WhenAll(writetasks.ToArray());

            Console.WriteLine($"Ingested {counter} records.");
        }

        private async Task SubmitBatchAsync(List<Record> records, int counter)
        {
            try
            {
                var writeRecordsRequest = new WriteRecordsRequest
                {
                    DatabaseName = Constants.DATABASE_NAME,
                    TableName = Constants.TABLE_NAME,
                    Records = records
                };
                WriteRecordsResponse response = await writeClient.WriteRecordsAsync(writeRecordsRequest);
                Console.WriteLine($"Processed {counter} records. Write records status code:{response.HttpStatusCode.ToString()}");
            }
            catch (Exception e)
            {
                Console.WriteLine("Write records failure:" + e.ToString());
            }
        }
    }
}
