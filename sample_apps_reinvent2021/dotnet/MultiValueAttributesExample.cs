using System;
using System.IO;
using System.Collections.Generic;
using Amazon.TimestreamWrite;
using Amazon.TimestreamWrite.Model;
using System.Threading.Tasks;

namespace TimestreamDotNetSample
{
    static class MultiMeasureValueConstants
    {
        public const string MultiMeasureValueSampleDb = "multiMeasureValueSampleDb";
        public const string MultiMeasureValueSampleTable = "multiMeasureValueSampleTable";
    }

    public class MultiValueAttributesExample
    {
        private readonly AmazonTimestreamWriteClient writeClient;

        public MultiValueAttributesExample(AmazonTimestreamWriteClient writeClient)
        {
            this.writeClient = writeClient;
        }

        public async Task WriteRecordsMultiMeasureValueSingleRecord()
        {
            Console.WriteLine("Writing records with multi value attributes");

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
                Time = currentTimeString
            };

            var cpuUtilization = new MeasureValue
            {
                Name = "cpu_utilization",
                Value = "13.6",
                Type = "DOUBLE"
            };

            var memoryUtilization = new MeasureValue
            {
                Name = "memory_utilization",
                Value = "40",
                Type = "DOUBLE"
            };

            var computationalRecord = new Record
            {
                MeasureName = "cpu_memory",
                MeasureValues = new List<MeasureValue> {cpuUtilization, memoryUtilization},
                MeasureValueType = "MULTI"
            };


            List<Record> records = new List<Record>();
            records.Add(computationalRecord);

            try
            {
                var writeRecordsRequest = new WriteRecordsRequest
                {
                    DatabaseName = MultiMeasureValueConstants.MultiMeasureValueSampleDb,
                    TableName = MultiMeasureValueConstants.MultiMeasureValueSampleTable,
                    Records = records,
                    CommonAttributes = commonAttributes
                };
                WriteRecordsResponse response = await writeClient.WriteRecordsAsync(writeRecordsRequest);
                Console.WriteLine($"Write records status code: {response.HttpStatusCode.ToString()}");
            }
            catch (Exception e)
            {
                Console.WriteLine("Write records failure:" + e.ToString());
            }
        }

        public async Task WriteRecordsMultiMeasureValueMultipleRecords()
        {
            Console.WriteLine("Writing records with multi value attributes mixture type");

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
                Time = currentTimeString
            };

            var cpuUtilization = new MeasureValue
            {
                Name = "cpu_utilization",
                Value = "13.6",
                Type = "DOUBLE"
            };

            var memoryUtilization = new MeasureValue
            {
                Name = "memory_utilization",
                Value = "40",
                Type = "DOUBLE"
            };

            var activeCores = new MeasureValue
            {
                Name = "active_cores",
                Value = "4",
                Type = "BIGINT"
            };

            var computationalRecord = new Record
            {
                MeasureName = "computational_utilization",
                MeasureValues = new List<MeasureValue> {cpuUtilization, memoryUtilization, activeCores},
                MeasureValueType = "MULTI"
            };

            var aliveRecord = new Record
            {
                MeasureName = "is_healthy",
                MeasureValue = "true",
                MeasureValueType = "BOOLEAN"
            };

            List<Record> records = new List<Record>();
            records.Add(computationalRecord);
            records.Add(aliveRecord);

            try
            {
                var writeRecordsRequest = new WriteRecordsRequest
                {
                    DatabaseName = MultiMeasureValueConstants.MultiMeasureValueSampleDb,
                    TableName = MultiMeasureValueConstants.MultiMeasureValueSampleTable,
                    Records = records,
                    CommonAttributes = commonAttributes
                };
                WriteRecordsResponse response = await writeClient.WriteRecordsAsync(writeRecordsRequest);
                Console.WriteLine($"Write records status code: {response.HttpStatusCode.ToString()}");
            }
            catch (Exception e)
            {
                Console.WriteLine("Write records failure:" + e.ToString());
            }
        }
    }
}