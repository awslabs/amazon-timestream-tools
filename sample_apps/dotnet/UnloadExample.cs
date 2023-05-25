using System;
 using System.IO;
 using System.Collections.Generic;
 using Amazon.TimestreamWrite;
 using Amazon.TimestreamQuery;
 using Amazon.S3;
 using Amazon.TimestreamWrite.Model;
 using System.Threading.Tasks;
 using System.Linq;

 namespace TimestreamDotNetSample
 {
     public class UnloadExample
     {
         private readonly AmazonTimestreamQueryClient queryClient;
         private readonly AmazonTimestreamWriteClient writeClient;
         private readonly AmazonS3Client s3Client;
         private readonly TimestreamDependencyHelper timestreamDependencyHelper;
         private readonly string bucketName;
         private readonly UnloadUtil unloadUtil;
         private readonly CrudAndSimpleIngestionExample crudAndSimpleIngestionExample;

         public UnloadExample(AmazonTimestreamQueryClient queryClient, AmazonTimestreamWriteClient writeClient, AmazonS3Client s3Client, string region, string accountId)
         {
             this.queryClient = queryClient;
             this.writeClient = writeClient;
             this.s3Client = s3Client;
             this.timestreamDependencyHelper = new TimestreamDependencyHelper();
             this.bucketName = Constants.S3_BUCKET_PREFIX + "-" + region + "-" + accountId;
             this.unloadUtil = new UnloadUtil(this.queryClient, this.s3Client, this.timestreamDependencyHelper, region, this.bucketName);
             this.crudAndSimpleIngestionExample = new CrudAndSimpleIngestionExample(writeClient);
         }

         private async Task WriteRecords(string csvFilePath)
         {
             List<Record> records = new List<Record>();
             long currentTime = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
             int counter = 0;
             string[] headers = new string[] {};
             int i = 0;
             List<Task> writetasks = new List<Task>();

             foreach (string line in File.ReadLines(csvFilePath))
             {
                 string[] columns = line.Split(',');

                 if (i == 0) {
                     headers = columns;
                     i++;
                     continue;
                 }

                 List<Dimension> dimensions = new List<Dimension> {
                     new Dimension { Name = headers[0], Value = columns[0] },
                     new Dimension { Name = headers[1], Value = columns[1] },
                     new Dimension { Name = headers[2], Value = columns[2] },
                     new Dimension { Name = headers[3], Value = columns[3] },
                     new Dimension { Name = headers[4], Value = columns[4] },
                     new Dimension { Name = headers[5], Value = columns[5] }
                 };

                 long recordTime = currentTime - counter * 50;

                 List<MeasureValue> measureValues = new List<MeasureValue>();

                 for(int j = 7; j < 10; j++)
                 {
                     if (columns[j] != "")
                     {
                         measureValues.Add(new MeasureValue
                         {
                             Name= headers[j],
                             Value= columns[j],
                             Type= "VARCHAR",
                         });
                     }
                 }

                 if (columns[10] != "")
                 {
                     measureValues.Add(new MeasureValue
                     {
                         Name= headers[10],
                         Value= columns[10],
                         Type= "DOUBLE",
                     });
                 }

                 var record = new Record {
                     Dimensions = dimensions,
                     MeasureName = "metrics",
                     MeasureValues = measureValues,
                     MeasureValueType = Amazon.TimestreamWrite.MeasureValueType.MULTI,
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
                 Console.WriteLine("Records found", records.Count);
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
                     TableName = Constants.UNLOAD_TABLE_NAME,
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

         public async Task Run(string csvFilePath, string region, bool skipDeletion)
         {
             try
             {
                 await timestreamDependencyHelper.CreateS3Bucket(this.s3Client, this.bucketName);
                 await crudAndSimpleIngestionExample.CreateDatabase(Constants.DATABASE_NAME);
                 await crudAndSimpleIngestionExample.CreateTable(Constants.DATABASE_NAME, Constants.UNLOAD_TABLE_NAME);
                 await WriteRecords(csvFilePath);
                 await this.unloadUtil.RunAllQueries();
             }
             finally
             {
                 if (!skipDeletion)
                 {
                     await timestreamDependencyHelper.DeleteS3Bucket(this.s3Client, this.bucketName);
                     await crudAndSimpleIngestionExample.DeleteTable(Constants.DATABASE_NAME, Constants.UNLOAD_TABLE_NAME);
                     await crudAndSimpleIngestionExample.DeleteDatabase(Constants.DATABASE_NAME);
                 }
             }
         }
     }
 }