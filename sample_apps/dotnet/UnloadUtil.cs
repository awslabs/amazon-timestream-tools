using Amazon.TimestreamQuery;
 using Amazon.TimestreamQuery.Model;
 using System.Threading.Tasks;
 using Amazon.S3;
 using System;
 using System.IO;
 using System.Collections.Generic;
 using System.Text.Json;
 using System.Linq;
 using Newtonsoft.Json.Linq;

 namespace TimestreamDotNetSample {
     public class UnloadUtil
     {
         private readonly AmazonTimestreamQueryClient queryClient;
         private readonly TimestreamDependencyHelper timestreamDependencyHelper;
         private readonly AmazonS3Client s3Client;
         private readonly string region;
         private readonly string bucketName;
         private List<UnloadQuery> queries = new List<UnloadQuery>();

         public UnloadUtil(AmazonTimestreamQueryClient queryClient, AmazonS3Client s3Client,
         TimestreamDependencyHelper timestreamDependencyHelper, string region, string bucketName)
         {
             this.queryClient = queryClient;
             this.timestreamDependencyHelper = timestreamDependencyHelper;
             this.s3Client = s3Client;
             this.region = region;
             this.bucketName = bucketName;

             constructQueries();
         }

         private void constructQueries()
         {
             string QUERY_1 = "SELECT user_id, ip_address, event, session_id, measure_name, time, query, quantity, product_id, channel FROM ";
             QUERY_1 = QUERY_1 + Constants.DATABASE_NAME + "." + Constants.UNLOAD_TABLE_NAME;
             QUERY_1 = QUERY_1 + " WHERE time BETWEEN ago(2d) AND now()";

             this.queries.Add(new UnloadQuery(QUERY_1, this.bucketName, "without_partition", "CSV", "GZIP", Array.Empty<string>()));
             string[] partitionedBy_1 = { "channel" };
             this.queries.Add(new UnloadQuery(QUERY_1, this.bucketName, "partition_by_channel", "CSV", "GZIP", partitionedBy_1));

             string QUERY_2 = "SELECT user_id, ip_address, channel, session_id, measure_name, time, query, quantity, product_id, event FROM ";
             QUERY_2 = QUERY_2 + Constants.DATABASE_NAME + "." + Constants.UNLOAD_TABLE_NAME;
             QUERY_2 = QUERY_2 + " WHERE time BETWEEN ago(2d) AND now()";
             string[] partitionedBy_2 = { "event" };
             this.queries.Add(new UnloadQuery(QUERY_2, this.bucketName, "partition_by_event", "CSV", "GZIP", partitionedBy_2));

             string QUERY_3 = "SELECT user_id, ip_address, session_id, measure_name, time, query, quantity, product_id, channel, event FROM ";
             QUERY_3 = QUERY_3 + Constants.DATABASE_NAME + "." + Constants.UNLOAD_TABLE_NAME;
             QUERY_3 = QUERY_3 + " WHERE time BETWEEN ago(2d) AND now()";
             string[] partitionedBy_3 = { "channel", "event" };
             this.queries.Add(new UnloadQuery(QUERY_3, this.bucketName, "partition_by_channel_and_event", "CSV", "GZIP", partitionedBy_3));
         }

         public async Task RunAllQueries()
         {
             for(int i = 0; i < this.queries.Count; i++)
             {
                 Console.WriteLine($"Running query {(i + 1)}:{queries[i].BuildQuery()}");
                 await RunQueryAsync(this.queries[i]);
             }
         }

         private async Task RunQueryAsync(UnloadQuery query)
         {
             try
             {
                 QueryRequest queryRequest = new QueryRequest();
                 queryRequest.QueryString = query.BuildQuery();
                 QueryResponse queryResponse = await queryClient.QueryAsync(queryRequest);
                 while (true)
                 {
                     if (queryResponse.NextToken == null)
                     {
                         var result = ParseQueryResult(query, queryResponse);
                         JObject manifest = await GetJsonFile(result["manifestFile"]);
                         JObject metadata = await GetJsonFile(result["metadataFile"]);
                         ParseColumns(query, metadata);
                         GetResultFiles(manifest);
                         break;
                     }
                     queryRequest.NextToken = queryResponse.NextToken;
                     queryResponse = await queryClient.QueryAsync(queryRequest);
                 }
             } catch(Exception e)
             {
                 Console.WriteLine(e.ToString());
             }
         }

         private Dictionary <string, string> ParseQueryResult(UnloadQuery query, QueryResponse response)
         {
             List<ColumnInfo> columnInfo = response.ColumnInfo;
             var options = new JsonSerializerOptions
             {
                 DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
             };
             List<String> columnInfoStrings = columnInfo.ConvertAll(x => JsonSerializer.Serialize(x, options));
             List<Row> rows = response.Rows;

             Console.WriteLine("ColumnInfo:" + string.Join(",", columnInfoStrings));
             Console.WriteLine("Data:");

             Dictionary <string, string> result = new Dictionary <string, string> ();
             foreach (Row row in rows)
             {
                 result = ParseRow(columnInfo, row);
             }
             return result;
         }

         private Dictionary <string, string> ParseRow(List<ColumnInfo> columnInfo, Row row)
         {
             List<Datum> data = row.Data;
             Dictionary <string, string> response = new Dictionary <string, string> ();
             for (int j = 0; j < data.Count; j++)
             {
                 ColumnInfo info = columnInfo[j];
                 Datum datum = data[j];
                 response[info.Name] = datum.ScalarValue;
             }
             return response;
         }

         private async Task<JObject> GetJsonFile(string uri)
         {
             string[] s3Values = uri.Replace("s3://", "").Split(new[] { '/' }, 2);
             Console.WriteLine($"Bucket={s3Values[0]}, Key={s3Values[1]}");
             string data = await timestreamDependencyHelper.GetS3Object(s3Client, s3Values[0], s3Values[1]);
             return JObject.Parse(data);
         }

         private void ParseColumns(UnloadQuery query, JObject metadata)
         {
             Console.WriteLine("Parsing Columns...");
             JArray columnInfo = metadata["ColumnInfo"] as JArray;
             List<string> columns = new List<string>();
             foreach (JObject column in columnInfo)
             {
                 int pos = Array.IndexOf(query.partitionedBy, column["Name"].ToString());
                 if (pos < 0)
                 {
                     columns.Add(column["Name"].ToString());
                 }
             }
             Console.WriteLine("Columns:" + string.Join(",", columns));
         }

         private void GetResultFiles(JObject manifest)
         {
             JArray resultFiles = manifest["result_files"] as JArray;
             Console.WriteLine($"Total files: {resultFiles.Count}");
             List<string> data = new List<string>();
             foreach (JObject item in resultFiles)
             {
                 string url = item.GetValue("url").ToString();
                 string[] s3Values = url.Replace("s3://", "").Split(new[] { '/' }, 2);
                 data.AddRange(timestreamDependencyHelper.GetGZippedData(s3Client, s3Values[0], s3Values[1]));
             }

             Console.WriteLine($"Number of rows: {data.Count}");
             // Only display first 10 lines in the file

             Console.WriteLine("Displaying first 10 rows...");
              var i = 0;
              foreach (var line in data)
              {
                  if (i > 10) break;
                  Console.WriteLine(line);
                  i++;
              }
         }
     }

     public class UnloadQuery
     {
         public readonly string query;
         public readonly string s3BucketLocation;
         public readonly string resultPrefix;
         public readonly string format;
         public readonly string compression;
         public readonly string[] partitionedBy;

         public UnloadQuery(string query, string s3BucketLocation, string resultPrefix, string format, string compression, string[] partitionedBy)
         {
             this.query = query;
             this.s3BucketLocation = s3BucketLocation;
             this.resultPrefix = resultPrefix;
             this.format = format;
             this.compression = compression;
             this.partitionedBy = partitionedBy;
         }

         public string BuildQuery()
         {
             var queryResultS3Location = "'s3://" + this.s3BucketLocation + "/" + this.resultPrefix + "/'";
             var unloadQuery = "UNLOAD(";
             unloadQuery = unloadQuery + this.query;
             unloadQuery = unloadQuery + ") ";
             unloadQuery = unloadQuery + " TO " + queryResultS3Location;
             unloadQuery = unloadQuery + " WITH ( ";

             if (this.partitionedBy.Length > 0)
             {
                 int i = 0;
                 string partitionedColumns = "";
                 foreach (string column in this.partitionedBy)
                 {
                     if (i == 0) {
                         partitionedColumns = partitionedColumns + "'" + column + "'";
                     } else
                     {
                         partitionedColumns = partitionedColumns + ",'" + column + "'";
                     }
                     i++;
                 }
                 unloadQuery = unloadQuery + " partitioned_by = ARRAY[" + partitionedColumns + "],";
             }

             unloadQuery = unloadQuery + " format='" + this.format  + "', ";
             unloadQuery = unloadQuery + "  compression='" + this.compression + "')";

             return unloadQuery;
         }
     }
 }