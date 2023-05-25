using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.IO;
using System.IO.Compression;
using Amazon;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using Amazon.S3;
using Amazon.S3.Model;

namespace TimestreamDotNetSample
{
    public class TimestreamDependencyHelper
    {
        public async Task<string> GetAccount()
        {
             var client = new AmazonSecurityTokenServiceClient();
             try
             {
                 var caller = await client.GetCallerIdentityAsync(new GetCallerIdentityRequest());
                 return caller.Account;
             }
             catch (Exception e)
             {
                  Console.WriteLine($"Failed to get Account {e}");
                  throw;
             }
        }
        public async Task<string> GetS3Object(IAmazonS3 s3Client, string bucketName, string key)
         {
             Task<GetObjectResponse> res = s3Client.GetObjectAsync(new GetObjectRequest()
             {
                 BucketName = bucketName,
                 Key = key
             });

             try
             {
                await res;
                using (var reader = new StreamReader(res.Result.ResponseStream))
                {
                     Console.WriteLine("Retrieved contents of object '{0}' in bucket '{1}'", bucketName, key);
                     return await reader.ReadToEndAsync();
                }
             }
             catch (Exception e)
            {
               Console.WriteLine($"Error while getting the object: {e}");
               throw;
            }
         }

         public List<string> GetGZippedData(IAmazonS3 s3Client, string bucketName, string key)
         {
             var lines = new List<string>();
             var s3Object = s3Client.GetObjectAsync(new GetObjectRequest()
            {
                BucketName = bucketName,
                Key = key
            }).GetAwaiter().GetResult();
             using (var decompressionStream = new GZipStream(s3Object.ResponseStream, CompressionMode.Decompress))
             {
                 using (var reader = new StreamReader(decompressionStream))
                 {
                     string line;
                     while ((line = reader.ReadLine()) != null)
                     {
                         lines.Add(line);
                     }
                 }
             }
             return lines;
         }

        public async Task CreateS3Bucket(IAmazonS3 s3Client, string bucketName)
        {
            try
            {
                Console.WriteLine("Creating S3 bucket..");

                await s3Client.PutBucketAsync(new PutBucketRequest()
                {
                    BucketName = bucketName
                });

                Console.WriteLine($"Successfully created S3 bucket : {bucketName}");
            }
            catch (Exception e)
            {
                Console.WriteLine($"S3 Bucket creation failed: {e}");
                throw;
            }
        }

        public async Task DeleteS3Bucket(IAmazonS3 s3Client, string bucketName)
        {
            try
            {
                Console.WriteLine($"Deleting S3 bucket {bucketName}");
                // To delete a bucket, all the objects in the bucket must be deleted first

                ListObjectsV2Request request = new ListObjectsV2Request()
                {
                    BucketName = bucketName
                };
                ListObjectsV2Response response;
                do
                {
                    response = await s3Client.ListObjectsV2Async(request);
                    foreach (var responseS3Object in response.S3Objects)
                    {
                        await s3Client.DeleteObjectAsync(new DeleteObjectRequest()
                        {
                            BucketName = bucketName,
                            Key = responseS3Object.Key
                        });
                    }

                    request = new ListObjectsV2Request()
                    {
                        BucketName = bucketName,
                        ContinuationToken = response.ContinuationToken
                    };
                } while (response.IsTruncated);

                await s3Client.DeleteBucketAsync(new DeleteBucketRequest()
                {
                    BucketName = bucketName
                });
            }
            catch (Exception e)
            {
                Console.WriteLine($"S3 Bucket Deletion failed: {e}");
            }
        }
    }
}