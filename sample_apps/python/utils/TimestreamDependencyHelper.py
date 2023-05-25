import boto3
import json


class TimestreamDependencyHelper:

    def __init__(self, region):
        self.region = region
        self.s3_client = boto3.client('s3', region_name=region)
        self.s3_resource = boto3.resource('s3')
        self.sts_client = boto3.client("sts")

    def get_account(self):
        return self.sts_client.get_caller_identity()["Account"]

    def create_s3_bucket(self, bucket_name):
        print("\nCreating S3 bucket " + bucket_name)
        try:
            if self.region == 'us-east-1':
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client.create_bucket(Bucket=bucket_name,
                                             CreateBucketConfiguration={'LocationConstraint': self.region})
            print("Successfully created S3 bucket: ", bucket_name)
            return bucket_name

        except self.s3_client.exceptions.BucketAlreadyOwnedByYou:
            print("Skipping Bucket Creation. Reason: Bucket " + bucket_name + " already exists.")
        except Exception as err:
            print("Failed creating S3 bucket: ", err)
            raise err

    def delete_s3_bucket(self, bucket_arn):
        print("Deleting S3 bucket")
        try:
            bucket = self.s3_resource.Bucket(bucket_arn)
            bucket.objects.all().delete()
            bucket.delete()
            print("Successfully deleted S3 bucket: ", bucket_arn)
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Failed to delete S3 bucket: ", err)

    def list_s3_objects(self, bucket_name, prefix):
        print("\nListing object in S3 bucket")
        try:
            bucket = self.s3_resource.Bucket(bucket_name)
            print("Successfully listed objects in S3 bucket: ", bucket_name)
            return bucket.objects.filter(Prefix=prefix)
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Failed to list objects in S3 bucket: ", err)
    
    def get_object(self, uri):
        try:
            bucket, key = uri.replace("s3://", "").split("/", 1)
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            return response['Body']
        except Exception as err:
            print("Failed to get the object for URI:", uri)
            raise err
