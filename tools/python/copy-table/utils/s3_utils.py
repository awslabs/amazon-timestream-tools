
import boto3
from utils.logger_utils import create_logger
import botocore
import boto3.s3.transfer as s3transfer
import tqdm
import os

class s3Utility:
    def __init__(self, region, multi_part_upload_chunk):
        botocore_config = botocore.config.Config(
            max_pool_connections=5000, retries={'max_attempts': 10})
        self.s3_client = boto3.client(
            's3', region_name=region, config=botocore_config)
        self.logger = create_logger("s3_logger")
        self.multi_part_upload_chunk = multi_part_upload_chunk
        self.region = region


    # create s3 bucket and return S3 bucket URI
    def create_s3_bucket(self, bucket_name):
        try:
            if self.region == 'us-east-1':
                response_s3 = self.s3_client.create_bucket(
                    Bucket=bucket_name
                )
            else:
                response_s3 = self.s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={
                        'LocationConstraint': self.region
                    }
                )

            bucket_s3_uri = 's3://' + bucket_name
            self.logger.info(f'S3 Bucket created successfully {bucket_s3_uri}')
        except self.s3_client.exceptions.BucketAlreadyOwnedByYou:
            self.logger.warning(f"Bucket {bucket_name} is already created and owned by you")
            bucket_s3_uri = 's3://' + bucket_name
        except Exception as err:
            self.logger.error("Creating bucket {bucket_name} failed :", err)
            raise
        
        return bucket_s3_uri

    # copy s3 objects from source to target
    def copy_s3_object_custom(self, source_bucket, source_key_list, destination_bucket, destination_key):
        bucket_size = 0
        try:

            transfer_config = s3transfer.TransferConfig(
                use_threads=True,
                max_concurrency=20,
                multipart_chunksize=self.multi_part_upload_chunk
            )

            progress = tqdm.tqdm(
                desc='upload',
                total=bucket_size, unit='B', unit_scale=1,
                position=0,
                bar_format='{desc:<10}{percentage:3.0f}%|{bar:10}{r_bar}')

            s3t = s3transfer.create_transfer_manager(
                self.s3_client, transfer_config)

            # Not adding only deskkey, as adding sourcekey more meaningful name instead of appending just with index
            destination_key_list = [
                destination_key+src for src in source_key_list]
            for src, dest in zip(source_key_list, destination_key_list):

                copy_source = {
                    'Bucket': source_bucket,
                    'Key': src
                }

                s3t.copy(copy_source=copy_source,
                         bucket=destination_bucket,
                         key=dest,
                         subscribers=[
                             s3transfer.ProgressCallbackInvoker(progress.update),],
                         )

            # close transfer job
            s3t.shutdown()
            progress.close()

        except Exception as err:
            self.logger.error(f'multipart upload failed with error : {err}')
            raise

    def copy_multiple_s3_objects(self, source_bucket, destination_bucket, source_s3_prefix, dest_s3_prefix):
        all_csv_files_list, partition_names = self.list_s3_object_custom(
            source_bucket, source_s3_prefix)
        self.logger.info(f'I am here {all_csv_files_list}')
        self.copy_s3_object_custom(
            source_bucket, all_csv_files_list, destination_bucket, dest_s3_prefix)

    def list_s3_object_custom(self, bucket_name, common_prefix=None):
        sources_keys_list = []
        all_files_list = []
        paginator = self.s3_client.get_paginator('list_objects_v2')

        try:
            operation_parameters = {'Bucket': bucket_name, 'Delimiter': '/'}

            if common_prefix is not None:
                operation_parameters = {
                    'Bucket': bucket_name, 'Prefix': common_prefix}
                # common_prefix_list_count = len(common_prefix.split("/")) - 1
            page_iterator = paginator.paginate(**operation_parameters)

            for page in page_iterator:
                if common_prefix is not None:
                    keys = [obj['Key'] for obj in page.get('Contents', [])]
                    all_files_list.extend(
                        [key for key in keys if key.endswith('.csv')])
                    parition_names = set(os.path.dirname(
                        file_path) + '/' for file_path in all_files_list)
                    sources_keys_list.extend(parition_names)
                else:
                    for folder in page.get('CommonPrefixes', []):
                        current_folder = folder['Prefix']
                        operation_parameters = {
                            'Bucket': bucket_name, 'Prefix': current_folder}
                        page_iterator = paginator.paginate(
                            **operation_parameters)
                        for page in page_iterator:
                            keys = [obj['Key']
                                    for obj in page.get('Contents', [])]
                            sources_keys_list.extend(
                                [key for key in keys if key.endswith('.csv')])

        except Exception as err:
            self.logger.error(
                f'Unable to list keys for the {bucket_name}: {err}')
            raise

        return all_files_list, sorted(list(set(sources_keys_list)))