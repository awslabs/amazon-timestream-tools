import boto3
import json


class TimestreamDependencyHelper:

    def __init__(self, region):
        self.region = region
        self.sns_client = boto3.client('sns', region_name=region)
        self.iam_client = boto3.client('iam', region_name=region)
        self.sqs_client = boto3.client('sqs', region_name=region)
        self.account_id = boto3.client("sts").get_caller_identity()["Account"]
        self.s3_client = boto3.client('s3', region_name=region)
        self.s3_resource = boto3.resource('s3')

    def create_sns(self, topic_name):
        print("\nCreating sns topic")
        try:
            response = self.sns_client.create_topic(Name=topic_name)
            print("Sns topic created successfully, topic arn:", response['TopicArn'])
            return response['TopicArn']
        except Exception as err:
            print("Failed creating sns: ", err)
            raise err

    def delete_sns(self, topic_arn):
        print("Deleting sns topic")
        try:
            self.sns_client.delete_topic(TopicArn=topic_arn)
            print("Sns topic deleted successfully")
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Failed deleting sns: ", err)

    def create_sqs_queue(self, queue_name):
        print("\nCreating sqs queue")
        try:
            response = self.sqs_client.create_queue(QueueName=queue_name)
            print("Sqs queue created successfully, queue url:", response['QueueUrl'])
            return response['QueueUrl']
        except Exception as err:
            print("Failed creating sqs: ", err)
            raise err

    def get_queue_arn(self, queue_url):
        try:
            response = self.sqs_client.get_queue_attributes(QueueUrl=queue_url,
                                                            AttributeNames=['QueueArn'])
            return response['Attributes']['QueueArn']
        except Exception as err:
            print("Failed getting sqs queue arn: ", err)
            raise err

    def set_sqs_access_policy(self, queue_url, queue_arn, topic_arn):
        access_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "topic-subscription-" + topic_arn,
                    "Effect": "Allow",
                    "Principal": {
                        "AWS": "*"
                    },
                    "Action": "sqs:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {
                        "ArnLike": {
                            "aws:SourceArn": topic_arn
                        }
                    }
                }
            ]
        }
        access_policy = json.dumps(access_policy)
        try:
            self.sqs_client.set_queue_attributes(QueueUrl=queue_url, Attributes={'Policy': access_policy})
        except Exception as err:
            print("Failed setting sqs queue policy: ", err)
            raise err

    def subscribe_to_sns_topic(self, topic_arn, queue_arn):
        print("Subscribing queue to sns topic")
        try:
            response = self.sns_client.subscribe(TopicArn=topic_arn,
                                                 Protocol='sqs',
                                                 Endpoint=queue_arn,
                                                 ReturnSubscriptionArn=True)
            print("Successfully subscribed to sns topic, subscription arn is :", response['SubscriptionArn'])
            return response['SubscriptionArn']
        except Exception as err:
            print("Failed subscribing to sns: ", err)
            raise err

    def unsubcribe_from_sns(self, subscription_arn):
        try:
            self.sns_client.unsubscribe(SubscriptionArn=subscription_arn)
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Failed unsubscribing from sns: ", err)

    def receive_message(self, queue_url):
        try:
            response = self.sqs_client.receive_message(QueueUrl=queue_url,
                                                       WaitTimeSeconds=20)
            if 'Messages' in response and response['Messages']:
                response = response['Messages'][0].get('Body', '{}')
                response = json.loads(response)
                return response
            return None
        except Exception as err:
            print("Error while receiving message: ", err)
            raise err

    def delete_sqs_queue(self, queue_url):
        print("\nDeleting sqs queue")
        try:
            self.sqs_client.delete_queue(QueueUrl=queue_url)
            print("Successfully deleted queue")
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Failed deleting queue: ", err)

    def create_role(self, role_name, stage, region):
        print("\nCreating IAM role for scheduled query")
        assumed_role_policy_document = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Effect': 'Allow',
                    'Principal': {
                        "Service": self.get_service_name(stage, region)
                    },
                    'Action': 'sts:AssumeRole'
                }
            ]
        }
        assumed_role_policy_document = json.dumps(assumed_role_policy_document)
        try:
            response = self.iam_client.create_role(Path='/',
                                                   RoleName=role_name,
                                                   AssumeRolePolicyDocument=assumed_role_policy_document
                                                   )
            print("Successfully created IAM role for accessing scheduled query: ", response['Role']['Arn'])
            return response['Role']['Arn']
        except self.iam_client.exceptions.EntityAlreadyExistsException:
            print("Role already exists")
            return f"arn:aws:iam::{self.account_id}:role/{role_name}"
        except Exception as err:
            print("Failed creating role: ", err)
            raise err

    def delete_role(self, role_arn):
        print("Deleting role: ", role_arn)
        try:
            self.iam_client.delete_role(RoleName=role_arn)
            print("Successfully deleted role: ", role_arn)
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Failed deleting role: ", err)

    def create_policy(self, policy_name):
        print("Creating policy for Scheduled Query access")
        policy_document = {
            'Version': '2012-10-17',
            'Statement': [
                {
                    'Action': [
                        'kms:Decrypt',
                        'sns:Publish',
                        'timestream:describeEndpoints',
                        'timestream:Select',
                        'timestream:SelectValues',
                        'timestream:WriteRecords',
                        's3:GetObject',
                        's3:List*',
                        's3:Put*',
                    ],
                    'Resource': '*',
                    'Effect': 'Allow'
                }
            ]
        }
        policy_document = json.dumps(policy_document)
        try:
            response = self.iam_client.create_policy(PolicyName=policy_name,
                                                     PolicyDocument=policy_document)
            print("Successfully created policy: ", response['Policy']['Arn'])
            return response['Policy']['Arn']
        except self.iam_client.exceptions.EntityAlreadyExistsException as e:
            print("Policy already exists")
            return f"arn:aws:iam::{self.account_id}:policy/{policy_name}"
        except Exception as err:
            print("Failed creating policy: ", err)
            raise err

    def delete_policy(self, policy_arn):
        print("Deleting policy")
        try:
            self.iam_client.delete_policy(PolicyArn=policy_arn)
            print("Successfully deleted policy: ", policy_arn)
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Failed deleting policy: ", err)

    def attach_policy_to_role(self, role_name, policy_arn):
        print("Attaching policy to role: ", role_name)
        try:
            self.iam_client.attach_role_policy(RoleName=role_name,
                                               PolicyArn=policy_arn)
            print("Successfully attached policy to role")
        except Exception as err:
            print("Failed attaching policy to role: ", err)
            raise err

    def detach_policy_from_role(self, role_name, policy_arn):
        print("\nDetaching policy from role: ", role_name)
        try:
            self.iam_client.detach_role_policy(RoleName=role_name,
                                               PolicyArn=policy_arn)
            print("Successfully detached policy from role")
        except Exception as err:
            # Not raising an exception here as we want other cleanup to continue
            print("Failed detaching policy from role: ", err)

    def get_service_name(self, stage, region):
        if stage == "prod":
            return "timestream.{}.amazonaws.com".format(region)
        else:
            return "{}.{}.timestream.aws.internal".format(region, stage)

    def create_s3_bucket(self, bucket_name):
        print("\nCreating S3 bucket")
        try:
            if self.region == 'us-east-1':
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                self.s3_client.create_bucket(Bucket=bucket_name,
                                             CreateBucketConfiguration={'LocationConstraint': self.region})
            print("Successfully created S3 bucket: ", bucket_name)
            return bucket_name
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
