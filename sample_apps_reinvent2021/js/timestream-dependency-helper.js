const constants = require('./constants');

var s3ErrorReportBucketName = null;

async function createSnsTopic() {
    console.log("Creating SNS Topic");
    const params = {
        Name: constants.TOPIC_NAME,
        Attributes: {
            'FifoTopic': 'true',
            'ContentBasedDeduplication': 'false'
        }
    }
    try {
        const data = await snsClient.createTopic(params).promise();
        console.log("Topic ARN is " + data.TopicArn);
        return data.TopicArn;
    } catch (err) {
        console.log(err);
        throw err;
    }  
}

async function deleteSnsTopic(topicArn) {
    console.log("Deleting SNS Topic");
    var deleteTopicPromise = snsClient.deleteTopic({TopicArn: topicArn}).promise();

    await deleteTopicPromise.then(
        function(data) {
          console.log("Topic Deleted");
        }).catch(
          function(err) {
          console.error(err, err.stack);
        });
}

async function createIamRole() {
    console.log("Creating Role");

    const ROLE_POLICY_FORMAT = {
        Version: '2012-10-17',
        Statement: [
          {
            Effect: 'Allow',
            Principal: { 
              Service: "timestream.amazonaws.com"
            },
            Action: 'sts:AssumeRole'
          }
        ]
    };
    var params = {
        AssumeRolePolicyDocument: JSON.stringify(ROLE_POLICY_FORMAT), 
        RoleName: constants.ROLE_NAME
    };

    try {
        const data = await iamClient.createRole(params).promise();
        console.log(data.Role.Arn);
        return data.Role.Arn;
    } catch (err) {
        console.log(err, err.stack);
        throw err;
    } 
}

async function deleteIamRole() {
    console.log("Deleting Role");
    var params = {
        RoleName: constants.ROLE_NAME
    };

    try {
        await iamClient.deleteRole(params).promise();
        console.log("Role: " + constants.ROLE_NAME + " deleted");
    } catch (err) {
        console.log(err, err.stack);
    } 
}

async function createSqsQueue() {
    console.log("Creating SQS Queue");
    var params = {
        QueueName: constants.QUEUE_NAME,
        Attributes: {
            'FifoQueue': 'true',
            'ContentBasedDeduplication': 'false'
        }
    };

    try {
        const data = await sqsClient.createQueue(params).promise();
        console.log("Success", data.QueueUrl);
        return data.QueueUrl;
    } catch (err) {
        console.log("Error", err);
        throw err;
    } 
}

async function deleteSqsQueue(queueUrl) {
    console.log("Deleting SQS Topic");

    var params = {
        QueueUrl: queueUrl
    };
      
    var deleteTopicPromise = sqsClient.deleteQueue(params).promise();

    await deleteTopicPromise.then(
        function(data) {
          console.log("Topic Deleted");
        }).catch(
          function(err) {
          console.error(err, err.stack);
        });
}

async function getQueueArn(queueUrl) {
    console.log("Getting Queue Arn");

    var params = {
        QueueUrl: queueUrl,
        AttributeNames: [
            'QueueArn'
        ]
    };

    try {
        const data = await sqsClient.getQueueAttributes(params).promise();
        console.log("queue_arn: " + data.Attributes.QueueArn);
        return data.Attributes.QueueArn;
    } catch (err) {
        console.log("Error", err);
        throw err;
    } 
}

async function subscribeToSnsTopic(topicArn, queueArn) {
    console.log("Subscribing to Sns Topic");
    var params = {
        Protocol: 'sqs', 
        TopicArn: topicArn, 
        Endpoint: queueArn
    };

    try {
        const data = await snsClient.subscribe(params).promise();
        console.log("Subscription ARN: " + data.SubscriptionArn);
        return data.SubscriptionArn;
    } catch (err) {
        console.log("Error", err);
        throw err;
    } 
}

async function unsubscribeFromSnsTopic (subscriptionArn) {
    console.log("UnSubscribing to Sns Topic");
    var params = {
        SubscriptionArn: subscriptionArn
    }

    try {
        const data = await snsClient.unsubscribe(params).promise();
        console.log("Successful Unsubscribed");
    } catch (err) {
        console.log("Error", err);
    } 
}

async function setSqsAccessPolicy (queueUrl, topicArn, queueArn) {
    console.log("Setting SQS access policy");

    const attributes = {
        Version: '2008-10-17',
        Statement: [
          {
            Sid: `topic-subscription-${topicArn}`,
            Effect: 'Allow',
            Principal: {
              AWS: '*'
            },
            Action: 'SQS:SendMessage',
            Resource: queueArn,
            Condition: {
              ArnEquals: {
                'aws:SourceArn': topicArn
              }
            }
          }
        ]
      };

    var params = {
        QueueUrl: queueUrl,
        Attributes: {
            Policy: JSON.stringify(attributes)
        }
    }
    
    try {
        const data = await sqsClient.setQueueAttributes(params).promise();
        console.log("SQS policy successful set");
    } catch (err) {
        console.log("Error", err);
        throw err;
    } 
}

async function createIAMPolicy() {
    console.log("Creating IAM Policy");

    const POLICY_DOCUMENT = {
        Version: '2012-10-17',
        Statement: [
          {
            Action: [
              'kms:Decrypt',
              'sns:Publish',
              'timestream:describeEndpoints',
              'timestream:Select',
              'timestream:SelectValues',
              'timestream:WriteRecords',
              's3:GetObject',
              's3:List*',
              's3:Put*'
            ],
            Resource: '*',
            Effect: 'Allow'
          }
        ]
    }

    var params = {
        PolicyDocument: JSON.stringify(POLICY_DOCUMENT),
        PolicyName: constants.POLICY_NAME,
    };

    try {
        const data = await iamClient.createPolicy(params).promise();
        console.log("IAM policy Arn: " + data.Policy.Arn);
        return data.Policy.Arn;
    } catch (err) {
        console.log("IAM policy creation failed: ", err);
        throw err;
    } 
}

async function deleteIAMPolicy(policyArn) {
    console.log("Deleting IAM Policy");
    var params = {
        PolicyArn: policyArn
    }

    try {
        const data = await iamClient.deletePolicy(params).promise();
        console.log("Successfully deleted the policy");
    } catch (err) {
        console.log("IAM policy deletion failed: ", err);
    } 
}

async function attachIAMRolePolicy(policyArn) {
    console.log("Attaching IAM Role Policy");
    var paramsRoleList = {
        RoleName: constants.ROLE_NAME
      };
     
      await iamClient.listAttachedRolePolicies(paramsRoleList, function(err, data) {
        if (err) {
          console.log("Error", err);
        } else {
          var myRolePolicies = data.AttachedPolicies;
          myRolePolicies.forEach(function (val, index, array) {
            if (myRolePolicies[index].PolicyName === policyArn) {
              console.log(constants.ROLE_NAME + "is already attached to this role.")
              process.exit();
            }
          });
          var params = {
            PolicyArn: policyArn,
            RoleName: constants.ROLE_NAME
          };
          iamClient.attachRolePolicy(params, function(err, data) {
            if (err) {
              console.log("Unable to attach policy to role", err);
              throw err;
            } else {
              console.log("Role attached successfully");
            }
          });
        }
      });
}

async function detachPolicy(policyArn) {
    console.log("Detaching IAM Role Policy");
    var params = {
        RoleName: constants.ROLE_NAME,
        PolicyArn: policyArn
    };

    try {
        const data = await iamClient.detachRolePolicy(params).promise();
        console.log("Successfully detached the policy");
    } catch (err) {
        console.log("IAM policy detach failed: ", err);
    } 
}

async function createS3Bucket(bucketName) {
    console.log("Creating S3 bucket for error reporting");
    const params = {
        Bucket: bucketName
    }

    try {
        const data = await s3Client.createBucket(params).promise(); 
        console.log("Bucket Created Successfully", data.Location);
        s3ErrorReportBucketName = bucketName;
        return bucketName;
    } catch (err) {
        if (err.statusCode === 409) {
            console.log("Bucket has been created already");
            s3ErrorReportBucketName = bucketName;
            return bucketName;
        }
        else {
            console.log("S3 bucket creation failed" + err)
            throw err;
        }
    }
}

async function deleteS3Bucket() {
    console.log("Deleting S3 Bucket");
    if (s3ErrorReportBucketName == null) {
        console.log("No S3 Bucket needs to be deleted");
        return;
    }
    const params = {
        Bucket: s3ErrorReportBucketName
    }
    try {
        const data = await s3Client.deleteBucket(params).promise();
        console.log("Bucket deleted Successfully");
    } catch (err) {
        console.log("Deletion of S3 error report bucket failed: ", err)
    }
}

async function clearBucket() {
    console.log("Clearing Bucket")
    var self = this;
    try {
        const data = await s3Client.listObjects({Bucket: s3ErrorReportBucketName}).promise(); 
        var items = data.Contents;
        for (var i = 0; i < items.length; i += 1) {
            var deleteParams = {Bucket: s3ErrorReportBucketName, Key: items[i].Key};
            await self.deleteObject(deleteParams);
        }
    } catch (err) {
        console.log("error listing bucket objects "+err);
    }
}

async function deleteObject(deleteParams) {
    try {
        const data = await s3Client.deleteObject(deleteParams).promise();
        console.log(deleteParams.Key + " deleted");
    } catch (err) {
        console.log("Object deletion failed");
    }
}

async function receiveMessage(queueUrl) {
    console.log("Receiving messages");
    var params = {
        QueueUrl: queueUrl,
        WaitTimeSeconds: 20
    }
    try {
        const data = await sqsClient.receiveMessage(params).promise();
        var messages = data.Messages;
        console.log(messages);
        if (typeof messages !== 'undefined' && messages.length > 0) {
            var messageBody = messages[0];
            return messageBody;
        }
    } catch (err) {
        console.log("Receiving messages failed: " + err);
        throw err;
    }
}

async function deleteMessage(queueUrl, recepitHandler) {
    console.log("Deleting messages");
    var params = {
        QueueUrl: queueUrl,
        ReceiptHandle: recepitHandler
    }

    try {
        const data = await sqsClient.deleteMessage(params).promise();
        console.log("Message deleted")
    } catch (err) {
        console.log("Deleting messages failed: " + err);
    }
}

async function getS3ErrorReportBucketName() {
    return s3ErrorReportBucketName;
}

module.exports = {createSnsTopic, deleteSnsTopic, createIamRole, deleteIamRole, createSqsQueue, deleteSqsQueue, 
    getQueueArn, subscribeToSnsTopic, unsubscribeFromSnsTopic, setSqsAccessPolicy, createIAMPolicy, deleteIAMPolicy,
    attachIAMRolePolicy, detachPolicy, createS3Bucket, deleteS3Bucket, clearBucket, deleteObject, receiveMessage,
    deleteMessage, getS3ErrorReportBucketName};