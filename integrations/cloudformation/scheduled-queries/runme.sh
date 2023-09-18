#!/bin/sh
S3BUCKET=$1

if [ -z "$S3BUCKET" ]
then
	echo "Please specify S3 bucket name for uploading cloudformation template"
	echo "usage:"
	echo "sh ./runme.sh <s3 bucket name>"
	exit -1
fi


aws s3 cp scheduled_query.yaml s3://${S3BUCKET}/cfn/
# aws cloudformation delete-stack --stack-name scheduled-query-example --region us-east-1
aws cloudformation create-stack \
  --capabilities CAPABILITY_IAM \
  --region us-east-1 \
  --stack-name scheduled-query-example \
  --template-url https://${S3BUCKET}.s3.amazonaws.com/cfn/scheduled_query.yaml #\
#  --parameters ParameterKey=Parm1,ParameterValue=test1 ParameterKey=Parm2,ParameterValue=test2