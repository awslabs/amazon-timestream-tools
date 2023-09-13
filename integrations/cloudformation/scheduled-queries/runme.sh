#!/bin/sh
aws s3 cp scheduled_query.yaml s3://funknor-s3-upload/cfn/
aws cloudformation delete-stack --stack-name myteststack --region us-east-1
aws cloudformation create-stack \
  --capabilities CAPABILITY_IAM \
  --region us-east-1 \
  --stack-name myteststack \
  --template-url https://funknor-s3-upload.s3.amazonaws.com/cfn/scheduled_query.yaml #\
#  --parameters ParameterKey=Parm1,ParameterValue=test1 ParameterKey=Parm2,ParameterValue=test2