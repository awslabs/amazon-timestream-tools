#!/bin/bash
set -eo pipefail

orig_dir="$PWD"
cd ./../../
mvn package
cd $orig_dir

ARTIFACT_BUCKET=$(cat test-run-bucket-name.txt)
aws cloudformation package --template-file cloudformation-lambda.yaml --s3-bucket $ARTIFACT_BUCKET --output-template-file out-cloudformation-lambda.yml

aws cloudformation deploy --template-file out-cloudformation-lambda.yml --stack-name sample-lambda-from-s3-csv-ingestion --parameter-overrides S3BucketToAccess=REPLACE_ME_BUCKET_NAME TargetTimestreamDatabaseName=REPLACE_ME_TIMESTREAM_DATABASE_NAME TargetTimestreamTableName=REPLACE_ME_TIMESTREAM_TABLE_NAME --capabilities CAPABILITY_IAM