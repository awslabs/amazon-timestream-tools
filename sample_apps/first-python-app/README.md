# Build Your First Timestream Enabled Python App

Use this repo to create a setup Amazon Timestream and Amazon Kinesis. Then connect to them both using the available scripts. You are able to stream metrics to Timestream using Kinesis or directly to Timestream. 

- **alarm_sensor.py**: A script that generates random alarm states
- **cleanup.py**: A script that cleans up all resouces created
- **initial_lambda_function.py**: The skeleton of the AWS Lambda Function to process records from Amazon kinesis to Amazon Timestream
- **Lambda-Role-Trust-Policy.json**: The trust policy for the AWS Lambda function
- **lambda_function.py**: The finalized AWS Lambda function
- **setup_kinesis.py**: A script that sets up Amazon Kinesis
- **setup_timestream.py**: A script that sets up Amazon Timestream
- **wave_sensor.py**: A script that generates sensor readings in a wave form