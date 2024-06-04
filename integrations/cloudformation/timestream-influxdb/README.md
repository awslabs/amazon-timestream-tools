# Create Amazon Timestream for InfluxDB instance and all necessary resources in CloudFormation 

## Summary

Timestream for InfluxDB makes it easy for application developers and DevOps teams to run fully managed InfluxDB databases on AWS for real-time time-series applications using open-source APIs. In minutes, you can create an InfluxDB database that handles demanding time-series workloads. With a few simple API calls, you can set up, migrate, operate, and scale an InfluxDB database on AWS with automated software patching, backups, and recovery.

## How to create Timestream for InfluxDB instance in CloudFormation 

This CloudFormation template creates the following resources that are needed to successfully create, connect to, and monitor a Timestream for InfluxDB instance:

#### VPC
* VPC
* Subnet(s)
* InternetGateway
* RouteTable
* SecurityGroup

#### S3
* Bucket

#### Timestream
* InfluxDBInstance

The `timestream_influxdb.yaml` template dynamically alters the network resources based on the configurations provided for the Timestream for InfluxDB instance such as `publiclyAccessible` and `deploymentType`. For users who want a very simple setup, the `timestream_influxdb_simple.yaml` template offers a simple setup that deploys a multi-AZ and publicly accessible instance.

## Content
- ``timestream_influxdb.yaml`` - Amazon CloudFormation Template that allows complex configuration
- ``timestream_influxdb_simple.yaml`` - Amazon CloudFormation Template that uses default values when possible



