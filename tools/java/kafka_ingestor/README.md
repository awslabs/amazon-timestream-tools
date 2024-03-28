# Publishing data to a Kafka topic
A script to generate a continuous stream of records and publish to a Kafka topic. 
We use [Apache JMeter](https://jmeter.apache.org/) test plan to publish thousands of messages read from the sample CSV file [purchase_history.csv](purchase_history.csv) to the Kafka topic configured as shown in the following diagram.

![DataFlow-jMeterToKafka.png](images%2FDataFlow-jMeterToKafka.png)

Additionally, you can check out the blog: [Real-time serverless data ingestion from your Kafka clusters into Amazon Timestream using Kafka Connect](https://aws.amazon.com/blogs/database/real-time-serverless-data-ingestion-from-your-kafka-clusters-into-amazon-timestream-using-kafka-connect/) to know more about setting up an end-to-end pipeline starting from a Kafka producer client machine that uses the jMeter test plan to publish messages to a Kafka topic to verifying the ingested records in an Amazon Timestream for LiveAnalytics table.  

--- 
## Dependencies
- Java (Tested with version 11)
- An Amazon MSK Cluster (Tested with version 2.8.1)
- kafka-clients.jar (Tested with version 2.8.1) 
- aws-msk-iam-auth.jar (Tested with version 1.1.9)
- Apache jMeter (Tested with version 5.6.2)

--- 
## Prerequisites

1. An Amazon MSK Cluster with IAM authentication enabled. Follow the instructions to [create an Amazon MSK cluster](https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster.html)
2. Create an Amazon IAM role following the instructions described in [create an IAM role](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-iam-role.html). This role will be assumed by a Kafka client machine that you would create in the next step to create a topic on the cluster and then to publish data to that topic.
3. Create a Kafka producer client machine following the instructions described in [create a client machine](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-machine.html).
4. Create a Kafka topic named _'purchase-history'_ following the instructions described in [create a topic](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html)

----
## How to use it

Connect to the Kafka producer client machine and execute the below steps as detailed.
following the instructions described in [Connect to your Linux instance using an SSH client](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/connect-linux-inst-ssh.html#connect-linux-inst-sshClient) 

### I. Configure Kafka Producer client
1. Download and extract the jmeter application in the Kafka producer machine:
````shell
cd ~
wget https://dlcdn.apache.org//jmeter/binaries/apache-jmeter-5.6.2.tgz
tar -xf apache-jmeter-5.6.2.tgz
````

2. Verify that jmeter is installed:
````shell
   cd apache-jmeter-5.6.2/bin/
   ./jmeter -v
````
The output should look like the following screenshot.

![ApachejMeterVersion.png](images%2FApachejMeterVersion.png)

3. Download the kafka-clients.jar file, which contains classes for working with Kafka, and place it in the jmeter /lib/ext directory:
```shell
cd ~
cd apache-jmeter-5.6.2/lib/ext 
wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.8.1/kafka-clients-2.8.1.jar
```

4. Download the msk-iam-auth-all.jar file, which contains classes for working with Amazon MSK that have been configured with IAM authentication, and place it in the jmeter /lib/ext directory:
```shell
cd ~ 
cd apache-jmeter-5.6.2/lib/ext 
wget https://github.com/aws/aws-msk-iam-auth/releases/download/v1.1.9/aws-msk-iam-auth-1.1.9-all.jar
````
5. Create a directory in the jMeter _/examples_ directory:
```shell
cd ~ 
cd apache-jmeter-5.6.2/bin/examples 
mkdir timeseries 
cd timeseries
````
6. Transfer both the files [purchase_history.csv](purchase_history.csv) and [Purchase_History_Publishing.jmx](Purchase_History_Publishing.jmx) to the created directory _/apache-jmeter-5.6.2/bin/examples/timeseries_ following the instructions described in [Transfer files to Linux instances using an SCP client](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/connect-linux-inst-ssh.html#linux-file-transfer-scp) and check if the files are transferred:
```shell
cd ~ 
ls apache-jmeter-5.6.2/bin/examples/timeseries
````
7. Set one of the broker endpoints in the environment variable by running the following command after replacing BootstrapServerString; follow the instructions described in [getting the bootstrap brokers](https://docs.aws.amazon.com/msk/latest/developerguide/msk-get-bootstrap-brokers.html#get-bootstrap-console) for the broker endpoint.
```shell   
export BOOTSTRAP_SERVER=<BootstrapServerString>
```
Note: You will get three endpoints for each of the brokers, and you only need one broker endpoint for this step, as shown in the following sample command. Using the MSK bootstrap server connection string with more than one will result into connection error.
```shell
export BOOTSTRAP_SERVER=b-3.mskcluster123456.9470qv.c4.kafka.eu-west-1.amazonaws.com:9098
```
8. Set the created topic name (_purchase-history_) in the environment variable
```shell   
export TOPIC=purchase-history
```
 
### II. Publish Messages

1. Verify if the environment variables are set:
```shell   
echo $BOOTSTRAP_SERVER 
echo $TOPIC
```
2. Run the jMeter test plan:
```shell   
cd ~ 
cd apache-jmeter-5.6.2/bin 
./jmeter.sh -n -t examples/timeseries/Purchase_History_Publishing.jmx -Jtopic=$TOPIC -Jbootstrap_server=$BOOTSTRAP_SERVER
```
3. The jMeter test plan reads the records from the sample csv file and then publishes them as messages in the Kafka topic. While it gets executed, a summary is printed on the console, similar to the following:
```shell   
[ec2-user@ip-10-0-0-165 bin]$ ./jmeter.sh -n -t examples/timeseries/Purchase_History_Publishing.jmx -Jtopic=$TOPIC -Jbootstrap_server=$BOOTSTRAP_SERVER
WARN StatusConsoleListener The use of package scanning to locate plugins is deprecated and will be removed in a future release
Creating summariser <summary>
Created the tree successfully using examples/timeseries/Purchase_History_Publishing.jmx
Starting standalone test @ 2023 Dec 29 06:54:31 UTC (1703832871222)
Waiting for possible Shutdown/StopTestNow/HeapDump/ThreadDump message on port 4445
Warning: Nashorn engine is planned to be removed from a future JDK release
summary + 196 in 00:00:25 = 7.7/s Avg: 121 Min: 78 Max: 1702 Err: 0 (0.00%) Active: 1 Started: 1 Finished: 0
summary + 351 in 00:00:30 = 11.7/s Avg: 84 Min: 64 Max: 152 Err: 0 (0.00%) Active: 1 Started: 1 Finished: 0
summary = 547 in 00:00:56 = 9.9/s Avg: 98 Min: 64 Max: 1702 Err: 0 (0.00%)
summary + 389 in 00:00:30 = 13.0/s Avg: 76 Min: 63 Max: 177 Err: 0 (0.00%) Active: 1 Started: 1 Finished: 0
summary = 936 in 00:01:26 = 10.9/s Avg: 89 Min: 63 Max: 1702 Err: 0 (0.00%)
summary + 407 in 00:00:30 = 13.5/s Avg: 73 Min: 61 Max: 146 Err: 0 (0.00%) Active: 1 Started: 1 Finished: 0
summary = 1343 in 00:01:56 = 11.6/s Avg: 84 Min: 61 Max: 1702 Err: 0 (0.00%)
summary + 410 in 00:00:30 = 13.7/s Avg: 72 Min: 60 Max: 139 Err: 0 (0.00%) Active: 1 Started: 1 Finished: 0
summary = 1753 in 00:02:26 = 12.0/s Avg: 81 Min: 60 Max: 1702 Err: 0 (0.00%)
summary + 423 in 00:00:30 = 14.1/s Avg: 70 Min: 60 Max: 144 Err: 0 (0.00%) Active: 1 Started: 1 Finished: 0
summary = 2176 in 00:02:55 = 12.4/s Avg: 79 Min: 60 Max: 1702 Err: 0 (0.00%)
summary + 428 in 00:00:30 = 14.3/s Avg: 69 Min: 60 Max: 122 Err: 0 (0.00%) Active: 1 Started: 1 Finished: 0
summary = 2604 in 00:03:26 = 12.7/s Avg: 77 Min: 60 Max: 1702 Err: 0 (0.00%)
summary + 423 in 00:00:30 = 14.1/s Avg: 70 Min: 60 Max: 160 Err: 0 (0.00%) Active: 1 Started: 1 Finished: 0
summary = 3027 in 00:03:56 = 12.9/s Avg: 76 Min: 60 Max: 1702 Err: 0 (0.00%)
summary + 412 in 00:00:30 = 13.8/s Avg: 72 Min: 60 Max: 184 Err: 0 (0.00%) Active: 1 Started: 1 Finished: 0
summary = 3439 in 00:04:25 = 13.0/s Avg: 76 Min: 60 Max: 1702 Err: 0 (0.00%)
summary + 388 in 00:00:30 = 12.9/s Avg: 77 Min: 61 Max: 155 Err: 0 (0.00%) Active: 1 Started: 1 Finished: 0
summary = 3827 in 00:04:56 = 12.9/s Avg: 76 Min: 60 Max: 1702 Err: 0 (0.00%)
summary + 56 in 00:00:05 = 12.3/s Avg: 80 Min: 62 Max: 187 Err: 0 (0.00%) Active: 0 Started: 1 Finished: 1
summary = 3883 in 00:05:00 = 12.9/s Avg: 76 Min: 60 Max: 1702 Err: 0 (0.00%)
Tidying up ... @ 2023 Dec 29 06:59:34 UTC (1703833174620)
... end of run
[ec2-user@ip-10-0-0-165 bin]$
```
4. Verify if the messages are published to the topic with the following command:
```shell
cd <path-to-your-kafka-installation>/bin
./kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP_SERVER --consumer.config client.properties --topic purchase-history --from-beginning
```
Note:
* Refer  [Produce and consume data](https://docs.aws.amazon.com/msk/latest/developerguide/produce-consume.html) to publish messages to a Kafka Topic / consume messages from a Kafka topic.
* Refer the steps described in [Create a topic](https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html) to know more about the _client.properties_ file  

And you start seeing the messages you published using the jMeter test plan. 