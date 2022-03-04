/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.amazonaws.samples.kinesis2timestream;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import com.amazonaws.samples.kinesis2timestream.kinesis.RoundRobinKinesisShardAssigner;
import com.amazonaws.samples.kinesis2timestream.model.MyHostBase;
import com.amazonaws.samples.kinesis2timestream.model.TimestreamRecordConverter;
import com.amazonaws.samples.kinesis2timestream.utils.ParameterToolUtils;
import com.amazonaws.samples.kinesis2timestream.model.TimestreamRecordDeserializer;
import com.amazonaws.samples.connectors.timestream.TimestreamSinkConfig;
import com.amazonaws.samples.connectors.timestream.TimestreamSink;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.timestreamwrite.model.Record;
import software.amazon.awssdk.services.timestreamwrite.model.WriteRecordsRequest;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);

	// Currently Timestream supports max. 100 records in single write request. Do not increase this value.
	private static final int MAX_TIMESTREAM_RECORDS_IN_WRITERECORDREQUEST = 100;
	private static final int MAX_CONCURRENT_WRITES_TO_TIMESTREAM = 1000;

	private static final String DEFAULT_STREAM_NAME = "TimestreamTestStream";
	private static final String DEFAULT_REGION_NAME = "us-east-1";

	public static DataStream<MyHostBase> createKinesisSource(StreamExecutionEnvironment env, ParameterTool parameter) throws Exception {

		//set Kinesis consumer properties
		Properties kinesisConsumerConfig = new Properties();
		//set the region the Kinesis stream is located in
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_REGION,
				parameter.get("Region", DEFAULT_REGION_NAME));
		//obtain credentials through the DefaultCredentialsProviderChain, which includes the instance metadata
		kinesisConsumerConfig.setProperty(AWSConfigConstants.AWS_CREDENTIALS_PROVIDER, "AUTO");

		String adaptiveReadSettingStr = parameter.get("SHARD_USE_ADAPTIVE_READS", "false");

		if(adaptiveReadSettingStr.equals("true")) {
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_USE_ADAPTIVE_READS, "true");
		} else {
			//poll new events from the Kinesis stream once every second
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS,
					parameter.get("SHARD_GETRECORDS_INTERVAL_MILLIS", "1000"));
			// max records to get in shot
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
					parameter.get("SHARD_GETRECORDS_MAX", "10000"));
		}

		//create Kinesis source
		FlinkKinesisConsumer<MyHostBase> flinkKinesisConsumer = new FlinkKinesisConsumer<>(
				//read events from the Kinesis stream passed in as a parameter
				parameter.get("InputStreamName", DEFAULT_STREAM_NAME),
				//deserialize events with EventSchema
				new TimestreamRecordDeserializer(),
				//using the previously defined properties
				kinesisConsumerConfig
		);
		flinkKinesisConsumer.setShardAssigner(new RoundRobinKinesisShardAssigner());

		return env
				.addSource(flinkKinesisConsumer)
				.name("KinesisSource");
	}

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<MyHostBase> mappedInput = createKinesisSource(env, parameter);

		String region = parameter.get("Region", "us-east-1");
		String databaseName = parameter.get("TimestreamDbName", "kdaflink");
		String tableName = parameter.get("TimestreamTableName", "kinesisdata");
		long memoryStoreTTLHours = Long.parseLong(parameter.get("MemoryStoreTTLHours", "24"));
		long magneticStoreTTLDays = Long.parseLong(parameter.get("MagneticStoreTTLDays", "7"));

		// EndpointOverride is optional. Learn more here: https://docs.aws.amazon.com/timestream/latest/developerguide/architecture.html#cells
		String endpointOverride = parameter.get("EndpointOverride", "");
		if (endpointOverride.isEmpty()) {
			endpointOverride = null;
		}

		TimestreamInitializer timestreamInitializer = new TimestreamInitializer(region, endpointOverride);
		timestreamInitializer.createDatabase(databaseName);
		timestreamInitializer.createTable(databaseName, tableName, memoryStoreTTLHours, magneticStoreTTLDays);

		TimestreamSink<MyHostBase> sink = new TimestreamSink<>(
				(recordObject, context) -> {
					return TimestreamRecordConverter.convert(recordObject);
				},
				(List<Record> records) -> {
					LOG.debug("Preparing WriteRecordsRequest with {} records", records.size());
					return WriteRecordsRequest.builder()
							.databaseName(databaseName)
							.tableName(tableName)
							.records(records)
							.build();
				},
				TimestreamSinkConfig.builder()
						.maxBatchSize(MAX_TIMESTREAM_RECORDS_IN_WRITERECORDREQUEST)
						.maxBufferedRequests(100 * MAX_TIMESTREAM_RECORDS_IN_WRITERECORDREQUEST)
						.maxInFlightRequests(MAX_CONCURRENT_WRITES_TO_TIMESTREAM)
						.maxTimeInBufferMS(15000)
						.emitSinkMetricsToCloudWatch(true)
						.writeClientConfig(TimestreamSinkConfig.WriteClientConfig.builder()
								.maxConcurrency(MAX_CONCURRENT_WRITES_TO_TIMESTREAM)
								.maxErrorRetry(10)
								.region(region)
								.requestTimeout(Duration.ofSeconds(20))
								.endpointOverride(endpointOverride)
								.build())
						.failureHandlerConfig(TimestreamSinkConfig.FailureHandlerConfig.builder()
								.failProcessingOnErrorDefault(true)
								.failProcessingOnRejectedRecordsException(true)
								.printFailedRequests(false)
								.build())
						.build()
		);
		mappedInput
				.sinkTo(sink)
				.disableChaining();
		env.execute("Flink Streaming Java API Skeleton");
	}
}
