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

package com.amazonaws.services.kinesisanalytics;

import com.amazonaws.services.kinesisanalytics.utils.ParameterToolUtils;
import com.amazonaws.services.kinesisanalytics.operators.TimestreamPointToAverage;
import com.amazonaws.services.timestream.TimestreamPoint;
import com.amazonaws.services.timestream.TimestreamSink;

import main.java.com.amazonaws.services.timestream.TimestreamInitializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;

import com.amazonaws.services.timestream.TimestreamPointSchema;

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

	private static final String DEFAULT_STREAM_NAME = "TimestreamTestStream";
	private static final String DEFAULT_REGION_NAME = "us-east-1";

	public static DataStream<TimestreamPoint> createKinesisSource(StreamExecutionEnvironment env, ParameterTool parameter) throws Exception {

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
			//max records to get in shot
			kinesisConsumerConfig.setProperty(ConsumerConfigConstants.SHARD_GETRECORDS_MAX,
					parameter.get("SHARD_GETRECORDS_MAX", "10000"));
		}

		//create Kinesis source
		DataStream<TimestreamPoint> kinesisStream = env.addSource(new FlinkKinesisConsumer<>(
				//read events from the Kinesis stream passed in as a parameter
				parameter.get("InputStreamName", DEFAULT_STREAM_NAME),
				//deserialize events with TimestreamPointSchema
				new TimestreamPointSchema(),
				//using the previously defined properties
				kinesisConsumerConfig
		)).name("KinesisSource");

		return kinesisStream;
	}

	public static void main(String[] args) throws Exception {
		ParameterTool parameter = ParameterToolUtils.fromArgsAndApplicationProperties(args);

		//set up the streaming execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		//enable event time processing
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000L);
        //get the aggregation window
        int aggregationWindowMinutes = Integer.parseInt(parameter.get("AggregationWindowMinutes", "5"));

		DataStream<TimestreamPoint> input = createKinesisSource(env, parameter)
				.rebalance();

		//define watermark strategy and how to assign timestamps for eventtime processing
		WatermarkStrategy<TimestreamPoint> wmStrategy = WatermarkStrategy
				.<TimestreamPoint>forBoundedOutOfOrderness(Duration.ofSeconds(3))
				//* 1000L as timestamps must be in millis
				.withTimestampAssigner((point, timestamp) -> (point.getTime() * 1000L));

		//perform aggregation on input
		SingleOutputStreamOperator<TimestreamPoint> averages = input
				//filter to only points with numeric measure value types
				.filter(point -> point.getMeasureValueType().toString().equals("DOUBLE") || point.getMeasureValueType().toString().equals("BIGINT"))
				.assignTimestampsAndWatermarks(wmStrategy)
				//group our data by key
				.keyBy(new KeySelector<TimestreamPoint, Integer>() {
					@Override
					public Integer getKey(TimestreamPoint point) throws Exception {
						return Objects.hash(point.getMeasureName(), point.getMeasureValueType(), point.getTimeUnit(), point.getDimensions());
					}
				})
				//the time window that our data will be aggregated by
				.window(TumblingEventTimeWindows.of(Time.minutes(aggregationWindowMinutes)))
				//accomodate late arriving records, which will generate a new aggregated record and update the old row in timestream
				.allowedLateness(Time.minutes(2))
				.apply(new TimestreamPointToAverage());

		String region = parameter.get("Region", "us-east-1").toString();
		String databaseName = parameter.get("TimestreamDbName", "kdaflink").toString();
		String tableName = parameter.get("TimestreamTableName", "kinesisdata1").toString();
		int batchSize = Integer.parseInt(parameter.get("TimestreamIngestBatchSize", "50"));

		TimestreamInitializer timestreamInitializer = new TimestreamInitializer(region);
		timestreamInitializer.createDatabase(databaseName);
		timestreamInitializer.createTable(databaseName, tableName);

		SinkFunction<TimestreamPoint> sink = new TimestreamSink(region, databaseName, tableName, batchSize);
		
		//write the data to timestream
		input.addSink(sink);
		averages.addSink(sink);
		
		LOG.info("Starting to consume events from stream {}", parameter.get("InputStreamName", DEFAULT_STREAM_NAME));
		LOG.info("Aggregation time window of {} minutes", aggregationWindowMinutes);

		// execute program
		env.execute("Flink Streaming Java API Timestream Aggregation");
	}
}