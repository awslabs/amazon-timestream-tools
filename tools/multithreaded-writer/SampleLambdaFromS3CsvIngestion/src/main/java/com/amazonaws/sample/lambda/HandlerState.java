package com.amazonaws.sample.lambda;

import com.amazonaws.sample.timestream.multithreaded.TimestreamWriter;
import com.amazonaws.sample.timestream.multithreaded.TimestreamWriterConfig;
import com.amazonaws.sample.timestream.multithreaded.TimestreamWriterImpl;
import com.amazonaws.sample.timestream.multithreaded.util.LogMetricsPublisher;
import lombok.Getter;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.Duration;

@Getter
public class HandlerState {
    private final S3Client s3 = S3Client.builder().region(Region.of(EnvVariablesHelper.getS3Region())).build();

    final TimestreamWriterConfig writerConfig = TimestreamWriterConfig.builder()
            .queueSize(EnvVariablesHelper.getTimestreamWriterQueueSize())
            .threadPoolSize(EnvVariablesHelper.getTimestreamWriterThreadPoolSize())
            .writeClient(TimestreamWriterConfig.defaultRecommendedWriteClient(EnvVariablesHelper.getTimestreamRegion()))
            .maxRetryDurationMs(EnvVariablesHelper.getMaxTimestreamRecordInsertionRetryDurationMs())
            .build();
    final TimestreamWriter writer = new TimestreamWriterImpl(writerConfig);

    final LogMetricsPublisher logMetricsPublisher = new LogMetricsPublisher(writer, Duration.ofSeconds(5));
}
