package com.amazonaws.sample.timestream.multithreaded;

import com.google.common.base.Preconditions;
import lombok.*;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.timestreamwrite.TimestreamWriteClient;
import software.amazon.awssdk.services.timestreamwrite.model.RetentionProperties;

import java.time.Duration;

@Builder()
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
public class TimestreamWriterConfig {
    private final int queueSize;
    private final int threadPoolSize;

    // Specify how long a given record should be retried inserting,
    // over and over again, in addition to the SDK retry policy.
    // Set 0 to just use SDK retry.
    // You may want to set it to higher values, when processing a Kinesis stream - to not loose records.
    private final long maxRetryDurationMs;

    private final TimestreamWriteClient writeClient;
    private final TimestreamResourceCreationConfig createTableIfNotExists;

    @RequiredArgsConstructor
    @Getter
    public static class TimestreamResourceCreationConfig {
        @NonNull
        private final RetentionProperties retentionProperties;
    }

    // From: https://docs.aws.amazon.com/timestream/latest/developerguide/code-samples.write-client.html
    public static TimestreamWriteClient defaultRecommendedWriteClient(final String region){
        ApacheHttpClient.Builder httpClientBuilder =
                ApacheHttpClient.builder();
        httpClientBuilder.maxConnections(5000);

        RetryPolicy.Builder retryPolicy =
                RetryPolicy.builder();
        retryPolicy.numRetries(10);

        ClientOverrideConfiguration.Builder overrideConfig =
                ClientOverrideConfiguration.builder();
        overrideConfig.apiCallAttemptTimeout(Duration.ofSeconds(20));
        overrideConfig.retryPolicy(retryPolicy.build());

        return TimestreamWriteClient.builder()
                .httpClientBuilder(httpClientBuilder)
                .overrideConfiguration(overrideConfig.build())
                .region(Region.of(region))
                .build();
    }

    public void validate() {
        Preconditions.checkArgument(maxRetryDurationMs >= 0, "Max retry duration (%s) must be >= %s", maxRetryDurationMs, 0);
        Preconditions.checkArgument(queueSize >= 1, "Queue size (%s) must be >= %s", queueSize, 1);
        Preconditions.checkArgument(threadPoolSize >= 1, "Thread pool size (%s) must be >= %s", threadPoolSize, 1);
        if (createTableIfNotExists != null) {
            Preconditions.checkArgument(createTableIfNotExists
                    .retentionProperties.memoryStoreRetentionPeriodInHours() >= 1,
                    "Memory Retention Period (%s) must be >= %s",
                    createTableIfNotExists.retentionProperties.memoryStoreRetentionPeriodInHours(), 1);
            Preconditions.checkArgument(createTableIfNotExists
                            .retentionProperties.magneticStoreRetentionPeriodInDays() >= 1,
                    "Magnetic Retention Period (%s) must be >= %s",
                    createTableIfNotExists.retentionProperties.magneticStoreRetentionPeriodInDays(), 1);
        }
    }
}
