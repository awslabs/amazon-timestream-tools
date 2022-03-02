package com.amazonaws.samples.kinesis2timestream.kinesis;

import java.io.Serializable;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.flink.streaming.connectors.kinesis.KinesisShardAssigner;
import org.apache.flink.streaming.connectors.kinesis.model.StreamShardHandle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Assign shards in Round Robin fashion
 * The reason we need Round Robin is for uniform distribution of shards across all operators
 * During experiment we found a minority of operators (KPUs/parallelism) weren't even assigned any traffic
 * After changing it to Round Robin, the problem is gone, and we see roughly same traffic across most operators
 */
public class RoundRobinKinesisShardAssigner implements KinesisShardAssigner, Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(
            RoundRobinKinesisShardAssigner.class);

    @Override
    public int assign(StreamShardHandle shard, int numParallelSubtasks) {
        try {
            return Integer.parseInt(shard.getShard().getShardId().substring(8)) % numParallelSubtasks;
        } catch (Exception ex) {
            var taskNumber = ThreadLocalRandom.current().nextInt(0, numParallelSubtasks);
            LOG.error("Failed to assign task for shard={}; fallback to task-{}.", shard, taskNumber, ex);
            return taskNumber;
        }
    }
}
