package com.github.sukhinin.flink.connectors.kafka.source.records;

import org.apache.kafka.common.TopicPartition;

import java.util.List;

public interface PartitionAssigner {
    List<TopicPartition> assign(List<TopicPartition> partitions);
}
