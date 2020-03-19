package com.github.sukhinin.flink.connectors.kafka.source.records;

import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;

public class RoundRobinPartitionAssigner implements PartitionAssigner {

    private final int rank;

    private final int total;

    public RoundRobinPartitionAssigner(int rank, int total) {
        this.rank = rank;
        this.total = total;
    }

    @Override
    public List<TopicPartition> assign(List<TopicPartition> partitions) {
        List<TopicPartition> assigned = new ArrayList<>();
        for (int i = rank; i < partitions.size(); i += total) {
            assigned.add(partitions.get(i));
        }
        return assigned;
    }
}
