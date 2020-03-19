package com.github.sukhinin.flink.connectors.kafka.source.records;

import java.io.Serializable;

public interface RecordsFetcherFactory extends Serializable {
    RecordsFetcher create(PartitionAssigner partitionAssigner);
}
