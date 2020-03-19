package com.github.sukhinin.flink.connectors.kafka.source.records;

import org.apache.kafka.clients.consumer.Consumer;

import java.io.Serializable;

public interface RecordsConsumerFactory extends Serializable {
    Consumer<byte[], byte[]> create();
}
