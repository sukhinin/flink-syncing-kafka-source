package com.github.sukhinin.flink.connectors.kafka.source.watermarks;

import org.apache.kafka.clients.consumer.Consumer;

import java.io.Serializable;

public interface WatermarksConsumerFactory extends Serializable {
    Consumer<String, Long> create();
}
