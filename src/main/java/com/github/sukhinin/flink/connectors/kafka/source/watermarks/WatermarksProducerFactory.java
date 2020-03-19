package com.github.sukhinin.flink.connectors.kafka.source.watermarks;

import org.apache.kafka.clients.producer.Producer;

import java.io.Serializable;

public interface WatermarksProducerFactory extends Serializable {
    Producer<String, Long> create();
}
