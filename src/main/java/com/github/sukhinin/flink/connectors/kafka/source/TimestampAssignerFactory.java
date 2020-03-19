package com.github.sukhinin.flink.connectors.kafka.source;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;

import java.io.Serializable;

public interface TimestampAssignerFactory<T> extends Serializable {
    AssignerWithPeriodicWatermarks<T> create();
}
