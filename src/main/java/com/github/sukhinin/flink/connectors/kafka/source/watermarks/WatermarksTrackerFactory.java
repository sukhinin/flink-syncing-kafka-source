package com.github.sukhinin.flink.connectors.kafka.source.watermarks;

import java.io.Serializable;

public interface WatermarksTrackerFactory extends Serializable {
    WatermarksTracker create();
}
