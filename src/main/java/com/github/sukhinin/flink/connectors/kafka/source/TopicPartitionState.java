package com.github.sukhinin.flink.connectors.kafka.source;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;

class TopicPartitionState<T> {

    private final AssignerWithPeriodicWatermarks<T> timestampAssigner;

    private volatile long currentOffset;

    private volatile long currentWatermark;

    private volatile boolean paused;

    public TopicPartitionState(AssignerWithPeriodicWatermarks<T> timestampAssigner) {
        this.timestampAssigner = timestampAssigner;
    }

    public AssignerWithPeriodicWatermarks<T> getTimestampAssigner() {
        return timestampAssigner;
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void setCurrentOffset(long currentOffset) {
        this.currentOffset = currentOffset;
    }

    public long getCurrentWatermark() {
        return currentWatermark;
    }

    public void setCurrentWatermark(long currentWatermark) {
        this.currentWatermark = currentWatermark;
    }

    public boolean isPaused() {
        return paused;
    }

    public int getPaused() {
        return paused ? 1 : 0;
    }

    public void setPaused(boolean paused) {
        this.paused = paused;
    }
}
