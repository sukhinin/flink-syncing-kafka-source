package com.github.sukhinin.flink.connectors.kafka.source.records;

import com.github.sukhinin.flink.connectors.kafka.source.utils.KafkaUtils;
import com.github.sukhinin.flink.connectors.kafka.source.utils.MetricUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class RecordsFetcher {

    private static final long HANDOVER_OFFER_TIMEOUT_MILLIS = 1000L;

    private static final long HANDOVER_POLL_TIMEOUT_MILLIS = 1000L;

    private static final long CONSUMER_POLL_TIMEOUT_MILLIS = 1000L;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private final SynchronousQueue<ConsumerRecords<byte[], byte[]>> handover = new SynchronousQueue<>();

    private final AtomicReference<Set<TopicPartition>> pauseRequests = new AtomicReference<>();

    private final Thread consumerThread = new Thread(this::runConsumerLoop);

    private final PartitionAssigner partitionAssigner;

    private final String topic;

    private final Consumer<byte[], byte[]> consumer;

    public RecordsFetcher(String topic, RecordsConsumerFactory consumerFactory, PartitionAssigner partitionAssigner) {
        this.topic = topic;
        this.consumer = consumerFactory.create();
        this.partitionAssigner = partitionAssigner;
        discoverAndAssignConsumerPartitions();
    }

    public void registerMetrics(MetricGroup metricGroup) {
        MetricUtils.register(consumer, metricGroup);
    }

    private void discoverAndAssignConsumerPartitions() {
        List<TopicPartition> discoveredPartitions = KafkaUtils.partitionsFor(consumer, topic);
        if (discoveredPartitions.isEmpty()) {
            throw new IllegalStateException("No partitions found for topic " + topic);
        }
        List<TopicPartition> assignedPartitions = partitionAssigner.assign(discoveredPartitions);
        consumer.assign(assignedPartitions);
    }

    public Set<TopicPartition> getAssignedPartitions() {
        return consumer.assignment();
    }

    public void seek(Map<TopicPartition, Long> partitionOffsets) {
        for (TopicPartition partition : consumer.assignment()) {
            Long offset = partitionOffsets.get(partition);
            if (offset != null && offset > 0) {
                consumer.seek(partition, offset);
            }
        }
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            consumerThread.start();
        }
    }

    public void stop() {
        try {
            consumerThread.join();
        } catch (InterruptedException e) {
            // Do nothing
        } finally {
            consumer.close();
        }
    }

    public ConsumerRecords<byte[], byte[]> poll() throws InterruptedException {
        checkAndThrowOnPendingError();
        while (running.get()) {
            ConsumerRecords<byte[], byte[]> records = handover.poll(HANDOVER_POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
            if (records != null) {
                return records;
            }
        }
        return ConsumerRecords.empty();
    }

    public void pause(Set<TopicPartition> partitions) {
        pauseRequests.set(new HashSet<>(partitions));
    }

    private void runConsumerLoop() {
        Duration consumerPollTimeout = Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MILLIS);
        try {
            while (running.get()) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(consumerPollTimeout);
                if (!records.isEmpty()) {
                    processRecordsBatch(records);
                }
                processPartitionPauseRequests();
            }
        } catch (Throwable t) {
            error.compareAndSet(null, t);
        }
    }

    private void processRecordsBatch(ConsumerRecords<byte[], byte[]> records) throws InterruptedException {
        while (running.get()) {
            if (handover.offer(records, HANDOVER_OFFER_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)) {
                break;
            }
        }
    }

    private void processPartitionPauseRequests() {
        Set<TopicPartition> shouldBePaused = pauseRequests.getAndSet(null);
        if (shouldBePaused != null) {
            // Pause assigned partitions specified in shouldBePaused set
            Set<TopicPartition> partitionsToPause = new HashSet<>(shouldBePaused);
            partitionsToPause.retainAll(consumer.assignment());
            consumer.pause(partitionsToPause);
            // Resume already paused partitions not specified in shouldBePaused set
            Set<TopicPartition> partitionsToResume = new HashSet<>(consumer.paused());
            partitionsToResume.removeAll(shouldBePaused);
            consumer.resume(partitionsToResume);
        }
    }

    private void checkAndThrowOnPendingError() {
        Throwable throwable = error.get();
        if (throwable != null) {
            throw new RuntimeException("Error fetching records", throwable);
        }
    }
}
