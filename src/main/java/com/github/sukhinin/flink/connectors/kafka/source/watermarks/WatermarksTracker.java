package com.github.sukhinin.flink.connectors.kafka.source.watermarks;

import com.github.sukhinin.flink.connectors.kafka.source.utils.KafkaUtils;
import com.github.sukhinin.flink.connectors.kafka.source.utils.MetricUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class WatermarksTracker {

    private static final long CONSUMER_POLL_TIMEOUT_MILLIS = 1000L;

    private final Map<String, Long> currentWatermarksByKey = new HashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final AtomicReference<Throwable> error = new AtomicReference<>();

    private final Thread consumerThread = new Thread(this::runConsumerLoop);

    private final String topic;

    private final String domain;

    private final Consumer<String, Long> consumer;

    private final Producer<String, Long> producer;

    public WatermarksTracker(String topic, String domain, WatermarksConsumerFactory consumerFactory, WatermarksProducerFactory producerFactory) {
        this.topic = topic;
        this.domain = domain;
        this.consumer = consumerFactory.create();
        this.producer = producerFactory.create();
        discoverAndAssignConsumerPartitions();
    }

    public void registerMetrics(MetricGroup metricGroup) {
        MetricUtils.register(consumer, metricGroup);
        MetricUtils.register(producer, metricGroup);
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
            producer.close();
        }
    }

    private void runConsumerLoop() {
        Duration consumerPollTimeout = Duration.ofMillis(CONSUMER_POLL_TIMEOUT_MILLIS);
        try {
            while (running.get()) {
                ConsumerRecords<String, Long> records = consumer.poll(consumerPollTimeout);
                if (!records.isEmpty()) {
                    processRecordsBatch(records);
                }
            }
        } catch (Throwable t) {
            error.compareAndSet(null, t);
        }
    }

    private void discoverAndAssignConsumerPartitions() {
        List<TopicPartition> discoveredPartitions = KafkaUtils.partitionsFor(consumer, topic);
        if (discoveredPartitions.isEmpty()) {
            throw new IllegalStateException("No partitions found for topic " + topic);
        }
        consumer.assign(discoveredPartitions);
        consumer.seekToBeginning(discoveredPartitions);
    }

    private void processRecordsBatch(ConsumerRecords<String, Long> records) {
        String domainPrefix = domain + ":";
        synchronized (currentWatermarksByKey) {
            for (ConsumerRecord<String, Long> record : records) {
                if (record.key().startsWith(domainPrefix)) {
                    currentWatermarksByKey.put(record.key(), record.value());
                }
            }
        }
    }

    public void submit(String operator, TopicPartition partition, long timestamp) {
        checkAndThrowOnPendingError();
        String key = domain + ":" + operator + ":" + partition.topic() + ":" + partition.partition();
        producer.send(new ProducerRecord<>(topic, key, timestamp));
    }

    public long getLowestWatermark() {
        checkAndThrowOnPendingError();
        synchronized (currentWatermarksByKey) {
            return currentWatermarksByKey.values().stream().min(Long::compareTo).orElse(Long.MAX_VALUE);
        }
    }

    private void checkAndThrowOnPendingError() {
        Throwable throwable = error.get();
        if (throwable != null) {
            throw new RuntimeException("Error tracking watermarks", throwable);
        }
    }
}
