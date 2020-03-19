package com.github.sukhinin.flink.connectors.kafka.source.utils;

import org.apache.flink.metrics.MetricGroup;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.Metric;

public final class MetricUtils {

    private static final String KAFKA_CONSUMER_METRICS_GROUP = "KafkaConsumer";

    private static final String KAFKA_PRODUCER_METRICS_GROUP = "KafkaProducer";

    public static void register(Consumer<?, ?> consumer, MetricGroup metricGroup) {
        MetricGroup group = metricGroup.addGroup(KAFKA_CONSUMER_METRICS_GROUP);
        for (Metric metric : consumer.metrics().values()) {
            group.addGroup(metric.metricName().group()).gauge(metric.metricName().name(), metric::metricValue);
        }
    }

    public static void register(Producer<?, ?> producer, MetricGroup metricGroup) {
        MetricGroup group = metricGroup.addGroup(KAFKA_PRODUCER_METRICS_GROUP);
        for (Metric metric : producer.metrics().values()) {
            group.addGroup(metric.metricName().group()).gauge(metric.metricName().name(), metric::metricValue);
        }
    }

    private MetricUtils() {
        throw new UnsupportedOperationException("It's illegal to create an instance of " + MetricUtils.class.getSimpleName());
    }
}
