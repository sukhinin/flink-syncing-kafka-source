package com.github.sukhinin.flink.connectors.kafka.source.utils;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public final class KafkaUtils {

    public static List<TopicPartition> partitionsFor(Consumer<?, ?> consumer, String topic) {
        List<PartitionInfo> topicPartitionInfoList = consumer.partitionsFor(topic);
        if (topicPartitionInfoList == null) {
            return Collections.emptyList();
        }
        List<TopicPartition> topicPartitionList = topicPartitionInfoList.stream()
                .map(info -> new TopicPartition(info.topic(), info.partition()))
                .sorted(Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition))
                .collect(Collectors.toList());
        return topicPartitionList;
    }

    private KafkaUtils() {
        throw new UnsupportedOperationException("It's illegal to create an instance of " + KafkaUtils.class.getSimpleName());
    }
}
