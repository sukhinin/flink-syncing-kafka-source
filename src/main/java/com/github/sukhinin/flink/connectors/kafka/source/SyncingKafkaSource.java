package com.github.sukhinin.flink.connectors.kafka.source;

import com.github.sukhinin.flink.connectors.kafka.source.records.PartitionAssigner;
import com.github.sukhinin.flink.connectors.kafka.source.records.RecordsFetcher;
import com.github.sukhinin.flink.connectors.kafka.source.records.RecordsFetcherFactory;
import com.github.sukhinin.flink.connectors.kafka.source.records.RoundRobinPartitionAssigner;
import com.github.sukhinin.flink.connectors.kafka.source.watermarks.WatermarksTracker;
import com.github.sukhinin.flink.connectors.kafka.source.watermarks.WatermarksTrackerFactory;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SyncingKafkaSource<T> extends RichParallelSourceFunction<T> implements CheckpointedFunction, ResultTypeQueryable<T> {

    private static final String RECORDS_FETCHER_METRIC_GROUP_NAME = "RecordsFetcher";

    private static final String WATERMARKS_TRACKER_METRIC_GROUP_NAME = "WatermarksTracker";

    private static final String OFFSETS_BY_TOPIC_PARTITION_METRIC_GROUP_NAME = "partition";

    private static final String CURRENT_OFFSETS_GAUGE_NAME = "currentOffsets";

    private static final String CURRENT_WATERMARKS_GAUGE_NAME = "currentWatermarks";

    private static final String PAUSED_PARTITIONS_GAUGE_NAME = "paused";

    private static final String OFFSETS_STATE_NAME = "topic-partition-offset-states";

    private static final Logger logger = LoggerFactory.getLogger(SyncingKafkaSource.class);

    private final RecordsFetcherFactory recordsFetcherFactory;

    private final WatermarksTrackerFactory watermarksTrackerFactory;

    private final KafkaDeserializationSchema<T> recordsDeserializationSchema;

    private final TimestampAssignerFactory<T> timestampAssignerFactory;

    private long watermarksSyncInterval = 1000L;

    private long watermarksSyncPauseThreshold = 60000L;

    private long watermarksSyncResumeThreshold = 5000L;

    private transient ListState<Tuple3<String, Integer, Long>> unionOffsetsState;

    private transient Map<TopicPartition, Long> restoredOffsets;

    private transient AtomicBoolean running;

    private transient Map<TopicPartition, TopicPartitionState<T>> topicPartitionStates;

    private transient RecordsFetcher recordsFetcher;

    private transient WatermarksTracker watermarksTracker;

    private transient SourceContext<T> sourceContext;

    public SyncingKafkaSource(RecordsFetcherFactory recordsFetcherFactory,
                              WatermarksTrackerFactory watermarksTrackerFactory,
                              KafkaDeserializationSchema<T> recordsDeserializationSchema,
                              TimestampAssignerFactory<T> timestampAssignerFactory) {
        this.recordsFetcherFactory = recordsFetcherFactory;
        this.watermarksTrackerFactory = watermarksTrackerFactory;
        this.recordsDeserializationSchema = recordsDeserializationSchema;
        this.timestampAssignerFactory = timestampAssignerFactory;
    }

    public long getWatermarksSyncInterval() {
        return watermarksSyncInterval;
    }

    public void setWatermarksSyncInterval(long watermarksSyncInterval) {
        if (watermarksSyncInterval <= 0) {
            throw new IllegalArgumentException("Watermarks sync interval must be positive");
        }
        this.watermarksSyncInterval = watermarksSyncInterval;
    }

    public long getWatermarksSyncPauseThreshold() {
        return watermarksSyncPauseThreshold;
    }

    public long getWatermarksSyncResumeThreshold() {
        return watermarksSyncResumeThreshold;
    }

    public void setWatermarksSyncThresholds(long watermarksSyncPauseThreshold, long watermarksSyncResumeThreshold) {
        if (watermarksSyncPauseThreshold <= 0) {
            throw new IllegalArgumentException("Watermarks pause threshold must be positive");
        }
        if (watermarksSyncResumeThreshold <= 0) {
            throw new IllegalArgumentException("Watermarks resume threshold must be positive");
        }
        if (watermarksSyncPauseThreshold <= watermarksSyncResumeThreshold) {
            throw new IllegalArgumentException("Watermarks pause threshold must be greater than resume threshold");
        }
        this.watermarksSyncPauseThreshold = watermarksSyncPauseThreshold;
        this.watermarksSyncResumeThreshold = watermarksSyncResumeThreshold;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        unionOffsetsState = context.getOperatorStateStore().getUnionListState(new ListStateDescriptor<>(
                OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple3<String, Integer, Long>>() {})));
        restoredOffsets = new HashMap<>();
        if (context.isRestored()) {
            unionOffsetsState.get().forEach(entry -> restoredOffsets.put(new TopicPartition(entry.f0, entry.f1), entry.f2));
            logger.info("Consumer subtask {} has restored offsets", getRuntimeContext().getTaskNameWithSubtasks());
        } else {
            logger.info("Consumer subtask {} has no saved state to restore", getRuntimeContext().getTaskNameWithSubtasks());
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        unionOffsetsState.clear();
        for (Map.Entry<TopicPartition, TopicPartitionState<T>> entry : topicPartitionStates.entrySet()) {
            TopicPartition partition = entry.getKey();
            TopicPartitionState<T> state = entry.getValue();
            unionOffsetsState.add(new Tuple3<>(partition.topic(), partition.partition(), state.getCurrentOffset()));
        }
    }

    @Override
    public void open(Configuration parameters) {
        running = new AtomicBoolean();
        createRecordsFetcher();
        createTopicPartitionStates();
        seekRecordsFetcherToPartitionOffsets();
        createWatermarkTracker();
        registerTopicPartitionMetrics();
    }

    private void createRecordsFetcher() {
        PartitionAssigner partitionAssigner = new RoundRobinPartitionAssigner(
                getRuntimeContext().getIndexOfThisSubtask(),
                getRuntimeContext().getNumberOfParallelSubtasks());
        RecordsFetcher recordsFetcher = recordsFetcherFactory.create(partitionAssigner);
        recordsFetcher.registerMetrics(getRuntimeContext().getMetricGroup().addGroup(RECORDS_FETCHER_METRIC_GROUP_NAME));
        this.recordsFetcher = recordsFetcher;
    }

    private void createTopicPartitionStates() {
        Set<TopicPartition> assignedPartitions = recordsFetcher.getAssignedPartitions();
        Map<TopicPartition, TopicPartitionState<T>> topicPartitionStates = assignedPartitions.stream()
                .collect(Collectors.toMap(Function.identity(), this::createTopicPartitionStateFor));
        if (topicPartitionStates.isEmpty()) {
            logger.info("Consumer subtask of {} has been assigned empty partition list " +
                    "and will not produce any data", getRuntimeContext().getTaskNameWithSubtasks());
        } else {
            String partitions = topicPartitionStates.keySet().stream()
                    .sorted(Comparator.comparing(TopicPartition::topic).thenComparing(TopicPartition::partition))
                    .map(TopicPartition::toString).collect(Collectors.joining(", "));
            logger.warn("Consumer subtask of {} has been assigned partitions {}",
                    getRuntimeContext().getTaskNameWithSubtasks(), partitions);
        }
        this.topicPartitionStates = topicPartitionStates;
    }

    private TopicPartitionState<T> createTopicPartitionStateFor(TopicPartition partition) {
        TopicPartitionState<T> state = new TopicPartitionState<>(timestampAssignerFactory.create());
        state.setCurrentOffset(restoredOffsets.getOrDefault(partition, 0L));
        state.setCurrentWatermark(Long.MIN_VALUE);
        state.setPaused(false);
        return state;
    }

    private void seekRecordsFetcherToPartitionOffsets() {
        Map<TopicPartition, Long> partitionOffsets = topicPartitionStates.entrySet().stream()
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue().getCurrentOffset()));
        recordsFetcher.seek(partitionOffsets);
    }

    private void createWatermarkTracker() {
        WatermarksTracker watermarksTracker = watermarksTrackerFactory.create();
        watermarksTracker.registerMetrics(getRuntimeContext().getMetricGroup().addGroup(WATERMARKS_TRACKER_METRIC_GROUP_NAME));
        this.watermarksTracker = watermarksTracker;
    }

    private void registerTopicPartitionMetrics() {
        for (Map.Entry<TopicPartition, TopicPartitionState<T>> entry : topicPartitionStates.entrySet()) {
            TopicPartition partition = entry.getKey();
            TopicPartitionState<T> state = entry.getValue();

            MetricGroup topicPartitionGroup = getRuntimeContext().getMetricGroup()
                    .addGroup(OFFSETS_BY_TOPIC_PARTITION_METRIC_GROUP_NAME, partition.toString());

            topicPartitionGroup.gauge(CURRENT_OFFSETS_GAUGE_NAME, state::getCurrentOffset);
            topicPartitionGroup.gauge(CURRENT_WATERMARKS_GAUGE_NAME, state::getCurrentWatermark);
            topicPartitionGroup.gauge(PAUSED_PARTITIONS_GAUGE_NAME, state::getPaused);
        }
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        if (running.compareAndSet(false, true)) {
            sourceContext = ctx;
            if (topicPartitionStates.isEmpty()) {
                // No partitions assigned to this consumer
                sourceContext.markAsTemporarilyIdle();
                runIdleLoop();
            } else {
                watermarksTracker.start();
                recordsFetcher.start();
                scheduleNextWatermarkEmission();
                scheduleNextWatermarkSync();
                runConsumerLoop();
            }
        }
    }

    @Override
    public void cancel() {
        if (running.compareAndSet(true, false)) {
            recordsFetcher.stop();
            watermarksTracker.stop();
        }
    }

    private void runConsumerLoop() throws Exception {
        while (running.get()) {
            ConsumerRecords<byte[], byte[]> records = recordsFetcher.poll();
            for (TopicPartition partition : records.partitions()) {
                TopicPartitionState<T> state = topicPartitionStates.get(partition);
                if (state == null) {
                    throw new IllegalStateException("No state found for partition " + partition);
                }
                // Guard against concurrent access from periodic assigner and state snapshots
                synchronized (sourceContext.getCheckpointLock()) {
                    for (ConsumerRecord<byte[], byte[]> record : records.records(partition)) {
                        T value = recordsDeserializationSchema.deserialize(record);
                        long timestamp = state.getTimestampAssigner().extractTimestamp(value, record.timestamp());
                        sourceContext.collectWithTimestamp(value, timestamp);
                        state.setCurrentOffset(record.offset());
                    }
                }
            }
        }
    }

    private void runIdleLoop() throws InterruptedException {
        while (running.get()) {
            Thread.sleep(100);
        }
    }

    private void scheduleNextWatermarkEmission() {
        long interval = getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();
        ProcessingTimeService timeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
        timeService.registerTimer(timeService.getCurrentProcessingTime() + interval, ts -> emitPeriodicWatermarks());
    }

    private void emitPeriodicWatermarks() {
        String operator = ((StreamingRuntimeContext) getRuntimeContext()).getOperatorUniqueID();
        long lowestPartitionWatermark = Long.MAX_VALUE;

        // Guard against concurrent access from consumer thread and state snapshots
        synchronized (sourceContext.getCheckpointLock()) {
            for (Map.Entry<TopicPartition, TopicPartitionState<T>> entry : topicPartitionStates.entrySet()) {
                TopicPartition partition = entry.getKey();
                TopicPartitionState<T> state = entry.getValue();
                Watermark watermark = state.getTimestampAssigner().getCurrentWatermark();
                if (watermark != null) {
                    lowestPartitionWatermark = Math.min(lowestPartitionWatermark, watermark.getTimestamp());
                    // Update watermark only when changed to reduce watermark synchronization overhead
                    if (state.getCurrentWatermark() < watermark.getTimestamp() || state.getCurrentWatermark() == Long.MIN_VALUE) {
                        watermarksTracker.submit(operator, partition, watermark.getTimestamp());
                        state.setCurrentWatermark(watermark.getTimestamp());
                    }
                }
            }
        }
        if (lowestPartitionWatermark != Long.MAX_VALUE) {
            sourceContext.emitWatermark(new Watermark(lowestPartitionWatermark));
        }
        scheduleNextWatermarkEmission();
    }

    private void scheduleNextWatermarkSync() {
        ProcessingTimeService timeService = ((StreamingRuntimeContext) getRuntimeContext()).getProcessingTimeService();
        timeService.registerTimer(timeService.getCurrentProcessingTime() + watermarksSyncInterval, ts -> syncPartitionWatermarks());
    }

    private void syncPartitionWatermarks() {
        long lowestGlobalWatermark = watermarksTracker.getLowestWatermark();
        boolean shouldNotifyRecordsFetcher = false;

        // Guard against concurrent access from consumer thread and state snapshots
        synchronized (sourceContext.getCheckpointLock()) {
            Set<TopicPartition> shouldBePaused = new HashSet<>();

            // For each assigned partition compare its current offset with the globally lowest offset across all
            // partitions and put partitions with excessive lead to a set of partitions that should be paused
            for (Map.Entry<TopicPartition, TopicPartitionState<T>> entry : topicPartitionStates.entrySet()) {
                TopicPartition partition = entry.getKey();
                TopicPartitionState<T> state = entry.getValue();
                Watermark watermark = state.getTimestampAssigner().getCurrentWatermark();
                if (watermark != null) {
                    long lead = watermark.getTimestamp() - lowestGlobalWatermark;
                    if (lead > watermarksSyncPauseThreshold && !state.isPaused()) {
                        // Partition is not currently paused and its lead has exceeded the pause threshold
                        state.setPaused(true);
                        shouldNotifyRecordsFetcher = true;
                        logger.debug("Partition {} is {} ms ahead and will be paused", partition.toString(), lead);
                    }
                    if (lead < watermarksSyncResumeThreshold && state.isPaused()) {
                        // Partition is currently paused and its lead has decreased below resume threshold
                        state.setPaused(false);
                        shouldNotifyRecordsFetcher = true;
                        logger.debug("Partition {} is {} ms ahead and will be resumed", partition.toString(), lead);
                    }
                }
                if (state.isPaused()) {
                    shouldBePaused.add(partition);
                }
            }

            // Notify records fetcher when the list of partitions that should be paused has been changed
            if (shouldNotifyRecordsFetcher) {
                recordsFetcher.pause(shouldBePaused);
                if (shouldBePaused.isEmpty()) {
                    logger.info("All partitions are running");
                } else {
                    String partitions = shouldBePaused.stream().map(TopicPartition::toString).collect(Collectors.joining(", "));
                    logger.info("Some partitions are paused: {}", partitions);
                }
            }
        }
        scheduleNextWatermarkSync();
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return recordsDeserializationSchema.getProducedType();
    }
}
