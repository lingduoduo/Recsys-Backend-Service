package com.recsys.online.flink;

import com.recsys.infrastructure.vectordb.VectorMath;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.Test;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.watermarkstatus.StatusWatermarkValve;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.util.Collector;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.DockerClientFactory;

import java.util.function.Consumer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class OnlineFeatureStreamingJobTest {

    @Test
    void assignsMovieBucketsStablyIncludingNegativeIds() {
        assertThat(OnlineFeatureStreamingJob.movieBucket(42, 7)).isEqualTo(0);
        assertThat(OnlineFeatureStreamingJob.movieBucket(-42, 7)).isEqualTo(0);
        assertThat(OnlineFeatureStreamingJob.movieBucket(Integer.MIN_VALUE, 7)).isEqualTo(5);
        assertThat(OnlineFeatureStreamingJob.movieBucket(-1, 7)).isEqualTo(6);
    }

    @Test
    void retainedStateTtlUsesWriteRefreshAndNeverReturnsExpiredValues() {
        var ttl = OnlineFeatureStreamingJob.stateTtl(1_800L);
        assertThat(ttl.isEnabled()).isTrue();
        assertThat(ttl.getTtl().toMilliseconds()).isEqualTo(1_800_000L);
        assertThat(ttl.getUpdateType()).isEqualTo(StateTtlConfig.UpdateType.OnCreateAndWrite);
        assertThat(ttl.getStateVisibility()).isEqualTo(StateTtlConfig.StateVisibility.NeverReturnExpired);
        assertThat(ttl.getCleanupStrategies().inRocksdbCompactFilter()).isTrue();
    }

    @Test
    void rejectsInvalidMovieBucketCounts() {
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.movieBucket(1, 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bucketCount");
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.movieBucket(1, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bucketCount");
    }

    @Test
    void rejectsNegativeTopKAllowedLateness() {
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateAllowedLatenessMs(-1L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("allowed-lateness");
        assertThat(OnlineFeatureStreamingJob.validateAllowedLatenessMs(0L)).isZero();
        assertThat(OnlineFeatureStreamingJob.FinalTopKWindowFunction.cleanupTimestamp(100L, 0L))
                .isEqualTo(101L);
    }

    @Test
    void validatesWatermarkIdleTimeout() {
        assertThat(OnlineFeatureStreamingJob.validateWatermarkIdleTimeoutMs(30_000L))
                .isEqualTo(30_000L);
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateWatermarkIdleTimeoutMs(0L))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("watermark-idle-timeout-ms");
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateWatermarkIdleTimeoutMs(-1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void topKSelectionHandlesTiesAndExtremeScores() {
        Map<Integer, Long> scores = new LinkedHashMap<>();
        scores.put(9, Long.MIN_VALUE);
        scores.put(7, Long.MAX_VALUE);
        scores.put(4, 0L);
        scores.put(2, Long.MAX_VALUE);

        assertThat(OnlineFeatureStreamingJob.selectTopK(scores, 2))
                .extracting(movie -> movie.movieId).containsExactly(2, 7);
        assertThat(OnlineFeatureStreamingJob.selectTopK(scores, Integer.MAX_VALUE))
                .extracting(movie -> movie.movieId).containsExactly(2, 7, 4, 9);
        assertThat(scores).containsAllEntriesOf(Map.of(
                9, Long.MIN_VALUE, 7, Long.MAX_VALUE, 4, 0L, 2, Long.MAX_VALUE));
    }

    @Test
    void topKSelectionHandlesEmptyAndInvalidLimits() {
        assertThat(OnlineFeatureStreamingJob.selectTopK(Map.of(), 10)).isEmpty();
        assertThat(OnlineFeatureStreamingJob.selectTopK(Map.of(1, 5L), 0)).isEmpty();
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.selectTopK(Map.of(), -1))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.selectTopK(Map.of(1, 5L), -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void topKSelectionMatchesFullSortAcrossCutoffs() {
        Random random = new Random(731);
        for (int round = 0; round < 20; round++) {
            Map<Integer, Long> scores = new LinkedHashMap<>();
            for (int id = 200; id > 0; id--) {
                scores.put(id, (long) random.nextInt(30) - 15);
            }
            List<Map.Entry<Integer, Long>> expected = new ArrayList<>(scores.entrySet());
            expected.sort((left, right) -> {
                int byScore = Long.compare(right.getValue(), left.getValue());
                return byScore != 0 ? byScore : Integer.compare(left.getKey(), right.getKey());
            });
            for (int k : new int[]{0, 1, 2, 10, 199, 200, 201, Integer.MAX_VALUE}) {
                var result = OnlineFeatureStreamingJob.selectTopK(scores, k);
                var selected = expected.subList(0, Math.min(k, expected.size()));
                assertThat(result).extracting(movie -> movie.movieId)
                        .containsExactlyElementsOf(selected.stream().map(Map.Entry::getKey).toList());
                assertThat(result).extracting(movie -> movie.score)
                        .containsExactlyElementsOf(selected.stream().map(Map.Entry::getValue).toList());
            }
        }
    }

    @Test
    void partialTopKIsBoundedAndUsesStableTieOrdering() throws Exception {
        var function = new OnlineFeatureStreamingJob.PartialTopKWindowFunction(2);
        List<OnlineFeatureStreamingJob.PartialTopK> output = new ArrayList<>();
        function.apply(3, new TimeWindow(0, 10), List.of(
                weightedEvent(3, 2), weightedEvent(1, 2), weightedEvent(2, 1)), collector(output));

        assertThat(output).hasSize(1);
        assertThat(output.get(0).windowEnd).isEqualTo(10);
        assertThat(output.get(0).bucket).isEqualTo(3);
        assertThat(output.get(0).movies).extracting(movie -> movie.movieId)
                .containsExactly(1, 3);
    }

    @Test
    void finalTopKMergesDuplicateMoviesAndOrdersTiesByMovieId() {
        var partials = List.of(
                partial(100, 0, scored(8, 3), scored(4, 2)),
                partial(100, 1, scored(8, 4), scored(2, 7), scored(3, 7)));

        List<OnlineFeatureStreamingJob.ScoredMovie> result =
                OnlineFeatureStreamingJob.FinalTopKWindowFunction.mergeTopK(partials, 3);

        assertThat(result).extracting(movie -> movie.movieId).containsExactly(2, 3, 8);
        assertThat(result).extracting(movie -> movie.score).containsExactly(7L, 7L, 7L);
    }

    @Test
    void multiChannelWatermarksWaitForDelayedPartialAndIgnoreIdleChannel() throws Exception {
        var function = new OnlineFeatureStreamingJob.FinalTopKWindowFunction(3, "hour", 60, 4, 10L);
        var operator = new KeyedProcessOperator<Long, OnlineFeatureStreamingJob.PartialTopK,
                OnlineFeatureStreamingJob.TopKSnapshot>(function);
        try (var harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator, OnlineFeatureStreamingJob.PartialTopK::windowEnd, org.apache.flink.api.common.typeinfo.Types.LONG)) {
            harness.open();
            StatusWatermarkValve watermarkValve = new StatusWatermarkValve(2);
            PushingAsyncDataInput.DataOutput<Object> alignedOutput = new PushingAsyncDataInput.DataOutput<>() {
                @Override public void emitRecord(StreamRecord<Object> record) {}
                @Override public void emitWatermark(Watermark watermark) throws Exception {
                    harness.processWatermark(watermark);
                }
                @Override public void emitWatermarkStatus(WatermarkStatus status) {}
                @Override public void emitLatencyMarker(LatencyMarker latencyMarker) {}
            };

            harness.processElement(new StreamRecord<>(partial(100, 0, scored(1, 3))));
            watermarkValve.inputWatermark(new Watermark(99), 0, alignedOutput);
            assertThat(harness.getCurrentWatermark()).isZero();
            watermarkValve.inputWatermarkStatus(WatermarkStatus.IDLE, 1, alignedOutput);
            assertThat(harness.getCurrentWatermark()).isEqualTo(99L);
            assertThat(snapshots(harness.getOutput())).isEmpty();

            harness.processElement(new StreamRecord<>(partial(100, 3, scored(2, 7))));
            assertThat(snapshots(harness.getOutput())).isEmpty();
            assertThat(harness.numKeyedStateEntries()).isPositive();

            watermarkValve.inputWatermark(new Watermark(100), 0, alignedOutput);
            assertThat(snapshots(harness.getOutput())).singleElement()
                    .satisfies(snapshot -> assertThat(snapshot.movies)
                            .extracting(movie -> movie.movieId).containsExactly(2, 1));
            assertThat(snapshots(harness.getOutput()).get(0).movies)
                    .extracting(movie -> movie.score).containsExactly(7L, 3L);

            harness.processElement(new StreamRecord<>(partial(100, 2, scored(3, 9))));
            watermarkValve.inputWatermark(new Watermark(110), 0, alignedOutput);
            assertThat(snapshots(harness.getOutput())).hasSize(1);
            assertThat(harness.numKeyedStateEntries()).isZero();

            harness.processElement(new StreamRecord<>(partial(100, 1, scored(4, 12))));
            watermarkValve.inputWatermark(new Watermark(200), 0, alignedOutput);
            assertThat(snapshots(harness.getOutput())).hasSize(1);
            assertThat(harness.numKeyedStateEntries()).isZero();

            var counterField = OnlineFeatureStreamingJob.FinalTopKWindowFunction.class
                    .getDeclaredField("latePartials");
            counterField.setAccessible(true);
            assertThat(((org.apache.flink.api.common.accumulators.LongCounter)
                    counterField.get(function)).getLocalValue()).isEqualTo(2L);
        }
    }

    private static List<OnlineFeatureStreamingJob.TopKSnapshot> snapshots(
            Iterable<Object> output) {
        List<OnlineFeatureStreamingJob.TopKSnapshot> snapshots = new ArrayList<>();
        for (Object value : output) {
            if (value instanceof StreamRecord<?> record
                    && record.getValue() instanceof OnlineFeatureStreamingJob.TopKSnapshot snapshot) {
                snapshots.add(snapshot);
            }
        }
        return snapshots;
    }

    @Test
    void twoStageTopKMatchesSingleStageOracleForDeterministicEvents() throws Exception {
        int bucketCount = 17;
        int topK = 20;
        Random random = new Random(8675309L);
        List<MovieEvent> events = new ArrayList<>();
        int[] weights = {1, 2, 3, 4, 8};
        for (int i = 0; i < 1_000; i++) {
            events.add(weightedEvent(random.nextInt(301) - 150,
                    weights[random.nextInt(weights.length)]));
        }

        List<OnlineFeatureStreamingJob.PartialTopK> partials = new ArrayList<>();
        for (int bucket = 0; bucket < bucketCount; bucket++) {
            List<MovieEvent> bucketEvents = new ArrayList<>();
            for (MovieEvent event : events) {
                if (OnlineFeatureStreamingJob.movieBucket(event.movieId, bucketCount) == bucket) {
                    bucketEvents.add(event);
                }
            }
            if (!bucketEvents.isEmpty()) {
                new OnlineFeatureStreamingJob.PartialTopKWindowFunction(topK)
                        .apply(bucket, new TimeWindow(0, 100), bucketEvents, collector(partials));
            }
        }

        List<OnlineFeatureStreamingJob.ScoredMovie> actual =
                OnlineFeatureStreamingJob.FinalTopKWindowFunction.mergeTopK(partials, topK);
        Map<Integer, Long> oracleScores = new java.util.HashMap<>();
        events.forEach(event -> oracleScores.merge(event.movieId, event.engagementWeight(), Long::sum));
        List<Map.Entry<Integer, Long>> oracle = oracleScores.entrySet().stream()
                .sorted(Map.Entry.<Integer, Long>comparingByValue(java.util.Comparator.reverseOrder())
                        .thenComparing(Map.Entry::getKey))
                .limit(topK).toList();

        assertThat(actual).extracting(movie -> movie.movieId)
                .containsExactlyElementsOf(oracle.stream().map(Map.Entry::getKey).toList());
        assertThat(actual).extracting(movie -> movie.score)
                .containsExactlyElementsOf(oracle.stream().map(Map.Entry::getValue).toList());
    }

    private static OnlineFeatureStreamingJob.PartialTopK partial(
            long windowEnd, int bucket, OnlineFeatureStreamingJob.ScoredMovie... movies) {
        return new OnlineFeatureStreamingJob.PartialTopK(windowEnd, bucket, List.of(movies));
    }

    private static OnlineFeatureStreamingJob.ScoredMovie scored(int movieId, long score) {
        return new OnlineFeatureStreamingJob.ScoredMovie(movieId, score);
    }

    private static MovieEvent weightedEvent(int movieId, int weight) {
        MovieEvent event = new MovieEvent();
        event.movieId = movieId;
        event.eventType = switch (weight) {
            case 8 -> "order";
            case 4 -> "rating";
            case 3 -> "like";
            case 2 -> "click";
            case 1 -> "view";
            default -> throw new IllegalArgumentException("unsupported test weight");
        };
        event.rating = weight == 4 ? 4 : null;
        event.watchMs = weight == 1 ? 30_000L : 0L;
        return event;
    }

    private static <T> Collector<T> collector(List<T> output) {
        return new Collector<>() {
            @Override public void collect(T record) { output.add(record); }
            @Override public void close() {}
        };
    }

    @Test
    void acceptsDefaultParallelismConfiguration() {
        assertThat(OnlineFeatureStreamingJob.validateConfiguration(ParameterTool.fromArgs(new String[0])))
                .isEqualTo(new OnlineFeatureStreamingJob.JobConfiguration(24, 24, 24, 128));
    }

    @Test
    void kafkaModesRequireDurableCheckpointsAndGenerationSpecificTopics() {
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateConfiguration(ParameterTool.fromArgs(
                new String[]{"--bootstrap.servers", "broker:9092"})))
                .hasMessageContaining("checkpoint-dir");
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateConfiguration(ParameterTool.fromArgs(
                new String[]{"--bootstrap.servers", "broker:9092", "--checkpoint-dir", "s3://cp",
                        "--topic", "recsys_events"})))
                .hasMessageContaining("movie_events_v2");
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateConfiguration(ParameterTool.fromArgs(
                new String[]{"--bootstrap.servers", "broker:9092", "--checkpoint-dir", "s3://cp",
                        "--bridge-mode", "true"})))
                .hasMessageContaining("legacy");
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateConfiguration(ParameterTool.fromArgs(
                new String[]{"--bootstrap.servers", "broker:9092", "--checkpoint-dir", "s3://cp",
                        "--bridge-mode", "true", "--topic", "recsys_events"})))
                .hasMessageContaining("bridge-replay-cutoff-ms");
        assertThat(OnlineFeatureStreamingJob.validateConfiguration(ParameterTool.fromArgs(
                new String[]{"--bootstrap.servers", "broker:9092", "--checkpoint-dir", "s3://cp"})))
                .isEqualTo(new OnlineFeatureStreamingJob.JobConfiguration(24, 24, 24, 128));
        assertThat(OnlineFeatureStreamingJob.validateConfiguration(ParameterTool.fromArgs(
                new String[]{"--bootstrap.servers", "broker:9092", "--checkpoint-dir", "s3://cp",
                        "--bridge-mode", "true", "--topic", "recsys_events",
                        "--bridge-replay-cutoff-ms", "1000", "--bridge-reference-time-ms", "2000"})))
                .isEqualTo(new OnlineFeatureStreamingJob.JobConfiguration(24, 24, 24, 128));
    }

    @Test
    void bridgeReplayCutoffRejectsOldOrUnclassifiableEventTimeAndAcceptsBoundary() {
        MovieEvent missing = event(1, "missing", 1); missing.eventTimeMillis = 0L;
        MovieEvent old = event(1, "old", 1); old.eventTimeMillis = 999L;
        MovieEvent boundary = event(1, "boundary", 1); boundary.eventTimeMillis = 1_000L;
        assertThat(OnlineFeatureStreamingJob.acceptsBridgeReplayEvent(missing, true, 1_000L)).isFalse();
        assertThat(OnlineFeatureStreamingJob.acceptsBridgeReplayEvent(old, true, 1_000L)).isFalse();
        assertThat(OnlineFeatureStreamingJob.acceptsBridgeReplayEvent(boundary, true, 1_000L)).isTrue();
        assertThat(OnlineFeatureStreamingJob.acceptsBridgeReplayEvent(old, false, -1L)).isTrue();
    }

    @Test
    void operatorSpecificBridgeEligibilityUsesExactExpiryBoundary() {
        MovieEvent event = event(1, "event", 1);
        event.eventTimeMillis = 1_000L;
        assertThat(OnlineFeatureStreamingJob.bridgeEligible(event, true, 30_999L, 30L)).isTrue();
        assertThat(OnlineFeatureStreamingJob.bridgeEligible(event, true, 31_000L, 30L)).isFalse();
        assertThat(OnlineFeatureStreamingJob.bridgeEligible(event, false, Long.MAX_VALUE, 1L)).isTrue();
    }

    @Test
    void rejectsLocalCheckpointStorageUnlessExplicitlyAllowedForTests() {
        for (String location : List.of("relative/checkpoints", "file:///tmp/checkpoints", "/tmp/checkpoints")) {
            assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateConfiguration(ParameterTool.fromArgs(
                    new String[]{"--bootstrap.servers", "broker:9092", "--checkpoint-dir", location})))
                    .as(location).hasMessageContaining("shared durable");
        }
        assertThat(OnlineFeatureStreamingJob.validateConfiguration(ParameterTool.fromArgs(new String[]{
                "--bootstrap.servers", "broker:9092", "--checkpoint-dir", "file:///tmp/checkpoints",
                "--allow-local-checkpoint-storage", "true"})))
                .isEqualTo(new OnlineFeatureStreamingJob.JobConfiguration(24, 24, 24, 128));
    }

    @Test
    void bridgeTerminalUsesNoOpButPreservesProductionSinkUid() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        var input = env.fromElements(new OnlineFeatureStreamingJob.StringFeatureUpdate(
                "key", "state", 1L, 60, "event"));
        var configuration = new OnlineFeatureStreamingJob.JobConfiguration(24, 1, 1, 128);
        OnlineFeatureStreamingJob.attachSink(input,
                new OnlineFeatureStreamingJob.RedisStringFeatureSink(LOCAL_REDIS),
                true,
                "redis-test-sink", "redis-test-sink-v1", configuration);

        var nodes = env.getStreamGraph().getStreamNodes().stream().toList();
        assertThat(nodes).anyMatch(node -> node.getOperatorName().contains("bridge-noop-redis-test-sink"));
        assertThat(nodes).noneMatch(node -> node.getOperatorName().matches(".*Sink: redis-test-sink.*"));
        assertThat(nodes.stream().filter(node -> node.getOperatorName().contains("sink")).toList())
                .allMatch(node -> !node.getOperatorFactory().getClass().getName().contains("Redis"));

        StreamExecutionEnvironment normalEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        var normalInput = normalEnv.fromElements(new OnlineFeatureStreamingJob.StringFeatureUpdate(
                "key", "state", 1L, 60, "event"));
        OnlineFeatureStreamingJob.attachSink(normalInput,
                new OnlineFeatureStreamingJob.RedisStringFeatureSink(LOCAL_REDIS),
                false,
                "redis-test-sink", "redis-test-sink-v1", configuration);
        assertThat(normalEnv.getStreamGraph().getStreamNodes()).anyMatch(
                node -> node.getOperatorName().contains("redis-test-sink"));
    }

    @Test
    void rejectsNonPositiveParallelism() {
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateConfiguration(
                ParameterTool.fromArgs(new String[]{"--operator-parallelism", "0"})))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operator-parallelism");
    }

    @Test
    void rejectsSourceParallelismAboveExpectedTopicPartitions() {
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateConfiguration(
                ParameterTool.fromArgs(new String[]{"--source-parallelism", "25"})))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("source-parallelism");
    }

    @Test
    void rejectsMaxParallelismBelowOperatorParallelism() {
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateConfiguration(
                ParameterTool.fromArgs(new String[]{
                        "--operator-parallelism", "24", "--max-parallelism", "16"})))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-parallelism");
    }

    @Test
    void rejectsMaxParallelismBelowSourceParallelism() {
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateConfiguration(
                ParameterTool.fromArgs(new String[]{
                        "--expected-topic-partitions", "24",
                        "--source-parallelism", "24",
                        "--operator-parallelism", "8",
                        "--max-parallelism", "16"})))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("max-parallelism")
                .hasMessageContaining("source-parallelism")
                .hasMessageContaining("operator-parallelism");
    }

    @Test
    void acceptsMaxParallelismEqualToSourceParallelism() {
        ParameterTool params = ParameterTool.fromArgs(new String[]{
                "--expected-topic-partitions", "24",
                "--source-parallelism", "24",
                "--operator-parallelism", "8",
                "--max-parallelism", "24"});

        assertThat(OnlineFeatureStreamingJob.validateConfiguration(params))
                .isEqualTo(new OnlineFeatureStreamingJob.JobConfiguration(24, 24, 8, 24));
    }

    @Test
    void preservesSameUserOrderWhenEventIdsHashDiffer() throws Exception {
        String firstId = "event-A";
        String secondId = "event-B";
        assertThat(firstId.hashCode()).isNotEqualTo(secondId.hashCode());

        Map<String, Long> backingState = new LinkedHashMap<>();
        @SuppressWarnings("unchecked")
        MapState<String, Long> mapState = mock(MapState.class);
        when(mapState.get(any())).thenAnswer(invocation -> backingState.get(invocation.getArgument(0)));
        org.mockito.Mockito.doAnswer(invocation -> backingState.put(
                invocation.getArgument(0), invocation.getArgument(1)))
                .when(mapState).put(any(), any());
        org.mockito.Mockito.doAnswer(invocation -> backingState.remove(invocation.getArgument(0)))
                .when(mapState).remove(any());

        RuntimeContext runtimeContext = mock(RuntimeContext.class);
        org.mockito.Mockito.doReturn(mapState).when(runtimeContext).getMapState(any());
        var function = new OnlineFeatureStreamingJob.DeduplicateEventsFunction(60);
        function.setRuntimeContext(runtimeContext);
        function.open(new Configuration());

        TimerService timerService = mock(TimerService.class);
        when(timerService.currentProcessingTime()).thenReturn(1_000L);
        @SuppressWarnings("unchecked")
        OnlineFeatureStreamingJob.DeduplicateEventsFunction.Context context = mock(
                OnlineFeatureStreamingJob.DeduplicateEventsFunction.Context.class);
        when(context.timerService()).thenReturn(timerService);
        List<MovieEvent> output = new ArrayList<>();
        Collector<MovieEvent> collector = new Collector<>() {
            @Override public void collect(MovieEvent record) { output.add(record); }
            @Override public void close() {}
        };

        MovieEvent first = event(7, firstId, 101);
        MovieEvent second = event(7, secondId, 102);
        function.processElement(first, context, collector);
        function.processElement(second, context, collector);
        function.processElement(first, context, collector);

        assertThat(output).extracting(event -> event.eventId)
                .containsExactly(firstId, secondId);
        org.mockito.Mockito.verify(mapState, org.mockito.Mockito.never()).entries();
    }

    private static MovieEvent event(int userId, String eventId, int movieId) {
        MovieEvent event = new MovieEvent();
        event.userId = userId;
        event.eventId = eventId;
        event.movieId = movieId;
        event.eventType = "click";
        return event;
    }

    static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379);

    private static final OnlineFeatureStreamingJob.RedisEndpoint LOCAL_REDIS = endpoint("localhost", 6379);

    private static OnlineFeatureStreamingJob.RedisEndpoint endpoint(String host, int port) {
        return new OnlineFeatureStreamingJob.RedisEndpoint(host, port, null, null, false);
    }

    private static GenericContainer<?> redis() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(),
                "Docker is required for Redis integration tests");
        if (!REDIS.isRunning()) REDIS.start();
        return REDIS;
    }

    /** Connects a short-lived Lettuce client for assertions, then shuts it down. */
    private static void withRedis(String host, int port, Consumer<RedisCommands<String, String>> body) {
        RedisClient client = RedisClient.create(RedisURI.create(host, port));
        try (StatefulRedisConnection<String, String> conn = client.connect(StringCodec.UTF8)) {
            body.accept(conn.sync());
        } finally {
            client.shutdown();
        }
    }

    @Test
    void equalTimestampUsesEventIdTieBreakerRegardlessOfArrivalOrder() throws Exception {
        GenericContainer<?> redis = redis();
        withRedis(redis.getHost(), redis.getMappedPort(6379), RedisCommands::flushall);
        var sink = new OnlineFeatureStreamingJob.RedisTopKSink(endpoint(redis.getHost(), redis.getMappedPort(6379)));
        sink.open(new Configuration());
        try {
            sink.apply(snapshot(1_000L, "b", scored(2, 9)));
            sink.apply(snapshot(1_000L, "a", scored(1, 10)));
            withRedis(redis.getHost(), redis.getMappedPort(6379), cmd ->
                    assertThat(cmd.zrevrange("topk:{last_hour}:value", 0, -1)).containsExactly("2"));
        } finally {
            sink.close();
        }
    }

    @Test
    void replayOfIdenticalTopKVersionIsNoOp() throws Exception {
        GenericContainer<?> redis = redis();
        withRedis(redis.getHost(), redis.getMappedPort(6379), RedisCommands::flushall);
        var sink = new OnlineFeatureStreamingJob.RedisTopKSink(endpoint(redis.getHost(), redis.getMappedPort(6379)));
        sink.open(new Configuration());
        try {
            var value = snapshot(2_000L, "replay", scored(2, 9));
            assertThat(sink.apply(value)).isEqualTo(1L);
            assertThat(sink.apply(value)).isZero();
        } finally {
            sink.close();
        }
    }

    @Test
    void atomicScriptUpdatesAllTopKRepresentationsAndLineage() throws Exception {
        GenericContainer<?> redis = redis();
        withRedis(redis.getHost(), redis.getMappedPort(6379), RedisCommands::flushall);
        var sink = new OnlineFeatureStreamingJob.RedisTopKSink(endpoint(redis.getHost(), redis.getMappedPort(6379)));
        sink.open(new Configuration());
        try {
            sink.apply(snapshot(3_000L, "evt-topk", scored(2, 9)));
            withRedis(redis.getHost(), redis.getMappedPort(6379), cmd -> {
                assertThat(cmd.zrevrange("topk:{last_hour}:value", 0, -1)).containsExactly("2");
                assertThat(cmd.zrevrange("feature:{last_hour}:hot_movies", 0, -1)).containsExactly("2");
                assertThat(cmd.get("feature:{last_hour}:trend")).isEqualTo("2:9");
                assertThat(cmd.get("topk:{last_hour}:version")).isEqualTo("3000|evt-topk");
                assertThat(cmd.smembers("lineage:{last_hour}:event:evt-topk"))
                        .containsExactlyInAnyOrder("topk:{last_hour}:value",
                                "feature:{last_hour}:hot_movies", "feature:{last_hour}:trend");
            });
        } finally {
            sink.close();
        }
    }

    private static OnlineFeatureStreamingJob.TopKSnapshot snapshot(
            long eventTime, String eventId, OnlineFeatureStreamingJob.ScoredMovie... movies) {
        return new OnlineFeatureStreamingJob.TopKSnapshot(
                "last_hour", List.of(movies), eventTime, 3600, eventId);
    }

    // ── pre-existing tests (unchanged) ──────────────────────────────────────

    @Test
    void encodeVectorUsesSpaceSeparator() throws Exception {
        var method = OnlineFeatureStreamingJob.UserEmbeddingFunction.class
                .getDeclaredMethod("encodeVector", double[].class);
        method.setAccessible(true);
        var fn = new OnlineFeatureStreamingJob.UserEmbeddingFunction(4, 3600);
        String encoded = (String) method.invoke(fn, new double[]{1.0, 0.0, 0.0, 0.0});
        assertThat(encoded).doesNotContain(",");
        assertThat(encoded).contains(" ");
    }

    @Test
    void encodedVectorIsParsableByVectorMath() throws Exception {
        var method = OnlineFeatureStreamingJob.UserEmbeddingFunction.class
                .getDeclaredMethod("encodeVector", double[].class);
        method.setAccessible(true);
        var fn = new OnlineFeatureStreamingJob.UserEmbeddingFunction(4, 3600);
        String encoded = (String) method.invoke(fn, new double[]{3.0, 4.0, 0.0, 0.0});
        float[] parsed = VectorMath.parseVector(encoded);
        assertThat(parsed).hasSize(4);
        assertThat(parsed[0]).isCloseTo(0.6f, org.assertj.core.data.Offset.offset(0.001f));
        assertThat(parsed[1]).isCloseTo(0.8f, org.assertj.core.data.Offset.offset(0.001f));
    }

    @Test
    void accumulatesRawCountsNotNormalisedValues() throws Exception {
        var rawMethod = OnlineFeatureStreamingJob.UserEmbeddingFunction.class
                .getDeclaredMethod("encodeRaw", double[].class);
        rawMethod.setAccessible(true);
        var encodeMethod = OnlineFeatureStreamingJob.UserEmbeddingFunction.class
                .getDeclaredMethod("encodeVector", double[].class);
        encodeMethod.setAccessible(true);
        var fn = new OnlineFeatureStreamingJob.UserEmbeddingFunction(4, 3600);
        double[] rawAfterFirst = {2.0, 0.0, 0.0, 0.0};
        String rawStored = (String) rawMethod.invoke(fn, rawAfterFirst);
        var parseMethod = OnlineFeatureStreamingJob.UserEmbeddingFunction.class
                .getDeclaredMethod("parseVector", String.class, int.class);
        parseMethod.setAccessible(true);
        double[] restored = (double[]) parseMethod.invoke(fn, rawStored, 4);
        restored[1] += 2.0;
        assertThat(restored[0]).isCloseTo(2.0, org.assertj.core.data.Offset.offset(0.001));
        assertThat(restored[1]).isCloseTo(2.0, org.assertj.core.data.Offset.offset(0.001));
        String redisOutput = (String) encodeMethod.invoke(fn, restored);
        float[] parsed = VectorMath.parseVector(redisOutput);
        assertThat(parsed[0]).isCloseTo(0.707f, org.assertj.core.data.Offset.offset(0.01f));
        assertThat(parsed[1]).isCloseTo(0.707f, org.assertj.core.data.Offset.offset(0.01f));
    }

    @Test
    void stringFeatureUpdateCarriesEventId() throws Exception {
        var ctor = OnlineFeatureStreamingJob.StringFeatureUpdate.class
                .getDeclaredConstructor(String.class, String.class, long.class, int.class, String.class);
        ctor.setAccessible(true);
        var update = ctor.newInstance("u2vEmb:1", "0.5 0.5", 1000L, 3600, "evt-test");
        var field = OnlineFeatureStreamingJob.StringFeatureUpdate.class.getDeclaredField("eventId");
        field.setAccessible(true);
        assertThat(field.get(update)).isEqualTo("evt-test");
    }

    // ── Task 2 tests (require Docker) ────────────────────────────────────────

    @Test
    void luaScriptWritesCompanionKeys() throws Exception {
        GenericContainer<?> redis = redis();
        String host = redis.getHost();
        int port = redis.getMappedPort(6379);

        var sink = new OnlineFeatureStreamingJob.RedisStringFeatureSink(endpoint(host, port));
        sink.open(new Configuration());
        try {
            var update = new OnlineFeatureStreamingJob.StringFeatureUpdate(
                    "u2vEmb:10", "0.5 0.5", 1000L, 3600, "evt-abc");
            sink.invoke(update, null);

            withRedis(host, port, cmd -> {
                assertThat(cmd.get("u2vEmb:10")).isEqualTo("0.5 0.5");
                assertThat(cmd.get("u2vEmb:10:last_event")).isEqualTo("evt-abc");
                assertThat(cmd.lrange("u2vEmb:10:event_history", 0, -1))
                        .containsExactly("evt-abc");
                assertThat(cmd.smembers("lineage:event:evt-abc"))
                        .containsExactly("u2vEmb:10");
            });
        } finally {
            sink.close();
        }
    }

    @Test
    void luaScriptSkipsLineageWhenNewerExists() throws Exception {
        GenericContainer<?> redis = redis();
        String host = redis.getHost();
        int port = redis.getMappedPort(6379);

        var sink = new OnlineFeatureStreamingJob.RedisStringFeatureSink(endpoint(host, port));
        sink.open(new Configuration());
        try {
            sink.invoke(new OnlineFeatureStreamingJob.StringFeatureUpdate(
                    "u2vEmb:20", "0.6 0.8", 2000L, 3600, "evt-first"), null);

            sink.invoke(new OnlineFeatureStreamingJob.StringFeatureUpdate(
                    "u2vEmb:20", "0.1 0.2", 1000L, 3600, "evt-stale"), null);

            withRedis(host, port, cmd -> {
                assertThat(cmd.get("u2vEmb:20")).isEqualTo("0.6 0.8");
                assertThat(cmd.get("u2vEmb:20:last_event")).isEqualTo("evt-first");
                assertThat(cmd.lrange("u2vEmb:20:event_history", 0, -1))
                        .containsExactly("evt-first");
                assertThat(cmd.smembers("lineage:event:evt-stale")).isEmpty();
            });
        } finally {
            sink.close();
        }
    }

    @Test
    void eventHistoryCapAtFive() throws Exception {
        GenericContainer<?> redis = redis();
        String host = redis.getHost();
        int port = redis.getMappedPort(6379);

        var sink = new OnlineFeatureStreamingJob.RedisStringFeatureSink(endpoint(host, port));
        sink.open(new Configuration());
        try {
            for (int i = 1; i <= 6; i++) {
                sink.invoke(new OnlineFeatureStreamingJob.StringFeatureUpdate(
                        "u2vEmb:30", "0.5 0.5", (long) i * 1000, 3600,
                        "evt-" + String.format("%03d", i)), null);
            }

            withRedis(host, port, cmd -> {
                var history = cmd.lrange("u2vEmb:30:event_history", 0, -1);
                assertThat(history).hasSize(5);
                assertThat(history.get(0)).isEqualTo("evt-002");
                assertThat(history.get(4)).isEqualTo("evt-006");
            });
        } finally {
            sink.close();
        }
    }

    @Test
    void fixedSixFormattingMatchesStringFormatByteForByte() {
        Random random = new Random(42);
        List<Double> values = new ArrayList<>(List.of(0.0, -0.0, 1.0, -1.0, 5e-7, 1.5e-6, 2.5e-6, 2.5e-7,
                -1e-9, 4.35e-6, 123_456_789.0, Double.MIN_VALUE, Double.MAX_VALUE,
                Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY));
        for (int i = 0; i < 200_000; i++) {
            values.add(switch (i % 5) {
                case 0 -> random.nextDouble();
                case 1 -> (double) random.nextInt(1_000_000);
                case 2 -> random.nextDouble() * 1e6;
                case 3 -> random.nextInt(1_000) / (double) (1 + random.nextInt(1_000));
                default -> Math.round(random.nextDouble() * 1e7) / 1e7;
            });
        }
        for (double value : values) {
            StringBuilder builder = new StringBuilder();
            OnlineFeatureStreamingJob.UserEmbeddingFunction.appendFixed6(builder, value);
            assertThat(builder.toString()).as("value %s", value)
                    .isEqualTo(String.format(java.util.Locale.ROOT, "%.6f", value));
        }
    }

    @Test
    void finalTopKCleansUpTimersRestoredFromADifferentAllowedLateness() throws Exception {
        org.apache.flink.runtime.checkpoint.OperatorSubtaskState snapshot;
        try (var harness = finalTopKHarness(10L)) {
            harness.open();
            harness.processElement(new StreamRecord<>(partial(100, 0, scored(1, 3))));
            harness.processWatermark(new Watermark(100));
            assertThat(snapshots(harness.getOutput())).hasSize(1);
            assertThat(harness.numKeyedStateEntries()).isPositive();
            snapshot = harness.snapshot(1L, 1L);
        }
        // Restored with a longer lateness, the pending cleanup timer (110) no longer matches
        // cleanupTimestamp(100, 50); it must still clear the window rather than leak it.
        try (var restored = finalTopKHarness(50L)) {
            restored.initializeState(snapshot);
            restored.open();
            restored.processWatermark(new Watermark(110));
            assertThat(restored.numKeyedStateEntries()).isZero();
            assertThat(snapshots(restored.getOutput())).isEmpty();
        }
    }

    private static KeyedOneInputStreamOperatorTestHarness<Long, OnlineFeatureStreamingJob.PartialTopK,
            OnlineFeatureStreamingJob.TopKSnapshot> finalTopKHarness(long allowedLatenessMs) throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                new KeyedProcessOperator<>(new OnlineFeatureStreamingJob.FinalTopKWindowFunction(
                        3, "hour", 60, 4, allowedLatenessMs)),
                OnlineFeatureStreamingJob.PartialTopK::windowEnd,
                org.apache.flink.api.common.typeinfo.Types.LONG);
    }

    @Test
    void sinkRecoversWhenRedisForgetsTheCachedScript() throws Exception {
        GenericContainer<?> redis = redis();
        String host = redis.getHost();
        int port = redis.getMappedPort(6379);
        withRedis(host, port, RedisCommands::flushall);

        var sink = new OnlineFeatureStreamingJob.RedisStringFeatureSink(endpoint(host, port));
        sink.open(new Configuration());
        try {
            sink.invoke(new OnlineFeatureStreamingJob.StringFeatureUpdate(
                    "u2vEmb:40", "0.1 0.2", 1000L, 3600, "evt-before"), null);
            // What a restart or failover to a fresh primary looks like to a long-lived sink.
            withRedis(host, port, RedisCommands::scriptFlush);
            sink.invoke(new OnlineFeatureStreamingJob.StringFeatureUpdate(
                    "u2vEmb:40", "0.3 0.4", 2000L, 3600, "evt-after"), null);

            withRedis(host, port, cmd -> {
                assertThat(cmd.get("u2vEmb:40")).isEqualTo("0.3 0.4");
                assertThat(cmd.lrange("u2vEmb:40:event_history", 0, -1))
                        .containsExactly("evt-before", "evt-after");
            });
        } finally {
            sink.close();
        }
    }
}
