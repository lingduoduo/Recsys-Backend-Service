package com.recsys.online.flink;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("docker")
@Testcontainers(disabledWithoutDocker = true)
class KafkaFlinkPartitionIntegrationTest {
    private static final int PARTITIONS = 24;

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @TempDir Path temporaryDirectory;
    private final List<JobClient> jobs = new ArrayList<>();

    @AfterEach
    void stopJobs() {
        jobs.forEach(job -> {
            try { job.cancel().get(20, TimeUnit.SECONDS); } catch (Exception ignored) { }
        });
        TestSink.clear();
    }

    @Test
    void kafkaSourceProductionGraphAndRescaledRestorePreserveOrderingDedupAndTopK() throws Exception {
        String topic = topic("graph");
        createTopic(topic, PARTITIONS);
        String firstRun = UUID.randomUUID().toString();
        JobClient first = start(topic, firstRun, 4, null);

        List<MovieEvent> initial = new ArrayList<>();
        for (int sequence = 0; sequence < 8; sequence++) {
            for (int user = 1; user <= 96; user++) {
                initial.add(event("event-" + user + '-' + sequence, user,
                        user % 31 + 1, 1_000L + sequence));
            }
        }
        Set<Integer> writtenPartitions = send(topic, initial);
        await(Duration.ofSeconds(45), () -> TestSink.events(firstRun).size() == initial.size());

        Map<Integer, List<Long>> perUser = TestSink.events(firstRun).stream()
                .collect(Collectors.groupingBy(e -> e.userId,
                        Collectors.mapping(e -> e.eventTimeMillis, Collectors.toList())));
        assertThat(perUser).hasSize(96);
        perUser.values().forEach(times -> assertThat(times)
                .containsExactly(1_000L, 1_001L, 1_002L, 1_003L, 1_004L, 1_005L, 1_006L, 1_007L));
        assertThat(writtenPartitions).hasSizeGreaterThanOrEqualTo(12);

        // Advance event time so the production partial/final Top-K stages emit a real window.
        List<MovieEvent> advance = java.util.stream.IntStream.rangeClosed(1, 96)
                .mapToObj(user -> event("advance-" + user, user, user % 31 + 1, 4_000L)).toList();
        send(topic, advance);
        await(Duration.ofSeconds(45), () -> snapshot(firstRun, 2_000L) != null);
        assertExactTopK(snapshot(firstRun, 2_000L), initial, 10);

        // Accumulate a second window without advancing its watermark. Its state must cross
        // the savepoint and be merged with records consumed after rescaling.
        List<MovieEvent> beforeSavepoint = scoredEvents("before", 500, 1, 8, 6_000L, false);
        MovieEvent sharedBeforeSavepoint = event("shared-event-id", 501, 15, 6_000L);
        beforeSavepoint.add(sharedBeforeSavepoint);
        // Count from the inputs, not the sink: the window-2000 snapshot can be emitted before every
        // advance record reaches the events sink, so a sink-size baseline is taken mid-delivery.
        int acceptedBeforeSavepoint = initial.size() + advance.size() + beforeSavepoint.size();
        send(topic, beforeSavepoint);
        await(Duration.ofSeconds(45), () -> TestSink.events(firstRun).size() == acceptedBeforeSavepoint);

        Path savepointDirectory = temporaryDirectory.resolve("savepoints");
        String savepoint = first.triggerSavepoint(savepointDirectory.toUri().toString())
                .get(60, TimeUnit.SECONDS);
        first.cancel().get(30, TimeUnit.SECONDS);
        jobs.remove(first);

        String secondRun = UUID.randomUUID().toString();
        JobClient restored = start(topic, secondRun, 6, savepoint);
        List<MovieEvent> afterRestore = scoredEvents("after", 700, 1, 15, 6_001L, true);
        MovieEvent sameUserDuplicate = event("shared-event-id", 501, 99, 6_002L);
        MovieEvent otherUserSameId = event("shared-event-id", 502, 14, 6_003L);
        MovieEvent continuityMarker = event("after-restore", 501, 100, 6_004L);
        List<MovieEvent> restoreInput = new ArrayList<>(afterRestore);
        restoreInput.add(sameUserDuplicate);
        restoreInput.add(otherUserSameId);
        restoreInput.add(continuityMarker);
        send(topic, restoreInput);
        int expectedAfterRestore = afterRestore.size() + 2; // duplicate is the sole rejected record
        await(Duration.ofSeconds(45), () -> TestSink.events(secondRun).size() == expectedAfterRestore);
        assertThat(TestSink.events(secondRun)).extracting(e -> e.eventId)
                .contains("after-restore", "shared-event-id");
        assertThat(TestSink.events(secondRun).stream()
                .filter(e -> "shared-event-id".equals(e.eventId))).singleElement()
                .extracting(e -> e.userId).isEqualTo(502);

        send(topic, java.util.stream.IntStream.rangeClosed(1, 96)
                .mapToObj(user -> event("restore-advance-" + user, user,
                        user % 31 + 1, 8_000L)).toList());
        await(Duration.ofSeconds(45), () -> snapshot(secondRun, 7_000L) != null);
        List<MovieEvent> combinedWindow = new ArrayList<>(beforeSavepoint);
        combinedWindow.addAll(afterRestore);
        combinedWindow.add(otherUserSameId);
        combinedWindow.add(continuityMarker);
        assertExactTopK(snapshot(secondRun, 7_000L), combinedWindow, 10);
        assertThat(restored.getJobID()).isNotEqualTo(first.getJobID());
    }

    @Test
    void productionValidationSeamRejectsRealTopicMismatch() throws Exception {
        String topic = topic("mismatch");
        createTopic(topic, PARTITIONS - 1);
        ParameterTool params = ParameterTool.fromMap(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers(), "topic", topic,
                "bridge-mode", "true", "checkpoint-dir", "file:///tmp/recsys-flink-it-checkpoints",
                "bridge-replay-cutoff-ms", "1",
                "bridge-reference-time-ms", "1",
                "allow-local-checkpoint-storage", "true"));
        OnlineFeatureStreamingJob.JobConfiguration configuration =
                new OnlineFeatureStreamingJob.JobConfiguration(PARTITIONS, PARTITIONS, 4, 128);
        assertThatThrownBy(() -> OnlineFeatureStreamingJob.validateKafkaTopic(params, configuration))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("23").hasMessageContaining("24");
    }

    private JobClient start(String topic, String run, int operatorParallelism, String savepoint) throws Exception {
        // The restore path must travel in the environment's configuration: when the job is
        // submitted, PipelineExecutorUtils.getJobGraph overwrites the job graph's restore settings
        // with the configuration's, so StreamGraph.setSavepointRestoreSettings is silently dropped.
        org.apache.flink.configuration.Configuration environment = new org.apache.flink.configuration.Configuration();
        if (savepoint != null) {
            SavepointRestoreSettings.toConfiguration(SavepointRestoreSettings.forPath(savepoint), environment);
        }
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(PARTITIONS, environment);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(500L);
        Map<String, String> values = new java.util.HashMap<>();
        values.put("bootstrap.servers", KAFKA.getBootstrapServers()); values.put("topic", topic);
        values.put("bridge-mode", "true"); values.put("checkpoint-dir", "file:///tmp/recsys-flink-it-checkpoints");
        values.put("bridge-replay-cutoff-ms", "1");
        values.put("bridge-reference-time-ms", "1");
        values.put("allow-local-checkpoint-storage", "true"); values.put("group.id", "partition-contract");
        values.put("expected-topic-partitions", "24"); values.put("source-parallelism", "24");
        values.put("operator-parallelism", Integer.toString(operatorParallelism)); values.put("max-parallelism", "128");
        values.put("kafka.partition.discovery.interval.ms", "100");
        ParameterTool params = ParameterTool.fromMap(values);
        OnlineFeatureStreamingJob.JobConfiguration configuration =
                OnlineFeatureStreamingJob.validateConfiguration(params);
        OnlineFeatureStreamingJob.PartitionGraph graph = OnlineFeatureStreamingJob.buildPartitionGraph(
                OnlineFeatureStreamingJob.buildEventStream(env, params, configuration), configuration,
                10, PARTITIONS, 1_000L, 100L, 200L);
        // The environment defaults to 24, but dedup runs at operatorParallelism; a mismatch inserts
        // a REBALANCE edge that scatters one user's records across sink subtasks and destroys the
        // per-user order this test asserts. Matching parallelism makes the edge FORWARD.
        graph.events().addSink(new TestSink<>(run, "events")).name("test-events-" + run)
                .setParallelism(operatorParallelism);
        graph.snapshots().addSink(new TestSink<>(run, "snapshots")).name("test-snapshots-" + run)
                .setParallelism(operatorParallelism);
        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobName("partition-contract-" + run);
        JobClient client = env.executeAsync(streamGraph);
        jobs.add(client);
        return client;
    }

    private static Set<Integer> send(String topic, List<MovieEvent> events) throws Exception {
        Set<Integer> partitions = ConcurrentHashMap.newKeySet();
        try (KafkaProducer<String, String> producer = producer()) {
            for (MovieEvent event : events) {
                partitions.add(producer.send(new ProducerRecord<>(topic,
                                Integer.toString(event.userId), json(event)))
                        .get(10, TimeUnit.SECONDS).partition());
            }
        }
        return partitions;
    }

    private static KafkaProducer<String, String> producer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(properties);
    }

    private static void createTopic(String topic, int partitions) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)))
                    .all().get(20, TimeUnit.SECONDS);
        }
    }

    private static void await(Duration timeout, BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) Thread.sleep(100L);
        assertThat(condition.getAsBoolean()).isTrue();
    }

    private static OnlineFeatureStreamingJob.TopKSnapshot snapshot(String run, long windowEnd) {
        return TestSink.snapshots(run).stream()
                .filter(value -> value.updatedAtMillis == windowEnd)
                .findFirst().orElse(null);
    }

    private static void assertExactTopK(OnlineFeatureStreamingJob.TopKSnapshot snapshot,
                                        List<MovieEvent> accepted, int topK) {
        Map<Integer, Long> scores = accepted.stream().collect(Collectors.groupingBy(
                event -> event.movieId, Collectors.summingLong(MovieEvent::engagementWeight)));
        List<Map.Entry<Integer, Long>> expected = scores.entrySet().stream()
                .sorted(Map.Entry.<Integer, Long>comparingByValue(java.util.Comparator.reverseOrder())
                        .thenComparing(Map.Entry::getKey))
                .limit(topK).toList();
        assertThat(snapshot.movies).hasSize(topK);
        assertThat(snapshot.movies).extracting(movie -> movie.movieId)
                .containsExactlyElementsOf(expected.stream().map(Map.Entry::getKey).toList())
                .doesNotHaveDuplicates();
        assertThat(snapshot.movies).extracting(movie -> movie.score)
                .containsExactlyElementsOf(expected.stream().map(Map.Entry::getValue).toList());
        assertThat(snapshot.movies.stream().map(movie ->
                OnlineFeatureStreamingJob.movieBucket(movie.movieId, PARTITIONS)).distinct().count())
                .as("the exact result must merge candidates from multiple partial Top-K buckets")
                .isGreaterThan(1L);
    }

    private static List<MovieEvent> scoredEvents(String prefix, int firstUser, int firstMovie,
                                                  int lastMovie, long timestamp, boolean descending) {
        List<MovieEvent> events = new ArrayList<>();
        int user = firstUser;
        for (int movie = firstMovie; movie <= lastMovie; movie++) {
            int repetitions = descending ? lastMovie - movie + 1 : movie;
            for (int repeat = 0; repeat < repetitions; repeat++) {
                events.add(event(prefix + '-' + movie + '-' + repeat, user++, movie, timestamp));
            }
        }
        return events;
    }

    private static MovieEvent event(String id, int user, int movie, long timestamp) {
        MovieEvent event = new MovieEvent();
        event.eventId = id; event.userId = user; event.movieId = movie;
        event.eventType = "click"; event.eventTimeMillis = timestamp;
        return event;
    }

    private static String json(MovieEvent event) {
        return "{\"eventId\":\"" + event.eventId + "\",\"userId\":" + event.userId
                + ",\"movieId\":" + event.movieId + ",\"eventType\":\"click\",\"eventTimeMillis\":"
                + event.eventTimeMillis + "}";
    }

    private static String topic(String prefix) { return prefix + '-' + UUID.randomUUID(); }

    static final class TestSink<T> extends RichSinkFunction<T> {
        private static final Map<String, CopyOnWriteArrayList<Object>> VALUES = new ConcurrentHashMap<>();
        private final String key;
        TestSink(String run, String stream) { key = run + ':' + stream; }
        @Override public void invoke(T value, Context context) { VALUES.computeIfAbsent(key,
                ignored -> new CopyOnWriteArrayList<>()).add(value); }
        static List<MovieEvent> events(String run) { return values(run + ":events"); }
        static List<OnlineFeatureStreamingJob.TopKSnapshot> snapshots(String run) { return values(run + ":snapshots"); }
        @SuppressWarnings("unchecked") private static <T> List<T> values(String key) {
            return (List<T>) (List<?>) VALUES.computeIfAbsent(key, ignored -> new CopyOnWriteArrayList<>());
        }
        static void clear() { VALUES.clear(); }
    }
}
