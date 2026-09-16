package com.demo.retrieval.measurement;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.model.FeedbackRequest;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.OptionalDouble;

@Service
public class RecommendationMeasurementService {
    private static final Logger log = LoggerFactory.getLogger(RecommendationMeasurementService.class);
    private static final Set<String> ENDPOINTS =
        Set.of("recommend", "feedback", "predict", "embedding", "profile");
    private static final Set<String> STAGES =
        Set.of("hydration", "redis_fetch", "scoring", "selection", "side_effects");
    private static final Set<String> FILTER_REASONS =
        Set.of("expired", "muted_product_type", "muted_genre", "muted_keyword", "muted_title", "unknown");
    private static final List<String> FEEDBACK_SIGNALS = List.of(
        "request_id", "rating", "negative_feedback_reason", "dwell_millis", "completion_rate");

    private final MeterRegistry registry;
    private final Duration[] latencyBuckets;
    private final Duration percentileWindow;
    private final FeatureCache featureCache;
    private final Map<String, ThroughputWindow> throughputByEndpoint = new LinkedHashMap<>();
    private final String safetyPolicyVersion;
    private final boolean noOp;
    private final Object freshnessLock = new Object();
    private final Object safetyLock = new Object();
    private final Object feedbackLock = new Object();
    private final Object requestLock = new Object();
    private long freshnessTotal;
    private long freshnessObserved;
    private long freshnessFresh;
    private long evaluatedCandidates;
    private final Map<String, Long> filterDecisionCounts = new LinkedHashMap<>();
    private long feedbackTotal;
    private final Map<String, Long> feedbackPresenceCounts = new LinkedHashMap<>();

    @Autowired
    public RecommendationMeasurementService(
        MeterRegistry registry, RecommendationProperties properties, FeatureCache featureCache
    ) {
        this(
            registry,
            latencyBuckets(properties),
            percentileWindow(properties),
            safetyPolicyVersion(properties),
            featureCache,
            throughputWindowSeconds(properties),
            System::currentTimeMillis,
            false
        );
    }

    private RecommendationMeasurementService(
        MeterRegistry registry, Duration[] latencyBuckets, Duration percentileWindow,
        String safetyPolicyVersion, FeatureCache featureCache, int throughputWindowSeconds,
        LongSupplier nowMillis, boolean noOp
    ) {
        this.registry = registry;
        this.latencyBuckets = latencyBuckets;
        this.percentileWindow = percentileWindow;
        this.safetyPolicyVersion = safetyPolicyVersion;
        this.featureCache = featureCache;
        this.noOp = noOp;
        ENDPOINTS.stream().sorted().forEach(endpoint -> throughputByEndpoint.put(
            endpoint, new ThroughputWindow(throughputWindowSeconds, nowMillis)));
    }

    public static RecommendationMeasurementService noOp() {
        return new RecommendationMeasurementService(
            new SimpleMeterRegistry(), new Duration[0], Duration.ofSeconds(300), "unknown",
            null, 60, System::currentTimeMillis, true);
    }

    public void recordRequest(String endpoint, Duration duration, boolean error) {
        recordRequest(endpoint, duration, error, false);
    }

    public void recordRequest(String endpoint, Duration duration, boolean error, boolean timeout) {
        try {
            if (noOp || !ENDPOINTS.contains(endpoint)) {
                return;
            }
            synchronized (requestLock) {
                requestTimer(endpoint).record(safeDuration(duration));
                throughputByEndpoint.get(endpoint).record();
                if (error) {
                    incrementRequestCounter("recommendation.request.errors", endpoint);
                }
                if (timeout) {
                    incrementRequestCounter("recommendation.request.timeouts", endpoint);
                }
            }
        } catch (RuntimeException e) {
            log.warn("Unable to record recommendation request measurement", e);
        }
    }

    public <T> T timeStage(String stage, Supplier<T> operation) {
        if (noOp || !STAGES.contains(stage)) {
            return operation.get();
        }
        long started = System.nanoTime();
        try {
            return operation.get();
        } finally {
            recordStage(stage, Duration.ofNanos(System.nanoTime() - started));
        }
    }

    public void recordFreshness(Collection<MovieProfile> selectedProfiles) {
        try {
            if (noOp || selectedProfiles == null) {
                return;
            }
            long total = 0;
            long observed = 0;
            long fresh = 0;
            for (MovieProfile profile : selectedProfiles) {
                total++;
                if (profile == null) {
                    continue;
                }
                observed++;
                if (profile.isNewRelease()) {
                    fresh++;
                }
                Counter.builder("recommendation.freshness.exposures")
                    .tags("source", "boolean_new_release", "fresh", Boolean.toString(profile.isNewRelease()))
                    .register(registry)
                    .increment();
            }
            synchronized (freshnessLock) {
                freshnessTotal += total;
                freshnessObserved += observed;
                freshnessFresh += fresh;
            }
        } catch (RuntimeException e) {
            log.warn("Unable to record recommendation freshness measurement", e);
        }
    }

    public void recordFilterDecisions(Collection<FilterDecision> decisions) {
        // This legacy entry point does not know how many candidates were evaluated,
        // so retain the decisions while leaving their rates intentionally undefined.
        recordFilterDecisions(0, decisions);
    }

    public void recordFilterDecisions(int candidatesEvaluated, Collection<FilterDecision> decisions) {
        try {
            if (noOp) {
                return;
            }
            synchronized (safetyLock) {
                evaluatedCandidates += Math.max(0, candidatesEvaluated);
                if (decisions == null) {
                    return;
                }
                for (FilterDecision decision : decisions) {
                    String reason = decision == null ? "unknown" : boundedReason(decision.reason());
                    Counter.builder("recommendation.filter.decisions")
                        .tag("reason", reason)
                        .register(registry)
                        .increment();
                    filterDecisionCounts.merge(reason, 1L, Long::sum);
                }
            }
        } catch (RuntimeException e) {
            log.warn("Unable to record recommendation filter measurement", e);
        }
    }

    public void recordFeedbackCoverage(FeedbackRequest request) {
        try {
            if (noOp || request == null) {
                return;
            }
            synchronized (feedbackLock) {
                feedbackTotal++;
                recordPresent("request_id", request.requestId() != null);
                recordPresent("rating", request.rating() != null);
                recordPresent("negative_feedback_reason", request.negativeFeedbackReason() != null);
                recordPresent("dwell_millis", request.dwellMillis() != null);
                recordPresent("completion_rate", request.completionRate() != null);
            }
        } catch (RuntimeException e) {
            log.warn("Unable to record recommendation feedback coverage", e);
        }
    }

    public MeasurementSnapshot snapshot() {
        return new MeasurementSnapshot(
            "2.1",
            snapshotSection("latency", this::latencySnapshot, this::unavailableLatency),
            snapshotSection("throughput", this::throughputSnapshot, this::unavailableThroughput),
            snapshotSection("cache", this::cacheSnapshot, this::unavailableCache),
            snapshotSection("freshness", this::freshnessSnapshot, this::unavailableFreshness),
            snapshotSection("safety", this::safetySnapshot, this::unavailableSafety),
            snapshotSection("feedback coverage", this::feedbackSnapshot, this::unavailableFeedback)
        );
    }

    private void recordStage(String stage, Duration duration) {
        try {
            Timer.builder("recommendation.stage.latency")
                .tag("stage", stage)
                .serviceLevelObjectives(latencyBuckets)
                .publishPercentiles(0.50, 0.95, 0.99)
                .distributionStatisticExpiry(percentileWindow)
                .distributionStatisticBufferLength(5)
                .register(registry)
                .record(safeDuration(duration));
        } catch (RuntimeException e) {
            log.warn("Unable to record recommendation stage measurement", e);
        }
    }

    private Timer requestTimer(String endpoint) {
        return Timer.builder("recommendation.request.latency")
            .tag("endpoint", endpoint)
            .serviceLevelObjectives(latencyBuckets)
            .publishPercentiles(0.50, 0.95, 0.99)
            .distributionStatisticExpiry(percentileWindow)
            .distributionStatisticBufferLength(5)
            .register(registry);
    }

    private void incrementRequestCounter(String name, String endpoint) {
        Counter.builder(name).tag("endpoint", endpoint).register(registry).increment();
    }

    private void recordPresent(String signal, boolean present) {
        if (!present || !FEEDBACK_SIGNALS.contains(signal)) {
            return;
        }
        Counter.builder("recommendation.feedback.presence")
            .tag("signal", signal)
            .register(registry)
            .increment();
        feedbackPresenceCounts.merge(signal, 1L, Long::sum);
    }

    private Map<String, Object> latencySnapshot() {
        Map<String, Object> endpoints = new LinkedHashMap<>();
        synchronized (requestLock) {
            ENDPOINTS.stream().sorted().forEach(endpoint -> endpoints.put(endpoint, endpointLatency(endpoint)));
        }
        Map<String, Object> stages = new LinkedHashMap<>();
        STAGES.stream().sorted().forEach(stage -> stages.put(stage, stageLatency(stage)));
        Map<String, Object> latency = new LinkedHashMap<>();
        latency.put("availability", "available");
        latency.put("unit", "milliseconds");
        latency.put("endpoints", endpoints);
        latency.put("stages", stages);
        return latency;
    }

    private Map<String, Object> endpointLatency(String endpoint) {
        Timer timer = registry.find("recommendation.request.latency").tag("endpoint", endpoint).timer();
        long count = timer == null ? 0L : timer.count();
        long errors = Math.round(counterCount("recommendation.request.errors", endpoint));
        long timeouts = Math.round(counterCount("recommendation.request.timeouts", endpoint));
        Map<String, Object> values = timerLatency(timer);
        values.put("count", count);
        values.put("errorCount", errors);
        values.put("errorRate", rate(errors, count));
        values.put("timeoutCount", timeouts);
        values.put("timeoutRate", rate(timeouts, count));
        return values;
    }

    private Map<String, Object> stageLatency(String stage) {
        Timer timer = registry.find("recommendation.stage.latency").tag("stage", stage).timer();
        Map<String, Object> values = timerLatency(timer);
        values.put("count", timer == null ? 0L : timer.count());
        return values;
    }

    private Map<String, Object> timerLatency(Timer timer) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("p50", percentile(timer, 0.50));
        values.put("p95", percentile(timer, 0.95));
        values.put("p99", percentile(timer, 0.99));
        return values;
    }

    private Map<String, Object> emptyLatency() {
        Map<String, Object> endpoints = new LinkedHashMap<>();
        ENDPOINTS.stream().sorted().forEach(endpoint -> endpoints.put(endpoint, emptyEndpointLatency()));
        Map<String, Object> stages = new LinkedHashMap<>();
        STAGES.stream().sorted().forEach(stage -> stages.put(stage, emptyStageLatency()));
        Map<String, Object> latency = new LinkedHashMap<>();
        latency.put("availability", "available");
        latency.put("unit", "milliseconds");
        latency.put("endpoints", endpoints);
        latency.put("stages", stages);
        return latency;
    }

    private Map<String, Object> unavailableLatency() {
        Map<String, Object> endpoints = new LinkedHashMap<>();
        ENDPOINTS.stream().sorted().forEach(endpoint -> endpoints.put(endpoint, unavailableEndpointLatency()));
        Map<String, Object> stages = new LinkedHashMap<>();
        STAGES.stream().sorted().forEach(stage -> stages.put(stage, unavailableStageLatency()));
        Map<String, Object> latency = new LinkedHashMap<>();
        latency.put("availability", "unavailable");
        latency.put("unit", "milliseconds");
        latency.put("endpoints", endpoints);
        latency.put("stages", stages);
        return latency;
    }

    private Map<String, Object> emptyEndpointLatency() {
        Map<String, Object> values = timerLatency(null);
        values.put("count", 0L);
        values.put("errorCount", 0L);
        values.put("errorRate", null);
        values.put("timeoutCount", 0L);
        values.put("timeoutRate", null);
        return values;
    }

    private Map<String, Object> emptyStageLatency() {
        Map<String, Object> values = timerLatency(null);
        values.put("count", 0L);
        return values;
    }

    private Map<String, Object> unavailableEndpointLatency() {
        Map<String, Object> values = timerLatency(null);
        values.put("count", null);
        values.put("errorCount", null);
        values.put("errorRate", null);
        values.put("timeoutCount", null);
        values.put("timeoutRate", null);
        return values;
    }

    private Map<String, Object> unavailableStageLatency() {
        Map<String, Object> values = timerLatency(null);
        values.put("count", null);
        return values;
    }

    private Map<String, Object> throughputSnapshot() {
        Map<String, Object> endpoints = new LinkedHashMap<>();
        throughputByEndpoint.forEach((endpoint, window) -> endpoints.put(endpoint, window.snapshot()));
        Map<String, Object> throughput = new LinkedHashMap<>();
        throughput.put("availability", "available");
        throughput.put("unit", "requests_per_second");
        throughput.put("endpoints", endpoints);
        return throughput;
    }

    private Map<String, Object> unavailableThroughput() {
        Map<String, Object> endpoints = new LinkedHashMap<>();
        ENDPOINTS.stream().sorted().forEach(endpoint -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("qps", null);
            values.put("windowRequests", null);
            values.put("windowSeconds", null);
            values.put("observedSeconds", null);
            endpoints.put(endpoint, values);
        });
        Map<String, Object> throughput = new LinkedHashMap<>();
        throughput.put("availability", "unavailable");
        throughput.put("unit", "requests_per_second");
        throughput.put("endpoints", endpoints);
        return throughput;
    }

    private Map<String, Object> cacheSnapshot() {
        Map<String, Object> caches = new LinkedHashMap<>();
        if (featureCache != null) {
            featureCache.stats().forEach((name, stats) -> {
                Map<String, Object> values = new LinkedHashMap<>();
                values.put("hitCount", stats.hitCount());
                values.put("missCount", stats.missCount());
                values.put("hitRate", rate(stats.hitCount(), stats.hitCount() + stats.missCount()));
                values.put("evictionCount", stats.evictionCount());
                values.put("estimatedSize", stats.estimatedSize());
                caches.put(name, values);
            });
        }
        Map<String, Object> cache = new LinkedHashMap<>();
        cache.put("availability", "available");
        cache.put("caches", caches);
        return cache;
    }

    private Map<String, Object> unavailableCache() {
        Map<String, Object> cache = new LinkedHashMap<>();
        cache.put("availability", "unavailable");
        cache.put("caches", new LinkedHashMap<String, Object>());
        return cache;
    }

    private Map<String, Object> freshnessSnapshot() {
        synchronized (freshnessLock) {
            Map<String, Object> freshness = new LinkedHashMap<>();
            freshness.put("exposures", freshnessTotal);
            freshness.put("availability", "available");
            freshness.put("coverage", rate(freshnessObserved, freshnessTotal));
            freshness.put("freshShare", rate(freshnessFresh, freshnessObserved));
            freshness.put("source", "boolean_new_release");
            return freshness;
        }
    }

    private Map<String, Object> safetySnapshot() {
        synchronized (safetyLock) {
            long totalDecisions = filterDecisionCounts.values().stream().mapToLong(Long::longValue).sum();
            Map<String, Object> reasons = new LinkedHashMap<>();
            FILTER_REASONS.stream().sorted().forEach(reason -> {
                long count = filterDecisionCounts.getOrDefault(reason, 0L);
                Map<String, Object> reasonValues = new LinkedHashMap<>();
                reasonValues.put("count", count);
                reasonValues.put("rate", rate(count, evaluatedCandidates));
                reasons.put(reason, reasonValues);
            });
            Map<String, Object> safety = new LinkedHashMap<>();
            safety.put("policyVersion", safetyPolicyVersion);
            safety.put("availability", "available");
            safety.put("evaluatedCandidates", evaluatedCandidates);
            safety.put("totalDecisions", totalDecisions);
            safety.put("reasons", reasons);
            safety.put("unknownShare", rate(filterDecisionCounts.getOrDefault("unknown", 0L), evaluatedCandidates));
            return safety;
        }
    }

    private Map<String, Object> feedbackSnapshot() {
        synchronized (feedbackLock) {
            Map<String, Object> signals = new LinkedHashMap<>();
            FEEDBACK_SIGNALS.forEach(signal -> {
                long present = feedbackPresenceCounts.getOrDefault(signal, 0L);
                Map<String, Object> value = new LinkedHashMap<>();
                value.put("present", present);
                value.put("coverage", rate(present, feedbackTotal));
                signals.put(signal, value);
            });
            Map<String, Object> feedback = new LinkedHashMap<>();
            feedback.put("total", feedbackTotal);
            feedback.put("availability", "available");
            feedback.put("signals", signals);
            return feedback;
        }
    }

    private Map<String, Object> unavailableFreshness() {
        Map<String, Object> freshness = new LinkedHashMap<>();
        freshness.put("availability", "unavailable");
        freshness.put("exposures", null);
        freshness.put("coverage", null);
        freshness.put("freshShare", null);
        freshness.put("source", "boolean_new_release");
        return freshness;
    }

    private Map<String, Object> unavailableSafety() {
        Map<String, Object> reasons = new LinkedHashMap<>();
        FILTER_REASONS.stream().sorted().forEach(reason -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("count", null);
            values.put("rate", null);
            reasons.put(reason, values);
        });
        Map<String, Object> safety = new LinkedHashMap<>();
        safety.put("availability", "unavailable");
        safety.put("policyVersion", safetyPolicyVersion);
        safety.put("evaluatedCandidates", null);
        safety.put("totalDecisions", null);
        safety.put("reasons", reasons);
        safety.put("unknownShare", null);
        return safety;
    }

    private Map<String, Object> unavailableFeedback() {
        Map<String, Object> signals = new LinkedHashMap<>();
        FEEDBACK_SIGNALS.forEach(signal -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("present", null);
            values.put("coverage", null);
            signals.put(signal, values);
        });
        Map<String, Object> feedback = new LinkedHashMap<>();
        feedback.put("availability", "unavailable");
        feedback.put("total", null);
        feedback.put("signals", signals);
        return feedback;
    }

    private Map<String, Object> snapshotSection(
        String section,
        Supplier<Map<String, Object>> snapshot,
        Supplier<Map<String, Object>> unavailable
    ) {
        try {
            return snapshot.get();
        } catch (RuntimeException e) {
            log.warn("Unable to assemble recommendation {} measurement snapshot", section, e);
            return unavailable.get();
        }
    }

    private double counterCount(String name, String endpoint) {
        Counter counter = registry.find(name).tag("endpoint", endpoint).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static Duration[] latencyBuckets(RecommendationProperties properties) {
        if (properties == null || properties.getMeasurements() == null) {
            return new Duration[0];
        }
        return properties.getMeasurements().getLatencyBucketsMs().stream()
            .filter(value -> value != null && value > 0)
            .distinct()
            .sorted(Comparator.naturalOrder())
            .map(Duration::ofMillis)
            .toArray(Duration[]::new);
    }

    private static Duration percentileWindow(RecommendationProperties properties) {
        return properties == null || properties.getMeasurements() == null
            ? Duration.ofSeconds(300)
            : Duration.ofSeconds(properties.getMeasurements().getPercentileWindowSeconds());
    }

    private static int throughputWindowSeconds(RecommendationProperties properties) {
        return properties == null || properties.getMeasurements() == null
            ? 60 : properties.getMeasurements().getThroughputWindowSeconds();
    }

    private static String safetyPolicyVersion(RecommendationProperties properties) {
        return properties == null || properties.getMeasurements() == null
            ? "unknown" : properties.getMeasurements().getSafetyPolicyVersion();
    }

    private static Duration safeDuration(Duration duration) {
        return duration == null || duration.isNegative() ? Duration.ZERO : duration;
    }

    private static String boundedReason(String reason) {
        return FILTER_REASONS.contains(reason) ? reason : "unknown";
    }

    private static Double percentile(Timer timer, double target) {
        if (timer == null) {
            return null;
        }
        OptionalDouble value = java.util.Arrays.stream(timer.takeSnapshot().percentileValues())
            .filter(percentile -> Double.compare(percentile.percentile(), target) == 0)
            .mapToDouble(percentile -> percentile.value(TimeUnit.MILLISECONDS))
            .max();
        return value.isPresent() ? value.getAsDouble() : null;
    }

    private static Double rate(long numerator, long denominator) {
        return denominator == 0 ? null : (double) numerator / denominator;
    }
}
