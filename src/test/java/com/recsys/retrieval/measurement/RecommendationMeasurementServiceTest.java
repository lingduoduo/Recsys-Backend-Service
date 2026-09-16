package com.demo.retrieval.measurement;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.model.FeedbackRequest;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecommendationMeasurementServiceTest {

    @Test
    void recordsOnlyBoundedRequestAndStageMeters() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationMeasurementService measurements = measurements(registry);

        measurements.recordRequest("recommend", Duration.ofMillis(12), false);
        assertEquals(1L, registry.find("recommendation.request.latency")
            .tag("endpoint", "recommend").timer().count());

        assertEquals("result", measurements.timeStage("scoring", () -> "result"));
        measurements.timeStage("unbounded-caller-input", () -> null);

        assertEquals(1L, registry.find("recommendation.stage.latency")
            .tag("stage", "scoring").timer().count());
        assertEquals(0L, registry.find("recommendation.stage.latency")
            .tag("stage", "unbounded-caller-input").timers().size());
    }

    @Test
    void collapsesFreeFormFilterReasonsAndRecordsOnlyFeedbackPresence() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationMeasurementService measurements = measurements(registry);

        measurements.recordFilterDecisions(List.of(new FilterDecision("customer supplied explanation")));
        measurements.recordFeedbackCoverage(new FeedbackRequest(
            "user-42", "item-9", true, 1.0, "request-123", 4.5,
            "not relevant", 900L, 0.7, null));

        assertEquals(1.0, registry.find("recommendation.filter.decisions")
            .tag("reason", "unknown").counter().count());
        assertEquals(1.0, registry.find("recommendation.feedback.presence")
            .tag("signal", "rating").counter().count());
        assertEquals(1.0, registry.find("recommendation.feedback.presence")
            .tag("signal", "negative_feedback_reason").counter().count());
        assertTrue(registry.getMeters().stream().flatMap(meter -> meter.getId().getTags().stream())
            .noneMatch(tag -> tag.getValue().equals("user-42")
                || tag.getValue().equals("item-9")
                || tag.getValue().equals("request-123")
                || tag.getValue().equals("not relevant")));
    }

    @Test
    void snapshotUsesMeasurementSchemaAndLatencyPercentiles() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationMeasurementService measurements = measurements(registry);
        measurements.recordRequest("feedback", Duration.ofMillis(20), false);

        MeasurementSnapshot snapshot = measurements.snapshot();
        Map<String, Object> values = snapshot.asMap();

        assertEquals("2.1", snapshot.schemaVersion());
        Map<?, ?> latency = (Map<?, ?>) values.get("latency");
        Map<?, ?> endpoints = (Map<?, ?>) latency.get("endpoints");
        Map<?, ?> feedback = (Map<?, ?>) endpoints.get("feedback");
        assertTrue(feedback.containsKey("p50"));
        assertTrue(feedback.containsKey("p95"));
        assertTrue(feedback.containsKey("p99"));
    }

    @Test
    void snapshotKeepsEndpointLatencySeparateInMillisecondsWithErrorAndTimeoutRates() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationMeasurementService measurements = measurements(registry);

        measurements.recordRequest("recommend", Duration.ofMillis(10), false);
        measurements.recordRequest("recommend", Duration.ofMillis(1000), true, true);
        measurements.recordRequest("feedback", Duration.ofMillis(20), false);
        measurements.timeStage("hydration", () -> "hydrated");

        Map<?, ?> latency = (Map<?, ?>) measurements.snapshot().asMap().get("latency");
        Map<?, ?> endpoints = (Map<?, ?>) latency.get("endpoints");
        Map<?, ?> recommend = (Map<?, ?>) endpoints.get("recommend");
        Map<?, ?> feedback = (Map<?, ?>) endpoints.get("feedback");
        Map<?, ?> stages = (Map<?, ?>) latency.get("stages");
        Map<?, ?> hydration = (Map<?, ?>) stages.get("hydration");

        assertEquals("milliseconds", latency.get("unit"));
        assertEquals(2L, recommend.get("count"));
        assertEquals(1L, recommend.get("errorCount"));
        assertEquals(0.5, recommend.get("errorRate"));
        assertEquals(1L, recommend.get("timeoutCount"));
        assertEquals(0.5, recommend.get("timeoutRate"));
        assertEquals(1L, feedback.get("count"));
        assertEquals(0L, feedback.get("errorCount"));
        assertTrue(((Number) feedback.get("p50")).doubleValue() < 100.0);
        assertEquals(1L, hydration.get("count"));
    }

    @Test
    void snapshotIncludesSafetyDenominatorPolicyAndFeedbackCoverageDenominator() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationMeasurementService measurements = measurements(registry);

        measurements.recordFilterDecisions(4, List.of(
            new FilterDecision("expired"),
            new FilterDecision("muted_product_type"),
            new FilterDecision("muted_genre"),
            new FilterDecision("muted_keyword"),
            new FilterDecision("muted_title"),
            new FilterDecision("unknown")
        ));
        measurements.recordFeedbackCoverage(new FeedbackRequest("u1", "i1", true, 1.0));
        measurements.recordFeedbackCoverage(new FeedbackRequest(
            "u2", "i2", true, 1.0, "r2", 5.0, null, null, null, null));

        Map<String, Object> values = measurements.snapshot().asMap();
        Map<?, ?> safety = (Map<?, ?>) values.get("safety");
        Map<?, ?> feedbackCoverage = (Map<?, ?>) values.get("feedbackCoverage");
        Map<?, ?> signals = (Map<?, ?>) feedbackCoverage.get("signals");
        Map<?, ?> rating = (Map<?, ?>) signals.get("rating");
        Map<?, ?> dwell = (Map<?, ?>) signals.get("dwell_millis");

        assertEquals("catalog-filter-v1", safety.get("policyVersion"));
        assertEquals(4L, safety.get("evaluatedCandidates"));
        assertEquals(6L, safety.get("totalDecisions"));
        assertEquals(0.25, safety.get("unknownShare"));
        assertEquals(2L, feedbackCoverage.get("total"));
        assertEquals(1L, rating.get("present"));
        assertEquals(0.5, rating.get("coverage"));
        assertEquals(0L, dwell.get("present"));
        assertEquals(0.0, dwell.get("coverage"));
    }

    @Test
    void concurrentFreshnessSnapshotsKeepCoverageAndFreshShareWithinBounds() throws Exception {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationMeasurementService measurements = measurements(registry);
        RecommendationProperties.MovieProfile fresh = new RecommendationProperties.MovieProfile();
        fresh.setNewRelease(true);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            executor.submit(() -> recordFreshness(start, measurements, fresh));
            executor.submit(() -> recordFreshness(start, measurements, fresh));
            start.countDown();
            for (int i = 0; i < 500; i++) {
                Map<?, ?> freshness = (Map<?, ?>) measurements.snapshot().asMap().get("freshness");
                assertInUnitInterval((Double) freshness.get("coverage"));
                assertInUnitInterval((Double) freshness.get("freshShare"));
            }
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void concurrentRequestSnapshotsKeepCountsAndRatesConsistent() throws Exception {
        RecommendationMeasurementService measurements = measurements(new SimpleMeterRegistry());
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            executor.submit(() -> recordRequests(start, measurements, true, true));
            executor.submit(() -> recordRequests(start, measurements, true, false));
            start.countDown();
            for (int i = 0; i < 1_000; i++) {
                Map<?, ?> latency = (Map<?, ?>) measurements.snapshot().asMap().get("latency");
                Map<?, ?> recommend = (Map<?, ?>) ((Map<?, ?>) latency.get("endpoints")).get("recommend");
                long count = (Long) recommend.get("count");
                long errors = (Long) recommend.get("errorCount");
                long timeouts = (Long) recommend.get("timeoutCount");
                assertTrue(errors <= count, "errors must not exceed request count");
                assertTrue(timeouts <= count, "timeouts must not exceed request count");
                assertInUnitInterval((Double) recommend.get("errorRate"));
                assertInUnitInterval((Double) recommend.get("timeoutRate"));
            }
        } finally {
            executor.shutdown();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @Test
    void snapshotMarksOnlyTheUnavailableSectionAsUnavailable() {
        MeterRegistry registry = mock(MeterRegistry.class);
        when(registry.find("recommendation.request.latency"))
            .thenThrow(new IllegalStateException("registry unavailable"));
        RecommendationProperties properties = new RecommendationProperties();
        RecommendationMeasurementService measurements = new RecommendationMeasurementService(
            registry, properties, new FeatureCache(properties));

        Map<String, Object> snapshot = measurements.snapshot().asMap();
        Map<?, ?> latency = (Map<?, ?>) snapshot.get("latency");
        Map<?, ?> recommend = (Map<?, ?>) ((Map<?, ?>) latency.get("endpoints")).get("recommend");
        Map<?, ?> freshness = (Map<?, ?>) snapshot.get("freshness");
        Map<?, ?> safety = (Map<?, ?>) snapshot.get("safety");
        Map<?, ?> feedback = (Map<?, ?>) snapshot.get("feedbackCoverage");
        Map<?, ?> throughput = (Map<?, ?>) snapshot.get("throughput");
        Map<?, ?> cache = (Map<?, ?>) snapshot.get("cache");

        assertEquals("unavailable", latency.get("availability"));
        assertNull(recommend.get("count"));
        assertNull(recommend.get("errorRate"));
        assertEquals("available", freshness.get("availability"));
        assertEquals("available", safety.get("availability"));
        assertEquals("available", feedback.get("availability"));
        assertEquals("available", throughput.get("availability"));
        assertEquals("available", cache.get("availability"));
    }

    private void recordFreshness(
        CountDownLatch start,
        RecommendationMeasurementService measurements,
        RecommendationProperties.MovieProfile fresh
    ) {
        try {
            start.await();
            for (int i = 0; i < 500; i++) {
                measurements.recordFreshness(List.of(fresh));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private void recordRequests(
        CountDownLatch start,
        RecommendationMeasurementService measurements,
        boolean error,
        boolean timeout
    ) {
        try {
            start.await();
            for (int i = 0; i < 1_000; i++) {
                measurements.recordRequest("recommend", Duration.ofMillis(1), error, timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private void assertInUnitInterval(Double value) {
        if (value != null) {
            assertTrue(value >= 0.0 && value <= 1.0, "ratio must be within [0, 1]");
        }
    }

    @Test
    void decaysLatencyPercentilesWhileKeepingCountsCumulative() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationProperties properties = new RecommendationProperties();
        properties.getMeasurements().setPercentileWindowSeconds(1);
        RecommendationMeasurementService measurements =
            new RecommendationMeasurementService(registry, properties, new FeatureCache(properties));

        measurements.recordRequest("recommend", Duration.ofMillis(900), false);
        assertTrue(((Number) endpoint(measurements, "recommend").get("p99")).doubleValue() > 100.0);

        await(2_000);
        measurements.recordRequest("recommend", Duration.ofMillis(1), false);
        Map<?, ?> after = endpoint(measurements, "recommend");

        assertTrue(((Number) after.get("p99")).doubleValue() < 100.0,
            "percentiles must decay out of the configured window");
        assertEquals(2L, after.get("count"), "counts stay cumulative");
        assertEquals(0.0, after.get("errorRate"), "error rate stays cumulative");
    }

    private Map<?, ?> endpoint(RecommendationMeasurementService measurements, String name) {
        Map<?, ?> latency = (Map<?, ?>) measurements.snapshot().asMap().get("latency");
        return (Map<?, ?>) ((Map<?, ?>) latency.get("endpoints")).get(name);
    }

    private void await(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Test
    void snapshotReportsThroughputAndCacheSectionsAtSchemaTwoOne() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        RecommendationProperties properties = new RecommendationProperties();
        FeatureCache cache = new FeatureCache(properties);
        RecommendationMeasurementService measurements =
            new RecommendationMeasurementService(registry, properties, cache);

        measurements.recordRequest("recommend", Duration.ofMillis(5), false);
        measurements.recordRequest("recommend", Duration.ofMillis(5), false);
        cache.getItemVector("missing");
        cache.putItemVector("present", new double[] {1.0});
        cache.getItemVector("present");

        Map<String, Object> values = measurements.snapshot().asMap();
        assertEquals("2.1", measurements.snapshot().schemaVersion());

        Map<?, ?> throughput = (Map<?, ?>) values.get("throughput");
        assertEquals("available", throughput.get("availability"));
        Map<?, ?> recommend = (Map<?, ?>) ((Map<?, ?>) throughput.get("endpoints")).get("recommend");
        assertEquals(2L, recommend.get("windowRequests"));
        assertEquals(60, recommend.get("windowSeconds"));
        Map<?, ?> feedback = (Map<?, ?>) ((Map<?, ?>) throughput.get("endpoints")).get("feedback");
        assertNull(feedback.get("qps"), "an endpoint with no traffic has an undefined rate");

        Map<?, ?> caches = (Map<?, ?>) values.get("cache");
        assertEquals("available", caches.get("availability"));
        Map<?, ?> vectors = (Map<?, ?>) ((Map<?, ?>) caches.get("caches")).get("item_vectors");
        assertEquals(1L, vectors.get("hitCount"));
        assertEquals(1L, vectors.get("missCount"));
        assertEquals(0.5, vectors.get("hitRate"));
        Map<?, ?> rewards = (Map<?, ?>) ((Map<?, ?>) caches.get("caches")).get("reward_stats");
        assertNull(rewards.get("hitRate"), "a cache with no lookups has an undefined hit rate");
    }

    @Test
    void noOpMeasurementsReportEmptyThroughputAndCacheSections() {
        Map<String, Object> values = RecommendationMeasurementService.noOp().snapshot().asMap();

        Map<?, ?> throughput = (Map<?, ?>) values.get("throughput");
        Map<?, ?> recommend = (Map<?, ?>) ((Map<?, ?>) throughput.get("endpoints")).get("recommend");
        assertNull(recommend.get("qps"));
        assertTrue(((Map<?, ?>) ((Map<?, ?>) values.get("cache")).get("caches")).isEmpty());
    }

    private RecommendationMeasurementService measurements(SimpleMeterRegistry registry) {
        RecommendationProperties properties = new RecommendationProperties();
        return new RecommendationMeasurementService(registry, properties, new FeatureCache(properties));
    }
}
