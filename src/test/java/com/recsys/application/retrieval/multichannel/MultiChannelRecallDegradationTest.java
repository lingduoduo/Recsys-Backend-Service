package com.recsys.application.retrieval.multichannel;

import com.recsys.domain.item.MovieCandidate;
import com.recsys.domain.recommendation.RecommendationQuery;
import com.recsys.application.retrieval.RecallChannel;
import com.recsys.resilience.FaultInjector;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;
import static com.recsys.application.retrieval.multichannel.RecallResult.DegradationOutcome.*;

class MultiChannelRecallDegradationTest {

    private static RecommendationQuery query() {
        return new RecommendationQuery("1", 10, Set.of(), null);
    }

    /** A channel whose recall always throws — used to force a degraded non-primary channel. */
    private static final class FailingChannel implements RecallChannel {
        private final String name;
        FailingChannel(String name) { this.name = name; }
        @Override public String name() { return name; }
        @Override public List<MovieCandidate> recall(RecommendationQuery q, int limit) {
            throw new IllegalStateException("boom");
        }
        @Override public List<MovieCandidate> recallPrimary(RecommendationQuery q, int limit) {
            throw new IllegalStateException("boom");
        }
    }

    private static final class OkChannel implements RecallChannel {
        @Override public String name() { return "ok"; }
        @Override public List<MovieCandidate> recall(RecommendationQuery q, int limit) {
            return List.of(new MovieCandidate("100", 0.9, "ok", java.util.Map.of()));
        }
        @Override public List<MovieCandidate> recallPrimary(RecommendationQuery q, int limit) {
            return recall(q, limit);
        }
    }

    @Test
    void nonPrimaryChannelErrorIsRecordedAndReportedButStillServes() {
        RecallDegradationMetrics metrics = new RecallDegradationMetrics();
        MultiChannelRecallService service = new MultiChannelRecallService(
                List.of(new OkChannel(), new FailingChannel("trending")),
                new ChannelHealthMonitor(),
                java.util.concurrent.Executors.newFixedThreadPool(2),
                200L, FaultInjector.NOOP, null,
                com.recsys.application.retrieval.coldstart.QuotaPolicy.defaultMovie(),
                metrics);

        RecallResult result = service.recallDetailed(query(), 10);

        assertThat(result.degradedChannels()).contains("trending");
        assertThat(result.candidates()).isNotEmpty(); // ok channel still served
        assertThat(result.outcome()).isEqualTo(PARTIAL);
        RecallDegradationMetrics.Snapshot s = metrics.snapshot();
        assertThat(s.totalRecalls()).isEqualTo(1);
        assertThat(s.degradedRecalls()).isEqualTo(1);
        assertThat(s.byChannel()).containsKey("trending");
    }

    @Test
    void multipleDegradedChannelsInOneRequestCountAsOneDegradedRequest() {
        // Regression for the bug where degradedRecalls incremented once per degraded
        // *channel* instead of once per degraded *request*, letting degradedRatio exceed 1.0.
        RecallDegradationMetrics metrics = new RecallDegradationMetrics();
        MultiChannelRecallService service = new MultiChannelRecallService(
                List.of(new FailingChannel("trending"), new FailingChannel("popularity")),
                new ChannelHealthMonitor(),
                java.util.concurrent.Executors.newFixedThreadPool(2),
                200L, FaultInjector.NOOP, null,
                com.recsys.application.retrieval.coldstart.QuotaPolicy.defaultMovie(),
                metrics);

        RecallResult result = service.recallDetailed(query(), 10);

        assertThat(result.degradedChannels()).containsExactlyInAnyOrder("trending", "popularity");
        assertThat(result.outcome()).isEqualTo(ALL_CHANNELS);
        RecallDegradationMetrics.Snapshot s = metrics.snapshot();
        assertThat(s.totalRecalls()).isEqualTo(1);
        assertThat(s.degradedRecalls()).isEqualTo(1);
        assertThat(s.degradedRatio()).isLessThanOrEqualTo(1.0);
        assertThat(s.degradedRatio()).isEqualTo(1.0, within(1e-9));
    }

    @Test
    void healthyEmptyRecallIsNotClassifiedAsDegraded() {
        RecallChannel empty = new RecallChannel() {
            @Override public String name() { return "empty"; }
            @Override public List<MovieCandidate> recall(RecommendationQuery q, int limit) { return List.of(); }
            @Override public List<MovieCandidate> recallPrimary(RecommendationQuery q, int limit) { return List.of(); }
        };
        MultiChannelRecallService service = new MultiChannelRecallService(List.of(empty));

        RecallResult result = service.recallDetailed(query(), 10);

        assertThat(result.candidates()).isEmpty();
        assertThat(result.degradedChannels()).isEmpty();
        assertThat(result.outcome()).isEqualTo(HEALTHY);
    }

    @Test
    void bulkheadRejectionIsClassifiedAsRejected() {
        // A pool with 0 queue slots (SynchronousQueue) and one busy thread rejects the second task.
        ThreadPoolExecutor tiny = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS, new SynchronousQueue<>());
        RecallDegradationMetrics metrics = new RecallDegradationMetrics();
        // Two channels both dispatched; one is guaranteed rejected under saturation.
        MultiChannelRecallService service = new MultiChannelRecallService(
                List.of(new SlowChannel(), new SlowChannel2()),
                new ChannelHealthMonitor(), tiny, 5000L, FaultInjector.NOOP, null,
                com.recsys.application.retrieval.coldstart.QuotaPolicy.defaultMovie(),
                metrics);

        RecallResult result = service.recallDetailed(query(), 10);

        assertThat(result.degradedChannels()).isNotEmpty();
        assertThat(metrics.snapshot().byChannel().values().stream()
                .anyMatch(m -> m.containsKey(RecallDegradationMetrics.Reason.REJECTED)))
                .isTrue();
        tiny.shutdownNow();
    }

    @Test
    void primaryChannelFailureThrowsAndIsNotCountedAsDegradation() {
        RecallDegradationMetrics metrics = new RecallDegradationMetrics();
        MultiChannelRecallService service = new MultiChannelRecallService(
                List.of(new FailingChannel("primary")),
                new ChannelHealthMonitor(),
                java.util.concurrent.Executors.newFixedThreadPool(1),
                200L, FaultInjector.NOOP, null,
                com.recsys.application.retrieval.coldstart.QuotaPolicy.defaultMovie(),
                metrics);

        assertThatThrownBy(() -> service.recallPrimaryDetailed(query(), 10))
                .isInstanceOf(MultiChannelRecallService.PrimaryRecallUnavailableException.class);
        // primary path does not touch the non-primary denominator
        assertThat(metrics.snapshot().totalRecalls()).isEqualTo(0);
        assertThat(metrics.snapshot().degradedRecalls()).isEqualTo(0);
    }

    private static final class SlowChannel implements RecallChannel {
        @Override public String name() { return "slow1"; }
        @Override public List<MovieCandidate> recall(RecommendationQuery q, int limit) {
            try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return List.of(new MovieCandidate("1", 0.5, "slow1", java.util.Map.of()));
        }
        @Override public List<MovieCandidate> recallPrimary(RecommendationQuery q, int limit) { return recall(q, limit); }
    }

    private static final class SlowChannel2 implements RecallChannel {
        @Override public String name() { return "slow2"; }
        @Override public List<MovieCandidate> recall(RecommendationQuery q, int limit) {
            try { Thread.sleep(300); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            return List.of(new MovieCandidate("2", 0.5, "slow2", java.util.Map.of()));
        }
        @Override public List<MovieCandidate> recallPrimary(RecommendationQuery q, int limit) { return recall(q, limit); }
    }

    // The cold-user probe runs before any channel is dispatched, so a Redis failure there used to escape
    // every degradation layer: the healthy channel was never consulted and the request 500'd.

    private static MultiChannelRecallService withUserEmbeddings(
            com.recsys.infrastructure.vectordb.EmbeddingStore userEmb, RecallDegradationMetrics metrics) {
        return new MultiChannelRecallService(
                List.of(new OkChannel()),
                new ChannelHealthMonitor(),
                java.util.concurrent.Executors.newFixedThreadPool(2),
                200L, FaultInjector.NOOP, userEmb,
                com.recsys.application.retrieval.coldstart.QuotaPolicy.defaultMovie(),
                metrics);
    }

    @Test
    void userEmbeddingProbeFailureTreatsUserAsColdAndStillServes() {
        var userEmb = org.mockito.Mockito.mock(com.recsys.infrastructure.vectordb.EmbeddingStore.class);
        org.mockito.Mockito.when(userEmb.getEmbedding(org.mockito.ArgumentMatchers.anyInt()))
                .thenThrow(new io.lettuce.core.RedisConnectionException("redis down"));
        RecallDegradationMetrics metrics = new RecallDegradationMetrics();

        RecallResult result = withUserEmbeddings(userEmb, metrics).recallDetailed(query(), 10);

        assertThat(result.candidates()).isNotEmpty(); // the healthy channel was consulted
        assertThat(result.degradedChannels()).containsExactly("user-embedding");
        assertThat(result.outcome()).isEqualTo(PARTIAL);
        assertThat(metrics.snapshot().byChannel()).containsKey("user-embedding");
        assertThat(metrics.snapshot().degradedRecalls()).isEqualTo(1);
    }

    @Test
    void userEmbeddingProbeFailureOnThePrimaryPathStillPropagates() {
        // Read-your-writes must not silently downgrade to a cold-user answer.
        var userEmb = org.mockito.Mockito.mock(com.recsys.infrastructure.vectordb.EmbeddingStore.class);
        org.mockito.Mockito.when(userEmb.getEmbeddingPrimary(org.mockito.ArgumentMatchers.anyInt()))
                .thenThrow(new io.lettuce.core.RedisConnectionException("redis down"));

        assertThatThrownBy(() -> withUserEmbeddings(userEmb, new RecallDegradationMetrics())
                .recallPrimaryDetailed(query(), 10))
                .isInstanceOf(io.lettuce.core.RedisConnectionException.class);
    }
}
