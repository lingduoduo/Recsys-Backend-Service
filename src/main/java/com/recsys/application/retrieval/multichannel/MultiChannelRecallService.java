package com.recsys.application.retrieval.multichannel;

import com.recsys.domain.item.MovieCandidate;
import com.recsys.domain.recommendation.RecommendationQuery;
import com.recsys.infrastructure.vectordb.EmbeddingStore;
import com.recsys.resilience.FaultInjector;
import com.recsys.application.retrieval.RecallChannel;
import com.recsys.application.retrieval.RecallScoring;
import com.recsys.application.retrieval.coldstart.QuotaPolicy;
import com.recsys.application.retrieval.coldstart.QuotaSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.recsys.application.retrieval.multichannel.RecallResult.DegradationOutcome.ALL_CHANNELS;
import static com.recsys.application.retrieval.multichannel.RecallResult.DegradationOutcome.HEALTHY;
import static com.recsys.application.retrieval.multichannel.RecallResult.DegradationOutcome.PARTIAL;

public class MultiChannelRecallService {
    private static final Logger log = LoggerFactory.getLogger(MultiChannelRecallService.class);
    private static final long DEFAULT_CHANNEL_TIMEOUT_MS = 200L;

    private final List<RecallChannel> channels;
    private final ChannelHealthMonitor healthMonitor;
    private final ExecutorService executor;
    private final long channelTimeoutMs;
    private final FaultInjector faultInjector;
    private final EmbeddingStore userEmbeddingStore;
    private final QuotaPolicy quotaPolicy;
    private final RecallDegradationMetrics degradationMetrics;
    private final RecallTaskMetrics taskMetrics;

    // Convenience constructor for tests. Production callers should supply a WorkerBulkhead
    // via the 5-arg constructor — ForkJoinPool.commonPool() is unsuitable for blocking I/O.
    public MultiChannelRecallService(List<RecallChannel> channels) {
        this(channels, new ChannelHealthMonitor(), ForkJoinPool.commonPool(),
                DEFAULT_CHANNEL_TIMEOUT_MS, FaultInjector.NOOP, null);
    }

    public MultiChannelRecallService(List<RecallChannel> channels,
                                     ChannelHealthMonitor healthMonitor,
                                     ExecutorService executor,
                                     long channelTimeoutMs,
                                     FaultInjector faultInjector) {
        this(channels, healthMonitor, executor, channelTimeoutMs, faultInjector, null);
    }

    public MultiChannelRecallService(List<RecallChannel> channels,
                                     ChannelHealthMonitor healthMonitor,
                                     ExecutorService executor,
                                     long channelTimeoutMs,
                                     FaultInjector faultInjector,
                                     EmbeddingStore userEmbeddingStore) {
        this(channels, healthMonitor, executor, channelTimeoutMs, faultInjector,
                userEmbeddingStore, QuotaPolicy.defaultMovie());
    }

    public MultiChannelRecallService(List<RecallChannel> channels,
                                     ChannelHealthMonitor healthMonitor,
                                     ExecutorService executor,
                                     long channelTimeoutMs,
                                     FaultInjector faultInjector,
                                     EmbeddingStore userEmbeddingStore,
                                     QuotaPolicy quotaPolicy) {
        this(channels, healthMonitor, executor, channelTimeoutMs, faultInjector,
                userEmbeddingStore, quotaPolicy, new RecallDegradationMetrics());
    }

    public MultiChannelRecallService(List<RecallChannel> channels,
                                     ChannelHealthMonitor healthMonitor,
                                     ExecutorService executor,
                                     long channelTimeoutMs,
                                     FaultInjector faultInjector,
                                     EmbeddingStore userEmbeddingStore,
                                     QuotaPolicy quotaPolicy,
                                     RecallDegradationMetrics degradationMetrics) {
        this(channels, healthMonitor, executor, channelTimeoutMs, faultInjector,
                userEmbeddingStore, quotaPolicy, degradationMetrics, RecallTaskMetrics.NOOP);
    }

    public MultiChannelRecallService(List<RecallChannel> channels,
                                     ChannelHealthMonitor healthMonitor,
                                     ExecutorService executor,
                                     long channelTimeoutMs,
                                     FaultInjector faultInjector,
                                     EmbeddingStore userEmbeddingStore,
                                     QuotaPolicy quotaPolicy,
                                     RecallDegradationMetrics degradationMetrics,
                                     RecallTaskMetrics taskMetrics) {
        if (channels == null || channels.isEmpty()) {
            throw new IllegalArgumentException("at least one recall channel is required");
        }
        this.channels            = List.copyOf(channels);
        this.healthMonitor       = Objects.requireNonNull(healthMonitor, "healthMonitor");
        this.executor            = Objects.requireNonNull(executor, "executor");
        this.channelTimeoutMs    = Math.max(1L, channelTimeoutMs);
        this.faultInjector       = faultInjector == null ? FaultInjector.NOOP : faultInjector;
        this.userEmbeddingStore  = userEmbeddingStore;
        this.quotaPolicy         = quotaPolicy == null ? QuotaPolicy.defaultMovie() : quotaPolicy;
        this.degradationMetrics  = degradationMetrics == null
                ? new RecallDegradationMetrics() : degradationMetrics;
        this.taskMetrics         = taskMetrics == null ? RecallTaskMetrics.NOOP : taskMetrics;
    }

    /** Builds a service from a per-port {@link RecallConfig}. */
    public static MultiChannelRecallService from(RecallConfig config) {
        java.util.Objects.requireNonNull(config, "config");
        return new MultiChannelRecallService(
                config.channels(),
                config.healthMonitor(),
                config.executor(),
                config.channelTimeoutMs(),
                config.faultInjector(),
                config.userEmbeddingStore(),
                config.quotaPolicy(),
                config.recallMetrics(),
                config.taskMetrics());
    }

    /** Degraded-channel name reported when the cold-user probe's embedding read fails. */
    static final String USER_EMBEDDING_PROBE = "user-embedding";

    public List<MovieCandidate> recall(RecommendationQuery query, int limit) {
        return recall(query, limit, false).candidates();
    }

    public List<MovieCandidate> recallPrimary(RecommendationQuery query, int limit) {
        return recall(query, limit, true).candidates();
    }

    public RecallResult recallDetailed(RecommendationQuery query, int limit) {
        return recall(query, limit, false);
    }

    public RecallResult recallPrimaryDetailed(RecommendationQuery query, int limit) {
        return recall(query, limit, true);
    }

    private RecallResult recall(RecommendationQuery query, int limit, boolean primary) {
        Objects.requireNonNull(query, "query");
        if (limit <= 0) return new RecallResult(List.of(), Set.of(), HEALTHY);
        if (!primary) degradationMetrics.recordTotal();

        QuotaSpec quota = null;
        RuntimeException probeFailure = null;
        if (userEmbeddingStore != null) {
            try {
                int userId = Integer.parseInt(query.userId());
                boolean isCold = (primary ? userEmbeddingStore.getEmbeddingPrimary(userId)
                        : userEmbeddingStore.getEmbedding(userId)) == null;
                quota = isCold ? quotaPolicy.cold(limit) : quotaPolicy.warm(limit);
            } catch (NumberFormatException e) {
                quota = quotaPolicy.cold(limit);
            } catch (RuntimeException e) {
                // The probe runs before any channel is dispatched, so a store failure here would escape
                // every per-channel degradation layer (a cold-cache Redis outage 500'd the request).
                // Off the primary path, an unknown embedding is answered as a cold user and reported as a
                // degraded "user-embedding" channel; the primary path keeps propagating (read-your-writes).
                if (primary) throw e;
                quota = quotaPolicy.cold(limit);
                probeFailure = e;
            }
        }

        // Retained task handles, not detached CompletableFuture.orTimeout chains: a timeout here
        // cancels the underlying task with interruption, so a channel stuck on a stalled Redis is
        // asked to stop rather than left occupying a worker for the full command timeout
        // (measured: caller degraded at 220 ms, next task waited 1790 ms for a thread). Channels
        // must preserve the interrupt rather than swallow it; a client that ignores interruption
        // still cannot exhaust memory because the executor's queue is the hard bound.
        List<Submitted> submitted = new ArrayList<>(channels.size());
        List<ChannelResult> results = new ArrayList<>(channels.size());
        for (RecallChannel channel : channels) {
            if (!healthMonitor.isAvailable(channel.name())) {
                if (primary) {
                    cancelAll(submitted);
                    throw new PrimaryRecallUnavailableException(
                            "Required primary channel is unavailable: " + channel.name());
                }
                log.debug("Channel '{}' is in backoff — skipping", channel.name());
                continue;
            }
            String name = channel.name();
            try {
                Future<ChannelResult> task = executor.submit(() -> {
                    faultInjector.maybeInject("channel:" + name);
                    // Over-fetch to limit so gap fill can pick unselected candidates
                    return new ChannelResult(name, primary
                            ? channel.recallPrimary(query, limit)
                            : channel.recall(query, limit), null);
                });
                submitted.add(new Submitted(name, task));
            } catch (RejectedExecutionException rex) {
                taskMetrics.recordRejected(name);
                if (primary) {
                    cancelAll(submitted);
                    throw new PrimaryRecallUnavailableException(
                            "Required primary channel was rejected: " + name, rex);
                }
                log.warn("Channel '{}' rejected by recall bulkhead (queue full)", name);
                results.add(new ChannelResult(name, List.of(), rex));
            }
        }

        // One shared deadline for the whole fan-out: every channel got the same budget from the
        // moment of submission, and awaiting them in order against it costs no channel any time.
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(channelTimeoutMs);
        for (int i = 0; i < submitted.size(); i++) {
            Submitted s = submitted.get(i);
            try {
                results.add(s.task().get(Math.max(0L, deadlineNanos - System.nanoTime()), TimeUnit.NANOSECONDS));
            } catch (TimeoutException te) {
                s.task().cancel(true);
                taskMetrics.recordTimeout(s.channel());
                if (primary) {
                    cancelAll(submitted.subList(i + 1, submitted.size()));
                    throw new PrimaryRecallUnavailableException(
                            "Required primary recall channel timed out: " + s.channel(), te);
                }
                results.add(new ChannelResult(s.channel(), List.of(), te));
            } catch (ExecutionException ee) {
                Throwable cause = ee.getCause() != null ? ee.getCause() : ee;
                if (primary) {
                    cancelAll(submitted.subList(i + 1, submitted.size()));
                    throw new PrimaryRecallUnavailableException("Required primary recall channel failed", cause);
                }
                results.add(new ChannelResult(s.channel(), List.of(), cause));
            } catch (java.util.concurrent.CancellationException ce) {
                results.add(new ChannelResult(s.channel(), List.of(), ce));
            } catch (InterruptedException ie) {
                // The caller is being cancelled (request timeout, shutdown): stop the work we
                // started, hand the flag back, and fail rather than merge a partial fan-out. This
                // holds for the non-primary path too — the old uninterruptible join() would have
                // returned a partial merge here; an interrupted caller has no use for one.
                cancelAll(submitted.subList(i, submitted.size()));
                Thread.currentThread().interrupt();
                throw new PrimaryRecallUnavailableException("Recall interrupted", ie);
            }
        }

        Map<String, List<MovieCandidate>> channelResults = new LinkedHashMap<>();
        Set<String> degradedChannels = new LinkedHashSet<>();
        if (probeFailure != null) {
            degradationMetrics.record(USER_EMBEDDING_PROBE, RecallDegradationMetrics.classify(probeFailure));
            degradedChannels.add(USER_EMBEDDING_PROBE);
            log.warn("User-embedding probe failed, recalling as a cold user: {}",
                    probeFailure.getMessage() != null ? probeFailure.getMessage()
                            : probeFailure.getClass().getSimpleName());
        }
        for (ChannelResult result : results) {
            if (result.error() != null) {
                healthMonitor.recordFailure(result.channel());
                if (!primary) {
                    degradationMetrics.record(result.channel(),
                            RecallDegradationMetrics.classify(result.error()));
                    degradedChannels.add(result.channel());
                }
                Throwable err = result.error();
                log.warn("Channel '{}' failed: {}", result.channel(),
                        err.getMessage() != null ? err.getMessage() : err.getClass().getSimpleName());
                continue;
            }
            healthMonitor.recordSuccess(result.channel());
            List<MovieCandidate> sorted = result.candidates().stream()
                    .sorted(RecallScoring.BY_SCORE_DESC)
                    .toList();
            channelResults.put(result.channel(), sorted);
        }
        if (!primary && !degradedChannels.isEmpty()) {
            degradationMetrics.recordDegradedRequest();
        }
        RecallResult.DegradationOutcome outcome = degradedChannels.isEmpty()
                ? HEALTHY
                : channelResults.isEmpty() ? ALL_CHANNELS : PARTIAL;
        if (!primary) {
            degradationMetrics.recordOutcome(outcome);
        }

        List<MovieCandidate> ranked = (quota == null)
                ? legacyMerge(channelResults, query, limit)
                : quotaMerge(channelResults, quota, query, limit);
        return new RecallResult(ranked, degradedChannels, outcome);
    }

    public static final class PrimaryRecallUnavailableException extends RuntimeException {
        public PrimaryRecallUnavailableException(String message) { super(message); }
        public PrimaryRecallUnavailableException(String message, Throwable cause) { super(message, cause); }
    }

    private List<MovieCandidate> legacyMerge(Map<String, List<MovieCandidate>> channelResults,
                                              RecommendationQuery query, int limit) {
        Map<String, MovieCandidate> merged = new LinkedHashMap<>();
        for (List<MovieCandidate> candidates : channelResults.values()) {
            for (MovieCandidate c : candidates) {
                if (query.excludedItemIds().contains(c.itemId())) continue;
                merged.merge(c.itemId(), c,
                        (existing, incoming) ->
                                incoming.score() > existing.score() ? incoming : existing);
            }
        }
        return merged.values().stream()
                .sorted(RecallScoring.BY_SCORE_DESC)
                .limit(limit)
                .toList();
    }

    private List<MovieCandidate> quotaMerge(Map<String, List<MovieCandidate>> channelResults,
                                             QuotaSpec quota,
                                             RecommendationQuery query, int limit) {
        Set<String> selectedIds = new LinkedHashSet<>();
        List<MovieCandidate> result = new ArrayList<>();

        for (RecallChannel channel : channels) {
            String name = channel.name();
            List<MovieCandidate> candidates = channelResults.getOrDefault(name, List.of());
            int channelSlots = quota.slotsFor(name);
            int count = 0;
            for (MovieCandidate c : candidates) {
                if (count >= channelSlots) break;
                if (!selectedIds.contains(c.itemId()) && !query.excludedItemIds().contains(c.itemId())) {
                    result.add(c);
                    selectedIds.add(c.itemId());
                    count++;
                }
            }
        }

        if (result.size() < limit) {
            Map<String, MovieCandidate> gapPool = new LinkedHashMap<>();
            for (List<MovieCandidate> candidates : channelResults.values()) {
                for (MovieCandidate c : candidates) {
                    if (!selectedIds.contains(c.itemId()) && !query.excludedItemIds().contains(c.itemId())) {
                        gapPool.merge(c.itemId(), c,
                                (a, b) -> b.score() > a.score() ? b : a);
                    }
                }
            }
            gapPool.values().stream()
                    .sorted(RecallScoring.BY_SCORE_DESC)
                    .limit(limit - result.size())
                    .forEach(result::add);
        }

        result.sort(RecallScoring.BY_SCORE_DESC);
        return result.size() > limit ? List.copyOf(result.subList(0, limit)) : List.copyOf(result);
    }

    private static void cancelAll(List<Submitted> tasks) {
        for (Submitted s : tasks) {
            s.task().cancel(true);
        }
    }

    private record Submitted(String channel, Future<ChannelResult> task) {}

    private record ChannelResult(String channel, List<MovieCandidate> candidates, Throwable error) {
        ChannelResult { Objects.requireNonNull(candidates, "candidates"); }
    }
}
