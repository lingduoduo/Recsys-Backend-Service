package com.recsys.application.online;
import com.recsys.domain.online.OnlineRecommendationResult;
import com.recsys.domain.online.OnlineRecommendationRequest;

import com.recsys.domain.item.Movie;
import com.recsys.domain.item.MovieCandidate;
import com.recsys.domain.recommendation.RecommendationQuery;
import com.recsys.domain.user.User;
import com.recsys.infrastructure.dataloading.DataManager;
import com.recsys.application.online.OnlineLearner;
import com.recsys.infrastructure.store.RecentHistoryStore;
import com.recsys.infrastructure.store.TrendingStore;
import com.recsys.application.retrieval.multichannel.MultiChannelRecallService;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Online recommendation = recall (shared MultiChannelRecallService) -> re-rank (OnlineLearner) ->
 * response snapshot (recent history + per-request trending window). Cold-start detection is handled
 * inside the recall service via the injected user-embedding store.
 */
public final class OnlineRecommendationService {

    private static final Logger log = LoggerFactory.getLogger(OnlineRecommendationService.class);

    private static final Set<String> ALLOWED_WINDOWS = Set.of("last_hour", "last_day", "last_month");
    private static final int RECENT_HISTORY_LIMIT = 3;

    private final DataManager dataManager;
    private final MultiChannelRecallService recallService;
    private final RecentHistoryStore recentHistoryStore;
    private final TrendingStore topkStore;
    private final OnlineLearner onlineLearner;
    private final Counter recentHistoryDegraded;
    private final Counter trendingDegraded;

    public OnlineRecommendationService(DataManager dataManager,
                                       MultiChannelRecallService recallService,
                                       RecentHistoryStore recentHistoryStore,
                                       TrendingStore topkStore,
                                       OnlineLearner onlineLearner) {
        this(dataManager, recallService, recentHistoryStore, topkStore, onlineLearner, null);
    }

    /** @param registry may be null, in which case degraded reads are logged but not counted. */
    public OnlineRecommendationService(DataManager dataManager,
                                       MultiChannelRecallService recallService,
                                       RecentHistoryStore recentHistoryStore,
                                       TrendingStore topkStore,
                                       OnlineLearner onlineLearner,
                                       MeterRegistry registry) {
        this.recentHistoryDegraded = degradedReadCounter(registry, "recent_history");
        this.trendingDegraded = degradedReadCounter(registry, "trending");
        this.dataManager = Objects.requireNonNull(dataManager, "dataManager");
        this.recallService = Objects.requireNonNull(recallService, "recallService");
        this.recentHistoryStore = Objects.requireNonNull(recentHistoryStore, "recentHistoryStore");
        this.topkStore = Objects.requireNonNull(topkStore, "topkStore");
        this.onlineLearner = onlineLearner == null ? new OnlineLearner() : onlineLearner;
    }

    public OnlineRecommendationResult recommend(OnlineRecommendationRequest request) {
        return recommend(request, false);
    }

    private static Counter degradedReadCounter(MeterRegistry registry, String read) {
        return registry == null ? null
                : Counter.builder("online_recommendation_degraded_reads_total")
                        .description("Snapshot reads outside the recall fan-out that failed and were degraded")
                        .tag("read", read)
                        .register(registry);
    }

    public OnlineRecommendationResult recommendPrimary(OnlineRecommendationRequest request) {
        try {
            return recommend(request, true);
        } catch (UnknownUserException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (RuntimeException e) {
            throw new PrimaryReadUnavailableException("Primary recommendation read failed", e);
        }
    }

    public static final class PrimaryReadUnavailableException extends RuntimeException {
        public PrimaryReadUnavailableException(String message, Throwable cause) { super(message, cause); }
    }

    private OnlineRecommendationResult recommend(OnlineRecommendationRequest request, boolean primaryFeatureRead) {
        User user = requireUser(request.userId());
        int k = Math.max(1, request.k());
        int recallLimit = Math.min(Math.max(k, 12), 10_000);
        String window = normalizeWindow(request.window());

        List<Integer> recentIds = primaryFeatureRead
                ? recentHistoryStore.getRecentMovieIdsPrimary(request.userId(), RECENT_HISTORY_LIMIT)
                : recentHistoryOrEmpty(request.userId());
        Set<String> excluded = new LinkedHashSet<>();
        for (int id : recentIds) excluded.add(String.valueOf(id));

        RecommendationQuery query =
                new RecommendationQuery(
                        String.valueOf(request.userId()), Math.min(k, 100), excluded, null);
        List<MovieCandidate> candidates = primaryFeatureRead
                ? recallService.recallPrimary(query, recallLimit)
                : recallService.recall(query, recallLimit);

        List<Movie> recentMovies = mapMovies(recentIds);
        List<Movie> trendingMovies = mapMovies(parseIds(primaryFeatureRead
                ? topkStore.getTopKIdsPrimary(window, k)
                : trendingOrEmpty(window, k)));

        List<Movie> recommendations = rerank(candidates, excluded, k);
        if (recommendations.isEmpty()) {
            recommendations = trendingMovies.stream().limit(k).toList();
        }

        return new OnlineRecommendationResult(
                user, window, "multichannel", recentMovies, trendingMovies, recommendations);
    }

    private List<Movie> rerank(List<MovieCandidate> candidates, Set<String> excluded, int k) {
        record Scored(int movieId, double score) {}
        List<Scored> scored = new ArrayList<>(candidates.size());
        for (MovieCandidate c : candidates) {
            if (excluded.contains(c.itemId())) continue;
            int movieId;
            try {
                movieId = Integer.parseInt(c.itemId());
            } catch (NumberFormatException e) {
                continue;
            }
            scored.add(new Scored(movieId, c.score() + onlineLearner.scoreAdjustment(movieId)));
        }
        return scored.stream()
                .sorted(Comparator.comparingDouble(Scored::score).reversed()
                        .thenComparingInt(Scored::movieId))
                .map(s -> dataManager.getMovieById(s.movieId()))
                .filter(Objects::nonNull)
                .limit(k)
                .toList();
    }

    // These two reads sit outside the recall fan-out, so none of its degradation layers cover them, and on a
    // cold cache their stores rethrow rather than serve stale. Unhandled, a Redis outage turned every
    // request into a 500 while readiness stayed green. The replica path degrades instead: no history means
    // nothing is excluded, no trending means an empty snapshot. The primary path keeps failing loudly —
    // read-your-writes answers 503 + Retry-After rather than a guess.
    private List<Integer> recentHistoryOrEmpty(int userId) {
        try {
            return recentHistoryStore.getRecentMovieIds(userId, RECENT_HISTORY_LIMIT);
        } catch (RuntimeException e) {
            degraded(recentHistoryDegraded, "recent history", e);
            return List.of();
        }
    }

    private List<String> trendingOrEmpty(String window, int k) {
        try {
            return topkStore.getTopKIds(window, k);
        } catch (RuntimeException e) {
            degraded(trendingDegraded, "trending snapshot", e);
            return List.of();
        }
    }

    private static void degraded(Counter counter, String read, RuntimeException e) {
        if (counter != null) {
            counter.increment();
        }
        log.warn("Degrading {} read to empty: {}", read,
                e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName());
    }

    private List<Movie> mapMovies(List<Integer> ids) {
        List<Movie> movies = new ArrayList<>(ids.size());
        for (int id : ids) {
            Movie m = dataManager.getMovieById(id);
            if (m != null) movies.add(m);
        }
        return List.copyOf(movies);
    }

    private static List<Integer> parseIds(List<String> raw) {
        List<Integer> ids = new ArrayList<>(raw.size());
        for (String s : raw) {
            try {
                ids.add(Integer.parseInt(s));
            } catch (NumberFormatException ignore) {
                // skip malformed ids from Redis
            }
        }
        return ids;
    }

    private static String normalizeWindow(String window) {
        String normalized = (window == null || window.isBlank()) ? "last_hour" : window.trim();
        if (!ALLOWED_WINDOWS.contains(normalized)) {
            throw new IllegalArgumentException("invalid window: " + normalized);
        }
        return normalized;
    }

    private User requireUser(int userId) {
        User user = dataManager.getUserById(userId);
        if (user == null) throw new UnknownUserException(userId);
        return user;
    }

    public static final class UnknownUserException extends RuntimeException {
        private final int userId;

        public UnknownUserException(int userId) {
            super("user not found");
            this.userId = userId;
        }

        public int userId() { return userId; }
    }
}
