package com.demo.retrieval.service.side_effects;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import com.demo.retrieval.service.grpo.GrpoEventPublisher;
import com.demo.retrieval.service.grpo.GrpoPolicyScorer;
import com.demo.retrieval.service.replay.ReplayEvent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class MovieLensServingSideEffects {
    private static final Logger log = LoggerFactory.getLogger(MovieLensServingSideEffects.class);
    public static final String METRICS_HASH_KEY = "bandit:metrics";
    public static final String EXPOSED_ITEMS_KEY = "bandit:exposed_items";
    public static final String REPLAY_PENDING_PREFIX = "replay:pending:";
    private static final int SERVED_HISTORY_LIMIT = 200;
    private static final int IMPRESSED_HISTORY_LIMIT = 500;

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final Duration pendingTtl;
    private final GrpoEventPublisher grpoPublisher;
    private final GrpoPolicyScorer grpoScorer;

    public MovieLensServingSideEffects(
        StringRedisTemplate redis, ObjectMapper objectMapper, Duration pendingTtl,
        GrpoEventPublisher grpoPublisher, GrpoPolicyScorer grpoScorer
    ) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.pendingTtl = pendingTtl;
        this.grpoPublisher = grpoPublisher;
        this.grpoScorer = grpoScorer;
    }

    public void recordServed(ServingSideEffectRequest request) {
        if (request.selected().isEmpty()) {
            return;
        }

        long now = System.currentTimeMillis();
        List<String> selectedMovieIds = request.selected().stream().map(ServedMovie::movieId).toList();
        List<String> servedHistory = mergeCapped(selectedMovieIds, request.recentlyServedMovieIds(), SERVED_HISTORY_LIMIT);
        List<String> impressedMovies = mergeCapped(selectedMovieIds, request.impressedMovieIds(), IMPRESSED_HISTORY_LIMIT);
        String requestKey = "recommendation:request:" + request.requestId();
        String servedHistoryKey = "user:" + request.userId() + ":served_history";
        String impressionsKey = "user:" + request.userId() + ":impressions";
        String algorithmMetricsKey = metricsHashKey(request.algorithm());
        Map<String, String> requestInfo = requestInfo(request, selectedMovieIds, now);

        redis.executePipelined(new SessionCallback<>() {
            @Override
            public Object execute(RedisOperations operations) throws DataAccessException {
                for (int i = 0; i < request.selected().size(); i++) {
                    ServedMovie movie = request.selected().get(i);
                    operations.opsForValue().increment("bandit:item:" + movie.movieId() + ":impressions", 1);
                    operations.opsForSet().add(EXPOSED_ITEMS_KEY, movie.movieId());
                    operations.opsForValue().set("bandit:last_served:" + movie.movieId(), String.valueOf(now));
                    serializeReplayContext(request, movie, i, request.selected().size(), now)
                        .ifPresent(payload -> operations.opsForValue().set(pendingReplayKey(request.userId(), movie.movieId()), payload, pendingTtl));
                }

                operations.opsForHash().put(servedHistoryKey, "movieIds", String.join(",", servedHistory));
                operations.opsForHash().put(servedHistoryKey, "updatedAt", String.valueOf(now));
                operations.opsForHash().put(impressionsKey, "movieIds", String.join(",", impressedMovies));
                operations.opsForHash().put(impressionsKey, "updatedAt", String.valueOf(now));
                operations.opsForHash().putAll(requestKey, requestInfo);
                incrementServingMetrics(operations, request, algorithmMetricsKey);
                return null;
            }
        });

        grpoPublisher.publish(request, now);
        // Here rather than during scoring because this is where slate position is known: the GRPO
        // feature vector is position-dependent, and no earlier stage has assigned one.
        grpoScorer.recordShadowSlate(request);
    }

    public static String pendingReplayKey(String userId, String movieId) {
        return REPLAY_PENDING_PREFIX + userId + ":" + movieId;
    }

    public static String metricsHashKey(String algorithm) {
        return METRICS_HASH_KEY + ":" + algorithm;
    }

    private void incrementServingMetrics(
        RedisOperations operations,
        ServingSideEffectRequest request,
        String algorithmMetricsKey
    ) {
        operations.opsForHash().increment(METRICS_HASH_KEY, "requests", 1L);
        operations.opsForHash().increment(algorithmMetricsKey, "requests", 1L);
        operations.opsForHash().increment(METRICS_HASH_KEY, "recommendations_served", request.servedCount());
        operations.opsForHash().increment(algorithmMetricsKey, "recommendations_served", request.servedCount());
        operations.opsForHash().increment(METRICS_HASH_KEY, "exploratory_impressions", request.exploratoryCount());
        operations.opsForHash().increment(algorithmMetricsKey, "exploratory_impressions", request.exploratoryCount());
        operations.opsForHash().increment(METRICS_HASH_KEY, "cold_start_impressions", request.coldStartCount());
        operations.opsForHash().increment(algorithmMetricsKey, "cold_start_impressions", request.coldStartCount());
        operations.opsForHash().increment(METRICS_HASH_KEY, "estimated_reward_total", request.estimatedRewardTotal());
        operations.opsForHash().increment(algorithmMetricsKey, "estimated_reward_total", request.estimatedRewardTotal());
        operations.opsForHash().increment(METRICS_HASH_KEY, "pseudo_regret_total", request.pseudoRegret());
        operations.opsForHash().increment(algorithmMetricsKey, "pseudo_regret_total", request.pseudoRegret());
        operations.opsForHash().increment(METRICS_HASH_KEY, "novelty_total", request.noveltyTotal());
        operations.opsForHash().increment(algorithmMetricsKey, "novelty_total", request.noveltyTotal());
    }

    private Optional<String> serializeReplayContext(
        ServingSideEffectRequest request,
        ServedMovie selected,
        int actionPosition,
        int slateSize,
        long timestamp
    ) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put(ReplayEvent.TYPE, ReplayEvent.EVENT_TYPE);
        event.put(ReplayEvent.SCHEMA_VERSION, ReplayEvent.SCHEMA_VERSION_VALUE);
        event.put(ReplayEvent.REQUEST_ID, request.requestId());
        event.put(ReplayEvent.USER, request.userId());
        event.put(ReplayEvent.STATE, request.state());
        event.put(ReplayEvent.ACTION_SPACE, request.candidateSnapshot().stream().map(this::candidateFeatures).toList());
        event.put(ReplayEvent.CANDIDATES, request.candidateSnapshot().stream().map(ServedMovie::movieId).toList());
        event.put(ReplayEvent.ACTION, selected.movieId());
        event.put(ReplayEvent.ACTION_POSITION, actionPosition);
        event.put(ReplayEvent.SLATE_SIZE, slateSize);
        event.put(ReplayEvent.POLICY, Map.of(
            "name", request.algorithm(),
            "rankingScore", selected.banditScore(),
            "explorationBonus", selected.explorationBonus(),
            "propensity", slateSize <= 0 ? 0.0 : 1.0 / slateSize
        ));
        event.put(ReplayEvent.MODEL_PREDICTIONS, selected.modelPredictions());
        event.put(ReplayEvent.ESTIMATED_REWARD, selected.estimatedReward());
        event.put(ReplayEvent.ONLINE_SCORE, selected.onlineScore());
        event.put(ReplayEvent.BANDIT_SCORE, selected.banditScore());
        event.put(ReplayEvent.COLD_START, selected.coldStart());
        event.put(ReplayEvent.TIMESTAMP, timestamp);
        try {
            return Optional.of(objectMapper.writeValueAsString(event));
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize replay context for user {} movie {}", request.userId(), selected.movieId(), e);
            return Optional.empty();
        }
    }

    private Map<String, Object> candidateFeatures(ServedMovie movie) {
        Map<String, Object> features = new LinkedHashMap<>();
        features.put("item", movie.movieId());
        features.put("modelPredictions", movie.modelPredictions());
        features.put("coldStart", movie.coldStart());
        features.put("impressions", movie.impressions());
        features.put("clicks", movie.clicks());
        return features;
    }

    private Map<String, String> requestInfo(ServingSideEffectRequest request, List<String> selectedMovieIds, long timestamp) {
        Map<String, String> info = new LinkedHashMap<>();
        info.put("userId", request.userId());
        info.put("algorithm", request.algorithm());
        info.put("selectedMovieIds", String.join(",", selectedMovieIds));
        info.put("candidateCount", String.valueOf(request.candidateSnapshot().size()));
        info.put("servedAt", String.valueOf(timestamp));
        return info;
    }

    private static List<String> mergeCapped(List<String> newest, List<String> previous, int limit) {
        Set<String> merged = new LinkedHashSet<>();
        newest.stream().filter(MovieLensServingSideEffects::notBlank).forEach(merged::add);
        previous.stream().filter(MovieLensServingSideEffects::notBlank).forEach(merged::add);
        List<String> result = new ArrayList<>(Math.min(merged.size(), limit));
        for (String id : merged) {
            if (result.size() >= limit) {
                break;
            }
            result.add(id);
        }
        return List.copyOf(result);
    }

    private static boolean notBlank(String value) {
        return value != null && !value.isBlank();
    }

    public record ServingSideEffectRequest(
        String requestId,
        String userId,
        String algorithm,
        Map<String, Object> state,
        List<ServedMovie> candidateSnapshot,
        List<ServedMovie> selected,
        List<String> recentlyServedMovieIds,
        List<String> impressedMovieIds,
        long coldStartCount,
        long exploratoryCount,
        long servedCount,
        double estimatedRewardTotal,
        double pseudoRegret,
        double noveltyTotal
    ) {
        public ServingSideEffectRequest {
            state = Map.copyOf(state);
            candidateSnapshot = List.copyOf(candidateSnapshot);
            selected = List.copyOf(selected);
            recentlyServedMovieIds = List.copyOf(recentlyServedMovieIds);
            impressedMovieIds = List.copyOf(impressedMovieIds);
        }
    }

    public record ServedMovie(
        String movieId,
        double estimatedReward,
        double onlineScore,
        double explorationBonus,
        double banditScore,
        boolean coldStart,
        long impressions,
        long clicks,
        Map<String, Object> modelPredictions
    ) {
        public ServedMovie {
            modelPredictions = Map.copyOf(modelPredictions);
        }
    }
}
