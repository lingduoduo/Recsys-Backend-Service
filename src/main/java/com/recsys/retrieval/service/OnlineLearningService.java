package com.demo.retrieval.service;

import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import org.springframework.lang.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.data.redis.core.SessionCallback;

/**
 * Online learning model: maintains per-item/genre/tag reward statistics in Redis,
 * updated in real-time from the feedback stream. Provides score estimates that
 * improve as more interaction data accumulates.
 */
@Service
public class OnlineLearningService {
    private static final String GLOBAL_KEY = "reward-model:global";
    private static final String ITEM_PREFIX = "reward-model:item:";
    private static final String GENRE_PREFIX = "reward-model:genre:";
    private static final String TAG_PREFIX = "reward-model:tag:";

    private final StringRedisTemplate redis;
    private final RecommendationProperties properties;
    private final FeatureCache featureCache;

    public OnlineLearningService(StringRedisTemplate redis, RecommendationProperties properties, FeatureCache featureCache) {
        this.redis = redis;
        this.properties = properties;
        this.featureCache = featureCache;
    }

    /**
     * Batch-warm the reward-stats cache for a set of candidates before the scoring loop.
     * Collects every key that {@link #score} will need (global + per-item + per-genre + per-tag),
     * skips keys already in the Caffeine cache, and fetches the rest in one pipeline flush.
     * After this call every subsequent {@link #score} invocation is a cache-only read
     * until entries expire (see recsys.cache.reward-ttl-seconds).
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void batchWarmRewardStats(Collection<String> itemIds, Map<String, MovieProfile> catalog) {
        Set<String> needed = new LinkedHashSet<>();
        needed.add(GLOBAL_KEY);
        for (String itemId : itemIds) {
            needed.add(ITEM_PREFIX + itemId);
            MovieProfile p = catalog.get(itemId);
            if (p != null) {
                normalize(p.getGenres()).forEach(g -> needed.add(GENRE_PREFIX + g));
                normalize(p.getTags()).forEach(t -> needed.add(TAG_PREFIX + t));
            }
        }
        List<String> cold = needed.stream()
            .filter(k -> featureCache.getRewardStats(k) == null)
            .toList();
        if (cold.isEmpty()) return;

        List<Object> results = redis.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(@NonNull org.springframework.data.redis.core.RedisOperations operations) {
                cold.forEach(key -> operations.opsForHash().entries((Object) key));
                return null;
            }
        });

        for (int i = 0; i < Math.min(cold.size(), results.size()); i++) {
            featureCache.putRewardStats(cold.get(i), parseStats((Map<Object, Object>) results.get(i)));
        }
    }

    /** Score a (item, profile) pair using accumulated reward statistics. Falls back to {@code fallback} when no data exists. */
    public double score(String itemId, MovieProfile profile, double fallback) {
        RecommendationProperties.RewardModel cfg = properties.getRewardModel();
        List<WeightedEstimate> contributions = new ArrayList<>();
        contributions.add(new WeightedEstimate(cfg.getGlobalWeight(), readRewardEstimate(GLOBAL_KEY)));
        contributions.add(new WeightedEstimate(cfg.getItemWeight(), readRewardEstimate(ITEM_PREFIX + itemId)));
        if (profile != null) {
            contributions.add(new WeightedEstimate(cfg.getGenreWeight(),
                aggregateFeatureEstimates(GENRE_PREFIX, normalize(profile.getGenres()))));
            contributions.add(new WeightedEstimate(cfg.getTagWeight(),
                aggregateFeatureEstimates(TAG_PREFIX, normalize(profile.getTags()))));
        }

        double weightedReward = 0.0;
        double totalWeight = 0.0;
        for (WeightedEstimate c : contributions) {
            if (c.estimate().count() > 0) {
                double weight = c.configWeight() * confidence(c.estimate().count());
                weightedReward += c.estimate().mean() * weight;
                totalWeight += weight;
            }
        }
        return totalWeight == 0.0 ? RecommendationConstants.clamp(fallback)
            : RecommendationConstants.clamp(weightedReward / totalWeight);
    }

    private record WeightedEstimate(double configWeight, RewardEstimate estimate) {
    }

    /**
     * Write reward stat increments into a caller-managed Redis pipeline.
     * Avoids opening a new connection per feedback event; all writes are queued
     * in the pipeline and flushed by the caller in one round-trip.
     * The keys that were written are added to {@code keysToInvalidate} so the
     * caller can purge them from the in-memory cache after the pipeline flushes.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void pipelineUpdate(
            org.springframework.data.redis.core.RedisOperations ops,
            String itemId, double reward, MovieProfile profile,
            java.util.List<String> keysToInvalidate) {
        pipelineIncrement(ops, GLOBAL_KEY, reward, keysToInvalidate);
        pipelineIncrement(ops, ITEM_PREFIX + itemId, reward, keysToInvalidate);
        if (profile == null) return;
        normalize(profile.getGenres()).forEach(g -> pipelineIncrement(ops, GENRE_PREFIX + g, reward, keysToInvalidate));
        normalize(profile.getTags()).forEach(t -> pipelineIncrement(ops, TAG_PREFIX + t, reward, keysToInvalidate));
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void pipelineIncrement(
            org.springframework.data.redis.core.RedisOperations ops,
            String key, double reward, java.util.List<String> keysToInvalidate) {
        ops.opsForHash().increment(key, "count", 1L);
        ops.opsForHash().increment(key, "reward_total", reward);
        keysToInvalidate.add(key);
    }

    private RewardEstimate aggregateFeatureEstimates(String prefix, Set<String> features) {
        long count = 0L;
        double rewardTotal = 0.0;
        for (String feature : features) {
            RewardEstimate estimate = readRewardEstimate(prefix + feature);
            count += estimate.count();
            rewardTotal += estimate.mean() * estimate.count();
        }
        return count == 0L ? new RewardEstimate(0L, 0.0) : new RewardEstimate(count, rewardTotal / count);
    }

    private RewardEstimate readRewardEstimate(@NonNull String key) {
        FeatureCache.RewardModelStats cached = featureCache.getRewardStats(key);
        if (cached != null) {
            return cached.count() == 0L
                ? new RewardEstimate(0L, 0.0)
                : new RewardEstimate(cached.count(), RecommendationConstants.clamp(cached.rewardTotal() / cached.count()));
        }
        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(key)).orElseGet(Map::of);
        FeatureCache.RewardModelStats stats = parseStats(raw);
        featureCache.putRewardStats(key, stats);
        return stats.count() == 0L
            ? new RewardEstimate(0L, 0.0)
            : new RewardEstimate(stats.count(), RecommendationConstants.clamp(stats.rewardTotal() / stats.count()));
    }

    private FeatureCache.RewardModelStats parseStats(Map<Object, Object> raw) {
        return new FeatureCache.RewardModelStats(
            raw == null ? 0L : readLong(raw.get("count")),
            raw == null ? 0.0 : readDouble(raw.get("reward_total")));
    }

    private double confidence(long count) {
        int minCount = Math.max(1, properties.getRewardModel().getMinFeatureCount());
        return Math.min(1.0, (double) count / minCount);
    }

    private Set<String> normalize(List<String> values) {
        return values == null ? Set.of() : values.stream()
            .filter(v -> v != null && !v.isBlank())
            .map(v -> v.toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
    }

    private long readLong(Object raw) {
        if (raw == null) return 0L;
        try { return Long.parseLong(raw.toString()); } catch (NumberFormatException e) { return 0L; }
    }

    private double readDouble(Object raw) {
        if (raw == null) return 0.0;
        try { return Double.parseDouble(raw.toString()); } catch (NumberFormatException e) { return 0.0; }
    }

    private record RewardEstimate(long count, double mean) {}
}
