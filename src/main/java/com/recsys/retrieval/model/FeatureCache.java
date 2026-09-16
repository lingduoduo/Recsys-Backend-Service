package com.recsys.retrieval.model;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.recsys.retrieval.config.RecommendationProperties;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Three-layer feature storage:
 *   Offline  — full model artifacts on the filesystem (loaded at startup by DeepLearningPredictionService).
 *   Redis    — real-time online features written by streaming jobs (embeddings, bandit counters, user history).
 *   In-memory — this class: Caffeine caches for the hottest Redis reads so the per-request round-trip count
 *               drops from O(N×features) to O(1) after the first population within each TTL window.
 *
 * Item vectors (i2vEmb:*): updated only when training jobs run; safe to cache for minutes.
 * Reward model stats (reward-model:*): updated on every feedback event; short TTL keeps staleness bounded.
 * GRPO policy weights (grpo:policy:weights): rewritten once per training micro-batch; the TTL matches that trigger.
 */
@Component
public class FeatureCache {

    private final Cache<String, double[]> itemVectors;
    private final Cache<String, RewardModelStats> rewardStats;
    private final Cache<String, double[]> grpoWeights;

    public FeatureCache(RecommendationProperties properties) {
        RecommendationProperties.Cache cfg = properties.getCache();
        this.itemVectors = Caffeine.newBuilder()
            .maximumSize(cfg.getItemVectorMaxSize())
            .expireAfterWrite(cfg.getItemVectorTtlSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();
        this.rewardStats = Caffeine.newBuilder()
            .maximumSize(cfg.getRewardMaxSize())
            .expireAfterWrite(cfg.getRewardTtlSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();
        this.grpoWeights = Caffeine.newBuilder()
            .maximumSize(1)   // one fixed key: GrpoPolicyScorer.WEIGHTS_KEY
            .expireAfterWrite(cfg.getGrpoWeightsTtlSeconds(), TimeUnit.SECONDS)
            .recordStats()
            .build();
    }

    // --- item vectors ---

    public double[] getItemVector(String item) {
        return itemVectors.getIfPresent(item);
    }

    public void putItemVector(String item, double[] vector) {
        itemVectors.put(item, vector != null ? vector : new double[0]);
    }

    public boolean hasItemVector(String item) {
        return itemVectors.getIfPresent(item) != null;
    }

    // --- reward model stats ---

    public RewardModelStats getRewardStats(String key) {
        return rewardStats.getIfPresent(key);
    }

    public void putRewardStats(String key, RewardModelStats stats) {
        rewardStats.put(key, stats);
    }

    public void invalidateRewardStats(String key) {
        rewardStats.invalidate(key);
    }

    // --- GRPO policy weights ---

    /** The parsed weight vector, an empty array when Redis held nothing usable, or null when not cached. */
    public double[] getGrpoWeights(String key) {
        return grpoWeights.getIfPresent(key);
    }

    public void putGrpoWeights(String key, double[] weights) {
        grpoWeights.put(key, weights != null ? weights : new double[0]);
    }

    public record RewardModelStats(long count, double rewardTotal) {}

    // --- statistics ---

    /**
     * Caffeine's counters, copied into a plain value so the measurement layer
     * never depends on the cache library.
     *
     * A presence probe through {@code hasItemVector} is a real cache read and
     * counts as a lookup, so item-vector lookups exceed the number of vectors
     * actually consumed.
     */
    public record CacheStatsView(long hitCount, long missCount, long evictionCount, long estimatedSize) {}

    public Map<String, CacheStatsView> stats() {
        Map<String, CacheStatsView> values = new LinkedHashMap<>();
        values.put("item_vectors", view(itemVectors));
        values.put("reward_stats", view(rewardStats));
        values.put("grpo_weights", view(grpoWeights));
        return values;
    }

    private static CacheStatsView view(Cache<String, ?> cache) {
        CacheStats stats = cache.stats();
        return new CacheStatsView(
            stats.hitCount(), stats.missCount(), stats.evictionCount(), cache.estimatedSize());
    }
}
