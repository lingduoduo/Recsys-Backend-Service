package com.recsys.retrieval.model;

import com.recsys.retrieval.config.RecommendationProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FeatureCacheStatsTest {

    @Test
    void reportsZeroedStatsBeforeAnyLookup() {
        Map<String, FeatureCache.CacheStatsView> stats = new FeatureCache(new RecommendationProperties()).stats();

        assertEquals(3, stats.size());
        assertEquals(0L, stats.get("item_vectors").hitCount());
        assertEquals(0L, stats.get("item_vectors").missCount());
        assertEquals(0L, stats.get("reward_stats").hitCount());
    }

    @Test
    void countsGrpoWeightLookupsSeparately() {
        FeatureCache cache = new FeatureCache(new RecommendationProperties());

        cache.getGrpoWeights("absent");
        cache.putGrpoWeights("key", new double[] {0.5});
        cache.getGrpoWeights("key");

        FeatureCache.CacheStatsView weights = cache.stats().get("grpo_weights");
        assertEquals(1L, weights.hitCount());
        assertEquals(1L, weights.missCount());
        assertEquals(0L, cache.stats().get("reward_stats").missCount());
    }

    @Test
    void countsItemVectorHitsAndMisses() {
        FeatureCache cache = new FeatureCache(new RecommendationProperties());

        cache.getItemVector("missing");
        cache.putItemVector("present", new double[] {1.0, 2.0});
        cache.getItemVector("present");

        FeatureCache.CacheStatsView vectors = cache.stats().get("item_vectors");
        assertEquals(1L, vectors.hitCount());
        assertEquals(1L, vectors.missCount());
        assertEquals(1L, vectors.estimatedSize());
    }

    @Test
    void countsRewardStatLookupsSeparately() {
        FeatureCache cache = new FeatureCache(new RecommendationProperties());

        cache.getRewardStats("absent");
        cache.putRewardStats("key", new FeatureCache.RewardModelStats(3L, 1.5));
        cache.getRewardStats("key");

        FeatureCache.CacheStatsView rewards = cache.stats().get("reward_stats");
        assertEquals(1L, rewards.hitCount());
        assertEquals(1L, rewards.missCount());
        assertEquals(0L, cache.stats().get("item_vectors").hitCount());
    }
}
