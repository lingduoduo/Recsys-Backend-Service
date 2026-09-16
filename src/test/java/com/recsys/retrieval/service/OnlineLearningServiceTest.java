package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.model.FeatureCache.RewardModelStats;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class OnlineLearningServiceTest {

    private OnlineLearningService serviceWith(FeatureCache cache, RecommendationProperties properties) {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations hashOps = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries(any())).thenReturn(null);
        return new OnlineLearningService(redis, properties, cache);
    }

    @Test
    void emptyStatsFallBackToClampedFallback() {
        RecommendationProperties properties = new RecommendationProperties();
        FeatureCache cache = new FeatureCache(properties);
        OnlineLearningService service = serviceWith(cache, properties);

        assertEquals(0.7, service.score("m1", null, 0.7), 1e-9);
    }

    @Test
    void blendsGlobalAndItemRewardMeansByConfigWeights() {
        RecommendationProperties properties = new RecommendationProperties();
        FeatureCache cache = new FeatureCache(properties);
        // global: mean 0.8, weight 0.15 ; item: mean 0.5, weight 0.45 ; confidence(10)=1.0 (minFeatureCount 3)
        cache.putRewardStats("reward-model:global", new RewardModelStats(10, 8.0));
        cache.putRewardStats("reward-model:item:m1", new RewardModelStats(10, 5.0));
        OnlineLearningService service = serviceWith(cache, properties);

        // (0.8*0.15 + 0.5*0.45) / (0.15 + 0.45) = 0.345 / 0.6 = 0.575
        assertEquals(0.575, service.score("m1", null, 0.0), 1e-9);
    }
}
