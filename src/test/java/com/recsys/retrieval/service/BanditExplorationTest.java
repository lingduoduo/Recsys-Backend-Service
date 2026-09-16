package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.model.FeatureCache;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@SuppressWarnings({"unchecked", "null"})
class BanditExplorationTest {

    private HybridRecommendationService service(RecommendationProperties properties) {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        FeatureCache featureCache = new FeatureCache(properties);
        return new HybridRecommendationService(
            redis, properties,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache, List.of());
    }

    @Test
    void ucbBonusFollowsConfidenceFormulaWhenUncapped() {
        HybridRecommendationService svc = service(new RecommendationProperties());
        // base 0.5, 100 impressions, 50 clicks -> posteriorMean 0.5; effectivePulls 102 (priorStrength 2)
        var score = svc.computeBanditArmScore(0.5, 100, 50, 1000, false);
        double expectedBonus = 0.75 * Math.sqrt(Math.log(1002.0) / (2.0 * 103.0)); // ~0.1373596, < cap 0.25
        assertEquals(0.5, score.posteriorMean(), 1e-9);
        assertEquals(expectedBonus, score.explorationBonus(), 1e-9);
        assertEquals(0.5 + expectedBonus, score.rankingScore(), 1e-9);
    }

    @Test
    void ucbBonusIsCappedAtMaxExplorationBonus() {
        HybridRecommendationService svc = service(new RecommendationProperties());
        // base 0.5, 1 impression, 0 clicks -> posteriorMean 0.4; raw bonus > 0.25 -> capped
        var score = svc.computeBanditArmScore(0.5, 1, 0, 2, false);
        assertEquals(0.4, score.posteriorMean(), 1e-9);
        assertEquals(0.25, score.explorationBonus(), 1e-9);
        assertEquals(0.65, score.rankingScore(), 1e-9);
    }

    @Test
    void thompsonReturnsPosteriorSampleWithinBounds() {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getBandit().setAlgorithm("thompson");
        HybridRecommendationService svc = service(properties);
        var score = svc.computeBanditArmScore(0.5, 10, 5, 100, false);
        assertEquals(0.5, score.posteriorMean(), 1e-9);
        assertTrue(score.rankingScore() >= 0.0 && score.rankingScore() <= 1.0);
        assertTrue(score.explorationBonus() <= 0.25 + 1e-9);
    }
}
