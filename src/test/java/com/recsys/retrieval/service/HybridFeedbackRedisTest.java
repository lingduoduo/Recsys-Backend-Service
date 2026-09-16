package com.demo.retrieval.service;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.model.FeedbackRequest;
import com.demo.retrieval.service.clients.UserMovieHistoryClient.UserMovieHistory;
import com.demo.retrieval.service.query_hydrators.MovieLensUserHistoryQueryHydrator;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.when;

/**
 * Real-Redis coverage for the RL feedback path (#112): pending-key idempotency (B1) and the
 * atomic Lua Q-update (B3). Docker-gated — the whole class is skipped when no Docker daemon is
 * available, so Docker-less builds stay green.
 */
@SuppressWarnings({"unchecked", "null"})
class HybridFeedbackRedisTest {

    private static GenericContainer<?> redisContainer;
    private static StringRedisTemplate redis;

    private static final String Q_KEY = "q-learning:q:" + TabularStateKey.hash(List.of("drama"), List.of());
    private static final String STATE_PAYLOAD =
        "{\"type\":\"rl_experience\",\"action\":\"m1\","
      + "\"state\":{\"recent\":[],\"genres\":[\"drama\"],\"tags\":[]}}";

    @BeforeAll
    static void startRedis() {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker not available");
        redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);
        redisContainer.start();
        LettuceConnectionFactory cf = new LettuceConnectionFactory(
            redisContainer.getHost(), redisContainer.getMappedPort(6379));
        cf.afterPropertiesSet();
        redis = new StringRedisTemplate(cf);
        redis.afterPropertiesSet();
    }

    @AfterAll
    static void stopRedis() {
        if (redisContainer != null) {
            redisContainer.stop();
        }
    }

    @BeforeEach
    void flush() {
        Set<String> keys = redis.keys("*");
        if (keys != null && !keys.isEmpty()) {
            redis.delete(keys);
        }
    }

    private HybridRecommendationService qLearningService() {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getBandit().setAlgorithm("q-learning");
        properties.setCatalog(new LinkedHashMap<>());
        FeatureCache featureCache = new FeatureCache(properties);
        return new HybridRecommendationService(
            redis, properties,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache,
            List.of(new MovieLensUserHistoryQueryHydrator(
                userId -> new UserMovieHistory(List.of(), List.of()))));
    }

    private double qValue(String item) {
        Object raw = redis.opsForHash().get(Q_KEY, item);
        return raw == null ? Double.NaN : Double.parseDouble(String.valueOf(raw));
    }

    // B1: the pending key is deleted on consume, and replayed feedback does not re-apply the update.
    @Test
    void feedbackDeletesPendingKeyAndReplayIsIdempotent() {
        HybridRecommendationService service = qLearningService();
        String pendingKey = MovieLensServingSideEffects.pendingReplayKey("u1", "m1");
        redis.opsForValue().set(pendingKey, STATE_PAYLOAD);

        service.recordFeedback(new FeedbackRequest("u1", "m1", true, 1.0));

        assertNull(redis.opsForValue().get(pendingKey), "pending key must be consumed");
        // q0=0, next=0 -> updated = alpha*reward = 0.1 * 1.0
        double afterFirst = qValue("m1");
        assertEquals(0.1, afterFirst, 1e-9);

        // Replay with the pending key already gone: no stored state -> no second Q change.
        service.recordFeedback(new FeedbackRequest("u1", "m1", true, 1.0));
        assertEquals(afterFirst, qValue("m1"), 1e-9);
    }

    // B3: two real consumes accumulate Q exactly per q <- q + alpha*(reward + gamma*next - q) via the Lua EVAL.
    @Test
    void luaQUpdateAccumulatesAcrossFeedback() {
        HybridRecommendationService service = qLearningService();
        String pendingKey = MovieLensServingSideEffects.pendingReplayKey("u1", "m1");

        redis.opsForValue().set(pendingKey, STATE_PAYLOAD);
        service.recordFeedback(new FeedbackRequest("u1", "m1", true, 1.0));
        double q1 = qValue("m1");

        redis.opsForValue().set(pendingKey, STATE_PAYLOAD);
        service.recordFeedback(new FeedbackRequest("u1", "m1", true, 1.0));
        double q2 = qValue("m1");

        // next state Q is 0 -> q2 = q1 + 0.1*(1.0 - q1)
        assertEquals(q1 + 0.1 * (1.0 - q1), q2, 1e-9);
        assertTrue(q2 > q1, "second update must move Q toward the reward");
    }
}
