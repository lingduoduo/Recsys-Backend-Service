package com.demo.retrieval.service.clients;

import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.service.clients.UserProfileValidation.Invalid;
import com.demo.retrieval.service.clients.UserProfileValidation.Result;
import com.demo.retrieval.service.clients.UserProfileValidation.Valid;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;

/** Reads the profile belonging to the run selected by the Redis active-run pointer. */
@Component
public class RedisUserProfileClient implements UserProfileClient {
    private static final Logger log = LoggerFactory.getLogger(RedisUserProfileClient.class);

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;
    private final String keyPrefix;

    public RedisUserProfileClient(
        StringRedisTemplate redis,
        ObjectMapper objectMapper,
        MeterRegistry meterRegistry,
        @Value("${recsys.user-profile.key-prefix:user-profile:v1}") String keyPrefix
    ) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public Optional<UserBehaviorProfile> getProfile(String userId) {
        Timer.Sample timer = Timer.start(meterRegistry);
        try {
            String runId = redis.opsForValue().get(keyPrefix + ":active-run");
            if (runId == null || runId.isBlank()) {
                return fallback("missing_active_run");
            }
            String rawProfile = redis.opsForValue().get(keyPrefix + ":" + runId + ":" + userId);
            Result result = UserProfileValidation.validate(rawProfile, userId, runId, objectMapper);
            if (result instanceof Valid valid) {
                return Optional.of(valid.profile());
            }
            String reason = ((Invalid) result).reason();
            if ("invalid_json".equals(reason)) {
                log.warn("Unable to parse user profile for user {}", userId);
            }
            return fallback(reason);
        } catch (RuntimeException e) {
            log.warn("User profile lookup failed for user {}", userId);
            return fallback("redis_error");
        } finally {
            timer.stop(Timer.builder("profile.lookup").register(meterRegistry));
        }
    }

    private Optional<UserBehaviorProfile> fallback(String reason) {
        Counter.builder("profile.lookup.fallback").tag("reason", reason).register(meterRegistry).increment();
        return Optional.empty();
    }
}
