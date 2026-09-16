package com.demo.retrieval.service.clients;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Redis-backed PastRequestTimestampsClient.
 *
 * Reads from user:{userId}:request_history, written by the recommendation
 * service each time it handles a request. Field: timestamps (comma-separated
 * epoch-millis longs, newest-first). Returns an empty list for unknown users.
 *
 * Mirrors the Rust PastRequestTimestampsClient which fetches non_polling
 * request timestamps from a dedicated store separate from the feature store.
 */
@Component
public class RedisPastRequestTimestampsClient implements PastRequestTimestampsClient {

    private final StringRedisTemplate redis;

    public RedisPastRequestTimestampsClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public List<Long> getTimestamps(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":request_history", "timestamps"));
    }

    private static List<Long> parseList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        return Arrays.stream(raw.toString().split(","))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .map(Long::parseLong)
            .toList();
    }
}
