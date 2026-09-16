package com.demo.retrieval.service.clients;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Redis-backed ServedHistoryClient.
 *
 * Reads from user:{userId}:served_history, written by the recommendation
 * service when it serves results. Field: movieIds (comma-separated,
 * newest-first). Returns an empty list for unknown users.
 *
 * Mirrors the Rust ServedHistoryClient.get_recent(HOME_TIMELINE). The Rust
 * version filters entries by a recency time window (recently_served_ids);
 * MovieLens omits time filtering because per-entry timestamps are not stored —
 * recency is enforced by capping the list size at write time instead.
 */
@Component
public class RedisServedHistoryClient implements ServedHistoryClient {

    private final StringRedisTemplate redis;

    public RedisServedHistoryClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public List<String> getRecentlyServedMovieIds(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":served_history", "movieIds"));
    }

    private static List<String> parseList(Object raw) {
        if (raw == null) {
            return List.of();
        }
        return Arrays.stream(raw.toString().split(","))
            .map(String::trim)
            .filter(s -> !s.isBlank())
            .toList();
    }
}
