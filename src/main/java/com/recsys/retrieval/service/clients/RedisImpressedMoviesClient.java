package com.demo.retrieval.service.clients;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Redis-backed ImpressedMoviesClient.
 *
 * Reads from the hash at user:{userId}:impressions, written by the impression
 * logging pipeline. Field: movieIds (comma-separated). Returns an empty list for
 * unknown users.
 *
 * Reads from a dedicated Redis key rather than the general feature store;
 * impression data has its own write path and freshness requirements separate
 * from ratings.
 */
@Component
public class RedisImpressedMoviesClient implements ImpressedMoviesClient {

    private final StringRedisTemplate redis;

    public RedisImpressedMoviesClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public List<String> getImpressedMovieIds(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":impressions", "movieIds"));
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
