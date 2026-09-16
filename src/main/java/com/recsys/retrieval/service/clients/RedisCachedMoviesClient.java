package com.demo.retrieval.service.clients;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Redis-backed CachedMoviesClient.
 *
 * Reads from the hash at user:{userId}:cached_movies, written by the offline
 * precomputation pipeline. Field: movieIds (comma-separated). Returns an empty
 * list for unknown users.
 *
 * Reads from a dedicated Redis key rather than the general feature store;
 * cached candidates are written by a separate offline precomputation path
 * with different freshness guarantees.
 */
@Component
public class RedisCachedMoviesClient implements CachedMoviesClient {

    private final StringRedisTemplate redis;

    public RedisCachedMoviesClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public List<String> getCachedMovieIds(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":cached_movies", "movieIds"));
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
