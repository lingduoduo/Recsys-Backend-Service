package com.demo.retrieval.service.clients;

import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RedisUserMovieHistoryClient implements UserMovieHistoryClient {
    private static final int HISTORY_SIZE = 50;

    private final StringRedisTemplate redis;

    public RedisUserMovieHistoryClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * Pipelines both list reads in a single Redis round-trip.
     */
    @Override
    @SuppressWarnings("unchecked")
    public UserMovieHistory getMovieHistory(String userId) {
        String recentKey = "user:" + userId + ":recent";
        String ratedKey  = "user:" + userId + ":rated";
        List<Object> results = redis.executePipelined(new SessionCallback<Object>() {
            @Override
            @SuppressWarnings("unchecked")
            public <K, V> Object execute(RedisOperations<K, V> ops) {
                RedisOperations<String, String> s = (RedisOperations<String, String>) ops;
                s.opsForList().range(recentKey, 0L, HISTORY_SIZE - 1L);
                s.opsForList().range(ratedKey,  0L, HISTORY_SIZE - 1L);
                return null;
            }
        });
        List<String> watched = results.get(0) != null ? (List<String>) results.get(0) : List.of();
        List<String> rated   = results.get(1) != null ? (List<String>) results.get(1) : List.of();
        return new UserMovieHistory(watched, rated);
    }
}
