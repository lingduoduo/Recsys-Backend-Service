package com.demo.retrieval.service.clients;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;

/**
 * Redis-backed ImpressionBloomFilterClient.
 *
 * Reads from the hash at user:{userId}:bloom_filter, maintained by the impression
 * bloom filter update pipeline. Field: bits (comma-separated longs). Returns an
 * empty list for unknown users.
 *
 * Mirrors the Rust ImpressionBloomFilterClient which reads from a dedicated thrift
 * service (SurfaceArea.HOME_TIMELINE). The Java simplifies the thrift structure
 * (bloom_filter, size_cap, false_positive_rate) to a flat list of bit-vector longs
 * since MovieLens does not need multi-surface bloom filter entries.
 */
@Component
public class RedisImpressionBloomFilterClient implements ImpressionBloomFilterClient {

    private final StringRedisTemplate redis;

    public RedisImpressionBloomFilterClient(StringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public List<Long> getBloomFilterBits(String userId) {
        return parseList(redis.opsForHash().get("user:" + userId + ":bloom_filter", "bits"));
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
