package com.recsys.retrieval.service.audit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Redis-backed store. SCAN is cursor-based so it never blocks Redis the way KEYS does; it may
 * return a key twice, hence the LinkedHashSet. Each readProfiles call is one pipelined round trip
 * (GET + TTL per user) on a connection dedicated to that call, which is what makes concurrent
 * chunks safe: no pipeline is ever shared between threads.
 */
@Component
public class RedisProfileAuditStore implements ProfileAuditStore {
    private static final long SCAN_COUNT_HINT = 1000L;

    private final StringRedisTemplate redis;
    private final String keyPrefix;

    public RedisProfileAuditStore(
        StringRedisTemplate redis,
        @Value("${recsys.retrieval.user-profile.key-prefix:user-profile:v1}") String keyPrefix
    ) {
        this.redis = redis;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public ScanResult scanUserIds(String pattern, int limit) {
        Set<String> userIds = new LinkedHashSet<>();
        ScanOptions options = ScanOptions.scanOptions().match(pattern).count(SCAN_COUNT_HINT).build();
        try (Cursor<String> cursor = redis.scan(options)) {
            while (cursor.hasNext() && userIds.size() < limit) {
                String[] segments = cursor.next().split(":");
                if (segments.length >= 2 && !segments[1].isEmpty()) {
                    userIds.add(segments[1]);
                }
            }
            return new ScanResult(new ArrayList<>(userIds), cursor.hasNext());
        }
    }

    @Override
    public Optional<String> activeRun() {
        return Optional.ofNullable(redis.opsForValue().get(keyPrefix + ":active-run"))
            .filter(run -> !run.isBlank());
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<RawProfile> readProfiles(String run, List<String> userIds) {
        List<Object> results = redis.executePipelined(new SessionCallback<Object>() {
            @Override
            public <K, V> Object execute(RedisOperations<K, V> ops) {
                RedisOperations<String, String> s = (RedisOperations<String, String>) ops;
                for (String userId : userIds) {
                    String key = keyPrefix + ":" + run + ":" + userId;
                    s.opsForValue().get(key);
                    s.getExpire(key);
                }
                return null;
            }
        });
        List<RawProfile> profiles = new ArrayList<>(userIds.size());
        for (int i = 0; i < userIds.size(); i++) {
            String json = (String) results.get(2 * i);
            Long ttl = (Long) results.get(2 * i + 1);
            profiles.add(new RawProfile(userIds.get(i), json, ttl == null ? -2L : ttl));
        }
        return profiles;
    }
}
