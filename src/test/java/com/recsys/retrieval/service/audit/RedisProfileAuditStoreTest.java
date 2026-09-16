package com.recsys.retrieval.service.audit;

import com.recsys.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.recsys.retrieval.service.audit.ProfileAuditStore.ScanResult;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class RedisProfileAuditStoreTest {

    private static Cursor<String> cursorOf(String... keys) {
        Cursor<String> cursor = mock(Cursor.class);
        Iterator<String> it = List.of(keys).iterator();
        when(cursor.hasNext()).thenAnswer(inv -> it.hasNext());
        when(cursor.next()).thenAnswer(inv -> it.next());
        return cursor;
    }

    @Test
    void scanExtractsDistinctUserIdsAndStopsAtLimit() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        Cursor<String> cursor = cursorOf("user:u1:features", "user:u2:features", "user:u1:features", "user:u3:features");
        when(redis.scan(any(ScanOptions.class))).thenReturn(cursor);
        RedisProfileAuditStore store = new RedisProfileAuditStore(redis, "user-profile:v1");

        ScanResult result = store.scanUserIds("user:*:features", 2);

        assertEquals(List.of("u1", "u2"), result.userIds());
        assertTrue(result.truncated());
    }

    @Test
    void scanReportsNotTruncatedWhenCursorIsExhausted() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        Cursor<String> cursor = cursorOf("user:u1:features", "malformed");
        when(redis.scan(any(ScanOptions.class))).thenReturn(cursor);
        RedisProfileAuditStore store = new RedisProfileAuditStore(redis, "user-profile:v1");

        ScanResult result = store.scanUserIds("user:*:features", 10);

        assertEquals(List.of("u1"), result.userIds());
        assertFalse(result.truncated());
    }

    @Test
    void activeRunIsEmptyWhenUnsetOrBlank() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        RedisProfileAuditStore store = new RedisProfileAuditStore(redis, "user-profile:v1");

        when(values.get("user-profile:v1:active-run")).thenReturn(null);
        assertEquals(Optional.empty(), store.activeRun());
        when(values.get("user-profile:v1:active-run")).thenReturn("  ");
        assertEquals(Optional.empty(), store.activeRun());
        when(values.get("user-profile:v1:active-run")).thenReturn("run-7");
        assertEquals(Optional.of("run-7"), store.activeRun());
    }

    @Test
    void readProfilesZipsPipelinedGetAndTtlByPosition() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        List<Object> pipelineResults = new ArrayList<>();
        when(values.get("user-profile:v1:run-7:u1")).thenAnswer(inv -> { pipelineResults.add("{\"a\":1}"); return null; });
        when(redis.getExpire("user-profile:v1:run-7:u1")).thenAnswer(inv -> { pipelineResults.add(3400L); return null; });
        when(values.get("user-profile:v1:run-7:u2")).thenAnswer(inv -> { pipelineResults.add(null); return null; });
        when(redis.getExpire("user-profile:v1:run-7:u2")).thenAnswer(inv -> { pipelineResults.add(-2L); return null; });
        when(redis.executePipelined(any(SessionCallback.class))).thenAnswer(inv -> {
            SessionCallback callback = inv.getArgument(0);
            pipelineResults.clear();
            callback.execute(redis);
            return new ArrayList<>(pipelineResults);
        });
        RedisProfileAuditStore store = new RedisProfileAuditStore(redis, "user-profile:v1");

        List<RawProfile> profiles = store.readProfiles("run-7", List.of("u1", "u2"));

        assertEquals(2, profiles.size());
        assertEquals(new RawProfile("u1", "{\"a\":1}", 3400L), profiles.get(0));
        assertEquals("u2", profiles.get(1).userId());
        assertNull(profiles.get(1).json());
        assertEquals(-2L, profiles.get(1).ttlSeconds());
    }
}
