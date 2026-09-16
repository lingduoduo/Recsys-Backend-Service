package com.recsys.retrieval.service.clients;

import com.recsys.retrieval.model.UserBehaviorProfile;
import com.recsys.retrieval.support.ContractFixtures;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
class RedisUserProfileClientTest {
    private static final String PROFILE_JSON = profileFixture()
        .replace("fixture-user", "u1")
        .replace("fixture-run", "run-7");

    @Test
    void readsTheActiveRunProfileAndPreservesExplicitNullMetrics() {
        Fixture fixture = fixture();
        when(fixture.values.get("user-profile:v1:active-run")).thenReturn("run-7");
        when(fixture.values.get("user-profile:v1:run-7:u1")).thenReturn(PROFILE_JSON.replace("\"sci-fi\"", "\"  SCI-FI  \""));

        Optional<UserBehaviorProfile> profile = fixture.client.getProfile("u1");

        assertTrue(profile.isPresent());
        assertEquals(null, profile.get().behavioralFeatures().averageRating());
        assertEquals("sci-fi", profile.get().preferences().genres().get(0).value());
        verify(fixture.values).get("user-profile:v1:active-run");
        verify(fixture.values).get("user-profile:v1:run-7:u1");
        assertEquals(1, fixture.registry.find("profile.lookup").timer().count());
    }

    @Test
    void returnsEmptyAndRecordsMissingActiveRunFallback() {
        Fixture fixture = fixture();
        when(fixture.values.get("user-profile:v1:active-run")).thenReturn(null);

        assertTrue(fixture.client.getProfile("u1").isEmpty());

        assertFallbackCount(fixture, "missing_active_run", 1);
    }

    @Test
    void returnsEmptyAndRecordsMissingProfileFallback() {
        Fixture fixture = fixture();
        when(fixture.values.get("user-profile:v1:active-run")).thenReturn("run-7");
        when(fixture.values.get("user-profile:v1:run-7:u1")).thenReturn(null);

        assertTrue(fixture.client.getProfile("u1").isEmpty());

        assertFallbackCount(fixture, "missing_profile", 1);
    }

    @Test
    void returnsEmptyAndRecordsInvalidJsonFallback() {
        Fixture fixture = fixture();
        when(fixture.values.get("user-profile:v1:active-run")).thenReturn("run-7");
        when(fixture.values.get("user-profile:v1:run-7:u1")).thenReturn("not-json");

        assertTrue(fixture.client.getProfile("u1").isEmpty());

        assertFallbackCount(fixture, "invalid_json", 1);
    }

    @Test
    void returnsEmptyAndRecordsUserMismatchFallback() {
        Fixture fixture = fixture();
        when(fixture.values.get("user-profile:v1:active-run")).thenReturn("run-7");
        when(fixture.values.get("user-profile:v1:run-7:u1")).thenReturn(PROFILE_JSON.replace("\"u1\"", "\"u2\""));

        assertTrue(fixture.client.getProfile("u1").isEmpty());

        assertFallbackCount(fixture, "user_mismatch", 1);
    }

    @Test
    void returnsEmptyAndRecordsRunMismatchFallback() {
        Fixture fixture = fixture();
        when(fixture.values.get("user-profile:v1:active-run")).thenReturn("run-7");
        when(fixture.values.get("user-profile:v1:run-7:u1")).thenReturn(
            PROFILE_JSON.replace("\"run-7\"", "\"run-8\"")
        );

        assertTrue(fixture.client.getProfile("u1").isEmpty());

        assertFallbackCount(fixture, "run_mismatch", 1);
    }

    @Test
    void returnsEmptyAndRecordsUnsupportedVersionFallback() {
        Fixture fixture = fixture();
        when(fixture.values.get("user-profile:v1:active-run")).thenReturn("run-7");
        when(fixture.values.get("user-profile:v1:run-7:u1")).thenReturn(PROFILE_JSON.replace("\"profile_version\":1", "\"profile_version\":2"));

        assertTrue(fixture.client.getProfile("u1").isEmpty());

        assertFallbackCount(fixture, "unsupported_version", 1);
    }

    @Test
    void returnsEmptyAndRecordsRedisErrorFallback() {
        Fixture fixture = fixture();
        when(fixture.values.get("user-profile:v1:active-run")).thenThrow(
            new RedisSystemException("redis unavailable", new IllegalStateException("connection refused"))
        );

        assertTrue(fixture.client.getProfile("u1").isEmpty());

        assertFallbackCount(fixture, "redis_error", 1);
    }

    private static void assertFallbackCount(Fixture fixture, String reason, double expected) {
        assertEquals(expected, fixture.registry.find("profile.lookup.fallback").tag("reason", reason).counter().count());
        assertEquals(1, fixture.registry.find("profile.lookup").timer().count());
    }

    private static Fixture fixture() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(values);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        return new Fixture(values, registry, new RedisUserProfileClient(redis, new ObjectMapper(), registry, "user-profile:v1"));
    }

    private static String profileFixture() {
        return ContractFixtures.text("user_profile_v1.json");
    }

    private record Fixture(
        ValueOperations<String, String> values,
        SimpleMeterRegistry registry,
        RedisUserProfileClient client
    ) {
    }
}
