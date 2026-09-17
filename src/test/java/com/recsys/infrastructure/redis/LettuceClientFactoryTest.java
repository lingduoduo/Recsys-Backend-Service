package com.recsys.infrastructure.redis;

import com.recsys.config.RedisProperties;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class LettuceClientFactoryTest {

    @Test
    void standaloneUriFromEnv_usesHostPortFromEnvironment() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(Map.of(
                "REDIS_MODE", "standalone",
                "REDIS_HOST", "redis.internal",
                "REDIS_PORT", "6380"
        ), Integer.MAX_VALUE);
        assertEquals("redis.internal", uri.getHost());
        assertEquals(6380, uri.getPort());
        assertNull(uri.getSentinelMasterId());
    }

    @Test
    void defaultModeIsStandaloneLocalhost() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(Map.of(), Integer.MAX_VALUE);
        assertEquals("localhost", uri.getHost());
        assertEquals(6379, uri.getPort());
    }

    @Test
    void sentinelModeBuildsSentinelUri() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(Map.of(
                "REDIS_MODE", "sentinel",
                "REDIS_SENTINEL_MASTER", "mymaster",
                "REDIS_SENTINEL_NODES", "sentinel-1:26379,sentinel-2:26379,sentinel-3:26379"
        ), Integer.MAX_VALUE);
        assertEquals("mymaster", uri.getSentinelMasterId());
        assertEquals(3, uri.getSentinels().size());
        assertEquals(26379, uri.getSentinels().get(0).getPort());
    }

    @Test
    void timeoutIsCappedToMaxWhenEnvUnset() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(Map.of(), 150);
        assertEquals(Duration.ofMillis(150), uri.getTimeout());
    }

    @Test
    void timeoutUsesSmallerOfEnvAndCap() {
        RedisURI capped = LettuceClientFactory.uriFromEnv(Map.of("REDIS_TIMEOUT_MS", "500"), 150);
        assertEquals(Duration.ofMillis(150), capped.getTimeout());

        RedisURI envWins = LettuceClientFactory.uriFromEnv(Map.of("REDIS_TIMEOUT_MS", "100"), 150);
        assertEquals(Duration.ofMillis(100), envWins.getTimeout());
    }

    @Test
    void timeoutUncappedWhenMaxIsIntMax() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(Map.of("REDIS_TIMEOUT_MS", "300"), Integer.MAX_VALUE);
        assertEquals(Duration.ofMillis(300), uri.getTimeout());
    }

    @Test
    void parsePortReturnsTheCallersFallbackOnInvalidValue() {
        assertEquals(6379, LettuceClientFactory.parsePort("notANumber", 6379));
        assertEquals(26379, LettuceClientFactory.parsePort("notANumber", 26379));
        assertEquals(26379, LettuceClientFactory.parsePort(null, 26379));
    }

    @Test
    void parsePortParsesValidPort() {
        assertEquals(6380, LettuceClientFactory.parsePort("6380", 6379));
    }

    /**
     * A sentinel node written with a trailing colon and no digits used to fall back to 6379 — the
     * Redis <em>data</em> port — so a typo in REDIS_SENTINEL_NODES silently pointed the client at
     * the wrong service instead of a sentinel. The same method already used 26379 for a node
     * written with no colon at all, so one method defaulted two ways for the same class of input.
     */
    @Test
    void aSentinelNodeWithAnUnparseablePortFallsBackToTheSentinelPort() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(Map.of(
                "REDIS_MODE", "sentinel",
                "REDIS_SENTINEL_NODES", "sentinel-a:,sentinel-b:notANumber,sentinel-c"
        ), Integer.MAX_VALUE);

        assertEquals(3, uri.getSentinels().size());
        assertEquals(26379, uri.getSentinels().get(0).getPort());
        assertEquals(26379, uri.getSentinels().get(1).getPort());
        assertEquals(26379, uri.getSentinels().get(2).getPort());
    }

    /** The standalone path keeps the data port as its fallback — REDIS_PORT is not a sentinel. */
    @Test
    void anUnparseableRedisPortStillFallsBackToTheDataPort() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(
                Map.of("REDIS_PORT", "notANumber"), Integer.MAX_VALUE);

        assertEquals(6379, uri.getPort());
    }

    @Test
    void defaultPoolKnobsReadFailFastLimitsFromEnvironment() {
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> cfg =
                LettuceClientFactory.poolConfig(LettuceClientFactory.defaultPoolKnobs(Map.of(
                        "REDIS_POOL_MAX_TOTAL", "80",
                        "REDIS_POOL_MAX_IDLE", "20",
                        "REDIS_POOL_MIN_IDLE", "4",
                        "REDIS_POOL_MAX_WAIT_MS", "125",
                        "REDIS_POOL_TEST_ON_BORROW", "false"
                )));

        assertEquals(80, cfg.getMaxTotal());
        assertEquals(20, cfg.getMaxIdle());
        assertEquals(4, cfg.getMinIdle());
        assertEquals(Duration.ofMillis(125), cfg.getMaxWaitDuration());
        assertFalse(cfg.getTestOnBorrow());
    }

    @Test
    void poolConfigEnablesIdleConnectionValidation() {
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> cfg =
                LettuceClientFactory.poolConfig(new RedisProperties.Pool());
        assertTrue(cfg.getTestWhileIdle(), "idle connections should be validated by the eviction sweep");
        assertEquals(-1, cfg.getNumTestsPerEvictionRun(), "every idle connection should be tested per sweep");
    }

    @Test
    void standaloneUriWithPasswordBuilds() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(Map.of(
                "REDIS_HOST", "localhost",
                "REDIS_PORT", "6379",
                "REDIS_PASSWORD", "secret"
        ), Integer.MAX_VALUE);
        assertEquals("localhost", uri.getHost());
        assertNotNull(uri.getCredentialsProvider(), "password should configure a credentials provider");
    }

    @Test
    void tlsIsOffByDefault() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(
                Map.of("REDIS_HOST", "cache"), Integer.MAX_VALUE);
        assertFalse(uri.isSsl(), "REDIS_TLS must default to false");
    }

    @Test
    void tlsFlagEnablesSslOnTheUri() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(
                Map.of("REDIS_HOST", "cache", "REDIS_TLS", "true"), Integer.MAX_VALUE);
        assertTrue(uri.isSsl(), "REDIS_TLS=true must produce an SSL RedisURI");
    }

    @Test
    void usernameProducesAnAclLogin() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(
                Map.of("REDIS_HOST", "cache", "REDIS_USERNAME", "catalog",
                        "REDIS_PASSWORD", "s3cret"),
                Integer.MAX_VALUE);
        assertEquals("catalog", uri.getUsername());
    }

    @Test
    void passwordWithoutUsernameStaysOnTheDefaultUser() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(
                Map.of("REDIS_HOST", "cache", "REDIS_PASSWORD", "s3cret"), Integer.MAX_VALUE);
        String username = uri.getUsername();
        assertTrue(username == null || username.isBlank(),
                "no REDIS_USERNAME means legacy default-user AUTH");
    }

    @Test
    void sentinelUriCarriesAuthAndTls() {
        RedisURI uri = LettuceClientFactory.uriFromEnv(
                Map.of("REDIS_MODE", "sentinel", "REDIS_SENTINEL_NODES", "s1:26379",
                        "REDIS_USERNAME", "catalog", "REDIS_PASSWORD", "s3cret",
                        "REDIS_TLS", "true"),
                Integer.MAX_VALUE);
        assertTrue(uri.isSsl());
        assertEquals("catalog", uri.getUsername());
    }

    /**
     * The replica URIs are built outside uriFromEnv, so a change that updates only the primary
     * path leaves every replica connection unauthenticated and in the clear while the primary
     * looks correct — and reads route to replicas, so that is most of the traffic. Nothing in the
     * diff makes the omission visible, which is why this assertion exists.
     */
    @Test
    void replicaUriInheritsAuthAndTls() {
        RedisURI uri = LettuceClientFactory.replicaUri(
                ReplicaConfig.parse("replica-a:6379@us-east-1b"),
                "catalog", "s3cret", true, 2000);
        assertEquals("replica-a", uri.getHost());
        assertEquals("catalog", uri.getUsername());
        assertTrue(uri.isSsl());
    }

    /**
     * The Spring-properties path is the model-serving production path, and it is a second,
     * independent copy of the same wiring: {@code uriFrom(props)} can lose the username or the
     * TLS flag while every env-var assertion above stays green.
     */
    @Test
    void springPropertiesUriCarriesAuthAndTls() {
        RedisProperties props = new RedisProperties();
        props.setHost("cache");
        props.setUsername("model");
        props.setPassword("s3cret");
        props.setTls(true);

        RedisURI uri = LettuceClientFactory.uriFrom(props);
        assertEquals("cache", uri.getHost());
        assertEquals("model", uri.getUsername());
        assertTrue(uri.isSsl(), "recsys.redis.tls must produce an SSL RedisURI");
    }

    @Test
    void springPropertiesSentinelUriCarriesAuthAndTls() {
        RedisProperties props = new RedisProperties();
        props.setMode("sentinel");
        props.setSentinelNodes("s1:26379");
        props.setUsername("model");
        props.setPassword("s3cret");
        props.setTls(true);

        RedisURI uri = LettuceClientFactory.uriFrom(props);
        assertEquals("mymaster", uri.getSentinelMasterId());
        assertEquals("model", uri.getUsername());
        assertTrue(uri.isSsl());
    }

    /** The props-path twin of {@link #replicaUriInheritsAuthAndTls}, and just as easy to omit. */
    @Test
    void springPropertiesReplicaUriInheritsAuthAndTls() {
        RedisProperties props = new RedisProperties();
        props.setUsername("model");
        props.setPassword("s3cret");
        props.setTls(true);

        RedisURI uri = LettuceClientFactory.replicaUriFrom(
                ReplicaConfig.parse("replica-a:6379@us-east-1b"), props);
        assertEquals("replica-a", uri.getHost());
        assertEquals("model", uri.getUsername());
        assertTrue(uri.isSsl());
    }

    /**
     * Proves {@code routerFrom} actually builds the replica executors rather than silently
     * falling back to the primary: a router with no replicas reads from the primary, so an
     * unparsed or dropped {@code replicaNodes} spec presents as working reads on the wrong node.
     */
    @Test
    void routerFromPropsBuildsReplicaExecutors() {
        RedisProperties props = new RedisProperties();
        props.setHost("primary");
        props.setPassword("s3cret");
        props.setReplicaNodes("replica-a:6379@us-east-1b");

        try (RedisReadReplicaRouter router = LettuceClientFactory.routerFrom(props)) {
            assertNotSame(router.writable(), router.readable(),
                    "reads must route to the configured replica, not back to the primary");
        }
    }

    @Test
    void blankPasswordWithoutTheOptOutIsRefused() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> LettuceClientFactory.requireAuthentication("", Map.of()));
        assertTrue(e.getMessage().contains("REDIS_PASSWORD"),
                "the message must name the variable to set");
        assertTrue(e.getMessage().contains("REDIS_ALLOW_NO_AUTH"),
                "the message must name the deliberate escape, or the reader has no way out");
    }

    @Test
    void blankPasswordIsAllowedWhenExplicitlyOptedOut() {
        assertDoesNotThrow(() ->
                LettuceClientFactory.requireAuthentication("", Map.of("REDIS_ALLOW_NO_AUTH", "true")));
    }

    @Test
    void aPasswordNeedsNoOptOut() {
        assertDoesNotThrow(() -> LettuceClientFactory.requireAuthentication("s3cret", Map.of()));
    }
}
