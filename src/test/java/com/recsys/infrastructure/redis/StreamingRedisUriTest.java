package com.recsys.infrastructure.redis;

import io.lettuce.core.RedisURI;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Asserts that {@link StreamingRedisUri#from} authenticates on exactly the same terms as
 * {@link LettuceClientFactory#standaloneUri}, the method every service's Redis connection is
 * built from. Every expectation here is built by calling {@code standaloneUri} with the same
 * arguments — never a hand-written {@link RedisURI} — because the failure this guards against
 * is the job path and the service path drifting apart: a hand-written expectation would let
 * both sides drift together in the same edit and never notice.
 *
 * <p>{@link RedisURI#equals} is not enough on its own: it compares {@code host}/{@code port}/
 * {@code database}/{@code socket}/{@code sentinelMasterId}/{@code sentinels} but — verified by
 * decompiling {@code RedisURI.equals} in lettuce-core 6.3.2 — never looks at username, password,
 * SSL, or timeout. Two URIs with completely different credentials or TLS settings are
 * {@code .equals()} to each other as long as host and port match, which would make an
 * equals-only version of this test pass even with {@link StreamingRedisUri#from} hardcoded to
 * send no username at all (see the Step 5 probe in the task report). So every case here also
 * compares those fields directly, read off the same {@code expected} object built from
 * {@code standaloneUri} — still never hand-written.
 */
class StreamingRedisUriTest {

    private static final String HOST = "redis.internal";
    private static final int PORT = 6380;

    /**
     * Compares every field a Redis client actually authenticates with, none of which
     * {@link RedisURI#equals} inspects.
     */
    private static void assertSameAuthentication(RedisURI expected, RedisURI actual) {
        assertEquals(expected, actual);
        assertEquals(expected.getUsername(), actual.getUsername(), "username");
        assertArrayEquals(expected.getPassword(), actual.getPassword(), "password");
        assertEquals(expected.isSsl(), actual.isSsl(), "ssl");
        assertEquals(expected.getTimeout(), actual.getTimeout(), "timeout");
    }

    @Test
    void sentinelSettingsDiscoverInsteadOfUsingTheStandaloneHost() {
        RedisURI uri = StreamingRedisUri.from("stale-host", 6379, "streaming", "secret", false,
                "sentinel", "mymaster", "sentinel-a:26379,sentinel-b:26380");
        assertEquals("mymaster", uri.getSentinelMasterId());
        assertEquals(2, uri.getSentinels().size());
        assertEquals("sentinel-a", uri.getSentinels().get(0).getHost());
        assertEquals(26380, uri.getSentinels().get(1).getPort());
        assertEquals("streaming", uri.getUsername());
        assertArrayEquals("secret".toCharArray(), uri.getPassword());
    }

    @Test
    void sentinelModeRejectsMissingDiscoveryAndUnknownModes() {
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> StreamingRedisUri.from("stale-host", 6379, "", "secret", false,
                        "sentinel", "mymaster", ""));
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class,
                () -> StreamingRedisUri.from("stale-host", 6379, "", "secret", false,
                        "typo", "mymaster", "sentinel:26379"));
    }

    @Test
    void anonymousWhenBothBlank() {
        RedisURI expected = LettuceClientFactory.standaloneUri(HOST, PORT, "", "", false,
                LettuceClientFactory.DEFAULT_TIMEOUT_MS);
        RedisURI actual = StreamingRedisUri.from(HOST, PORT, "", "", false);
        assertSameAuthentication(expected, actual);
    }

    @Test
    void legacyAuthWhenOnlyPassword() {
        RedisURI expected = LettuceClientFactory.standaloneUri(HOST, PORT, "", "s3cret", false,
                LettuceClientFactory.DEFAULT_TIMEOUT_MS);
        RedisURI actual = StreamingRedisUri.from(HOST, PORT, "", "s3cret", false);
        assertSameAuthentication(expected, actual);
    }

    @Test
    void aclAuthWhenUsernameAndPassword() {
        RedisURI expected = LettuceClientFactory.standaloneUri(HOST, PORT, "streaming-job", "s3cret",
                false, LettuceClientFactory.DEFAULT_TIMEOUT_MS);
        RedisURI actual = StreamingRedisUri.from(HOST, PORT, "streaming-job", "s3cret", false);
        assertSameAuthentication(expected, actual);
    }

    @Test
    void usernameWithoutPasswordStillAcl() {
        RedisURI expected = LettuceClientFactory.standaloneUri(HOST, PORT, "streaming-job", "", false,
                LettuceClientFactory.DEFAULT_TIMEOUT_MS);
        RedisURI actual = StreamingRedisUri.from(HOST, PORT, "streaming-job", "", false);
        assertSameAuthentication(expected, actual);
    }

    @Test
    void tlsOnAndOff() {
        RedisURI expectedTlsOn = LettuceClientFactory.standaloneUri(HOST, PORT, "streaming-job",
                "s3cret", true, LettuceClientFactory.DEFAULT_TIMEOUT_MS);
        RedisURI actualTlsOn = StreamingRedisUri.from(HOST, PORT, "streaming-job", "s3cret", true);
        assertSameAuthentication(expectedTlsOn, actualTlsOn);

        RedisURI expectedTlsOff = LettuceClientFactory.standaloneUri(HOST, PORT, "streaming-job",
                "s3cret", false, LettuceClientFactory.DEFAULT_TIMEOUT_MS);
        RedisURI actualTlsOff = StreamingRedisUri.from(HOST, PORT, "streaming-job", "s3cret", false);
        assertSameAuthentication(expectedTlsOff, actualTlsOff);

        assertEquals(true, actualTlsOn.isSsl());
        assertEquals(false, actualTlsOff.isSsl());
    }

    @Test
    void blankAndNullAreTreatedAlike() {
        RedisURI blankUsernameBlankPassword = StreamingRedisUri.from(HOST, PORT, "", "", false);
        RedisURI nullUsernameNullPassword = StreamingRedisUri.from(HOST, PORT, null, null, false);
        assertSameAuthentication(blankUsernameBlankPassword, nullUsernameNullPassword);
        assertSameAuthentication(
                LettuceClientFactory.standaloneUri(HOST, PORT, "", "", false,
                        LettuceClientFactory.DEFAULT_TIMEOUT_MS),
                nullUsernameNullPassword);

        RedisURI blankUsernamePassword = StreamingRedisUri.from(HOST, PORT, "", "s3cret", false);
        RedisURI nullUsernamePassword = StreamingRedisUri.from(HOST, PORT, null, "s3cret", false);
        assertSameAuthentication(blankUsernamePassword, nullUsernamePassword);
        assertSameAuthentication(
                LettuceClientFactory.standaloneUri(HOST, PORT, "", "s3cret", false,
                        LettuceClientFactory.DEFAULT_TIMEOUT_MS),
                nullUsernamePassword);

        RedisURI usernameBlankPassword = StreamingRedisUri.from(HOST, PORT, "streaming-job", "", false);
        RedisURI usernameNullPassword = StreamingRedisUri.from(HOST, PORT, "streaming-job", null, false);
        assertSameAuthentication(usernameBlankPassword, usernameNullPassword);
        assertSameAuthentication(
                LettuceClientFactory.standaloneUri(HOST, PORT, "streaming-job", "", false,
                        LettuceClientFactory.DEFAULT_TIMEOUT_MS),
                usernameNullPassword);
    }
}
