package com.recsys.config;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisNode;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins that the Spring Data Redis connection factory is built from recsys.redis and runs the
 * same credential guard as the raw-Lettuce path. If this drifts back to Spring Boot's
 * spring.data.redis.* autoconfiguration, an unauthenticated Redis connection becomes reachable
 * on 8080 with no refusal anywhere.
 */
class RetrievalRedisConfigTest {

    private static RedisProperties standalone() {
        RedisProperties props = new RedisProperties();
        props.setHost("redis.example.internal");
        props.setPort(6380);
        props.setPassword("s3cret");
        props.setTimeoutMs(1500);
        return props;
    }

    @Test
    void buildsTheFactoryFromRecsysRedisNotSpringDataRedis() {
        LettuceConnectionFactory factory =
            RetrievalRedisConfig.connectionFactory(standalone(), Map.of());

        RedisStandaloneConfiguration cfg = factory.getStandaloneConfiguration();
        assertThat(cfg).isNotNull();
        assertThat(cfg.getHostName()).isEqualTo("redis.example.internal");
        assertThat(cfg.getPort()).isEqualTo(6380);
        assertThat(new String(cfg.getPassword().get())).isEqualTo("s3cret");
    }

    @Test
    void refusesAnUnauthenticatedConnection() {
        RedisProperties props = standalone();
        props.setPassword("");

        assertThatThrownBy(() -> RetrievalRedisConfig.connectionFactory(props, Map.of()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("REDIS_PASSWORD");
    }

    @Test
    void honoursTheSentinelMode() {
        RedisProperties props = standalone();
        props.setMode("sentinel");
        props.setSentinelMaster("mymaster");
        props.setSentinelNodes("sentinel-a:26379,sentinel-b:26379");

        LettuceConnectionFactory factory = RetrievalRedisConfig.connectionFactory(props, Map.of());

        assertThat(factory.getSentinelConfiguration()).isNotNull();
        assertThat(factory.getSentinelConfiguration().getMaster().getName()).isEqualTo("mymaster");
    }

    /**
     * LettuceClientFactory.sentinelUri defaults a colon-less sentinel node to port 26379.
     * RedisSentinelConfiguration (via RedisNode.fromString) throws IllegalArgumentException on a
     * bare hostname with no port. This pins that the bridge normalizes nodes the same way the
     * raw-Lettuce path does, so recsys.redis.sentinel-nodes means the same thing on both stacks.
     * Also exercises the whitespace-padding and trailing-comma handling the split/strip/filter
     * chain already claims to provide.
     */
    @Test
    void sentinelNodeWithoutAPortDefaultsToTheStandardSentinelPortLikeTheRawLettucePath() {
        RedisProperties props = standalone();
        props.setMode("sentinel");
        props.setSentinelMaster("mymaster");
        props.setSentinelNodes(" sentinel-a:26380 , sentinel-b , ");

        LettuceConnectionFactory factory = RetrievalRedisConfig.connectionFactory(props, Map.of());

        RedisSentinelConfiguration sentinel = factory.getSentinelConfiguration();
        assertThat(sentinel).isNotNull();
        Set<RedisNode> sentinels = sentinel.getSentinels();
        assertThat(sentinels)
            .extracting(RedisNode::getHost, RedisNode::getPort)
            .containsExactlyInAnyOrder(
                org.assertj.core.groups.Tuple.tuple("sentinel-a", 26380),
                org.assertj.core.groups.Tuple.tuple("sentinel-b", 26379));
    }

    /**
     * LettuceClientFactory.parsePort falls back to 6379 (the Redis data port, not the sentinel
     * port) on an unparseable port string, which would silently misdirect a sentinel client.
     * The bridge deliberately does not mirror that fallback: a trailing-colon entry like this
     * one must be rejected outright, with a message that names the property and the offending
     * value rather than a generic failure from inside Spring Data.
     */
    @Test
    void rejectsASentinelNodeWithATrailingColonAndNoPort() {
        RedisProperties props = standalone();
        props.setMode("sentinel");
        props.setSentinelMaster("mymaster");
        props.setSentinelNodes("sentinel-a:");

        assertThatThrownBy(() -> RetrievalRedisConfig.connectionFactory(props, Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("recsys.redis.sentinel-nodes")
            .hasMessageContaining("sentinel-a:");
    }

    @Test
    void rejectsASentinelNodeWithANonNumericPort() {
        RedisProperties props = standalone();
        props.setMode("sentinel");
        props.setSentinelMaster("mymaster");
        props.setSentinelNodes("sentinel-a:abc");

        assertThatThrownBy(() -> RetrievalRedisConfig.connectionFactory(props, Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("recsys.redis.sentinel-nodes")
            .hasMessageContaining("sentinel-a:abc");
    }

    @Test
    void rejectsASentinelNodeWithAnOutOfRangePort() {
        RedisProperties props = standalone();
        props.setMode("sentinel");
        props.setSentinelMaster("mymaster");
        props.setSentinelNodes("sentinel-a:70000");

        assertThatThrownBy(() -> RetrievalRedisConfig.connectionFactory(props, Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("recsys.redis.sentinel-nodes")
            .hasMessageContaining("sentinel-a:70000");
    }

    /**
     * A bare, unbracketed IPv6 literal has more than one colon and no port segment.
     * LettuceClientFactory.sentinelUri would mangle this into host "fe80:" port "1" via its own
     * lastIndexOf(':') split; the bridge rejects it instead of reproducing that mangling.
     */
    @Test
    void rejectsABareUnbracketedIpv6Literal() {
        RedisProperties props = standalone();
        props.setMode("sentinel");
        props.setSentinelMaster("mymaster");
        props.setSentinelNodes("fe80::1");

        assertThatThrownBy(() -> RetrievalRedisConfig.connectionFactory(props, Map.of()))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("recsys.redis.sentinel-nodes")
            .hasMessageContaining("fe80::1");
    }
}
