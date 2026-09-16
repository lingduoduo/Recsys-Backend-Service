package com.recsys.config;

import com.recsys.infrastructure.redis.LettuceClientFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.RedisConfiguration.WithPassword;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Bridges the retrieval code's Spring Data Redis access onto this service's own Redis
 * configuration.
 *
 * <p>The retrieval tree reaches Redis through {@link StringRedisTemplate} in 28 classes. Letting
 * Spring Boot autoconfigure that template would create a second connection pool configured by
 * {@code spring.data.redis.*}, independent of {@code recsys.redis} and — the point — invisible to
 * {@link LettuceClientFactory}'s credential guard. An existing security control would become
 * bypassable on 8080 simply by merging this code. The factory is built here instead, from the
 * same properties and behind the same guard as the raw-Lettuce executors in {@link RedisConfig}.
 *
 * <p>Today what keeps Boot's {@code RedisAutoConfiguration} from also running — and opening that
 * second, unguarded pool — is {@code @ConditionalOnMissingBean}: because this class already
 * registers a {@code LettuceConnectionFactory} and a {@code StringRedisTemplate}, Boot's
 * autoconfiguration backs off. That is weaker than an explicit
 * {@code spring.autoconfigure.exclude}: renaming or removing either bean here silently
 * re-enables the {@code spring.data.redis.*} path, with no compile error to catch it. An explicit
 * exclusion on {@code ModelApplication} is a follow-up, not yet in place.
 *
 * <p>Consequence worth knowing: {@code spring.data.redis.*} is inert in this application. Tests
 * that point at an ephemeral Redis must set {@code recsys.redis.host} / {@code recsys.redis.port}.
 */
@Configuration
public class RetrievalRedisConfig {

    static LettuceConnectionFactory connectionFactory(RedisProperties props, Map<String, String> env) {
        LettuceClientFactory.requireAuthentication(props, env);

        LettuceClientConfiguration.LettuceClientConfigurationBuilder client =
            LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(props.getTimeoutMs()));
        if (props.isTls()) {
            client.useSsl();
        }

        if ("sentinel".equalsIgnoreCase(props.getMode())) {
            Set<String> nodes = Arrays.stream(props.getSentinelNodes().split(","))
                .map(String::strip)
                .filter(node -> !node.isEmpty())
                .map(RetrievalRedisConfig::withDefaultSentinelPort)
                .collect(Collectors.toCollection(LinkedHashSet::new));
            RedisSentinelConfiguration sentinel =
                new RedisSentinelConfiguration(props.getSentinelMaster(), nodes);
            applyCredentials(sentinel, props);
            return new LettuceConnectionFactory(sentinel, client.build());
        }

        RedisStandaloneConfiguration standalone =
            new RedisStandaloneConfiguration(props.getHost(), props.getPort());
        applyCredentials(standalone, props);
        return new LettuceConnectionFactory(standalone, client.build());
    }

    private static final int DEFAULT_SENTINEL_PORT = 26379;
    private static final int MIN_PORT = 1;
    private static final int MAX_PORT = 65535;

    /**
     * Normalizes one {@code recsys.redis.sentinel-nodes} entry, matching
     * {@code LettuceClientFactory.sentinelUri}'s handling of a bare hostname (no colon at all):
     * that path silently defaults such an entry to port 26379, and so does this one — without
     * it, {@code RedisSentinelConfiguration} (via {@code RedisNode.fromString}) would throw on
     * the same input the raw-Lettuce path accepts.
     *
     * <p>What this deliberately does <b>not</b> mirror is {@code LettuceClientFactory.parsePort}'s
     * fallback to port 6379 on an unparseable port string. 6379 is the Redis <em>data</em> port;
     * silently pointing a sentinel client at it instead of 26379 is a latent bug in the
     * raw-Lettuce path (visible even within {@code sentinelUri} itself, which uses 26379 for the
     * colon-less case but would fall through to 6379 for something like {@code "sentinel-a:"}),
     * and new code should not copy it. Sentinel is a live deployment mode — see
     * {@code k8s/eks-shared/network-policy-elasticache-patch.yaml} — so a malformed entry here
     * fails loudly, with a message naming the property and the offending value, instead of
     * either guessing wrong or throwing a generic "Unparseable port number" from deep inside
     * Spring Data.
     */
    private static String withDefaultSentinelPort(String node) {
        if (isBracketedHostWithPort(node)) {
            return node;
        }

        int colonCount = (int) node.chars().filter(c -> c == ':').count();
        if (colonCount == 0) {
            return node + ":" + DEFAULT_SENTINEL_PORT;
        }
        if (colonCount > 1) {
            throw malformedSentinelNode(node);
        }

        String portPart = node.substring(node.lastIndexOf(':') + 1);
        if (!isValidPort(portPart)) {
            throw malformedSentinelNode(node);
        }
        return node;
    }

    /** {@code RedisNode.fromString}'s bracketed-IPv6-with-port form, e.g. {@code [fe80::1]:26379}. */
    private static boolean isBracketedHostWithPort(String node) {
        if (!node.startsWith("[")) return false;
        int close = node.indexOf(']');
        if (close < 0 || close + 1 >= node.length() || node.charAt(close + 1) != ':') return false;
        return isValidPort(node.substring(close + 2));
    }

    private static boolean isValidPort(String portPart) {
        if (portPart.isEmpty()) return false;
        try {
            int port = Integer.parseInt(portPart);
            return port >= MIN_PORT && port <= MAX_PORT;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    private static IllegalArgumentException malformedSentinelNode(String node) {
        return new IllegalArgumentException(
                "recsys.redis.sentinel-nodes has a malformed entry: '" + node + "'. Each entry "
                        + "must be either a bare host (defaults to port " + DEFAULT_SENTINEL_PORT
                        + ") or host:port with a port between " + MIN_PORT + " and " + MAX_PORT + ".");
    }

    private static void applyCredentials(WithPassword config, RedisProperties props) {
        if (props.getUsername() != null && !props.getUsername().isBlank()) {
            config.setUsername(props.getUsername());
        }
        if (props.getPassword() != null && !props.getPassword().isBlank()) {
            config.setPassword(props.getPassword());
        }
    }

    @Bean
    public LettuceConnectionFactory retrievalRedisConnectionFactory(RedisProperties props) {
        return connectionFactory(props, System.getenv());
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate(LettuceConnectionFactory factory) {
        return new StringRedisTemplate(factory);
    }
}
