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

    /**
     * Mirrors {@code LettuceClientFactory.sentinelUri}'s handling of a colon-less sentinel node:
     * that path silently defaults such an entry to port 26379, but
     * {@code RedisSentinelConfiguration} (via {@code RedisNode.fromString}) throws on the same
     * input. Without this, one {@code recsys.redis.sentinel-nodes} value would behave two
     * different ways inside the same JVM — the raw-Lettuce path guessing, this path failing
     * Spring context startup.
     */
    private static String withDefaultSentinelPort(String node) {
        return node.lastIndexOf(':') > 0 ? node : node + ":26379";
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
