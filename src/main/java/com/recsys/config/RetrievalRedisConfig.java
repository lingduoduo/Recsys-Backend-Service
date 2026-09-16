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
 * bypassable on 8080 simply by merging this code. So {@code RedisAutoConfiguration} is excluded
 * on {@code ModelApplication} and the factory is built here, from the same properties and behind
 * the same guard as the raw-Lettuce executors in {@link RedisConfig}.
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
