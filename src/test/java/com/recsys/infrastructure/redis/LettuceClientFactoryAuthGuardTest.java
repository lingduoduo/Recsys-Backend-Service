package com.recsys.infrastructure.redis;

import com.recsys.config.RedisProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * The properties-taking overload exists so RetrievalRedisConfig, which lives in com.recsys.config,
 * can run the same guard the raw-Lettuce path runs. An env map is passed explicitly rather than
 * read from System.getenv() because Surefire sets REDIS_ALLOW_NO_AUTH=true for the whole suite,
 * which would make the refusal case unobservable.
 */
class LettuceClientFactoryAuthGuardTest {

    private static RedisProperties propsWithPassword(String password) {
        RedisProperties props = new RedisProperties();
        props.setPassword(password);
        return props;
    }

    @Test
    void refusesABlankPasswordWithoutAnExplicitOptIn() {
        assertThatThrownBy(() ->
                LettuceClientFactory.requireAuthentication(propsWithPassword(""), Map.of()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("REDIS_PASSWORD");
    }

    @Test
    void allowsABlankPasswordWhenTheOptInIsSet() {
        assertDoesNotThrow(() -> LettuceClientFactory.requireAuthentication(
            propsWithPassword(""), Map.of("REDIS_ALLOW_NO_AUTH", "true")));
    }

    @Test
    void allowsAConfiguredPassword() {
        assertDoesNotThrow(() -> LettuceClientFactory.requireAuthentication(
            propsWithPassword("s3cret"), Map.of()));
        assertThat(propsWithPassword("s3cret").getPassword()).isEqualTo("s3cret");
    }
}
