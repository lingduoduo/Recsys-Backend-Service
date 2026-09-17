package com.recsys.retrieval.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.recsys.retrieval.service.audit.RedisProfileAuditStore;
import com.recsys.retrieval.service.clients.RedisUserProfileClient;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@code recsys.retrieval.user-profile.key-prefix} has no {@code RecommendationProperties} field
 * -- {@link RedisUserProfileClient} and {@link RedisProfileAuditStore} bind it purely through a
 * constructor {@code @Value}. {@link RetrievalValuePrefixTest} catches the "spelled under the
 * bare recsys prefix" class of mistake, but it can't tell a correctly-spelled key that resolves
 * from one that silently falls back to its hard-coded default -- nothing short of actually
 * binding the property and observing behaviour would notice that. This does: it registers the
 * real classes (not stand-ins) in a Spring context, overrides the property to a value that
 * differs from the default, and asserts the Redis key each class builds carries the override.
 */
@SpringJUnitConfig(UserProfileKeyPrefixBindingTest.Config.class)
@TestPropertySource(properties = "recsys.retrieval.user-profile.key-prefix=custom-user-profile-prefix")
class UserProfileKeyPrefixBindingTest {

    @Autowired
    private RedisUserProfileClient userProfileClient;

    @Autowired
    private RedisProfileAuditStore profileAuditStore;

    @Autowired
    private ValueOperations<String, String> values;

    @BeforeEach
    void resetMock() {
        // The Spring context (and its mock beans) is cached and shared across both @Test methods
        // in this class; without a reset, the second test's verify() would also see the first
        // test's invocation on the same mock.
        reset(values);
    }

    @Test
    void redisUserProfileClientUsesTheConfiguredPrefixNotItsDefault() {
        when(values.get(anyString())).thenReturn(null);

        userProfileClient.getProfile("u1");

        verify(values).get("custom-user-profile-prefix:active-run");
    }

    @Test
    void redisProfileAuditStoreUsesTheConfiguredPrefixNotItsDefault() {
        when(values.get(anyString())).thenReturn(null);

        profileAuditStore.activeRun();

        verify(values).get("custom-user-profile-prefix:active-run");
    }

    @Configuration
    @Import({RedisUserProfileClient.class, RedisProfileAuditStore.class})
    static class Config {

        // static + PropertySourcesPlaceholderConfigurer: must run as a BeanFactoryPostProcessor
        // before the imported classes' @Value constructor parameters are resolved, exactly like
        // Spring Boot's autoconfigured one does for the real application.
        @Bean
        static PropertySourcesPlaceholderConfigurer placeholderConfigurer() {
            return new PropertySourcesPlaceholderConfigurer();
        }

        @Bean
        @SuppressWarnings("unchecked")
        ValueOperations<String, String> values() {
            return mock(ValueOperations.class);
        }

        @Bean
        StringRedisTemplate redis(ValueOperations<String, String> values) {
            StringRedisTemplate template = mock(StringRedisTemplate.class);
            when(template.opsForValue()).thenReturn(values);
            return template;
        }

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }
}
