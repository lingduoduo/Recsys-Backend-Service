package com.recsys.config;

import com.recsys.api.rest.ModelApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@link ModelApplication}'s exclusion of Spring Boot's {@code RedisAutoConfiguration} and
 * {@code RedisRepositoriesAutoConfiguration} two ways: directly, by reflecting on the
 * {@code @SpringBootApplication} annotation itself, and behaviourally, by asserting the assembled
 * context has exactly one {@link RedisConnectionFactory} bean and it is
 * {@link RetrievalRedisConfig}'s {@code recsys.redis}-backed one. See that class's javadoc for why
 * the exclusion exists: without it, Boot would autoconfigure a second, unguarded connection
 * factory from {@code spring.data.redis.*}, invisible to {@code LettuceClientFactory}'s
 * {@code REDIS_ALLOW_NO_AUTH} guard.
 *
 * <p>The reflective check is load-bearing on its own, not redundant with the behavioural ones:
 * verified by hand that removing the exclusion while leaving both {@code RetrievalRedisConfig}
 * beans in place does NOT change the bean-count/host-identity checks below -- Boot's own
 * connection-factory bean is separately guarded by
 * {@code @ConditionalOnMissingBean(RedisConnectionFactory.class)}, a type-based check that still
 * sees {@link RetrievalRedisConfig}'s factory and backs off regardless of the top-level exclusion.
 * Also verified by hand: a second same-typed {@code RedisConnectionFactory} bean does not fail
 * only the bean-count assertion -- it makes the whole context fail to load, because some other,
 * unqualified consumer of the type elsewhere in the graph ({@code RetrievalRedisConfig}'s own
 * {@code stringRedisTemplate(LettuceConnectionFactory)}) throws
 * {@code NoUniqueBeanDefinitionException} first. All three tests in this class then fail with that
 * same context-load error, which is itself still a correct (if less specific) signal that
 * something is wrong. Full transcript in the Task 9 fix-round-1 report.
 */
@SpringBootTest(classes = ModelApplication.class, properties = {
        "recsys.redis.host=recsys-redis-canary-host",
        "spring.data.redis.host=autoconfigured-redis-canary-host"
})
class RedisAutoConfigurationExclusionTest {

    @Autowired
    private ApplicationContext context;

    // Qualified by bean name, not by type alone, so this field's own resolution stays precise if
    // a second RedisConnectionFactory bean is ever present -- though as verified by hand (see the
    // class javadoc), an unqualified consumer elsewhere in the real bean graph would still fail
    // context load first in that scenario; this qualifier only keeps this test's own intent
    // unambiguous, it does not by itself prevent that.
    @Autowired
    @Qualifier("retrievalRedisConnectionFactory")
    private LettuceConnectionFactory connectionFactory;

    @Test
    void modelApplicationExcludesBothRedisAutoConfigurations() {
        SpringBootApplication annotation = ModelApplication.class.getAnnotation(SpringBootApplication.class);

        assertThat(annotation.exclude())
                .as("ModelApplication's @SpringBootApplication(exclude = ...) must name both "
                        + "RedisAutoConfiguration and RedisRepositoriesAutoConfiguration -- see "
                        + "RetrievalRedisConfig's javadoc for why")
                .contains(RedisAutoConfiguration.class, RedisRepositoriesAutoConfiguration.class);
    }

    @Test
    void exactlyOneRedisConnectionFactoryBeanExistsAndItIsRetrievalRedisConfigs() {
        assertThat(context.getBeanNamesForType(RedisConnectionFactory.class))
                .as("a second RedisConnectionFactory bean in the context means a second Redis "
                        + "pool exists somewhere, reachable from spring.data.redis.* and outside "
                        + "REDIS_ALLOW_NO_AUTH's guard -- a plain @Autowired LettuceConnectionFactory "
                        + "field would either silently pick one of the two or fail with an unrelated "
                        + "NoUniqueBeanDefinitionException instead of naming the real problem")
                .containsExactly("retrievalRedisConnectionFactory");
    }

    @Test
    void theInjectedConnectionFactoryIsBuiltFromRecsysRedisNotSpringDataRedis() {
        RedisStandaloneConfiguration standalone = connectionFactory.getStandaloneConfiguration();

        assertThat(standalone).isNotNull();
        assertThat(standalone.getHostName())
                .as("the LettuceConnectionFactory in context must be RetrievalRedisConfig's "
                        + "recsys.redis-backed bean, not one Spring Boot autoconfigured from "
                        + "spring.data.redis.* -- if this reports the autoconfigured host, "
                        + "RedisAutoConfiguration is no longer excluded from ModelApplication and "
                        + "REDIS_ALLOW_NO_AUTH no longer covers every Redis connection on 8080")
                .isEqualTo("recsys-redis-canary-host");
    }
}
