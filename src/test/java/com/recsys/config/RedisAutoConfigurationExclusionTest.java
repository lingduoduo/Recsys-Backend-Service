package com.recsys.config;

import com.recsys.api.rest.ModelApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@link ModelApplication}'s explicit exclusion of Spring Boot's
 * {@code RedisAutoConfiguration}. See {@link RetrievalRedisConfig}'s class javadoc for why the
 * exclusion exists: without it, Boot would autoconfigure a second, unguarded
 * {@link LettuceConnectionFactory} from {@code spring.data.redis.*}, invisible to
 * {@code LettuceClientFactory}'s {@code REDIS_ALLOW_NO_AUTH} guard.
 *
 * <p>{@code recsys.redis.host} and {@code spring.data.redis.host} are set here to two different,
 * distinctive values. Only {@code RetrievalRedisConfig}'s factory reads {@code recsys.redis.*}, so
 * asserting the injected factory's standalone host equals the {@code recsys.redis} value fails
 * loudly the moment autoconfiguration wins instead. Verified by hand while writing this test:
 * removing the exclusion alone did NOT fail it (Boot's own connection-factory bean is separately
 * guarded by {@code @ConditionalOnMissingBean(RedisConnectionFactory.class)}, which still saw
 * {@link RetrievalRedisConfig}'s factory and backed off); removing the exclusion together with
 * {@link RetrievalRedisConfig}'s {@code retrievalRedisConnectionFactory} bean did fail it, with
 * the assertion reporting the autoconfigured canary host. Both files were restored after. See the
 * Task 9 report for the full transcript.
 */
@SpringBootTest(classes = ModelApplication.class, properties = {
        "recsys.redis.host=recsys-redis-canary-host",
        "spring.data.redis.host=autoconfigured-redis-canary-host"
})
class RedisAutoConfigurationExclusionTest {

    @Autowired
    private LettuceConnectionFactory connectionFactory;

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
