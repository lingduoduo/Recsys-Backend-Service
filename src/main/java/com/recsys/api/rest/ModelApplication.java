package com.recsys.api.rest;

import com.recsys.config.FeatureFlagConfig;
import com.recsys.config.ABTestConfig;
import com.recsys.config.HealthProperties;
import com.recsys.config.RecommendationCacheProperties;
import com.recsys.config.LoginProperties;
import com.recsys.config.ModelServingProperties;
import com.recsys.config.RedisProperties;
import com.recsys.config.SubmitTokenProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Import;

@SpringBootApplication(
        scanBasePackages = {"com.recsys.api", "com.recsys.config", "com.recsys.exception",
                "com.recsys.metrics", "com.recsys.jvm", "com.recsys.tracing",
                "com.recsys.ratelimit", "com.recsys.loadshed", "com.recsys.resilience",
                "com.recsys.health", "com.recsys.application", "com.recsys.retrieval"},
        // RetrievalRedisConfig builds StringRedisTemplate from recsys.redis so the
        // REDIS_ALLOW_NO_AUTH guard covers it. Autoconfiguration would build a second,
        // unguarded pool from spring.data.redis.*. RedisAutoConfigurationExclusionTest asserts
        // directly (by reflecting on this annotation) that both classes below stay excluded, and
        // separately asserts the assembled context has exactly one RedisConnectionFactory bean.
        // RedisRepositoriesAutoConfiguration is excluded alongside it: nothing in this codebase
        // declares a @RedisHash entity or a Redis repository, but the registrar activates
        // unconditionally once spring-data-redis is on the classpath and then fails fast looking
        // for a bean literally named "redisTemplate" — the one RedisAutoConfiguration would have
        // supplied had it not just been excluded for the reason above.
        exclude = {RedisAutoConfiguration.class, RedisRepositoriesAutoConfiguration.class})
@Import(FeatureFlagConfig.class)
@EnableConfigurationProperties({
        HealthProperties.class,
        ABTestConfig.class,
        RecommendationCacheProperties.class,
        SubmitTokenProperties.class,
        LoginProperties.class,
        RedisProperties.class,
        ModelServingProperties.class,
        com.recsys.retrieval.config.RecommendationProperties.class
})
public class ModelApplication {
    public static void main(String[] args) {
        SpringApplication.run(ModelApplication.class, args);
    }
}
