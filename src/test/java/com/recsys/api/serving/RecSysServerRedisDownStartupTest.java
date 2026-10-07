package com.recsys.api.serving;

import com.recsys.infrastructure.cache.LocalEmbeddingCache;
import com.recsys.infrastructure.dataloading.DataLoader;
import com.recsys.infrastructure.redis.RedisEmbeddingStore;
import com.recsys.infrastructure.redis.RedisExecutor;
import io.lettuce.core.RedisConnectionException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.mock;

/**
 * 6010 must start with Redis unreachable. The startup Redis work — seeding absent embeddings and warming the
 * heap caches with entries the classpath lacks — is repair, not a precondition: the classpath preload already
 * gives both caches everything the sample data has. Before this change the first {@code MGET} threw out of
 * {@code main} and the process exited with code 1.
 */
class RecSysServerRedisDownStartupTest {

    /** Every Redis command fails the way an unreachable server does. */
    private static RedisExecutor unreachableRedis() {
        return mock(RedisExecutor.class, invocation -> {
            throw new RedisConnectionException("Unable to connect to localhost:6399");
        });
    }

    @Test
    void startupRedisRepairIsBestEffortAndThePreloadStillServes() {
        RedisExecutor redis = unreachableRedis();
        RedisEmbeddingStore embStore = new RedisEmbeddingStore(redis, "i2vEmb");
        RedisEmbeddingStore userEmbStore = new RedisEmbeddingStore(redis, "u2vEmb");
        Map<Integer, float[]> movies = DataLoader.loadMovieEmbeddings();
        LocalEmbeddingCache embCache = new LocalEmbeddingCache(embStore);
        embCache.preload(movies);
        LocalEmbeddingCache userEmbCache = new LocalEmbeddingCache(userEmbStore);
        userEmbCache.preload(DataLoader.loadUserEmbeddings());

        assertThatCode(() -> RecSysServer.repairAndWarmFromRedis(embStore, userEmbStore, embCache, userEmbCache))
                .doesNotThrowAnyException();

        int someMovie = movies.keySet().iterator().next();
        assertThat(embCache.getEmbedding(someMovie)).as("classpath-preloaded embedding").isNotNull();
    }
}
