package com.recsys.infrastructure.redis;

import com.recsys.infrastructure.vectordb.EmbeddingStore;
import com.recsys.infrastructure.vectordb.VectorMath;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.KeyValue;
import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.SetArgs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.Objects;
import java.util.function.IntConsumer;

public class RedisEmbeddingStore implements EmbeddingStore {
    private static final Logger log = LoggerFactory.getLogger(RedisEmbeddingStore.class);
    private final RedisExecutor exec;
    private final String keyPrefix;
    private final int mgetBatchSize;
    // Cache avalanche — random TTL jitter: staggers expiry across keys so a batch write does not
    // cause all entries to expire simultaneously and trigger a thundering herd on Redis.
    // jitterFraction=0.1 means 0..10% of baseTtl is added as a random offset.
    private final double jitterFraction;
    // Overall wall-clock budget for loadAll's SCAN loop; a slow/large Redis can't block startup.
    private final long loadAllTimeoutMs;
    private final LongSupplier clock;

    public RedisEmbeddingStore(RedisExecutor exec, String keyPrefix) {
        this(exec, keyPrefix, 0.1);
    }

    RedisEmbeddingStore(RedisExecutor exec, String keyPrefix, double jitterFraction) {
        this(exec, keyPrefix, jitterFraction, readIntEnv("REDIS_EMBEDDING_MGET_BATCH_SIZE", 500));
    }

    RedisEmbeddingStore(RedisExecutor exec, String keyPrefix, double jitterFraction, int mgetBatchSize) {
        this(exec, keyPrefix, jitterFraction, mgetBatchSize,
                readLongEnv("REDIS_LOADALL_TIMEOUT_MS", 30_000L), System::currentTimeMillis);
    }

    RedisEmbeddingStore(RedisExecutor exec, String keyPrefix, double jitterFraction, int mgetBatchSize,
                        long loadAllTimeoutMs, LongSupplier clock) {
        this.exec = exec;
        this.keyPrefix = keyPrefix;
        this.jitterFraction = Math.max(0.0, Math.min(0.5, jitterFraction));
        this.mgetBatchSize = Math.max(1, mgetBatchSize);
        this.loadAllTimeoutMs = Math.max(0L, loadAllTimeoutMs);
        this.clock = clock;
    }

    // Adds uniform positive jitter in [0, jitter] to baseTtl to spread expiry times.
    // Positive-only jitter avoids shortening the caller's intended minimum freshness window.
    long jitteredTtlMillis(long baseTtlSeconds) {
        if (baseTtlSeconds <= 0) return baseTtlSeconds;
        long baseMs = TimeUnit.SECONDS.toMillis(baseTtlSeconds);
        if (jitterFraction <= 0.0) return Math.max(1L, baseMs);
        long maxJitterMs = Math.max(1L, Math.round(baseMs * jitterFraction));
        long jitterMs = ThreadLocalRandom.current().nextLong(maxJitterMs + 1L);
        return Math.max(1L, baseMs + jitterMs);
    }

    @Override
    public float[] getEmbedding(int movieId) {
        String v = exec.executeRead(c -> c.get(keyPrefix + ":" + movieId));
        if (v == null || v.isBlank()) return null;
        return VectorMath.parseVector(v);
    }

    @Override
    public float[] getEmbeddingPrimary(int id) {
        String v = exec.executePrimaryRead(c -> c.get(keyPrefix + ":" + id));
        return v == null || v.isBlank() ? null : VectorMath.parseVector(v);
    }

    public void setEmbeddingNoTtl(int movieId, float[] vector) {
        exec.execute(c -> c.set(keyPrefix + ":" + movieId, toVectorString(vector)));
    }

    // ttlSeconds <= 0 means no expiry
    @Override
    public void setEmbedding(int movieId, float[] vector, long ttlSeconds) {
        if (ttlSeconds <= 0) {
            setEmbeddingNoTtl(movieId, vector);
            return;
        }
        SetArgs args = SetArgs.Builder.px(jitteredTtlMillis(ttlSeconds));
        exec.execute(c -> c.set(keyPrefix + ":" + movieId, toVectorString(vector), args));
    }

    // Bulk write — mirrors the Spark/Scala pattern of iterating model vectors after training.
    // Uses a pipeline to send all SETs in one round-trip instead of N.
    // Each key gets an independently jittered millisecond TTL to stagger expiry (cache avalanche / random TTL):
    // when ttlSeconds > 0, every pipeline.set() call draws a fresh random jitter so entries
    // written in the same batch do not all expire at the same moment.
    @Override
    public void setEmbeddings(Map<Integer, float[]> vectors, long ttlSeconds) {
        if (vectors == null || vectors.isEmpty()) return;
        exec.executePipelined(conn -> {
            var async = conn.async();
            List<RedisFuture<?>> futures = new ArrayList<>(vectors.size());
            for (Map.Entry<Integer, float[]> entry : vectors.entrySet()) {
                String key = keyPrefix + ":" + entry.getKey();
                String value = toVectorString(entry.getValue());
                if (ttlSeconds > 0) {
                    futures.add(async.set(key, value, SetArgs.Builder.px(jitteredTtlMillis(ttlSeconds))));
                } else {
                    futures.add(async.set(key, value));
                }
            }
            conn.flushCommands();
            LettuceFutures.awaitAll(Duration.ofSeconds(5), futures.toArray(new RedisFuture[0]));
        });
    }

    /**
     * Repairs a partially populated store: writes only the entries whose keys are
     * <em>absent</em> from Redis, in one pipelined batch, and returns the ids written.
     *
     * <p>Redis runs as an LRU cache ({@code allkeys-lru} at {@code maxmemory}), so entries
     * written with no TTL can still be evicted. An all-or-nothing "seed only when the store
     * is empty" guard therefore never repairs a <em>partial</em> eviction — Redis is
     * non-empty, so the evicted subset stays missing until the whole keyspace is cleared.
     *
     * <p>Presence is checked against the <b>primary</b>: a lagging replica would report a
     * live key as absent and trigger a pointless rewrite of it.
     */
    public Set<Integer> writeMissing(Map<Integer, float[]> vectors, long ttlSeconds) {
        if (vectors == null || vectors.isEmpty()) return Set.of();

        List<Integer> ids = new ArrayList<>(vectors.keySet());
        Map<Integer, float[]> missing = new LinkedHashMap<>();

        for (int start = 0; start < ids.size(); start += mgetBatchSize) {
            int end = Math.min(ids.size(), start + mgetBatchSize);
            final String[] keys = new String[end - start];
            for (int i = start; i < end; i++) {
                keys[i - start] = keyPrefix + ":" + ids.get(i);
            }
            List<KeyValue<String, String>> values = exec.executePrimaryRead(c -> c.mget(keys));
            for (int j = 0; j < values.size(); j++) {
                String value = values.get(j).getValueOrElse(null);
                if (value != null && !value.isBlank()) continue;
                int id = ids.get(start + j);
                missing.put(id, vectors.get(id));
            }
        }

        if (missing.isEmpty()) return Set.of();
        setEmbeddings(missing, ttlSeconds);
        return Collections.unmodifiableSet(new LinkedHashSet<>(missing.keySet()));
    }

    @Override
    public Map<Integer, float[]> getEmbeddings(Collection<Integer> movieIds) {
        return readBatched(movieIds, null);
    }

    /**
     * Like {@link #getEmbeddings} but a value that does not parse is skipped and its id handed to
     * {@code onCorrupt}, instead of aborting the whole read. For the periodic item-embedding refresh:
     * with the strict read, one corrupt key would fail every pass forever.
     */
    public Map<Integer, float[]> getEmbeddingsLenient(Collection<Integer> ids, IntConsumer onCorrupt) {
        return readBatched(ids, Objects.requireNonNull(onCorrupt, "onCorrupt"));
    }

    private Map<Integer, float[]> readBatched(Collection<Integer> movieIds, IntConsumer onCorrupt) {
        Map<Integer, float[]> embeddings = new HashMap<>();
        if (movieIds == null || movieIds.isEmpty()) return embeddings;

        List<Integer> ids = new ArrayList<>(new LinkedHashSet<>(movieIds));
        for (int start = 0; start < ids.size(); start += mgetBatchSize) {
            int end = Math.min(ids.size(), start + mgetBatchSize);
            String[] keys = new String[end - start];
            for (int i = start; i < end; i++) {
                keys[i - start] = keyPrefix + ":" + ids.get(i);
            }
            final int batchStart = start;
            List<KeyValue<String, String>> values = exec.executeRead(c -> c.mget(keys));
            for (int j = 0; j < values.size(); j++) {
                String value = values.get(j).getValueOrElse(null);
                if (value == null || value.isBlank()) continue;
                int id = ids.get(batchStart + j);
                try {
                    embeddings.put(id, VectorMath.parseVector(value));
                } catch (IllegalArgumentException e) {   // NumberFormatException included
                    if (onCorrupt == null) throw e;
                    onCorrupt.accept(id);
                }
            }
        }
        return embeddings;
    }

    // SCAN all keys matching the prefix and MGET each page immediately.
    // Processing per scan page (≤500 keys at a time) caps peak heap use: the old approach
    // accumulated every key name into a List<String> then issued one unbounded MGET,
    // holding all keys + all serialised vectors simultaneously — an OOM / Full-GC risk
    // for large embedding stores.
    public Map<Integer, float[]> loadAll() {
        Map<Integer, float[]> result = new HashMap<>();
        ScanArgs scanArgs = ScanArgs.Builder.matches(keyPrefix + ":*").limit(500);
        long start = clock.getAsLong();

        return exec.execute(c -> {
            KeyScanCursor<String> cursor = c.scan(scanArgs);
            while (true) {
                List<String> pageKeys = cursor.getKeys();
                if (!pageKeys.isEmpty()) {
                    List<KeyValue<String, String>> values = c.mget(pageKeys.toArray(new String[0]));
                    for (int i = 0; i < pageKeys.size(); i++) {
                        String val = values.get(i).getValueOrElse(null);
                        if (val == null || val.isBlank()) continue;
                        int sep = pageKeys.get(i).lastIndexOf(':');
                        if (sep < 0) continue;
                        try {
                            int id = Integer.parseInt(pageKeys.get(i).substring(sep + 1));
                            result.put(id, VectorMath.parseVector(val));
                        } catch (NumberFormatException ignore) {}
                    }
                }
                if (loadAllTimeoutMs > 0L && clock.getAsLong() - start > loadAllTimeoutMs) {
                    log.warn("loadAll exceeded {}ms budget after {} entries; returning partial result",
                            loadAllTimeoutMs, result.size());
                    break;
                }
                if (cursor.isFinished()) break;
                cursor = c.scan(cursor, scanArgs);
            }
            return result;
        });
    }

    public Map<Integer, float[]> loadValidEmbeddings(Set<Integer> knownMovieIds) {
        Map<Integer, float[]> all = loadAll();
        int scannedCount = all.size();
        all.keySet().retainAll(knownMovieIds);
        log.info("Loaded {} valid embeddings (scanned {} keys in Redis, {} known movies)",
                all.size(), scannedCount, knownMovieIds.size());
        return all;
    }

    @Override
    public Set<Integer> scanIds(int maxKeys) {
        Set<Integer> ids = new HashSet<>();

        int countHint = Math.min(maxKeys, 500);
        ScanArgs args = ScanArgs.Builder.matches(keyPrefix + ":*").limit(countHint);

        return exec.execute(c -> {
            KeyScanCursor<String> cursor = c.scan(args);
            while (true) {
                for (String key : cursor.getKeys()) {
                    int idx = key.lastIndexOf(':');
                    if (idx >= 0 && idx + 1 < key.length()) {
                        try {
                            ids.add(Integer.parseInt(key.substring(idx + 1)));
                        } catch (NumberFormatException ignore) {
                            // skip malformed keys
                        }
                    }
                    if (ids.size() >= maxKeys) return ids;
                }

                if (cursor.isFinished()) break;
                cursor = c.scan(cursor, args);
            }
            return ids;
        });
    }

    private static String toVectorString(float[] vec) {
        StringBuilder sb = new StringBuilder(vec.length * 12);
        for (int i = 0; i < vec.length; i++) {
            if (i > 0) sb.append(' ');
            sb.append(vec[i]);
        }
        return sb.toString();
    }

    private static int readIntEnv(String envName, int defaultValue) {
        String raw = System.getenv(envName);
        if (raw == null || raw.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private static long readLongEnv(String envName, long defaultValue) {
        String raw = System.getenv(envName);
        if (raw == null || raw.isBlank()) return defaultValue;
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
