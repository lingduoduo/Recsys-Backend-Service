package com.recsys.infrastructure.cache;

import com.recsys.infrastructure.resilience.BloomFilterGuard;
import com.recsys.infrastructure.resilience.SingleFlight;
import com.recsys.infrastructure.redis.RedisEmbeddingStore;
import com.recsys.infrastructure.vectordb.EmbeddingStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

/**
 * JVM-heap cache (Tier 3) in front of a backing EmbeddingStore (Tier 2, typically Redis).
 *
 * Read-through: a cache miss fetches from the backing store and populates the cache.
 * Write-through: setEmbedding writes to both the backing store and the cache atomically.
 *
 * Item embeddings are written infrequently (only when a new model trains) and read on
 * every similarity-search request. The cache is bounded with access-order LRU eviction
 * to prevent arbitrary ids from growing heap until Full GC/OOM.
 *
 * Call warmUp() at startup to pre-populate the cache from Redis so the first requests
 * are served entirely from heap rather than paying a Redis round-trip.
 *
 * Cache penetration protection:
 *  - Bloom filter: after warmUp/preload populates the filter with all valid IDs, requests
 *    for IDs definitively absent (mightContain = false) are short-circuited without a
 *    Redis round-trip.  The filter is inactive until a bulk load runs.
 *  - Null sentinel: absent IDs confirmed by the backing store are cached for
 *    NULL_SENTINEL_TTL_MS so that repeated queries for the same absent ID do not each
 *    trigger a backing-store round-trip.
 *
 * Multi-level cache (cache avalanche mitigation):
 *  Tier-1 (file system) → Tier-2 (Redis via backing store) → Tier-3 (this JVM heap).
 *  A Redis outage degrades gracefully: this cache continues serving hot embeddings
 *  from heap; only cold misses fall through to the unavailable tier.
 */
public class LocalEmbeddingCache implements EmbeddingStore {

    private static final Logger log = LoggerFactory.getLogger(LocalEmbeddingCache.class);
    private static final int DEFAULT_MAX_ENTRIES = 100_000;
    // Short null-sentinel TTL: absorbs burst penetration without staling too long.
    private static final long NULL_SENTINEL_TTL_MS = 30_000L;
    private static final long MISS_WAIT_TIMEOUT_MS = 2_000L;

    private final EmbeddingStore backingStore;
    private final int maxEntries;
    private final Cache<Integer, float[]> cache;

    // Cache penetration guard — Bloom filter: activated after warmUp/preload loads all valid IDs.
    private final BloomFilterGuard bloom;
    private volatile boolean bloomPopulated = false;

    // Cache penetration guard — Null sentinel: bounded Caffeine cache of recently-absent IDs.
    // maximumSize caps heap under penetration attack; expireAfterWrite gives the TTL behaviour
    // (membership == "recently confirmed absent") without unbounded growth or manual expiry.
    private static final int NULL_SENTINEL_MAX = 100_000;
    private final Cache<Integer, Boolean> nullSentinels = Caffeine.newBuilder()
            .maximumSize(NULL_SENTINEL_MAX)
            .expireAfterWrite(java.time.Duration.ofMillis(NULL_SENTINEL_TTL_MS))
            .executor(Runnable::run)
            .build();
    private final SingleFlight<Integer, float[]> missSingleFlight = new SingleFlight<>(MISS_WAIT_TIMEOUT_MS);

    public LocalEmbeddingCache(EmbeddingStore backingStore) {
        this(backingStore, readIntEnv("LOCAL_EMBEDDING_CACHE_MAX_ENTRIES", DEFAULT_MAX_ENTRIES));
    }

    @Override
    public float[] getEmbeddingPrimary(int id) { return backingStore.getEmbeddingPrimary(id); }

    LocalEmbeddingCache(EmbeddingStore backingStore, int maxEntries) {
        this(backingStore, maxEntries, new BloomFilterGuard(Math.max(1, maxEntries) * 2, 0.01));
    }

    LocalEmbeddingCache(EmbeddingStore backingStore, int maxEntries, BloomFilterGuard bloom) {
        this.backingStore = backingStore;
        this.maxEntries = Math.max(1, maxEntries);
        this.bloom = bloom;
        this.cache = Caffeine.newBuilder()
                .maximumSize(this.maxEntries)
                .recordStats()
                .executor(Runnable::run) // synchronous maintenance -> deterministic size/stats
                .build();
    }

    /**
     * Bulk-loads all embeddings from the backing store into the heap cache.
     * Should be called once at server startup, after seeding Redis from the file system.
     * Also activates Bloom filter protection against cache penetration.
     */
    public void warmUp() {
        if (!(backingStore instanceof RedisEmbeddingStore redisStore)) return;
        Map<Integer, float[]> all = redisStore.loadAll();
        if (!all.isEmpty()) {
            populateAll(all);
            bloomPopulated = true;
        }
        log.info("LocalEmbeddingCache warmed up with {} embeddings from Redis", all.size());
    }

    /**
     * Pre-loads embeddings directly from a file-system-sourced map (Tier 1 → Tier 3 shortcut).
     * Avoids a Redis round-trip when the classpath data is already available at startup.
     * Also activates Bloom filter protection against cache penetration.
     */
    public void preload(Map<Integer, float[]> embeddings) {
        if (embeddings != null && !embeddings.isEmpty()) {
            populateAll(embeddings);
            bloomPopulated = true;
        }
        log.info("LocalEmbeddingCache preloaded {} embeddings from file system",
                embeddings == null ? 0 : embeddings.size());
    }

    @Override
    public float[] getEmbedding(int id) {
        // 1. Bloom filter guard (active only after warm-up to ensure completeness).
        if (bloomPopulated && !bloom.mightContain(id)) return null;

        // 2. Null sentinel: skip backing store for recently confirmed absent IDs.
        if (nullSentinels.getIfPresent(id) != null) return null;

        float[] cached = cache.getIfPresent(id);
        if (cached != null) return cached;

        return missSingleFlight.execute(id, () -> loadMissingEmbedding(id));
    }

    @Override
    public Map<Integer, float[]> getEmbeddings(Collection<Integer> ids) {
        if (ids == null || ids.isEmpty()) return Collections.emptyMap();

        Map<Integer, float[]> present = cache.getAllPresent(ids);
        Map<Integer, float[]> result = new HashMap<>(present);
        Set<Integer> misses = new LinkedHashSet<>();
        for (int id : ids) {
            if (!present.containsKey(id)) misses.add(id);
        }

        if (misses.isEmpty()) return result;

        // Apply penetration guards to the miss set (outside sync block to reduce contention).
        Set<Integer> toFetch = new LinkedHashSet<>();
        for (int id : misses) {
            if (bloomPopulated && !bloom.mightContain(id)) continue;
            if (nullSentinels.getIfPresent(id) != null) continue;
            toFetch.add(id);
        }

        if (!toFetch.isEmpty()) {
            Map<Integer, float[]> fetched = backingStore.getEmbeddings(toFetch);
            for (int id : toFetch) {
                if (fetched.containsKey(id)) {
                    bloom.add(id);
                } else {
                    nullSentinels.put(id, Boolean.TRUE);
                }
            }
            putAll(fetched);
            result.putAll(fetched);
        }

        return result;
    }

    /**
     * Cache-only update for a value just read from the backing store (the periodic item-embedding
     * refresh). Unlike {@link #setEmbedding} it never writes back — that would echo to Redis the value
     * just read from it. Adds the id to the Bloom filter so an item first written after startup is no
     * longer rejected, and clears any null sentinel.
     */
    public void refresh(int id, float[] vector) {
        if (vector == null) return;
        bloom.add(id);
        nullSentinels.invalidate(id);
        put(id, vector);
    }

    @Override
    public void setEmbedding(int id, float[] vector, long ttlSeconds) {
        backingStore.setEmbedding(id, vector, ttlSeconds);
        bloom.add(id);
        nullSentinels.invalidate(id);
        put(id, vector);
    }

    @Override
    public void setEmbeddings(Map<Integer, float[]> vectors, long ttlSeconds) {
        backingStore.setEmbeddings(vectors, ttlSeconds);
        putAll(vectors);
    }

    @Override
    public Set<Integer> scanIds(int maxKeys) {
        return backingStore.scanIds(maxKeys);
    }

    public int cacheSize() {
        return (int) cache.estimatedSize();
    }

    public int maxEntries() {
        return maxEntries;
    }

    public CacheStats stats() {
        return cache.stats();
    }

    boolean isBloomPopulated() { return bloomPopulated; }

    int inflightMisses() { return missSingleFlight.inflightCount(); }

    int nullSentinelCount() { return (int) nullSentinels.estimatedSize(); }

    private float[] loadMissingEmbedding(int id) {
        if (nullSentinels.getIfPresent(id) != null) return null;

        // asMap().get bypasses stats recording so this single-flight double-check
        // does not inflate the miss count (the primary getEmbedding read already recorded it).
        float[] cached = cache.asMap().get(id);
        if (cached != null) return cached;

        float[] fromStore = backingStore.getEmbedding(id);
        if (fromStore != null) {
            bloom.add(id);
            put(id, fromStore);
        } else {
            nullSentinels.put(id, Boolean.TRUE);
        }
        return fromStore;
    }

    // Bulk put without activating bloomPopulated (used by setEmbeddings — partial write).
    private void putAll(Map<Integer, float[]> embeddings) {
        if (embeddings == null || embeddings.isEmpty()) return;
        embeddings.forEach((id, vec) -> {
            if (id != null && vec != null) {
                cache.put(id, vec);
                bloom.add(id);
                nullSentinels.invalidate(id);
            }
        });
    }

    // Bulk put for a complete snapshot (warmUp/preload); caller sets bloomPopulated.
    private void populateAll(Map<Integer, float[]> embeddings) {
        putAll(embeddings);
    }

    private void put(Integer id, float[] vector) {
        if (id == null || vector == null) return;
        cache.put(id, vector);
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
}
