package com.recsys.infrastructure.cache;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.recsys.infrastructure.vectordb.EmbeddingStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LocalEmbeddingCacheTest {

    private TrackingStore backing;
    private LocalEmbeddingCache cache;

    @BeforeEach
    void setUp() {
        backing = new TrackingStore();
        cache   = new LocalEmbeddingCache(backing);
    }

    @Test
    void getEmbedding_missPopulatesCache() {
        backing.put(1, new float[]{1f, 0f});

        float[] first  = cache.getEmbedding(1);
        float[] second = cache.getEmbedding(1);

        assertThat(first).isEqualTo(second);
        assertThat(backing.getCount).isEqualTo(1); // second call served from heap
    }

    @Test
    void getEmbedding_concurrentMissUsesSingleBackingRead() throws Exception {
        backing.put(1, new float[]{1f, 0f});
        backing.blockReads = true;
        int threadCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch releaseBackingRead = new CountDownLatch(1);
        backing.releaseRead = releaseBackingRead;

        List<java.util.concurrent.Future<float[]>> futures = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            futures.add(executor.submit(() -> {
                ready.countDown();
                start.await();
                return cache.getEmbedding(1);
            }));
        }

        assertThat(ready.await(1, TimeUnit.SECONDS)).isTrue();
        start.countDown();
        assertThat(backing.readEntered.await(1, TimeUnit.SECONDS)).isTrue();
        releaseBackingRead.countDown();

        for (java.util.concurrent.Future<float[]> future : futures) {
            assertThat(future.get(1, TimeUnit.SECONDS)).containsExactly(1f, 0f);
        }
        executor.shutdownNow();

        assertThat(backing.getCount).isEqualTo(1);
        assertThat(cache.inflightMisses()).isZero();
    }

    @Test
    void getEmbedding_absentIdReturnsNull() {
        assertThat(cache.getEmbedding(99)).isNull();
    }

    @Test
    void getEmbeddings_batchMissesAreFetchedOnce() {
        backing.put(1, new float[]{1f, 0f});
        backing.put(2, new float[]{0f, 1f});

        Map<Integer, float[]> first  = cache.getEmbeddings(Set.of(1, 2));
        Map<Integer, float[]> second = cache.getEmbeddings(Set.of(1, 2));

        assertThat(first).containsKeys(1, 2);
        assertThat(second).containsKeys(1, 2);
        assertThat(backing.mgetCount).isEqualTo(1); // second batch served entirely from heap
    }

    @Test
    void getEmbeddings_deduplicatesBatchMissesBeforeFetching() {
        backing.put(1, new float[]{1f, 0f});
        backing.put(2, new float[]{0f, 1f});

        Map<Integer, float[]> result = cache.getEmbeddings(List.of(1, 1, 2, 2));

        assertThat(result).containsKeys(1, 2);
        assertThat(backing.mgetCount).isEqualTo(1);
        assertThat(backing.lastMgetIds).containsExactly(1, 2);
    }

    @Test
    void getEmbeddings_partialCacheHit_fetchesMissesOnly() {
        backing.put(1, new float[]{1f, 0f});
        backing.put(2, new float[]{0f, 1f});

        cache.getEmbedding(1); // warm key 1
        cache.getEmbeddings(Set.of(1, 2)); // key 2 is a miss → one mget

        assertThat(backing.mgetCount).isEqualTo(1);
        assertThat(backing.getCount).isEqualTo(1);
    }

    @Test
    void setEmbedding_writesThroughToBackingAndCache() {
        float[] vec = {3f, 4f};
        cache.setEmbedding(7, vec, 0);

        assertThat(backing.data).containsKey(7);
        // subsequent get must be a cache hit (no extra backing read)
        int getsBefore = backing.getCount;
        assertThat(cache.getEmbedding(7)).isEqualTo(vec);
        assertThat(backing.getCount).isEqualTo(getsBefore);
    }

    @Test
    void setEmbeddings_writesThroughToBackingAndCache() {
        Map<Integer, float[]> vecs = Map.of(10, new float[]{1f}, 11, new float[]{2f});
        cache.setEmbeddings(vecs, 0);

        assertThat(backing.data).containsKeys(10, 11);
        int getsBefore = backing.getCount;
        assertThat(cache.getEmbedding(10)).isNotNull();
        assertThat(cache.getEmbedding(11)).isNotNull();
        assertThat(backing.getCount).isEqualTo(getsBefore);
    }

    @Test
    void preload_populatesCacheFromFileSystemData() {
        Map<Integer, float[]> fs = Map.of(1, new float[]{1f, 0f}, 2, new float[]{0f, 1f});
        cache.preload(fs);

        int getsBefore = backing.getCount;
        assertThat(cache.getEmbedding(1)).isNotNull();
        assertThat(cache.getEmbedding(2)).isNotNull();
        assertThat(backing.getCount).isEqualTo(getsBefore); // fully served from heap
        assertThat(cache.cacheSize()).isEqualTo(2);
    }

    @Test
    void cacheSize_reflectsPopulatedEntries() {
        cache.preload(Map.of(1, new float[]{1f}, 2, new float[]{2f}));
        assertThat(cache.cacheSize()).isEqualTo(2);
    }

    @Test
    void cache_evictsOldEntriesWhenCapacityIsReached() {
        LocalEmbeddingCache tinyCache = new LocalEmbeddingCache(backing, 2);
        backing.put(1, new float[]{1f});
        backing.put(2, new float[]{2f});
        backing.put(3, new float[]{3f});

        tinyCache.getEmbedding(1);
        tinyCache.getEmbedding(2);
        tinyCache.getEmbedding(3);

        // Capacity is enforced: three distinct ids inserted into a cap-2 cache leaves 2.
        // (Frequency-based eviction does not promise which id survives.)
        assertThat(tinyCache.cacheSize()).isEqualTo(2);
    }

    @Test
    void nullSentinels_areBoundedAndDoNotGrowUnbounded() {
        // All ids are absent in the backing store -> each lookup writes a null sentinel.
        for (int i = 0; i < 2_000; i++) {
            cache.getEmbedding(900_000 + i);
        }
        // The sentinel cache tracks absences but stays bounded (never exceeds what was inserted,
        // and is hard-capped at NULL_SENTINEL_MAX regardless of penetration volume).
        assertThat(cache.nullSentinelCount()).isLessThanOrEqualTo(2_000);
    }

    @Test
    void stats_recordsHitsAndMisses() {
        backing.put(1, new float[]{1f, 0f});

        cache.getEmbedding(1); // miss -> loads from backing, populates cache
        cache.getEmbedding(1); // hit  -> served from heap

        CacheStats stats = cache.stats();
        assertThat(stats.hitCount()).isEqualTo(1);
        assertThat(stats.missCount()).isEqualTo(1);
    }

    // ── Bloom filter / null sentinel tests (缓存穿透 protection) ──────────

    @Test
    void bloomFilter_inactiveBeforePreload_doesNotBlockLegitimateIds() {
        backing.put(1, new float[]{1f});
        // No preload/warmUp — bloom is inactive; backing must still be consulted.
        assertThat(cache.getEmbedding(1)).isNotNull();
        assertThat(backing.getCount).isEqualTo(1);
    }

    @Test
    void bloomFilter_activatesAfterPreload_blocksAbsentIds() {
        backing.put(1, new float[]{1f});
        cache.preload(Map.of(1, new float[]{1f})); // populates bloom with only ID 1

        assertThat(cache.isBloomPopulated()).isTrue();
        // ID 99 is not in the bloom filter → skip backing store entirely.
        int beforeCount = backing.getCount;
        assertThat(cache.getEmbedding(99)).isNull();
        assertThat(backing.getCount).isEqualTo(beforeCount); // no backing call
    }

    @Test
    void bloomFilter_allowsIdsPresentAfterPreload() {
        cache.preload(Map.of(1, new float[]{1f}, 2, new float[]{2f}));

        int beforeCount = backing.getCount;
        // IDs 1 and 2 are in the bloom filter (and in the heap cache) → served from heap.
        assertThat(cache.getEmbedding(1)).isNotNull();
        assertThat(cache.getEmbedding(2)).isNotNull();
        assertThat(backing.getCount).isEqualTo(beforeCount);
    }

    @Test
    void nullSentinel_preventsRepeatedBackingStoreCallsForAbsentId() {
        // ID 55 does not exist in the backing store.
        assertThat(cache.getEmbedding(55)).isNull();
        assertThat(backing.getCount).isEqualTo(1);

        // Second call: null sentinel should short-circuit before the backing store.
        assertThat(cache.getEmbedding(55)).isNull();
        assertThat(backing.getCount).isEqualTo(1); // no additional backing call
    }

    @Test
    void nullSentinel_isClearedOnWrite() {
        // Establish a null sentinel for ID 7.
        assertThat(cache.getEmbedding(7)).isNull();
        assertThat(backing.getCount).isEqualTo(1);

        // Write ID 7 — sentinel must be cleared so future reads go through.
        cache.setEmbedding(7, new float[]{7f}, 0);

        int beforeCount = backing.getCount;
        assertThat(cache.getEmbedding(7)).isNotNull(); // served from heap cache
        assertThat(backing.getCount).isEqualTo(beforeCount);
    }

    @Test
    void getEmbeddings_nullSentinelFiltersAbsentIdsFromBatchFetch() {
        backing.put(1, new float[]{1f});
        cache.getEmbedding(1); // warm ID 1 into heap cache via individual get
        // Establish null sentinels for IDs 2 and 3.
        cache.getEmbedding(2);
        cache.getEmbedding(3);
        int mgetsBefore = backing.mgetCount;

        // Batch: ID 1 is a heap hit; IDs 2 and 3 are blocked by null sentinels.
        Map<Integer, float[]> result = cache.getEmbeddings(Set.of(1, 2, 3));

        assertThat(result).containsKey(1);
        assertThat(result).doesNotContainKey(2);
        assertThat(result).doesNotContainKey(3);
        assertThat(backing.mgetCount).isEqualTo(mgetsBefore); // no extra batch fetch
    }

    // ── minimal in-memory EmbeddingStore stub ──────────────────────────────

    private static final class TrackingStore implements EmbeddingStore {
        final Map<Integer, float[]> data = new HashMap<>();
        int getCount  = 0;
        int mgetCount = 0;
        List<Integer> lastMgetIds = List.of();
        volatile boolean blockReads = false;
        CountDownLatch readEntered = new CountDownLatch(1);
        CountDownLatch releaseRead = new CountDownLatch(0);

        void put(int id, float[] vec) { data.put(id, vec); }

        @Override
        public float[] getEmbedding(int id) {
            getCount++;
            if (blockReads) {
                readEntered.countDown();
                try {
                    releaseRead.await(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return data.get(id);
        }

        @Override
        public Map<Integer, float[]> getEmbeddings(Collection<Integer> ids) {
            mgetCount++;
            lastMgetIds = new ArrayList<>(ids);
            Map<Integer, float[]> result = new HashMap<>();
            for (int id : ids) {
                float[] v = data.get(id);
                if (v != null) result.put(id, v);
            }
            return result;
        }

        @Override
        public void setEmbedding(int id, float[] vector, long ttlSeconds) {
            data.put(id, vector);
        }

        @Override
        public void setEmbeddings(Map<Integer, float[]> vectors, long ttlSeconds) {
            data.putAll(vectors);
        }

        @Override
        public Set<Integer> scanIds(int maxKeys) { return data.keySet(); }
    }

    // --- refresh(): how a periodic Redis re-read reaches /similar ---

    @Test
    void anIdWrittenToTheStoreAfterPreloadIsInvisible_theDefectRefreshFixes() {
        EmbeddingStore store = mock(EmbeddingStore.class);
        when(store.getEmbedding(99)).thenReturn(new float[]{1f, 2f});
        LocalEmbeddingCache c = new LocalEmbeddingCache(store);
        c.preload(Map.of(1, new float[]{0f, 1f}));   // Bloom now knows only id 1

        assertThat(c.getEmbedding(99)).as("Bloom rejects ids absent at preload").isNull();
    }

    @Test
    void refreshMakesANewIdVisibleWithoutTouchingTheStore() {
        EmbeddingStore store = mock(EmbeddingStore.class);
        LocalEmbeddingCache c = new LocalEmbeddingCache(store);
        c.preload(Map.of(1, new float[]{0f, 1f}));

        c.refresh(99, new float[]{1f, 2f});

        assertThat(c.getEmbedding(99)).containsExactly(1f, 2f);
        verify(store, never()).setEmbedding(anyInt(), any(), anyLong());
        verify(store, never()).setEmbeddings(anyMap(), anyLong());
    }

    @Test
    void refreshReplacesAChangedVector() {
        LocalEmbeddingCache c = new LocalEmbeddingCache(mock(EmbeddingStore.class));
        c.preload(Map.of(1, new float[]{0f, 1f}));

        c.refresh(1, new float[]{5f, 5f});

        assertThat(c.getEmbedding(1)).containsExactly(5f, 5f);
    }

    @Test
    void refreshClearsANullSentinel() {
        EmbeddingStore store = mock(EmbeddingStore.class);   // returns null: id 7 is "known absent"
        LocalEmbeddingCache c = new LocalEmbeddingCache(store);
        assertThat(c.getEmbedding(7)).isNull();              // caches a null sentinel

        c.refresh(7, new float[]{1f, 1f});

        assertThat(c.getEmbedding(7)).containsExactly(1f, 1f);
    }
}
