package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.SearchResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Readers never block and never see a torn block, a duplicate id, an id that was never inserted,
 * or a score that matches neither the previous nor the current vector of that id.
 */
class SpannVectorIndexConcurrencyTest {

    @TempDir Path dir;

    @Test
    void readersSeeOnlyLiveConsistentEntriesWhileTheWriterSplitsMergesAndCompacts() throws Exception {
        SpannConfig cfg = SpannConfig.defaults().withDir(dir).withPostingMin(2).withPostingMax(8)
                .withNprobe(1_000_000).withCompactRatio(0.4).withRegionBytes(1 << 20);
        List<float[]> base = SpannTestVectors.mixture(200, 8, 4, 21L);
        Map<Integer, float[]> data = SpannTestVectors.asMap(base);
        // Every vector an id has ever had, so a reader's score can be checked against all of them.
        ConcurrentHashMap<Integer, List<float[]>> history = new ConcurrentHashMap<>();
        data.forEach((id, v) -> history.put(id, new java.util.concurrent.CopyOnWriteArrayList<>(List.of(v))));

        try (SpannVectorIndex idx = new SpannVectorIndex(data, cfg)) {
            List<float[]> queries = SpannTestVectors.mixture(16, 8, 4, 22L);
            AtomicBoolean stop = new AtomicBoolean(false);
            AtomicLong searches = new AtomicLong();
            List<String> violations = new java.util.concurrent.CopyOnWriteArrayList<>();
            ExecutorService pool = Executors.newFixedThreadPool(5);
            List<Future<?>> readers = new ArrayList<>();
            for (int r = 0; r < 4; r++) {
                readers.add(pool.submit(() -> {
                    int qi = 0;
                    while (!stop.get()) {
                        float[] q = queries.get(qi++ % queries.size());
                        List<SearchResult> hits = idx.search(q, 20, Set.of());
                        searches.incrementAndGet();
                        Set<Integer> seen = new HashSet<>();
                        double prev = Double.POSITIVE_INFINITY;
                        for (SearchResult h : hits) {
                            if (!seen.add(h.id())) violations.add("duplicate id " + h.id());
                            if (h.score() > prev) violations.add("unsorted at id " + h.id());
                            prev = h.score();
                            List<float[]> versions = history.get(h.id());
                            if (versions == null) { violations.add("unknown id " + h.id()); continue; }
                            boolean matches = false;
                            for (float[] v : versions) {
                                if (Math.abs(com.recsys.infrastructure.vectordb.VectorMath.innerProduct(q, v) - h.score()) < 1e-4) { matches = true; break; }
                            }
                            if (!matches) violations.add("score of id " + h.id() + " matches no version");
                        }
                    }
                }));
            }
            Future<?> writer = pool.submit(() -> {
                java.util.Random rng = new java.util.Random(23);
                int nextId = 1000;
                for (int i = 0; i < 600; i++) {
                    float[] v = base.get(rng.nextInt(base.size())).clone();
                    v[0] += rng.nextFloat();
                    int id;
                    if (rng.nextInt(3) == 0) {                       // overwrite an existing id
                        id = rng.nextInt(200);
                        history.get(id).add(v);
                    } else {                                          // insert a new id
                        id = nextId++;
                        history.put(id, new java.util.concurrent.CopyOnWriteArrayList<>(List.of(v)));
                    }
                    idx.addOrUpdate(id, v);
                }
            });
            try {
                writer.get(60, TimeUnit.SECONDS);
            } finally {
                stop.set(true);
                pool.shutdownNow();
            }
            for (Future<?> f : readers) f.get(10, TimeUnit.SECONDS);

            assertThat(violations).isEmpty();
            assertThat(searches.get()).isGreaterThan(100);
            SpannStats st = idx.stats();
            assertThat(st.splits()).isGreaterThan(0);
            assertThat(st.compactions()).isGreaterThan(0);
            assertThat(st.entriesLive()).isEqualTo(history.size());
        }
    }
}
