package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.ExactVectorIndex;
import com.recsys.infrastructure.vectordb.LshVectorIndex;
import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorIndex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Opt-in: {@code mvn test -DexcludedGroups="" -Dgroups=load -Dtest=SpannBenchmarkLoadTest}.
 * Asserts the acceptance bar from the spec on synthetic data; prints all three backends.
 * Wall-clock is printed but never asserted.
 */
@Tag("load")
class SpannBenchmarkLoadTest {

    private static final int N = 200_000, DIM = 64, COMPONENTS = 64, QUERIES = 1_000, K = 10;
    private static final long DATA_SEED = 101L, QUERY_SEED = 102L;

    @TempDir Path dir;

    record Row(String name, long retainedHeapBytes, double recallAt10, double distancesPerQuery, long buildMs, long queryMs) {}

    @Test
    void spannMeetsTheAcceptanceBar() throws Exception {
        List<float[]> queries = SpannTestVectors.mixture(QUERIES, DIM, COMPONENTS, QUERY_SEED);
        List<Set<Integer>> truth = groundTruth(queries);

        Row exact = measure("exact", ExactVectorIndex::new, queries, truth, null);
        Row lsh = measure("lsh", LshVectorIndex::new, queries, truth, null);
        SpannConfig cfg = SpannConfig.defaults().withDir(dir);
        Row spann = measure("spann", m -> new SpannVectorIndex(m, cfg), queries, truth, SpannVectorIndex.class);

        System.out.printf("%n%-6s %14s %10s %16s %9s %9s%n", "index", "retainedHeapMB", "recall@10", "distances/query", "buildMs", "queryMs");
        for (Row r : List.of(exact, lsh, spann)) {
            System.out.printf("%-6s %14.1f %10.3f %16.0f %9d %9d%n", r.name(), r.retainedHeapBytes() / 1048576.0,
                    r.recallAt10(), r.distancesPerQuery(), r.buildMs(), r.queryMs());
        }
        System.out.printf("machine: %s, JVM %s, heap max %d MB%n", System.getProperty("os.arch"),
                System.getProperty("java.version"), Runtime.getRuntime().maxMemory() / 1048576);

        assertThat(spann.retainedHeapBytes()).isLessThanOrEqualTo((long) (0.25 * exact.retainedHeapBytes()));
        assertThat(spann.recallAt10()).isGreaterThanOrEqualTo(0.90);
        assertThat(spann.distancesPerQuery()).isLessThan(N);
    }

    private List<Set<Integer>> groundTruth(List<float[]> queries) {
        ExactVectorIndex exact = new ExactVectorIndex(SpannTestVectors.asMap(SpannTestVectors.mixture(N, DIM, COMPONENTS, DATA_SEED)));
        List<Set<Integer>> truth = new ArrayList<>(queries.size());
        for (float[] q : queries) {
            Set<Integer> ids = new HashSet<>();
            for (SearchResult r : exact.search(q, K, Set.of())) ids.add(r.id());
            truth.add(ids);
        }
        return truth;
    }

    private Row measure(String name, Function<Map<Integer, float[]>, VectorIndex> factory, List<float[]> queries,
                        List<Set<Integer>> truth, Class<?> statsClass) throws Exception {
        long baseline = usedHeapAfterGc();
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(N, DIM, COMPONENTS, DATA_SEED));
        long t0 = System.nanoTime();
        VectorIndex idx = factory.apply(data);
        long buildMs = (System.nanoTime() - t0) / 1_000_000;
        data = null;                                                   // only the index stays reachable
        long retained = usedHeapAfterGc() - baseline;

        long before = idx instanceof SpannVectorIndex s ? s.stats().distanceComputations() : 0;
        int hits = 0;
        long q0 = System.nanoTime();
        for (int i = 0; i < queries.size(); i++) {
            for (SearchResult r : idx.search(queries.get(i), K, Set.of())) if (truth.get(i).contains(r.id())) hits++;
        }
        long queryMs = (System.nanoTime() - q0) / 1_000_000;
        double distances = idx instanceof SpannVectorIndex s
                ? (s.stats().distanceComputations() - before) / (double) queries.size()
                : (name.equals("exact") ? N : Double.NaN);
        double recall = hits / (double) (queries.size() * K);
        idx.close();
        return new Row(name, retained, recall, distances, buildMs, queryMs);
    }

    private static long usedHeapAfterGc() throws InterruptedException {
        MemoryMXBean mx = ManagementFactory.getMemoryMXBean();
        long last = Long.MAX_VALUE;
        for (int i = 0; i < 5; i++) {                                  // poll until the reading settles
            System.gc();
            Thread.sleep(100);
            long used = mx.getHeapMemoryUsage().getUsed();
            if (Math.abs(used - last) < 1_000_000) return used;
            last = used;
        }
        return last;
    }
}
