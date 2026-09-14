package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.ExactVectorIndex;
import com.recsys.infrastructure.vectordb.SearchResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Opt-in: {@code mvn test -DexcludedGroups="" -Dgroups=load -Dtest=SpannProbeCurveLoadTest}.
 * Measures the recall/nprobe operating curve of a single built SPANN index (built once, since
 * the build takes roughly five minutes) using the package-private probe-count override, so the
 * controller can see what nprobe it actually takes to clear the 0.90 recall bar and what that
 * costs in distance computations. Only the exhaustive-probe recall is asserted; every other row
 * is reported, not asserted.
 */
@Tag("load")
class SpannProbeCurveLoadTest {

    private static final int N = 200_000, DIM = 64, COMPONENTS = 64, QUERIES = 1_000, K = 10;
    private static final long DATA_SEED = 101L, QUERY_SEED = 102L;
    private static final int[] PROBE_COUNTS = {8, 32, 128, 512, 1024, 3125};

    @TempDir Path dir;

    @Test
    void recallRisesWithProbeCount() throws Exception {
        List<float[]> queries = SpannTestVectors.mixture(QUERIES, DIM, COMPONENTS, QUERY_SEED);
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(N, DIM, COMPONENTS, DATA_SEED));

        List<Set<Integer>> truth = groundTruth(data, queries);

        SpannConfig cfg = SpannConfig.defaults().withDir(dir);
        SpannVectorIndex spann = new SpannVectorIndex(data, cfg);
        try {
            System.out.printf("%n%-6s %10s %16s %9s%n", "nprobe", "recall@10", "distances/query", "queryMs");
            double lastRecall = 0.0;
            long lastDistancesPerQuery = 0;
            for (int nprobe : PROBE_COUNTS) {
                long before = spann.stats().distanceComputations();
                long t0 = System.nanoTime();
                int hits = 0;
                for (int i = 0; i < queries.size(); i++) {
                    List<SearchResult> results = spann.search(queries.get(i), K, Set.of(), nprobe);
                    for (SearchResult r : results) if (truth.get(i).contains(r.id())) hits++;
                }
                long queryMs = (System.nanoTime() - t0) / 1_000_000;
                long after = spann.stats().distanceComputations();
                double distancesPerQuery = (after - before) / (double) queries.size();
                double recall = hits / (double) (queries.size() * K);

                System.out.printf("%-6d %10.3f %16.0f %9d%n", nprobe, recall, distancesPerQuery, queryMs);
                lastRecall = recall;
                lastDistancesPerQuery = Math.round(distancesPerQuery);
            }
            System.out.printf("centroidsLive: %d%n", spann.stats().centroidsLive());

            assertThat(lastRecall)
                    .as("recall@10 at the highest probe count (%d, %d distances/query) must reach the 0.90 bar",
                            PROBE_COUNTS[PROBE_COUNTS.length - 1], lastDistancesPerQuery)
                    .isGreaterThanOrEqualTo(0.90);
        } finally {
            spann.close();
        }
    }

    private List<Set<Integer>> groundTruth(Map<Integer, float[]> data, List<float[]> queries) {
        ExactVectorIndex exact = new ExactVectorIndex(data);
        List<Set<Integer>> truth = new ArrayList<>(queries.size());
        for (float[] q : queries) {
            Set<Integer> ids = new HashSet<>();
            for (SearchResult r : exact.search(q, K, Set.of())) ids.add(r.id());
            truth.add(ids);
        }
        return truth;
    }
}
