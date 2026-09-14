package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.ExactVectorIndex;
import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorMath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannVectorIndexSearchTest {

    @TempDir Path dir;

    private SpannConfig cfg() {
        // Order matters: SpannConfig validates on every with* call (SpannConfigTest documents
        // this as intentional), and defaults() has postingMin=16 — calling withPostingMax(16)
        // first would transiently violate postingMin < postingMax/2 before withPostingMin(2)
        // ever runs. Setting postingMin down to 2 first avoids that invalid intermediate state;
        // the final SpannConfig (postingMax=16, postingMin=2) is identical either way.
        return SpannConfig.defaults().withDir(dir).withPostingMin(2).withPostingMax(16).withRegionBytes(1 << 20);
    }

    /** nprobe covering every centroid: SPANN must equal exact. */
    private SpannConfig exhaustive() {
        return cfg().withNprobe(1_000_000);
    }

    @Test
    void exhaustiveProbe_equalsExactIndex() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(500, 8, 6, 1L));
        List<float[]> queries = SpannTestVectors.mixture(20, 8, 6, 2L);
        ExactVectorIndex exact = new ExactVectorIndex(data);
        try (SpannVectorIndex spann = new SpannVectorIndex(data, exhaustive())) {
            for (float[] q : queries) {
                List<SearchResult> e = exact.search(q, 10, Set.of());
                List<SearchResult> s = spann.search(q, 10, Set.of());
                assertThat(s).usingRecursiveFieldByFieldElementComparator().containsExactlyElementsOf(e);
            }
        }
    }

    @Test
    void everyIdIsRetrievableAndStatsCountPostings() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(200, 4, 4, 3L));
        try (SpannVectorIndex spann = new SpannVectorIndex(data, exhaustive())) {
            for (Map.Entry<Integer, float[]> e : data.entrySet()) {
                // Inner product does not make a vector its own nearest neighbour (a longer aligned
                // vector scores higher), so "retrievable" means: present, with its own score.
                float[] v = e.getValue();
                List<SearchResult> all = spann.search(v, 200, Set.of());
                assertThat(all).hasSize(200).contains(new SearchResult(e.getKey(), VectorMath.innerProduct(v, v)));
            }
            SpannStats st = spann.stats();
            assertThat(st.entriesLive()).isEqualTo(200);
            assertThat(st.centroidsLive()).isBetween(1, 200);
            assertThat(st.fileBytes()).isPositive();
            assertThat(spann.dimension()).isEqualTo(4);
            assertThat(spann.name()).isEqualTo("spann");
        }
    }

    @Test
    void defaultProbe_findsTheNearestOfAWellClusteredCorpus() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(2000, 16, 8, 4L));
        List<float[]> queries = SpannTestVectors.mixture(50, 16, 8, 5L);
        ExactVectorIndex exact = new ExactVectorIndex(data);
        int hits = 0;
        // ~250 centroids over 8 clusters, so 16 probes is ~6% of centroids, half of one cluster's
        // postings; the production bar at the default nprobe belongs to the benchmark.
        try (SpannVectorIndex spann = new SpannVectorIndex(data, cfg().withNprobe(16))) {
            long before = spann.stats().distanceComputations();
            for (float[] q : queries) {
                Set<Integer> truth = new java.util.HashSet<>();
                for (SearchResult r : exact.search(q, 10, Set.of())) truth.add(r.id());
                for (SearchResult r : spann.search(q, 10, Set.of())) if (truth.contains(r.id())) hits++;
            }
            long computed = spann.stats().distanceComputations() - before;
            assertThat(hits / 500.0).isGreaterThanOrEqualTo(0.9);
            // CPU proxy: far fewer scorings than exact's 2000 per query (build-time assignment excluded).
            assertThat(computed).isLessThan(50L * 2000);
        }
    }

    @Test
    void exclusionsAreHonoured() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(100, 4, 2, 6L));
        try (SpannVectorIndex spann = new SpannVectorIndex(data, exhaustive())) {
            float[] q = data.get(7);
            // Present with its own score (inner product does not make a vector its own top hit)…
            assertThat(spann.search(q, 100, Set.of())).contains(new SearchResult(7, VectorMath.innerProduct(q, q)));
            // …and absent once excluded; a null exclusion set means nothing excluded.
            assertThat(spann.search(q, 100, Set.of(7))).hasSize(99).extracting(SearchResult::id).doesNotContain(7);
            assertThat(spann.search(q, 5, null)).hasSize(5);
        }
    }

    @Test
    void equalScoresOrderByIdAscendingInCutAndOutput() {
        // Four identical vectors with ids chosen to collide in the id map's hash buckets (cf. #324).
        float[] same = {1f, 0f};
        Map<Integer, float[]> data = Map.of(17, same, 33, same, 2, same, 20, same, 99, new float[]{-1f, 0f});
        try (SpannVectorIndex spann = new SpannVectorIndex(data, exhaustive())) {
            assertThat(spann.search(new float[]{1f, 0f}, 4, Set.of())).extracting(SearchResult::id).containsExactly(2, 17, 20, 33);
            assertThat(spann.search(new float[]{1f, 0f}, 2, Set.of())).extracting(SearchResult::id).containsExactly(2, 17);
        }
    }

    @Test
    void wideningFallback_fillsKWhenTheFirstProbeCannot() {
        // Two far-apart blobs; nprobe=1 lands in one blob; asking for more than that blob holds
        // must widen to the other blob rather than return short.
        Map<Integer, float[]> data = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 10; i++) data.put(i, new float[]{i * 0.01f, 0f});
        for (int i = 10; i < 20; i++) data.put(i, new float[]{100f + i * 0.01f, 0f});
        try (SpannVectorIndex spann = new SpannVectorIndex(data, cfg().withPostingMax(12).withNprobe(1))) {
            assertThat(spann.stats().centroidsLive()).isGreaterThanOrEqualTo(2);
            List<SearchResult> hits = spann.search(new float[]{0f, 0f}, 15, Set.of());
            assertThat(hits).hasSize(15);
            assertThat(spann.stats().fallbackWidenings()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void emptyIndex_searchesEmpty() {
        try (SpannVectorIndex spann = new SpannVectorIndex(Map.of(), cfg())) {
            assertThat(spann.search(new float[]{1f}, 3, Set.of())).isEmpty();
            assertThat(spann.dimension()).isZero();
            assertThat(spann.stats().centroidsLive()).isZero();
        }
    }

    @Test
    void nullQueryOrNonPositiveKOrWrongDimension_isEmpty() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(30, 3, 2, 7L));
        try (SpannVectorIndex spann = new SpannVectorIndex(data, exhaustive())) {
            assertThat(spann.search(null, 3, Set.of())).isEmpty();
            assertThat(spann.search(new float[]{1f, 2f, 3f}, 0, Set.of())).isEmpty();
            assertThat(spann.search(new float[]{1f, 2f}, 3, Set.of())).isEmpty();
        }
    }

    @Test
    void mixedDimensionsAreRejectedAtBuild() {
        Map<Integer, float[]> bad = Map.of(1, new float[]{1f, 2f}, 2, new float[]{1f});
        assertThatThrownBy(() -> new SpannVectorIndex(bad, cfg()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dimension");
        // No index file may be left behind by a failed build.
        assertThat(dir.toFile().listFiles()).isEmpty();
    }
}
