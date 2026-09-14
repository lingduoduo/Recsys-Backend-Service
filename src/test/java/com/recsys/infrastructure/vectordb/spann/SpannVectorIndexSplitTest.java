package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorMath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SpannVectorIndexSplitTest {

    @TempDir Path dir;

    private SpannConfig cfg(int postingMax) {
        return SpannConfig.defaults().withDir(dir).withPostingMin(0).withPostingMax(postingMax)
                .withNprobe(1_000_000).withRegionBytes(1 << 20);
    }

    @Test
    void insertingPastPostingMax_splitsAndKeepsEveryPostingBounded() {
        try (SpannVectorIndex idx = new SpannVectorIndex(Map.of(0, new float[]{0f, 0f}), cfg(4))) {
            for (int i = 1; i < 30; i++) idx.addOrUpdate(i, new float[]{i * 0.1f, (i % 3) * 0.1f});
            SpannStats st = idx.stats();
            assertThat(st.splits()).isGreaterThanOrEqualTo(1);
            assertThat(st.entriesLive()).isEqualTo(30);
            assertThat(st.centroidsLive()).isGreaterThanOrEqualTo(8);          // 30 entries / max 4
            for (int i = 0; i < 30; i++) {
                float[] v = {i * 0.1f, (i % 3) * 0.1f};
                assertThat(idx.search(v, 100, Set.of())).contains(new SearchResult(i, VectorMath.innerProduct(v, v)));
            }
            List<SearchResult> all = idx.search(new float[]{1f, 0f}, 100, Set.of());
            assertThat(all).hasSize(30).extracting(SearchResult::id).doesNotHaveDuplicates();
        }
    }

    @Test
    void build_splitsOversizedPostings() {
        // 40 near-identical points force k-means to put most of them in one posting of max 8.
        Map<Integer, float[]> m = new LinkedHashMap<>();
        for (int i = 0; i < 40; i++) m.put(i, new float[]{i * 0.001f, 0f});
        try (SpannVectorIndex idx = new SpannVectorIndex(m, cfg(8))) {
            assertThat(idx.stats().entriesLive()).isEqualTo(40);
            assertThat(idx.stats().centroidsLive()).isGreaterThanOrEqualTo(5);
            for (int i = 0; i < 40; i++) {
                float[] v = {i * 0.001f, 0f};
                assertThat(idx.search(v, 100, Set.of())).contains(new SearchResult(i, VectorMath.innerProduct(v, v)));
            }
        }
    }

    @Test
    void reassignment_movesAVectorThatIsNowNearerToANewCentroid() {
        // Nine points, postingMax 9 -> ceil(9 / 4.5) = 2 centroids: A = four points around the
        // origin plus p = (4.5, 0) (nearer A's mean (0.9, 0.2) than B's (10.25, 0)), B = four
        // points around (10, 0). Six inserts around (7.5, 0) all land in B (nearer than A),
        // overflow it at the 10th entry, and its 2-means split yields centroids ~(7.5, 0) and
        // ~(10.25, 0). p is now 3.0 from (7.5, 0) but 3.6 from A's mean, so reassignment must
        // move it; A's other four points stay.
        Map<Integer, float[]> m = new LinkedHashMap<>();
        m.put(0, new float[]{0f, 0f}); m.put(1, new float[]{1f, 0f}); m.put(2, new float[]{0f, 1f}); m.put(3, new float[]{-1f, 0f});
        m.put(4, new float[]{4.5f, 0f});                                                // p
        m.put(10, new float[]{10f, 0f}); m.put(11, new float[]{10f, 1f}); m.put(12, new float[]{10f, -1f}); m.put(13, new float[]{11f, 0f});
        try (SpannVectorIndex idx = new SpannVectorIndex(m, cfg(9).withReassignProbe(4))) {
            assertThat(idx.stats().centroidsLive()).isEqualTo(2);          // premise guard
            int id = 20;
            for (float[] v : new float[][]{{7f, 0f}, {7f, 1f}, {7f, -1f}, {8f, 0f}, {8f, 1f}, {8f, -1f}}) {
                idx.addOrUpdate(id++, v);
            }
            assertThat(idx.stats().splits()).isGreaterThanOrEqualTo(1);
            assertThat(idx.stats().reassigned()).isGreaterThanOrEqualTo(1);
            assertThat(idx.stats().entriesLive()).isEqualTo(15);
            // p is still exactly once retrievable, with its own vector (4.5^2 = 20.25).
            List<SearchResult> all = idx.search(new float[]{4.5f, 0f}, 50, Set.of());
            assertThat(all).hasSize(15).contains(new SearchResult(4, 20.25));
            assertThat(all).extracting(SearchResult::id).doesNotHaveDuplicates();
        }
    }
}
