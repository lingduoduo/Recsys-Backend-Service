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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class SpannVectorIndexUpdateTest {

    @TempDir Path dir;

    private SpannConfig cfg() {
        return SpannConfig.defaults().withDir(dir).withPostingMin(0).withPostingMax(100)
                .withNprobe(1_000_000).withRegionBytes(1 << 20);
    }

    private static Map<Integer, float[]> twoBlobs() {
        Map<Integer, float[]> m = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) m.put(i, new float[]{i * 0.1f, 0f});
        for (int i = 10; i < 20; i++) m.put(i, new float[]{50f + i * 0.1f, 0f});
        return m;
    }

    @Test
    void insertNewId_isSearchable() {
        try (SpannVectorIndex idx = new SpannVectorIndex(twoBlobs(), cfg())) {
            float[] v = {0.35f, 0f};
            idx.addOrUpdate(42, v);
            List<SearchResult> hits = idx.search(v, 100, Set.of());
            assertThat(hits).hasSize(21).contains(new SearchResult(42, VectorMath.innerProduct(v, v)));
            assertThat(idx.stats().entriesLive()).isEqualTo(21);
        }
    }

    @Test
    void overwriteInSamePosting_hidesTheOldVector() {
        try (SpannVectorIndex idx = new SpannVectorIndex(twoBlobs(), cfg())) {
            idx.addOrUpdate(3, new float[]{0.95f, 0f});            // still nearest to the first blob
            List<SearchResult> hits = idx.search(new float[]{1f, 0f}, 100, Set.of());
            assertThat(hits).hasSize(20).extracting(SearchResult::id).doesNotHaveDuplicates();
            // id 3 scores with its NEW vector (0.95); the old (0.3) is gone.
            assertThat(scoreOf(hits, 3)).isCloseTo(0.95, within(1e-6));
            assertThat(idx.stats().entriesLive()).isEqualTo(20);
        }
    }

    @Test
    void overwriteMovingToAnotherPosting_isVisibleExactlyOnce() {
        // postingMax 16 -> ceil(20 / 8) = 3 centroids over two blobs, so a move can cross postings.
        try (SpannVectorIndex idx = new SpannVectorIndex(twoBlobs(), cfg().withPostingMax(16))) {
            int before = idx.stats().centroidsLive();
            assertThat(before).isGreaterThanOrEqualTo(2);
            idx.addOrUpdate(3, new float[]{51.05f, 0f});           // moves from blob A to blob B
            List<SearchResult> all = idx.search(new float[]{1f, 0f}, 30, Set.of());
            assertThat(all).hasSize(20).extracting(SearchResult::id).doesNotHaveDuplicates().contains(3);
            assertThat(scoreOf(all, 3)).isCloseTo(51.05, within(1e-4));
            assertThat(idx.stats().deadBytes()).isPositive();
        }
    }

    private static double scoreOf(List<SearchResult> hits, int id) {
        return hits.stream().filter(r -> r.id() == id).mapToDouble(SearchResult::score).findFirst().orElseThrow();
    }

    @Test
    void firstInsertIntoEmptyIndex_setsTheDimension() {
        try (SpannVectorIndex idx = new SpannVectorIndex(Map.of(), cfg())) {
            idx.addOrUpdate(1, new float[]{1f, 2f, 3f});
            assertThat(idx.dimension()).isEqualTo(3);
            assertThat(idx.search(new float[]{1f, 2f, 3f}, 1, Set.of()).get(0).id()).isEqualTo(1);
            idx.addOrUpdate(2, new float[]{-1f, -2f, -3f});
            assertThat(idx.search(new float[]{-1f, -2f, -3f}, 1, Set.of()).get(0).id()).isEqualTo(2);
        }
    }

    @Test
    void dimensionMismatchIsRejectedBeforeAnyWrite() {
        try (SpannVectorIndex idx = new SpannVectorIndex(twoBlobs(), cfg())) {
            long bytes = idx.stats().fileBytes();
            assertThatThrownBy(() -> idx.addOrUpdate(7, new float[]{1f, 2f, 3f}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dimension");
            assertThatThrownBy(() -> idx.addOrUpdate(7, null)).isInstanceOf(IllegalArgumentException.class);
            assertThat(idx.stats().fileBytes()).isEqualTo(bytes);
        }
    }
}
