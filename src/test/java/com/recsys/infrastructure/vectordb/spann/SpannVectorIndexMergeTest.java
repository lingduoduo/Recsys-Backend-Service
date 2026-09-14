package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorMath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SpannVectorIndexMergeTest {

    @TempDir Path dir;

    private SpannConfig cfg() {
        return SpannConfig.defaults().withDir(dir).withPostingMin(3).withPostingMax(16)
                .withNprobe(1_000_000).withRegionBytes(1 << 20);
    }

    private static Map<Integer, float[]> twoBlobs() {
        Map<Integer, float[]> m = new LinkedHashMap<>();
        for (int i = 0; i < 8; i++) m.put(i, new float[]{i * 0.1f, 0f});
        for (int i = 10; i < 18; i++) m.put(i, new float[]{50f + i * 0.1f, 0f});
        return m;
    }

    @Test
    void shrinkingBelowPostingMin_mergesIntoTheNearestPosting() {
        try (SpannVectorIndex idx = new SpannVectorIndex(twoBlobs(), cfg())) {
            assertThat(idx.stats().centroidsLive()).isEqualTo(2);
            // Move six of blob A's eight ids into blob B: A drops to 2 < postingMin 3.
            for (int i = 0; i < 6; i++) idx.addOrUpdate(i, new float[]{52f + i * 0.1f, 0f});
            SpannStats st = idx.stats();
            assertThat(st.merges()).isGreaterThanOrEqualTo(1);
            assertThat(st.entriesLive()).isEqualTo(16);
            assertThat(idx.search(new float[]{0.7f, 0f}, 20, Set.of()))
                    .hasSize(16).extracting(SearchResult::id).doesNotHaveDuplicates().contains(6, 7);
            // The two survivors are still retrievable with their own vectors.
            float[] q = {1f, 0f};
            assertThat(idx.search(q, 20, Set.of())).contains(new SearchResult(7, VectorMath.innerProduct(q, new float[]{0.7f, 0f})));
        }
    }

    @Test
    void mergeNeverRunsOnTheLastLivePosting() {
        Map<Integer, float[]> m = new LinkedHashMap<>();
        for (int i = 0; i < 4; i++) m.put(i, new float[]{i, 0f});
        try (SpannVectorIndex idx = new SpannVectorIndex(m, cfg().withPostingMax(20))) {
            assertThat(idx.stats().centroidsLive()).isEqualTo(1);
            idx.addOrUpdate(0, new float[]{100f, 0f});       // same posting; live count stays 4
            idx.addOrUpdate(1, new float[]{100f, 1f});
            assertThat(idx.stats().merges()).isZero();
            assertThat(idx.stats().centroidsLive()).isEqualTo(1);
            assertThat(idx.stats().entriesLive()).isEqualTo(4);
        }
    }
}
