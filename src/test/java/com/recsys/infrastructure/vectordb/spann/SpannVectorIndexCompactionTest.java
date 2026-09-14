package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.ExactVectorIndex;
import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorMath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannVectorIndexCompactionTest {

    @TempDir Path dir;

    private SpannConfig cfg() {
        return SpannConfig.defaults().withDir(dir).withPostingMin(2).withPostingMax(16)
                .withNprobe(1_000_000).withCompactRatio(0.3).withRegionBytes(1 << 20);
    }

    @Test
    void repeatedOverwrites_triggerCompactionAndPreserveEveryResult() throws Exception {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(60, 4, 3, 11L));
        try (SpannVectorIndex idx = new SpannVectorIndex(data, cfg())) {
            Path first = ((MappedPostingStore) storeOf(idx)).path();
            for (int round = 0; round < 20; round++) {
                float[] v = data.get(5).clone();
                v[0] += round * 1e-3f;
                idx.addOrUpdate(5, v);
                data.put(5, v);
            }
            SpannStats st = idx.stats();
            assertThat(st.compactions()).isGreaterThanOrEqualTo(1);
            assertThat(st.deadBytes()).isLessThan((long) (0.3 * st.fileBytes()) + 1);
            assertThat(Files.exists(first)).isFalse();                  // old file gone
            assertThat(Files.list(dir).count()).isEqualTo(1);           // exactly one live file
            ExactVectorIndex exact = new ExactVectorIndex(data);
            for (float[] q : SpannTestVectors.mixture(10, 4, 3, 12L)) {
                List<SearchResult> e = exact.search(q, 8, Set.of());
                assertThat(idx.search(q, 8, Set.of())).usingRecursiveFieldByFieldElementComparator().containsExactlyElementsOf(e);
            }
        }
    }

    @Test
    void writeFailure_leavesThePreviousSnapshotServing() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(40, 4, 2, 13L));
        AtomicBoolean fail = new AtomicBoolean(false);
        try (SpannVectorIndex idx = new SpannVectorIndex(data, cfg(), () -> new PostingStore() {
            private final MappedPostingStore real = new MappedPostingStore(dir, 1 << 20);
            @Override public long append(Block block) {
                if (fail.get()) throw new UncheckedIOException(new java.io.IOException("disk full"));
                return real.append(block);
            }
            @Override public Block read(long offset) { return real.read(offset); }
            @Override public long bytes() { return real.bytes(); }
            @Override public void close() { real.close(); }
        })) {
            float[] q = data.get(3);
            List<SearchResult> before = idx.search(q, 5, Set.of());
            SpannStats statsBefore = idx.stats();
            fail.set(true);
            assertThatThrownBy(() -> idx.addOrUpdate(99, new float[]{1f, 1f, 1f, 1f}))
                    .isInstanceOf(UncheckedIOException.class).hasMessageContaining("disk full");
            assertThat(idx.search(q, 5, Set.of())).usingRecursiveFieldByFieldElementComparator().containsExactlyElementsOf(before);
            assertThat(idx.stats().entriesLive()).isEqualTo(statsBefore.entriesLive());
            assertThat(idx.stats().fileBytes()).isEqualTo(statsBefore.fileBytes());
            fail.set(false);
            float[] v = {1f, 1f, 1f, 1f};
            idx.addOrUpdate(99, v);                                     // recovers without a rebuild
            // Present with its own score: a longer aligned vector legitimately outranks it under inner product.
            assertThat(idx.search(v, 100, Set.of())).contains(new SearchResult(99, VectorMath.innerProduct(v, v)));
        }
    }

    /** Test-only access to the live store, via the package-private snapshot accessor added in Step 3. */
    private static PostingStore storeOf(SpannVectorIndex idx) {
        return idx.snapshotForTests().store();
    }
}
