package com.recsys.infrastructure.vectordb;

import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class EmbeddingLSHTest {

    @Test
    void add_newVecAppearsInCandidates() {
        EmbeddingLSH lsh = new EmbeddingLSH(Map.of(1, new float[]{1f, 0f}));
        lsh.add(99, new float[]{1f, 0f});
        assertThat(lsh.candidates(new float[]{1f, 0f})).contains(99);
    }

    @Test
    void add_duplicateIdIsAddedAgainWithoutError() {
        EmbeddingLSH lsh = new EmbeddingLSH(Map.of(1, new float[]{1f, 0f}));
        lsh.add(1, new float[]{0f, 1f}); // should not throw
        assertThat(lsh.candidates(new float[]{0f, 1f})).contains(1);
    }

    // The periodic item-embedding refresh calls add() for every changed vector, on every pod, for the
    // pod's lifetime. Each call used to append to the new bucket and never leave the old one, so bucket
    // lists (and the per-request candidate HashSet built from them) grew with uptime.

    @Test
    void reAddingTheSameIdKeepsBucketMembershipConstant() {
        float[] a = {1f, 0f, 0f};
        float[] c = {-1f, 0f, 0f};   // opposite direction: a different bucket for every hyperplane
        EmbeddingLSH lsh = new EmbeddingLSH(Map.of(1, a, 2, new float[]{0f, 1f, 0f}));

        for (int i = 0; i < 100; i++) {
            lsh.add(1, i % 2 == 0 ? c : a);   // moves between buckets
            lsh.add(2, new float[]{0f, 1f, 0f});   // same bucket every time
        }

        assertThat(lsh.bucketEntryCount()).as("one entry per id").isEqualTo(2);
    }

    @Test
    void aMovedIdIsFoundAtItsNewVectorAndNotAtItsOld() {
        float[] a = {1f, 0f, 0f};
        float[] c = {-1f, 0f, 0f};
        EmbeddingLSH lsh = new EmbeddingLSH(Map.of(1, a, 2, new float[]{0f, 1f, 0f}));

        lsh.add(1, c);

        assertThat(lsh.candidates(c)).contains(1);
        assertThat(lsh.candidates(a)).doesNotContain(1);
    }
}
