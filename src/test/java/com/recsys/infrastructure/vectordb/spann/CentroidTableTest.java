package com.recsys.infrastructure.vectordb.spann;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CentroidTableTest {

    private static CentroidTable three() {
        return CentroidTable.empty()
                .withAdded(new float[]{0f, 0f}, 0L, 5)
                .withAdded(new float[]{10f, 0f}, 100L, 6)
                .withAdded(new float[]{20f, 0f}, 200L, 7);
    }

    @Test
    void withAdded_allocatesSlotsInOrder() {
        CentroidTable t = three();
        assertThat(t.size()).isEqualTo(3);
        assertThat(t.aliveCount()).isEqualTo(3);
        assertThat(t.centroid(1)).containsExactly(10f, 0f);
        assertThat(t.offset(2)).isEqualTo(200L);
        assertThat(t.live(0)).isEqualTo(5);
        assertThat(t.aliveSlots()).containsExactly(0, 1, 2);
    }

    @Test
    void mutatorsDoNotChangeTheOriginal() {
        CentroidTable t = three();
        CentroidTable t2 = t.withRepointed(1, 999L, 1).withKilled(0);
        assertThat(t.offset(1)).isEqualTo(100L);
        assertThat(t.alive(0)).isTrue();
        assertThat(t2.offset(1)).isEqualTo(999L);
        assertThat(t2.live(1)).isEqualTo(1);
        assertThat(t2.alive(0)).isFalse();
        assertThat(t2.aliveCount()).isEqualTo(2);
        assertThat(t2.aliveSlots()).containsExactly(1, 2);
    }

    @Test
    void killedSlotsKeepTheirNumberAndNewSlotsAppend() {
        CentroidTable t = three().withKilled(1).withAdded(new float[]{30f, 0f}, 300L, 1);
        assertThat(t.size()).isEqualTo(4);
        assertThat(t.alive(1)).isFalse();
        assertThat(t.centroid(3)).containsExactly(30f, 0f);
    }

    @Test
    void withKilled_releasesTheDeadSlotsCentroidWithoutTouchingTheOriginal() {
        CentroidTable t = three();
        CentroidTable killed = t.withKilled(1);
        assertThat(killed.alive(1)).isFalse();
        assertThat(killed.centroid(1)).isNull();          // the vector is released, not retained
        assertThat(t.centroid(1)).containsExactly(10f, 0f); // the shared array was copied, not mutated
    }

    @Test
    void nearest_ignoresDeadSlots() {
        CentroidTable t = three();
        assertThat(t.nearest(new float[]{9f, 0f})).isEqualTo(1);
        assertThat(t.withKilled(1).nearest(new float[]{9f, 0f})).isEqualTo(0);
        assertThat(CentroidTable.empty().nearest(new float[]{9f, 0f})).isEqualTo(-1);
    }

    @Test
    void nearestN_ordersAscendingAndHonoursExclusions() {
        CentroidTable t = three();
        assertThat(t.nearestN(new float[]{12f, 0f}, 3)).containsExactly(1, 2, 0);
        assertThat(t.nearestN(new float[]{12f, 0f}, 2, 1)).containsExactly(2, 0);
        assertThat(t.nearestN(new float[]{12f, 0f}, 10, 1, 2)).containsExactly(0);
    }

    @Test
    void distancesToAlive_matchesL2sqInSlotOrder() {
        CentroidTable t = three();
        double[] d = t.distancesToAlive(new float[]{0f, 0f}, t.aliveSlots());
        assertThat(d).containsExactly(0.0, 100.0, 400.0);
    }

    @Test
    void growsPastInitialCapacity() {
        CentroidTable t = CentroidTable.empty();
        for (int i = 0; i < 100; i++) t = t.withAdded(new float[]{i}, i, 1);
        assertThat(t.size()).isEqualTo(100);
        assertThat(t.centroid(99)).containsExactly(99f);
    }
}
