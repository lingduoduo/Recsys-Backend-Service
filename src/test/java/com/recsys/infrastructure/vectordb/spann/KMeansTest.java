package com.recsys.infrastructure.vectordb.spann;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class KMeansTest {

    @Test
    void l2sq_isSquaredEuclideanDistance() {
        assertThat(KMeans.l2sq(new float[]{0f, 0f}, new float[]{3f, 4f})).isCloseTo(25.0, within(1e-9));
    }

    @Test
    void cluster_twoWellSeparatedBlobs_findsBothCentres() {
        Random rng = new Random(1);
        List<float[]> v = new ArrayList<>();
        for (int i = 0; i < 100; i++) v.add(new float[]{(float) rng.nextGaussian() * 0.1f, (float) rng.nextGaussian() * 0.1f});
        for (int i = 0; i < 100; i++) v.add(new float[]{10f + (float) rng.nextGaussian() * 0.1f, (float) rng.nextGaussian() * 0.1f});

        float[][] c = KMeans.cluster(v, 2, 42L, 10);

        assertThat(c).hasDimensions(2, 2);
        double[] xs = {c[0][0], c[1][0]};
        java.util.Arrays.sort(xs);
        assertThat(xs[0]).isCloseTo(0.0, within(0.2));
        assertThat(xs[1]).isCloseTo(10.0, within(0.2));
    }

    @Test
    void cluster_isDeterministicForASeed() {
        Random rng = new Random(2);
        List<float[]> v = new ArrayList<>();
        for (int i = 0; i < 50; i++) v.add(new float[]{rng.nextFloat(), rng.nextFloat(), rng.nextFloat()});
        assertThat(KMeans.cluster(v, 5, 9L, 5)).isDeepEqualTo(KMeans.cluster(v, 5, 9L, 5));
    }

    @Test
    void cluster_kAtLeastN_returnsTheVectorsThemselves() {
        List<float[]> v = List.of(new float[]{1f, 2f}, new float[]{3f, 4f});
        float[][] c = KMeans.cluster(v, 5, 0L, 3);
        assertThat(c).hasNumberOfRows(2);
        assertThat(c[0]).containsExactly(1f, 2f);
        assertThat(c[1]).containsExactly(3f, 4f);
    }

    @Test
    void nearest_skipsDeadCentroidsAndReturnsMinusOneWhenNoneAlive() {
        float[][] c = {{0f, 0f}, {10f, 0f}, {20f, 0f}};
        assertThat(KMeans.nearest(new float[]{9f, 0f}, c, null)).isEqualTo(1);
        assertThat(KMeans.nearest(new float[]{9f, 0f}, c, new boolean[]{true, false, true})).isEqualTo(0);
        assertThat(KMeans.nearest(new float[]{9f, 0f}, c, new boolean[]{false, false, false})).isEqualTo(-1);
    }
}
