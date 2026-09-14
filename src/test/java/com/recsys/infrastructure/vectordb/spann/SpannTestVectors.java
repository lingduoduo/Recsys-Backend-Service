package com.recsys.infrastructure.vectordb.spann;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Seeded Gaussian-mixture vectors: clustering is meaningful, unlike uniform noise. */
final class SpannTestVectors {

    private SpannTestVectors() {}

    static List<float[]> mixture(int n, int dim, int components, long seed) {
        Random rng = new Random(seed);
        float[][] centres = new float[components][dim];
        for (float[] c : centres) for (int d = 0; d < dim; d++) c[d] = (float) (rng.nextGaussian() * 5.0);
        List<float[]> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            float[] c = centres[rng.nextInt(components)];
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) v[d] = c[d] + (float) rng.nextGaussian();
            out.add(v);
        }
        return out;
    }

    /** ids 0..n-1 in insertion order. */
    static Map<Integer, float[]> asMap(List<float[]> vectors) {
        Map<Integer, float[]> m = new LinkedHashMap<>();
        for (int i = 0; i < vectors.size(); i++) m.put(i, vectors.get(i));
        return m;
    }
}
