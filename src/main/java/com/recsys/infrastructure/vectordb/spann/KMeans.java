package com.recsys.infrastructure.vectordb.spann;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Seeded k-means++ / Lloyd clustering on L2 distance. Small and dependency-free by design. */
final class KMeans {

    private KMeans() {}

    static double l2sq(float[] a, float[] b) {
        double s = 0;
        for (int i = 0; i < a.length; i++) {
            double d = (double) a[i] - b[i];
            s += d * d;
        }
        return s;
    }

    /** Returns {@code min(k, n)} centroids. Empty clusters keep their previous centroid. */
    static float[][] cluster(List<float[]> vectors, int k, long seed, int maxIterations) {
        int n = vectors.size();
        if (n == 0 || k <= 0) return new float[0][];
        if (k >= n) {
            float[][] out = new float[n][];
            for (int i = 0; i < n; i++) out[i] = vectors.get(i).clone();
            return out;
        }
        int dim = vectors.get(0).length;
        Random rng = new Random(seed);
        float[][] centroids = seedPlusPlus(vectors, k, rng);
        int[] assign = new int[n];
        for (int iter = 0; iter < maxIterations; iter++) {
            boolean changed = false;
            for (int i = 0; i < n; i++) {
                int c = nearest(vectors.get(i), centroids, null);
                if (c != assign[i]) { assign[i] = c; changed = true; }
            }
            if (!changed && iter > 0) break;
            double[][] sum = new double[k][dim];
            int[] count = new int[k];
            for (int i = 0; i < n; i++) {
                float[] v = vectors.get(i);
                int c = assign[i];
                count[c]++;
                for (int d = 0; d < dim; d++) sum[c][d] += v[d];
            }
            for (int c = 0; c < k; c++) {
                if (count[c] == 0) continue;
                for (int d = 0; d < dim; d++) centroids[c][d] = (float) (sum[c][d] / count[c]);
            }
        }
        return centroids;
    }

    static int nearest(float[] v, float[][] centroids, boolean[] alive) {
        int best = -1;
        double bestD = Double.POSITIVE_INFINITY;
        for (int i = 0; i < centroids.length; i++) {
            if (alive != null && !alive[i]) continue;
            double d = l2sq(v, centroids[i]);
            if (d < bestD) { bestD = d; best = i; }
        }
        return best;
    }

    private static float[][] seedPlusPlus(List<float[]> vectors, int k, Random rng) {
        int n = vectors.size();
        float[][] centroids = new float[k][];
        centroids[0] = vectors.get(rng.nextInt(n)).clone();
        double[] minD = new double[n];
        Arrays.fill(minD, Double.POSITIVE_INFINITY);
        for (int c = 1; c < k; c++) {
            double total = 0;
            for (int i = 0; i < n; i++) {
                double d = l2sq(vectors.get(i), centroids[c - 1]);
                if (d < minD[i]) minD[i] = d;
                total += minD[i];
            }
            int chosen = n - 1;
            if (total > 0) {
                double r = rng.nextDouble() * total, acc = 0;
                for (int i = 0; i < n; i++) {
                    acc += minD[i];
                    if (acc >= r) { chosen = i; break; }
                }
            } else {
                chosen = rng.nextInt(n);
            }
            centroids[c] = vectors.get(chosen).clone();
        }
        return centroids;
    }
}
