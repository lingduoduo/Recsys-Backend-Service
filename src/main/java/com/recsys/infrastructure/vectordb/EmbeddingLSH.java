package com.recsys.infrastructure.vectordb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

// Sign-based random projection LSH (SimHash) for cosine similarity.
// Each embedding is hashed to a long bitmask by checking the sign of its dot product
// against numTables random Gaussian hyperplanes. Embeddings with similar cosine angles
// tend to land in the same bucket. Hamming-1 probing widens recall at modest cost.
public class EmbeddingLSH {

    private static final int DEFAULT_NUM_TABLES = 16;
    private static final long DEFAULT_SEED = 42L;

    private final float[][] hyperplanes; // [numTables][dim]
    private final ConcurrentHashMap<Long, List<Integer>> buckets;
    // Each id's current bucket, so an update moves the id instead of leaving a copy behind.
    private final ConcurrentHashMap<Integer, Long> bucketOf = new ConcurrentHashMap<>();
    private final int numTables;

    public EmbeddingLSH(Map<Integer, float[]> embeddings) {
        this(embeddings, DEFAULT_NUM_TABLES, DEFAULT_SEED);
    }

    public EmbeddingLSH(Map<Integer, float[]> embeddings, int numTables, long seed) {
        if (embeddings.isEmpty()) throw new IllegalArgumentException("embeddings must not be empty");
        if (numTables < 1 || numTables > 63) throw new IllegalArgumentException("numTables must be 1–63");
        int dim = embeddings.values().iterator().next().length;
        this.numTables = numTables;
        this.hyperplanes = randomHyperplanes(numTables, dim, seed);
        this.buckets = buildBuckets(embeddings);
    }

    // Returns the candidate set: the exact bucket plus all Hamming-1 neighbors.
    // Probing 1-bit flips trades a modestly larger set for meaningful recall gains
    // when the dataset is small or the embedding dimension is high.
    public Set<Integer> candidates(float[] query) {
        long h = hash(query);
        Set<Integer> result = new HashSet<>(buckets.getOrDefault(h, List.of()));
        for (int i = 0; i < numTables; i++) {
            result.addAll(buckets.getOrDefault(h ^ (1L << i), List.of()));
        }
        return result;
    }

    // Add or move an embedding at runtime. Safe to call concurrently with candidates() (ConcurrentHashMap
    // and CopyOnWriteArrayList); concurrent add() calls for one id must be serialized by the caller, as
    // CandidateGenerator.updateEmbedding does. An id lives in exactly one bucket: the periodic item-embedding
    // refresh calls this for every changed vector for the pod's lifetime, and appending without leaving the
    // old bucket grew the bucket lists (and every query's candidate set) with uptime. The id joins its new
    // bucket before leaving the old one, so a concurrent query never finds it in neither.
    public void add(int id, float[] vec) {
        long h = hash(vec);
        Long previous = bucketOf.get(id);
        if (previous != null && previous == h) return;   // already in the right bucket
        buckets.computeIfAbsent(h, k -> new CopyOnWriteArrayList<>()).add(id);
        bucketOf.put(id, h);
        if (previous != null) {
            List<Integer> old = buckets.get(previous);
            if (old != null) old.remove(Integer.valueOf(id));
        }
    }

    /** Total ids across all buckets; with one entry per id this equals the number of distinct ids. */
    int bucketEntryCount() {
        return buckets.values().stream().mapToInt(List::size).sum();
    }

    long hash(float[] vec) {
        long h = 0;
        for (int i = 0; i < numTables; i++) {
            double dot = 0;
            float[] hp = hyperplanes[i];
            for (int j = 0; j < hp.length; j++) dot += hp[j] * vec[j];
            if (dot >= 0) h |= (1L << i);
        }
        return h;
    }

    private ConcurrentHashMap<Long, List<Integer>> buildBuckets(Map<Integer, float[]> embeddings) {
        ConcurrentHashMap<Long, List<Integer>> map = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, float[]> e : embeddings.entrySet()) {
            long h = hash(e.getValue());
            map.computeIfAbsent(h, k -> new CopyOnWriteArrayList<>()).add(e.getKey());
            bucketOf.put(e.getKey(), h);
        }
        return map;
    }

    private static float[][] randomHyperplanes(int numTables, int dim, long seed) {
        Random rng = new Random(seed);
        float[][] planes = new float[numTables][dim];
        for (float[] plane : planes) {
            for (int i = 0; i < dim; i++) plane[i] = (float) rng.nextGaussian();
        }
        return planes;
    }
}
