package com.recsys.infrastructure.vectordb;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Exhaustive Sum-of-MaxSim search over every indexed document — the multi-vector analogue
 * of {@link ExactVectorIndex}. Cost per query is {@code Σ_docs |query| × |doc| × dim}, so
 * this is a reranker over a candidate set or a small corpus, not an ANN index.
 */
public class ExactMultiVectorIndex implements MultiVectorIndex {

    /** Score ascending, then id descending — so {@code reversed()} is score desc, id asc. */
    private static final Comparator<SearchResult> WORST_FIRST =
            Comparator.comparingDouble(SearchResult::score)
                    .thenComparing(SearchResult::id, Comparator.reverseOrder());

    private final ConcurrentHashMap<Integer, float[][]> documents;

    public ExactMultiVectorIndex(Map<Integer, float[][]> documents) {
        this.documents = new ConcurrentHashMap<>(documents);
    }

    @Override
    public List<SearchResult> search(float[][] query, int k, Set<Integer> excludeIds) {
        return search(documents, query, k, excludeIds);
    }

    @Override
    public void addOrUpdate(int id, float[][] tokens) {
        documents.put(id, tokens);
    }

    @Override
    public String name() {
        return "exact-multivector";
    }

    /** One-shot search over an ad hoc candidate map, for callers that already hold the tokens. */
    public static List<SearchResult> search(Map<Integer, float[][]> documents, float[][] query,
                                            int k, Set<Integer> excludeIds) {
        if (query == null || k <= 0) return List.of();
        Set<Integer> excluded = Objects.requireNonNullElse(excludeIds, Set.of());

        // Heap head is the *worst* retained hit: lowest score, and among equal scores the
        // highest id. Ties are common when documents share tokens (co-rated bags overlap
        // heavily), and the order of equal scores must not depend on map iteration — two
        // pods emitting different bodies for the same CDN cache key would churn the edge.
        PriorityQueue<SearchResult> best = new PriorityQueue<>(WORST_FIRST);
        for (Map.Entry<Integer, float[][]> entry : documents.entrySet()) {
            int id = entry.getKey();
            if (excluded.contains(id)) continue;
            double score = MultiVectorMath.sumOfMaxSim(query, entry.getValue());
            if (score == Double.NEGATIVE_INFINITY) continue;
            SearchResult hit = new SearchResult(id, score);
            if (best.size() < k) {
                best.offer(hit);
            } else if (WORST_FIRST.compare(hit, best.peek()) > 0) {
                best.poll();
                best.offer(hit);
            }
        }

        List<SearchResult> results = new ArrayList<>(best);
        results.sort(WORST_FIRST.reversed());
        return results;
    }
}
