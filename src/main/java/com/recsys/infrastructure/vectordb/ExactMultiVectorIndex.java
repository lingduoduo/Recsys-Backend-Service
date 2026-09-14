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

        PriorityQueue<SearchResult> best = new PriorityQueue<>(Comparator.comparingDouble(SearchResult::score));
        for (Map.Entry<Integer, float[][]> entry : documents.entrySet()) {
            int id = entry.getKey();
            if (excluded.contains(id)) continue;
            double score = MultiVectorMath.sumOfMaxSim(query, entry.getValue());
            if (score == Double.NEGATIVE_INFINITY) continue;
            if (best.size() < k) {
                best.offer(new SearchResult(id, score));
            } else if (score > best.peek().score()) {
                best.poll();
                best.offer(new SearchResult(id, score));
            }
        }

        List<SearchResult> results = new ArrayList<>(best);
        results.sort(Comparator.comparingDouble(SearchResult::score).reversed());
        return results;
    }
}
