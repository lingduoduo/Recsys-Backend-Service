package com.recsys.infrastructure.vectordb;

import java.util.List;
import java.util.Set;

/**
 * Multi-vector counterpart of {@link VectorIndex}: a query is a bag of token vectors and
 * each indexed document is a bag of token vectors, scored by
 * {@link MultiVectorMath#sumOfMaxSim}. Results are sorted by score descending.
 */
public interface MultiVectorIndex {
    List<SearchResult> search(float[][] query, int k, Set<Integer> excludeIds);

    String name();

    default void addOrUpdate(int id, float[][] tokens) {}
}
