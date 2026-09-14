package com.recsys.infrastructure.vectordb;

import java.util.List;
import java.util.Set;

public interface VectorIndex {
    List<SearchResult> search(float[] query, int k, Set<Integer> excludeIds);

    String name();

    default void addOrUpdate(int id, float[] vec) {}

    /** Releases any off-heap or file-system resources. Default: nothing to release. */
    default void close() {}
}
