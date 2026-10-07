package com.recsys.infrastructure.vectordb;

import java.util.Collection;
import java.util.Map;
import java.util.function.IntConsumer;

/**
 * Reads item vectors for a set of ids, skipping (and reporting to {@code onCorrupt}) any value that does
 * not parse. Ids absent from the store are simply missing from the result.
 */
@FunctionalInterface
public interface ItemEmbeddingReader {
    Map<Integer, float[]> read(Collection<Integer> ids, IntConsumer onCorrupt);
}
