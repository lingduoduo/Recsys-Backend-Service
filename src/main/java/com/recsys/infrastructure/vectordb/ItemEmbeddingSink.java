package com.recsys.infrastructure.vectordb;

/** Something an item-embedding refresh pushes a changed vector into (a recall index, a heap cache). */
@FunctionalInterface
public interface ItemEmbeddingSink {
    void apply(int id, float[] vector);
}
