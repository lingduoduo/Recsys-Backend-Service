package com.recsys.infrastructure.vectordb.spann;

import java.io.Closeable;

/**
 * Append-only store of immutable posting blocks. The seam between {@link SpannVectorIndex} and
 * the file system; tests inject a throwing wrapper through it.
 */
public interface PostingStore extends Closeable {

    record Block(int[] ids, float[][] vectors) {
        public int count() { return ids.length; }
    }

    /** Writes the block and returns its offset. Never modifies an existing block. */
    long append(Block block);

    Block read(long offset);

    /** Bytes used so far, padding included — the denominator of the dead-bytes ratio. */
    long bytes();

    static int encodedBytes(int count, int dim) {
        return 8 + count * (4 + 4 * dim);
    }

    @Override
    void close();
}
