package com.recsys.infrastructure.vectordb;

/**
 * Multi-vector ("late interaction") scoring primitives. A query and a document are each a
 * bag of token vectors rather than one pooled vector; the score compares every query token
 * against every document token and keeps only the best match per query token.
 *
 * <p>Contract mirrors {@link VectorMath#innerProduct}: any input that cannot be scored —
 * null, empty, a null token, or a dimension mismatch on <em>any</em> pair — returns
 * {@link Double#NEGATIVE_INFINITY} so index code can skip it the same way it skips an
 * unscorable single vector. It never throws.
 */
public final class MultiVectorMath {

    private MultiVectorMath() {
    }

    /**
     * Sum of MaxSim: {@code Σ_i max_j ⟨query[i], doc[j]⟩}.
     *
     * <p>Similarity is the raw inner product, same as the single-vector path. Callers that
     * want cosine semantics (as ColBERT assumes) must L2-normalise token vectors before they
     * are stored or queried; this method does not normalise. Cost is
     * {@code |query| × |doc| × dim}.
     */
    public static double sumOfMaxSim(float[][] query, float[][] doc) {
        if (query == null || doc == null || query.length == 0 || doc.length == 0) {
            return Double.NEGATIVE_INFINITY;
        }
        double total = 0.0;
        for (float[] q : query) {
            double best = Double.NEGATIVE_INFINITY;
            for (float[] d : doc) {
                double sim = VectorMath.innerProduct(q, d);
                // innerProduct reports null / width mismatch as -inf; that poisons the whole
                // score rather than just this pair — a partial comparison would still rank.
                if (sim == Double.NEGATIVE_INFINITY) return Double.NEGATIVE_INFINITY;
                if (sim > best) best = sim;
            }
            total += best;
        }
        return total;
    }
}
