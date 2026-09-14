package com.recsys.infrastructure.vectordb;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class MultiVectorMathTest {

    // Each query token picks its best-matching document token; the score is the sum
    // of those per-token maxima (ColBERT-style late interaction).
    @Test
    void sumOfMaxSim_handComputedExample() {
        float[][] query = {
                {1f, 0f},   // best doc token: [1,0] -> 1.0 (vs [0,1] -> 0, [0.5,0.5] -> 0.5)
                {0f, 1f},   // best doc token: [0,1] -> 1.0
                {1f, 1f},   // best doc token: [1,0] or [0,1] -> 1.0 (vs [0.5,0.5] -> 1.0 tie)
        };
        float[][] doc = {{1f, 0f}, {0f, 1f}, {0.5f, 0.5f}};
        assertThat(MultiVectorMath.sumOfMaxSim(query, doc)).isCloseTo(3.0, within(1e-9));
    }

    @Test
    void sumOfMaxSim_singleTokenEachSide_equalsInnerProduct() {
        float[] q = {1f, 2f, 3f, 4f, 5f};
        float[] d = {5f, 4f, 3f, 2f, 1f};
        assertThat(MultiVectorMath.sumOfMaxSim(new float[][]{q}, new float[][]{d}))
                .isCloseTo(VectorMath.innerProduct(q, d), within(1e-9));
    }

    @Test
    void sumOfMaxSim_takesMaxNotSum_overDocumentTokens() {
        // A single query token against two doc tokens scoring 2 and 3 must yield 3, not 5.
        float[][] query = {{1f, 0f}};
        float[][] doc = {{2f, 0f}, {3f, 0f}};
        assertThat(MultiVectorMath.sumOfMaxSim(query, doc)).isCloseTo(3.0, within(1e-9));
    }

    @Test
    void sumOfMaxSim_invariantToDocumentTokenOrder() {
        float[][] query = {{1f, 0f}, {0f, 1f}};
        float[][] doc = {{0.9f, 0.1f}, {0.2f, 0.8f}, {0.5f, 0.5f}};
        float[][] shuffled = {{0.5f, 0.5f}, {0.2f, 0.8f}, {0.9f, 0.1f}};
        assertThat(MultiVectorMath.sumOfMaxSim(query, doc))
                .isCloseTo(MultiVectorMath.sumOfMaxSim(query, shuffled), within(1e-9));
    }

    @Test
    void sumOfMaxSim_negativeSimilaritiesStillPickTheLargest() {
        // Max of {-3, -1} is -1, and a negative contribution must not be clamped to zero.
        float[][] query = {{1f, 0f}};
        float[][] doc = {{-3f, 0f}, {-1f, 0f}};
        assertThat(MultiVectorMath.sumOfMaxSim(query, doc)).isCloseTo(-1.0, within(1e-9));
    }

    @Test
    void sumOfMaxSim_nullQuery_returnsNegativeInfinity() {
        assertThat(MultiVectorMath.sumOfMaxSim(null, new float[][]{{1f}}))
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void sumOfMaxSim_nullDoc_returnsNegativeInfinity() {
        assertThat(MultiVectorMath.sumOfMaxSim(new float[][]{{1f}}, null))
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void sumOfMaxSim_emptyQuery_returnsNegativeInfinity() {
        // No query tokens means no evidence; -inf makes the index skip it, the same way a
        // null single vector is skipped, rather than reporting a misleading 0.
        assertThat(MultiVectorMath.sumOfMaxSim(new float[0][], new float[][]{{1f}}))
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void sumOfMaxSim_emptyDoc_returnsNegativeInfinity() {
        assertThat(MultiVectorMath.sumOfMaxSim(new float[][]{{1f}}, new float[0][]))
                .isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void sumOfMaxSim_dimensionMismatchOnAnyPair_returnsNegativeInfinity() {
        // The second doc token has the wrong width; the whole score is invalid, not just
        // that pair, because a partial comparison would silently rank the doc.
        float[][] query = {{1f, 0f}};
        float[][] doc = {{1f, 0f}, {1f}};
        assertThat(MultiVectorMath.sumOfMaxSim(query, doc)).isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    void sumOfMaxSim_nullTokenInsideDoc_returnsNegativeInfinity() {
        float[][] query = {{1f, 0f}};
        float[][] doc = {{1f, 0f}, null};
        assertThat(MultiVectorMath.sumOfMaxSim(query, doc)).isEqualTo(Double.NEGATIVE_INFINITY);
    }
}
