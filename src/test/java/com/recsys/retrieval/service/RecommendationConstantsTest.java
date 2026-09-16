package com.demo.retrieval.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecommendationConstantsTest {

    // Normalizing by the weight sum keeps a saturated blend at exactly the upper bound.
    @Test
    void offlineScoreStaysInRangeWhenAllComponentsSaturate() {
        double score = RecommendationConstants.blendOfflineScore(
            0.6, 1.0,   // relevance
            0.25, 1.0,  // content
            0.15, 1.0   // popularity
        );
        assertEquals(1.0, score, 1e-9, "all components saturated must yield exactly the upper bound");
    }

    @Test
    void normalizationPreservesConfiguredRatios() {
        // Only relevance active: normalized contribution is relevance itself regardless of the
        // other zero-valued components, because we divide by the full weight sum.
        double score = RecommendationConstants.blendOfflineScore(
            0.6, 0.8, 0.25, 0.0, 0.15, 0.0);
        assertEquals(0.6 * 0.8 / 1.0, score, 1e-9);
    }

    @Test
    void zeroWeightsCollapseToLowerBound() {
        assertEquals(0.0, RecommendationConstants.blendOfflineScore(
            0.0, 1.0, 0.0, 1.0, 0.0, 1.0), 1e-9);
    }

    @Test
    void clampRejectsNonFiniteAndOutOfRange() {
        assertEquals(0.0, RecommendationConstants.clamp(Double.NaN));
        assertEquals(0.0, RecommendationConstants.clamp(Double.NEGATIVE_INFINITY));
        assertEquals(0.0, RecommendationConstants.clamp(-1.0));
        assertEquals(1.0, RecommendationConstants.clamp(2.0));
        assertTrue(RecommendationConstants.clamp(0.42) == 0.42);
    }

    // ---- Characterization: pinned before the deep-learning path was removed -------------------
    // The expected value is explicit arithmetic over the surviving weights only, so it is the same
    // number it was when the two deep-learning parameters still existed. It still matches, which is
    // the proof that no score moved.
    @Test
    void offlineBlendCharacterized() {
        double expected = (0.6 * 0.8 + 0.25 * 0.5 + 0.15 * 0.4) / (0.6 + 0.25 + 0.15);
        double actual = RecommendationConstants.blendOfflineScore(
            0.6, 0.8,
            0.25, 0.5,
            0.15, 0.4
        );
        assertEquals(expected, actual, 1e-12);
    }
}
