package com.demo.retrieval.service;

/**
 * Single source of truth for the scoring cutoffs that were previously scattered as magic
 * numbers across the ranking pipeline. Values in {@code application.yml} (bandit exploration,
 * reward-model, Q-learning) remain the operator-tunable surface; the constants here are the
 * in-code cutoffs that shape the {@code offlineScore -> learnedPrior -> banditScore} chain and
 * must keep every intermediate score inside the {@code [0, 1]} range the rest of the pipeline
 * (Beta prior, reward blend, outcome scorer) assumes.
 */
public final class RecommendationConstants {

    private RecommendationConstants() {
    }

    /** Lower/upper bound every intermediate score is clamped to. */
    public static final double SCORE_LOWER_BOUND = 0.0;
    public static final double SCORE_UPPER_BOUND = 1.0;

    /**
     * Pseudo-count strength of the Beta prior built from {@code learnedPrior}. With strength 2.0
     * the prior mean is {@code (1 + 2p) / 4}, i.e. the offline signal is shrunk toward 0.5 and a
     * handful of impressions quickly dominate. Raise it to make the offline prior persist longer.
     */
    public static final double WARM_PRIOR_STRENGTH = 2.0;

    /** Multiplier applied to {@code coldStartBoost} when deriving the cold-start prior strength. */
    public static final double COLD_START_PRIOR_STRENGTH_MULTIPLIER = 4.0;

    /** Split of the content score between genre and tag Jaccard overlap (must sum to 1.0). */
    public static final double CONTENT_GENRE_WEIGHT = 0.7;
    public static final double CONTENT_TAG_WEIGHT = 0.3;

    /** Clamp to {@code [SCORE_LOWER_BOUND, SCORE_UPPER_BOUND]}; NaN/Infinity collapse to the lower bound. */
    public static double clamp(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return SCORE_LOWER_BOUND;
        }
        return Math.max(SCORE_LOWER_BOUND, Math.min(SCORE_UPPER_BOUND, value));
    }

    /**
     * Blend the three offline components into a proper convex combination in {@code [0, 1]}.
     *
     * <p>The configured weights do not necessarily sum to 1.0, so a raw weighted sum can exceed 1.0
     * and be silently clipped downstream, flattening the top of the score range. Normalizing by the
     * weight sum keeps the configured ratios while guaranteeing the result stays in range.
     *
     * @return the normalized offline score, or {@link #SCORE_LOWER_BOUND} when all weights are zero.
     */
    public static double blendOfflineScore(
        double relevanceWeight, double relevance,
        double contentWeight, double content,
        double popularityWeight, double popularity
    ) {
        double weightSum = relevanceWeight + contentWeight + popularityWeight;
        if (weightSum <= 0.0) {
            return SCORE_LOWER_BOUND;
        }
        double weighted = (relevanceWeight * clamp(relevance))
            + (contentWeight * clamp(content))
            + (popularityWeight * clamp(popularity));
        return clamp(weighted / weightSum);
    }
}
