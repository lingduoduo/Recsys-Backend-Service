package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;

import java.util.StringJoiner;

/**
 * The GRPO policy's feature vector — one definition, read by the serving-side emitter and by the
 * shadow scorer, so a weight vector fit in Scala is never applied to a different layout here.
 *
 * Every value is already computed during scoring; building this vector adds no model call to the
 * request path. The version prefix travels on the wire because weights and features must agree:
 * a consumer that reads an unknown version drops the row rather than misaligning them.
 */
public final class GrpoFeatures {

    public static final int DIM = 9;
    public static final String VERSION = "v2";

    private GrpoFeatures() {}

    public static double[] of(ServedMovie movie) {
        return of(movie.banditScore(), movie.estimatedReward(), movie.onlineScore(),
            movie.explorationBonus(), movie.coldStart(), movie.impressions(), movie.clicks());
    }

    /**
     * Same layout as {@link #of(ServedMovie)}, taken as primitives so a caller with no
     * {@code ServedMovie} on hand -- the re-rank site in HybridRecommendationService has a
     * private {@code ScoredCandidate} instead -- can still build the one true feature vector
     * rather than a second, potentially-drifting implementation of this contract.
     */
    public static double[] of(
        double banditScore,
        double estimatedReward,
        double onlineScore,
        double explorationBonus,
        boolean coldStart,
        long impressions,
        long clicks
    ) {
        return new double[] {
            1.0,                                    // 0 bias
            banditScore,                             // 1
            estimatedReward,                         // 2
            onlineScore,                              // 3
            explorationBonus,                         // 4
            coldStart ? 1.0 : 0.0,                    // 5
            Math.log1p(impressions),                  // 6
            Math.log1p(clicks),                       // 7
            clicks / (double) (impressions + 1)       // 8 smoothed CTR
        };
    }

    public static String pack(double[] x) {
        StringJoiner joiner = new StringJoiner(",");
        for (double value : x) {
            joiner.add(Double.toString(value));
        }
        return VERSION + ":" + joiner;
    }

    /** The behavior policy's logit. Falls back to banditScore, which predictionScore derives from. */
    public static double predictionScore(ServedMovie movie) {
        Object raw = movie.modelPredictions().get("predictionScore");
        return raw instanceof Number number ? number.doubleValue() : movie.banditScore();
    }
}
