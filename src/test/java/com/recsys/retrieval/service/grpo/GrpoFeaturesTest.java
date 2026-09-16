package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpoFeaturesTest {

    private ServedMovie movie(double banditScore, long impressions, long clicks, Map<String, Object> predictions) {
        return new ServedMovie("m1", 0.4, 0.3, 0.05, banditScore, false, impressions, clicks, predictions);
    }

    @Test
    void producesExactlyDimValues() {
        double[] x = GrpoFeatures.of(movie(0.7, 10, 2, Map.of()));
        assertEquals(GrpoFeatures.DIM, x.length);
    }

    @Test
    void firstDimensionIsTheBiasTerm() {
        double[] x = GrpoFeatures.of(movie(0.7, 10, 2, Map.of()));
        assertEquals(1.0, x[0]);
    }

    @Test
    void countsEnterLogarithmicallySoAPopularItemDoesNotDominate() {
        double[] few = GrpoFeatures.of(movie(0.5, 10, 0, Map.of()));
        double[] many = GrpoFeatures.of(movie(0.5, 100_000, 0, Map.of()));
        // log1p keeps a 10,000x impression gap inside one order of magnitude.
        assertTrue(many[6] < 5.0 * few[6], "impressions must be compressed, got " + many[6] + " vs " + few[6]);
    }

    @Test
    void packCarriesTheVersionPrefix() {
        String packed = GrpoFeatures.pack(new double[] {1.0, 0.5});
        assertTrue(packed.startsWith(GrpoFeatures.VERSION + ":"), packed);
        assertEquals("v2:1.0,0.5", packed);
    }

    @Test
    void predictionScoreComesFromTheModelPredictionsMap() {
        assertEquals(0.83, GrpoFeatures.predictionScore(movie(0.5, 0, 0, Map.of("predictionScore", 0.83))));
    }

    @Test
    void predictionScoreFallsBackToBanditScoreWhenAbsent() {
        // An absent key must not silently become 0.0: a zero logit is a real, wrong policy claim,
        // whereas banditScore is the score predictionScore is derived from.
        assertEquals(0.61, GrpoFeatures.predictionScore(movie(0.61, 0, 0, Map.of())));
    }

    @Test
    void servedMovieOverloadAndPrimitiveOverloadAgreeOnTheSameInputs() {
        // The re-rank site (HybridRecommendationService) has a ScoredCandidate, not a ServedMovie,
        // so it must call the primitive overload. The two must never drift, or a weight vector
        // fit against one layout would silently misalign against the other.
        ServedMovie m = movie(0.7, 10, 2, Map.of());
        double[] viaServedMovie = GrpoFeatures.of(m);
        double[] viaPrimitives = GrpoFeatures.of(
            m.banditScore(), m.estimatedReward(), m.onlineScore(),
            m.explorationBonus(), m.coldStart(), m.impressions(), m.clicks());
        assertArrayEquals(viaServedMovie, viaPrimitives, 1e-12);
    }

    @Test
    void theTwoOverloadsAgreeAfterTheSameRoundingTheRealCallSitesApply() {
        // HybridRecommendationService.toServedMovie rounds estimatedReward/onlineScore/
        // explorationBonus/banditScore to 3dp before constructing a ServedMovie, and
        // GrpoImpressionEvents packs training's grpo_x from that rounded ServedMovie. The re-rank
        // site must round the same way before calling the primitive overload, or serving scores
        // unrounded values while training was fit on rounded ones -- precisely the mismatch v2 was
        // built to remove. Unlike the identical-inputs test above, this one starts from unrounded
        // inputs and rounds on both sides, so a future divergence in rounding (or a dropped round()
        // call on either path) fails here even though the plain identical-inputs test would not.
        double banditScore = 0.123456;
        double estimatedReward = 0.654321;
        double onlineScore = 0.333333;
        double explorationBonus = 0.070707;
        long impressions = 42;
        long clicks = 7;

        ServedMovie roundedMovie = new ServedMovie(
            "m1", round(estimatedReward), round(onlineScore), round(explorationBonus),
            round(banditScore), false, impressions, clicks, Map.of());
        double[] viaServedMovie = GrpoFeatures.of(roundedMovie);
        double[] viaPrimitives = GrpoFeatures.of(
            round(banditScore), round(estimatedReward), round(onlineScore),
            round(explorationBonus), false, impressions, clicks);
        assertArrayEquals(viaServedMovie, viaPrimitives, 1e-12);
    }

    private static double round(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }
}
