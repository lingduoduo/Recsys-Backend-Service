package com.recsys.retrieval.service.grpo;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.recsys.retrieval.config.RecommendationProperties;
import com.recsys.retrieval.model.FeatureCache;
import com.recsys.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.recsys.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "rawtypes"})
class GrpoPolicyScorerTest {

    private final ServedMovie movie =
        new ServedMovie("m1", 0.4, 0.3, 0.05, 0.7, false, 10, 2, Map.of("predictionScore", 0.7));

    private StringRedisTemplate lastRedis;

    private GrpoPolicyScorer scorer(String mode, String weights, String version) {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getGrpo().setMode(mode);
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations hashOps = mock(HashOperations.class);
        when(redis.opsForHash()).thenReturn(hashOps);
        when(hashOps.entries(anyString())).thenReturn(weights == null ? Map.of()
            : Map.of("weights", weights, "feature_version", version, "dim", "9"));
        lastRedis = redis;
        return new GrpoPolicyScorer(redis, properties, new FeatureCache(properties));
    }

    /**
     * Distinct weights, not all-ones: with a uniform vector the dot product collapses to the sum of
     * the features and any permutation of weight-to-feature indices scores identically, so the test
     * could not see a misaligned loop.
     */
    private String distinctWeights() {
        return java.util.stream.IntStream.rangeClosed(1, GrpoFeatures.DIM)
            .mapToObj(i -> Double.toString((double) i))
            .collect(Collectors.joining(","));
    }

    /** Isolates feature 1 (banditScore) so a slate's GRPO order is exactly its banditScore order. */
    private String weightsOnBanditScoreOnly() {
        String[] w = new String[GrpoFeatures.DIM];
        java.util.Arrays.fill(w, "0.0");
        w[1] = "1.0";
        return String.join(",", w);
    }

    private ServingSideEffectRequest slate(List<ServedMovie> selected) {
        return new ServingSideEffectRequest(
            "req-1", "u1", "ucb", Map.of(), selected, selected,
            List.of(), List.of(), 0, 0, selected.size(), 0.0, 0.0, 0.0);
    }

    private ServedMovie itemWithBanditScore(String id, double banditScore) {
        return new ServedMovie(id, 0.0, 0.0, 0.0, banditScore, false, 0, 0, Map.of());
    }

    /**
     * Feature vectors with every dimension zero except index 1 (banditScore), so that under
     * {@link #weightsOnBanditScoreOnly()} the raw w·x for each candidate is exactly the given
     * value and hand computation of the min-max normalization is trivial.
     */
    private List<double[]> vectorsIsolatingBanditScore(double... values) {
        List<double[]> vectors = new java.util.ArrayList<>();
        for (double value : values) {
            double[] x = new double[GrpoFeatures.DIM];
            x[1] = value;
            vectors.add(x);
        }
        return vectors;
    }

    /** Captures what GrpoPolicyScorer logs while the block runs. */
    private List<String> capturingLogs(Consumer<Void> block) {
        ch.qos.logback.classic.Logger logger =
            (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(GrpoPolicyScorer.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            block.accept(null);
        } finally {
            logger.detachAppender(appender);
        }
        return appender.list.stream()
            .filter(e -> e.getLevel() == Level.INFO)
            .map(ILoggingEvent::getFormattedMessage)
            .toList();
    }

    /** A two-item slate: the smallest one recordShadowSlate will log. */
    private ServingSideEffectRequest twoItemSlate() {
        return slate(List.of(itemWithBanditScore("m1", 0.9), itemWithBanditScore("m2", 0.1)));
    }

    /** The weight-guard contract: unusable weights mean no shadow line, never an exception. */
    private void assertLogsNoShadowLine(GrpoPolicyScorer s) {
        assertTrue(capturingLogs(ignored -> s.recordShadowSlate(twoItemSlate())).isEmpty());
    }

    @Test
    void offModeClaimsNoBlendWeight() {
        assertEquals(0.0, scorer("off", distinctWeights(), "v2").blendWeight());
    }

    @Test
    void offModeReadsNothingFromRedis() {
        // The rollout's first promise: with the flag off, the serving path pays nothing at all.
        GrpoPolicyScorer s = scorer("off", distinctWeights(), "v2");
        s.recordShadowSlate(twoItemSlate());
        verify(lastRedis, never()).opsForHash();
    }

    @Test
    void shadowModeReadsWeightsButClaimsNoBlendWeight() {
        GrpoPolicyScorer s = scorer("shadow", distinctWeights(), "v2");
        assertEquals(0.0, s.blendWeight(), "shadow must never move a recommendation");
        s.recordShadowSlate(twoItemSlate());
        verify(lastRedis).opsForHash();
    }

    @Test
    void onModeClaimsOnlyTheUnclaimedBlendWeight() {
        GrpoPolicyScorer s = scorer("on", distinctWeights(), "v2");
        assertEquals(GrpoPolicyScorer.ON_BLEND_WEIGHT, s.blendWeight());
        // MovieLensOutcomeScorer's weights sum to 0.85; taking more than 0.15 would silently
        // change every existing score.
        assertTrue(s.blendWeight() <= 0.15);
    }

    @Test
    void anUnrecognisedModeIsTreatedAsOff() {
        GrpoPolicyScorer s = scorer("enabled", distinctWeights(), "v2");
        assertEquals(0.0, s.blendWeight());
        s.recordShadowSlate(twoItemSlate());
        verify(lastRedis, never()).opsForHash();
    }

    @Test
    void weightsOfADifferentFeatureVersionAreIgnored() {
        // The exact cutover hazard: v1 weights left behind in Redis after the v2 rollout must be
        // refused, not silently applied against the v2 (9-wide) feature layout.
        assertLogsNoShadowLine(scorer("shadow", distinctWeights(), "v1"));
    }

    @Test
    void aV1WidthWeightVectorIsIgnoredEvenWhenTaggedV2() {
        // Belt and suspenders on the same cutover hazard: even if the version tag were (wrongly)
        // left as v2, a stale 10-wide v1 vector must not be applied against 9 v2 features.
        String tenWide = java.util.stream.IntStream.rangeClosed(1, 10)
            .mapToObj(i -> Double.toString((double) i))
            .collect(Collectors.joining(","));
        assertLogsNoShadowLine(scorer("shadow", tenWide, "v2"));
    }

    @Test
    void aMissingWeightVectorLogsNothingRatherThanFailing() {
        assertLogsNoShadowLine(scorer("shadow", null, "v2"));
    }

    @Test
    void aNonFiniteWeightIsUnusable() {
        // Double.parseDouble accepts "NaN": without the finite check every score would be NaN,
        // which sorts unpredictably instead of degrading to the no-weights case.
        String withNaN = "NaN," + String.join(",", java.util.Collections.nCopies(GrpoFeatures.DIM - 1, "1.0"));
        assertLogsNoShadowLine(scorer("shadow", withNaN, "v2"));
        String withInf = "Infinity," + String.join(",", java.util.Collections.nCopies(GrpoFeatures.DIM - 1, "1.0"));
        assertLogsNoShadowLine(scorer("shadow", withInf, "v2"));
    }

    @Test
    void weightsAreReadFromRedisOncePerTtlWindow() {
        // The vector changes once per training micro-batch; re-reading it on every slate is what
        // the feature cache exists to stop.
        GrpoPolicyScorer s = scorer("shadow", weightsOnBanditScoreOnly(), "v2");
        s.recordShadowSlate(twoItemSlate());
        s.recordShadowSlate(twoItemSlate());
        verify(lastRedis, times(1)).opsForHash();
    }

    @Test
    void anAbsentVectorIsNotReReadWithinTheTtl() {
        GrpoPolicyScorer s = scorer("shadow", null, "v2");
        s.recordShadowSlate(twoItemSlate());
        s.recordShadowSlate(twoItemSlate());
        verify(lastRedis, times(1)).opsForHash();
    }

    @Test
    void pairwiseConcordanceIsOneWhenTheScoresReproduceTheServedOrder() {
        assertEquals(1.0, GrpoPolicyScorer.pairwiseConcordance(new double[] {3.0, 2.0, 1.0}), 1e-9);
    }

    @Test
    void pairwiseConcordanceIsZeroOnAReversedOrder() {
        assertEquals(0.0, GrpoPolicyScorer.pairwiseConcordance(new double[] {1.0, 2.0, 3.0}), 1e-9);
    }

    @Test
    void pairwiseConcordanceCountsOnlyStrictlyOrderedPairs() {
        // One of three pairs agrees: (0,1) ties, (0,2) and (1,2) — 2.0 > 1.0 for both.
        assertEquals(2.0 / 3.0, GrpoPolicyScorer.pairwiseConcordance(new double[] {2.0, 2.0, 1.0}), 1e-9);
        // A tie is not agreement: the served order picked a winner and the policy did not.
        assertEquals(0.0, GrpoPolicyScorer.pairwiseConcordance(new double[] {1.0, 1.0}), 1e-9);
    }

    @Test
    void shadowModeLogsOneLinePerSlate() {
        GrpoPolicyScorer s = scorer("shadow", weightsOnBanditScoreOnly(), "v2");
        // Descending banditScore under a banditScore-only weight vector: the GRPO order reproduces
        // the served order exactly, so concordance is 1.0.
        List<String> lines = capturingLogs(ignored -> s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.9),
            itemWithBanditScore("m2", 0.5),
            itemWithBanditScore("m3", 0.1)))));
        assertEquals(1, lines.size(), "one line per slate, not one per item");
        assertTrue(lines.get(0).contains("requestId=req-1"), lines.get(0));
        assertTrue(lines.get(0).contains("slateSize=3"), lines.get(0));
        assertTrue(lines.get(0).contains("pairwiseConcordance=1.0"), lines.get(0));
    }

    @Test
    void shadowModeLogsADisagreeingSlateAsSuch() {
        GrpoPolicyScorer s = scorer("shadow", weightsOnBanditScoreOnly(), "v2");
        List<String> lines = capturingLogs(ignored -> s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.1),
            itemWithBanditScore("m2", 0.9)))));
        assertTrue(lines.get(0).contains("pairwiseConcordance=0.0"), lines.get(0));
    }

    @Test
    void offModeLogsNothing() {
        GrpoPolicyScorer s = scorer("off", distinctWeights(), "v2");
        assertTrue(capturingLogs(ignored -> s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.9),
            itemWithBanditScore("m2", 0.1))))).isEmpty());
    }

    @Test
    void onModeLogsNothing() {
        // In `on` the score already drives the order it would be graded against.
        GrpoPolicyScorer s = scorer("on", distinctWeights(), "v2");
        assertTrue(capturingLogs(ignored -> s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.9),
            itemWithBanditScore("m2", 0.1))))).isEmpty());
    }

    @Test
    void aSlateWithNoUsableWeightsLogsNothing() {
        GrpoPolicyScorer s = scorer("shadow", null, "v2");
        assertTrue(capturingLogs(ignored -> s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.9),
            itemWithBanditScore("m2", 0.1))))).isEmpty());
    }

    @Test
    void aSingleItemSlateLogsNothing() {
        GrpoPolicyScorer s = scorer("shadow", weightsOnBanditScoreOnly(), "v2");
        assertTrue(capturingLogs(ignored ->
            s.recordShadowSlate(slate(List.of(itemWithBanditScore("m1", 0.9))))).isEmpty());
    }

    @Test
    void reRankOrderChangesWhenGrpoDisagreesWithTheIncumbentOrder() {
        // Incumbent (finalScores) narrowly favors candidate 0 (0.52 > 0.50). GRPO's raw score
        // favors candidate 1 so strongly that even the 0.10 blend weight flips the order.
        GrpoPolicyScorer s = scorer("on", weightsOnBanditScoreOnly(), "v2");
        List<double[]> vectors = vectorsIsolatingBanditScore(10.0, 30.0);
        Optional<int[]> order = s.reRankOrder(vectors, new double[] {0.52, 0.50});
        assertTrue(order.isPresent());
        assertArrayEquals(new int[] {1, 0}, order.get());
    }

    @Test
    void reRankOrderIsEmptyInOffMode() {
        GrpoPolicyScorer s = scorer("off", weightsOnBanditScoreOnly(), "v2");
        List<double[]> vectors = vectorsIsolatingBanditScore(10.0, 30.0);
        assertTrue(s.reRankOrder(vectors, new double[] {0.52, 0.50}).isEmpty());
    }

    @Test
    void reRankOrderIsEmptyInShadowMode() {
        // Shadow must never move an order, even when it would compute a different one.
        GrpoPolicyScorer s = scorer("shadow", weightsOnBanditScoreOnly(), "v2");
        List<double[]> vectors = vectorsIsolatingBanditScore(10.0, 30.0);
        assertTrue(s.reRankOrder(vectors, new double[] {0.52, 0.50}).isEmpty());
    }

    @Test
    void offAndShadowModeReturnTheExactIncumbentOrderNotJustAnEmptyOptional() {
        // Reproduces exactly what HybridRecommendationService.applyGrpoReRank does with the
        // Optional<int[]> contract: when reRankOrder is empty the caller keeps its own list as-is.
        // The same vectors and finalScores are used as the "changes order" test above -- if this
        // guard against emptiness were ever weakened to a permutation, the resulting order would
        // visibly differ from incumbentOrder here, not merely throw.
        List<String> incumbentOrder = List.of("a", "b");
        List<double[]> vectors = vectorsIsolatingBanditScore(10.0, 30.0);
        double[] finalScores = {0.52, 0.50};

        for (String mode : List.of("off", "shadow")) {
            GrpoPolicyScorer s = scorer(mode, weightsOnBanditScoreOnly(), "v2");
            Optional<int[]> reRankOrder = s.reRankOrder(vectors, finalScores);
            List<String> served = reRankOrder
                .map(order -> java.util.Arrays.stream(order).mapToObj(incumbentOrder::get).toList())
                .orElse(incumbentOrder);
            assertEquals(incumbentOrder, served, "mode=" + mode);
        }
    }

    @Test
    void reRankOrderIsEmptyWithFewerThanTwoCandidates() {
        GrpoPolicyScorer s = scorer("on", weightsOnBanditScoreOnly(), "v2");
        assertTrue(s.reRankOrder(vectorsIsolatingBanditScore(10.0), new double[] {0.5}).isEmpty());
    }

    @Test
    void reRankOrderIsEmptyWhenWeightsAreMissing() {
        GrpoPolicyScorer s = scorer("on", null, "v2");
        List<double[]> vectors = vectorsIsolatingBanditScore(10.0, 30.0);
        assertTrue(s.reRankOrder(vectors, new double[] {0.52, 0.50}).isEmpty());
    }

    @Test
    void reRankOrderIsEmptyWhenFeatureVersionMismatches() {
        GrpoPolicyScorer s = scorer("on", weightsOnBanditScoreOnly(), "v1");
        List<double[]> vectors = vectorsIsolatingBanditScore(10.0, 30.0);
        assertTrue(s.reRankOrder(vectors, new double[] {0.52, 0.50}).isEmpty());
    }

    @Test
    void reRankOrderIsEmptyWhenEveryDotProductTies() {
        // Min-max normalization is degenerate (0/0) when every candidate scores the same: there is
        // no preference for GRPO to express, so it must not manufacture an arbitrary tie-break.
        GrpoPolicyScorer s = scorer("on", weightsOnBanditScoreOnly(), "v2");
        List<double[]> vectors = vectorsIsolatingBanditScore(20.0, 20.0, 20.0);
        assertTrue(s.reRankOrder(vectors, new double[] {0.9, 0.5, 0.1}).isEmpty());
    }

    @Test
    void reRankOrderBlendArithmeticMatchesAHandComputedExpectation() {
        // w isolates feature index 1, so raw w·x is exactly that feature: 30, 10, 20 for
        // candidates 0, 1, 2. min=10, max=30, range=20, so:
        //   norm0 = (30-10)/20 = 1.00   norm1 = (10-10)/20 = 0.00   norm2 = (20-10)/20 = 0.50
        // ON_BLEND_WEIGHT = 0.10, blended against finalScores {0.50, 0.52, 0.10}:
        //   adjusted0 = 0.9*0.50 + 0.1*1.00 = 0.45 + 0.10 = 0.55
        //   adjusted1 = 0.9*0.52 + 0.1*0.00 = 0.468
        //   adjusted2 = 0.9*0.10 + 0.1*0.50 = 0.09 + 0.05 = 0.14
        // Descending adjusted order is candidate 0, then 1, then 2 -- note the incumbent order by
        // finalScores alone would have been {1, 0, 2}.
        GrpoPolicyScorer s = scorer("on", weightsOnBanditScoreOnly(), "v2");
        List<double[]> vectors = vectorsIsolatingBanditScore(30.0, 10.0, 20.0);
        Optional<int[]> order = s.reRankOrder(vectors, new double[] {0.50, 0.52, 0.10});
        assertTrue(order.isPresent());
        assertArrayEquals(new int[] {0, 1, 2}, order.get());
    }

    @Test
    void reRankOrderNeverThrowsWhenRedisFails() {
        // Same contract as recordShadowSlate: an operator error in Redis (e.g. `SET
        // grpo:policy:weights <string>` leaving the key the wrong type) must degrade to "don't
        // re-rank," not fail every request while every other Redis read on the path succeeds.
        RecommendationProperties properties = new RecommendationProperties();
        properties.getGrpo().setMode("on");
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForHash()).thenThrow(new IllegalStateException("WRONGTYPE"));
        GrpoPolicyScorer s = new GrpoPolicyScorer(redis, properties, new FeatureCache(properties));
        List<double[]> vectors = vectorsIsolatingBanditScore(10.0, 30.0);
        assertTrue(s.reRankOrder(vectors, new double[] {0.52, 0.50}).isEmpty());
    }

    @Test
    void aFailingSlateNeverThrowsIntoTheServingPath() {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getGrpo().setMode("shadow");
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForHash()).thenThrow(new IllegalStateException("redis down"));
        GrpoPolicyScorer s = new GrpoPolicyScorer(redis, properties, new FeatureCache(properties));
        s.recordShadowSlate(slate(List.of(
            itemWithBanditScore("m1", 0.9), itemWithBanditScore("m2", 0.1))));
    }
}
