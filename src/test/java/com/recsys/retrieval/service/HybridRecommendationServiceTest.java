package com.demo.retrieval.service;

import com.demo.retrieval.event.RecsysEventAvroCodec;
import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.measurement.RecommendationMeasurementService;
import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.RecommendationResult;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.clients.UserMovieHistoryClient.UserMovieHistory;
import com.demo.retrieval.service.clients.MovieLensFeatureClient;
import com.demo.retrieval.service.clients.UserProfileClient;
import com.demo.retrieval.service.grpo.GrpoFeatures;
import com.demo.retrieval.service.grpo.GrpoPolicyScorer;
import com.demo.retrieval.service.query_hydrators.MovieLensUserHistoryQueryHydrator;
import com.demo.retrieval.service.query_hydrators.QueryHydrator;
import com.demo.retrieval.service.query_hydrators.UserBehaviorProfileQueryHydrator;
import com.demo.retrieval.service.query_hydrators.UserMovieFeaturesQueryHydrator;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

@SuppressWarnings({"unchecked", "null"})
class HybridRecommendationServiceTest {
    @Test
    void recommendsFreshMoviesFromMovieLensBehaviorSignals() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        ListOperations<String, String> lists = mock(ListOperations.class);
        SetOperations<String, String> sets = mock(SetOperations.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        ZSetOperations<String, String> sortedSets = mock(ZSetOperations.class);
        when(redis.opsForHash()).thenReturn(hashes);
        when(redis.opsForList()).thenReturn(lists);
        when(redis.opsForSet()).thenReturn(sets);
        when(redis.opsForValue()).thenReturn(values);
        when(redis.opsForZSet()).thenReturn(sortedSets);
        when(sets.size(any())).thenReturn(1L);

        when(sortedSets.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                ZSetOperations.TypedTuple.of("watched", 100.0),
                ZSetOperations.TypedTuple.of("rated", 90.0),
                ZSetOperations.TypedTuple.of("fresh", 80.0),
                ZSetOperations.TypedTuple.of("unclassified", 1.0)
            )));
        when(values.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> {
                if (key.startsWith("i2vEmb:")) {
                    return "1.0 0.0";
                }
                return "0";
            }).toList();
        });

        RecommendationProperties properties = new RecommendationProperties();
        properties.getCandidateGeneration().setColdStartPoolSize(1);
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("watched", movie("drama"));
        catalog.put("rated", movie("comedy"));
        catalog.put("fresh", movie("sci-fi"));
        properties.setCatalog(catalog);

        FeatureCache featureCache = new FeatureCache(properties);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        HybridRecommendationService service = new HybridRecommendationService(
            redis,
            properties,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache,
            List.of(new MovieLensUserHistoryQueryHydrator(
                userId -> new UserMovieHistory(List.of("watched"), List.of("rated"))
            )),
            new RecommendationMeasurementService(meterRegistry, properties, featureCache)
        );

        RecommendationResult result = service.recommend("u1", 1);

        assertEquals(List.of("fresh"), result.recommendations());
        assertFalse(result.recommendations().contains("watched"));
        assertFalse(result.recommendations().contains("rated"));
        assertEquals(1.0, meterRegistry.find("recommendation.filter.decisions")
            .tag("reason", "unknown").counter().count());
        for (String stage : List.of("hydration", "redis_fetch", "scoring", "selection", "side_effects")) {
            assertEquals(1L, meterRegistry.find("recommendation.stage.latency").tag("stage", stage).timer().count());
        }
    }

    @Test
    void deriveTasteProfilePullsGenresFromSeedItemsCatalog() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        RecommendationProperties properties = new RecommendationProperties();
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("watched", movie("drama"));
        properties.setCatalog(catalog);
        FeatureCache featureCache = new FeatureCache(properties);
        HybridRecommendationService service = new HybridRecommendationService(
            redis, properties,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache, List.of());

        HybridRecommendationService.TasteProfile profile = service.deriveTasteProfile(
            List.of("watched"), List.of(), MovieLensUserFeatures.forUser("u1"), List.of());

        assertTrue(profile.genres().containsKey("drama"));
    }

    @Test
    void deriveTasteProfileKeepsExplicitAndSeedWeightsAheadOfBehavioralPreferences() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        RecommendationProperties properties = new RecommendationProperties();
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("watched", movie("sci-fi"));
        properties.setCatalog(catalog);
        FeatureCache featureCache = new FeatureCache(properties);
        HybridRecommendationService service = new HybridRecommendationService(
            redis, properties,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache, List.of());
        MovieLensUserFeatures features = new MovieLensUserFeatures("u1", List.of("sci-fi"), 0.0, 0, List.of())
            .withBehaviorPreferences(Map.of("sci-fi", 0.2, "drama", 0.3), Map.of("space", 0.4));

        HybridRecommendationService.TasteProfile profile = service.deriveTasteProfile(
            List.of("watched"), List.of(), features, List.of());

        assertEquals(1.0, profile.genres().get("sci-fi"));
        assertEquals(0.3, profile.genres().get("drama"));
        assertEquals(0.4, profile.tags().get("space"));
    }

    @Test
    void profiledUserRanksStrongerMatchingGenreAboveTiedPopularity() {
        UserBehaviorProfile profile = new UserBehaviorProfile(
            "u1", 1, "run", "now", null, 1L,
            new UserBehaviorProfile.Preferences(
                List.of(
                    new UserBehaviorProfile.Preference("sci-fi", 0.9, 1L),
                    new UserBehaviorProfile.Preference("drama", 0.3, 1L)
                ),
                List.of()),
            null, List.of());

        assertEquals(List.of("sci-fi", "drama"),
            recommendForTiedGenres(userId -> Optional.of(profile)).recommendations());
    }

    @Test
    void missingBehaviorProfilePreservesTiedPopularityBaselineOrdering() {
        assertEquals(List.of("drama", "sci-fi"),
            recommendForTiedGenres(userId -> Optional.empty()).recommendations());
    }

    /**
     * UserMovieFeaturesQueryHydrator loads the baseline feature record and its update *replaces*
     * userFeatures rather than merging into it, so every hydrator that layers onto userFeatures
     * has its contribution discarded unless the loader runs first. Spring sorts the injected
     * hydrator list with AnnotationAwareOrderComparator over whatever order classpath scanning
     * produced, and that order is not ours to control, so pin the outcome for every discovery
     * order instead of the one scanning happens to hand us.
     */
    @Test
    void behaviorProfileReachesScoringInEveryHydratorDiscoveryOrder() {
        UserBehaviorProfile profile = new UserBehaviorProfile(
            "u1", 1, "run", "now", null, 1L,
            new UserBehaviorProfile.Preferences(
                List.of(
                    new UserBehaviorProfile.Preference("sci-fi", 0.9, 1L),
                    new UserBehaviorProfile.Preference("drama", 0.3, 1L)
                ),
                List.of()),
            null, List.of());
        UserProfileClient profileClient = userId -> Optional.of(profile);
        MovieLensFeatureClient featureClient = userId -> Optional.empty();

        for (List<QueryHydrator<ScoredMoviesQuery>> discovered : List.of(
            List.of(
                new UserBehaviorProfileQueryHydrator(profileClient),
                new UserMovieFeaturesQueryHydrator(featureClient)),
            List.of(
                new UserMovieFeaturesQueryHydrator(featureClient),
                new UserBehaviorProfileQueryHydrator(profileClient))
        )) {
            List<QueryHydrator<ScoredMoviesQuery>> injected = new ArrayList<>(discovered);
            AnnotationAwareOrderComparator.sort(injected);

            assertEquals(List.of("sci-fi", "drama"), recommendForTiedGenres(injected).recommendations(),
                "discovery order " + discovered.stream().map(h -> h.getClass().getSimpleName()).toList());
        }
    }

    private static RecommendationResult recommendForTiedGenres(UserProfileClient profileClient) {
        return recommendForTiedGenres(List.of(new UserBehaviorProfileQueryHydrator(profileClient)));
    }

    private static RecommendationResult recommendForTiedGenres(
        List<QueryHydrator<ScoredMoviesQuery>> hydrators) {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        ListOperations<String, String> lists = mock(ListOperations.class);
        SetOperations<String, String> sets = mock(SetOperations.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        ZSetOperations<String, String> sortedSets = mock(ZSetOperations.class);
        when(redis.opsForHash()).thenReturn(hashes);
        when(redis.opsForList()).thenReturn(lists);
        when(redis.opsForSet()).thenReturn(sets);
        when(redis.opsForValue()).thenReturn(values);
        when(redis.opsForZSet()).thenReturn(sortedSets);
        when(sets.size(any())).thenReturn(1L);
        when(sortedSets.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                ZSetOperations.TypedTuple.of("drama", 100.0),
                ZSetOperations.TypedTuple.of("sci-fi", 100.0)
            )));
        when(values.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> key.startsWith("i2vEmb:") ? "1.0 0.0" : "0").toList();
        });

        RecommendationProperties properties = new RecommendationProperties();
        properties.getCandidateGeneration().setColdStartPoolSize(2);
        properties.getBandit().setRelevanceWeight(0.0);
        properties.getBandit().setContentWeight(1.0);
        properties.getBandit().setPopularityWeight(0.0);
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("drama", movie("drama"));
        catalog.put("sci-fi", movie("sci-fi"));
        properties.setCatalog(catalog);
        FeatureCache featureCache = new FeatureCache(properties);
        HybridRecommendationService service = new HybridRecommendationService(
            redis, properties,
            new OnlineLearningService(redis, properties, featureCache), featureCache,
            hydrators);

        return service.recommend("u1", 2);
    }

    /**
     * Call-site characterization. The two pure-function characterization tests
     * (RecommendationConstantsTest, MovieLensOutcomeScorerTest) build their own inputs and call the
     * functions directly, so neither pins how HybridRecommendationService fills in the positional
     * components of ScoringInput (11 bare values) and ScoredCandidate (18). Deleting or transposing
     * a component there compiles and passes every other test. This drives the full recommend(...)
     * path with a fully-controlled input and pins the resulting diagnostics numbers, which are
     * reachable only through those two constructors.
     *
     * <p>Inputs are chosen so the three offline signals are mutually distinct — relevance
     * cos([1,0],[0.28,0.96]) = 0.28, a partial genre/tag content match, popularity 5/100 = 0.05 — so a
     * transposition changes the result rather than swapping equal values.
     */
    @Test
    void recommendPinsDiagnosticScoresThroughTheScoringCallSite() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        ListOperations<String, String> lists = mock(ListOperations.class);
        SetOperations<String, String> sets = mock(SetOperations.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        ZSetOperations<String, String> sortedSets = mock(ZSetOperations.class);
        when(redis.opsForHash()).thenReturn(hashes);
        when(redis.opsForList()).thenReturn(lists);
        when(redis.opsForSet()).thenReturn(sets);
        when(redis.opsForValue()).thenReturn(values);
        when(redis.opsForZSet()).thenReturn(sortedSets);
        when(sets.size(any())).thenReturn(1L);
        when(values.get("uEmb:u1")).thenReturn("1.0 0.0");

        when(sortedSets.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                ZSetOperations.TypedTuple.of("seen", 100.0),
                ZSetOperations.TypedTuple.of("m1", 5.0)
            )));
        when(values.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> {
                if (key.startsWith("i2vEmb:")) {
                    return "0.28 0.96";
                }
                if (key.startsWith("uEmb:")) {
                    return "1.0 0.0";
                }
                return "0";
            }).toList();
        });

        RecommendationProperties properties = new RecommendationProperties();
        properties.getCandidateGeneration().setColdStartPoolSize(1);
        MovieProfile candidate = new MovieProfile();
        candidate.setGenres(List.of("drama"));
        candidate.setTags(List.of("space"));
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("seen", movie("drama"));
        catalog.put("m1", candidate);
        properties.setCatalog(catalog);

        FeatureCache featureCache = new FeatureCache(properties);
        HybridRecommendationService service = new HybridRecommendationService(
            redis,
            properties,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache,
            List.of(new MovieLensUserHistoryQueryHydrator(
                userId -> new UserMovieHistory(List.of("seen"), List.of())
            )),
            new RecommendationMeasurementService(new SimpleMeterRegistry(), properties, featureCache)
        );

        RecommendationResult result = service.recommend("u1", 1);

        assertEquals(List.of("m1"), result.recommendations());
        Map<String, Object> row = result.candidateDiagnostics().get(0);
        assertEquals("m1", row.get("item"));
        assertEquals(0.28, row.get("relevanceScore"));
        assertEquals(0.7, row.get("contentScore"));
        assertEquals(0.596, row.get("estimatedReward"));
        assertEquals(0.616, row.get("weightedOutcomeScore"));
        assertEquals(0.841, row.get("predictionScore"));
        assertEquals(0.617, row.get("rewardModelScore"));
    }

    /**
     * Proves the wiring, not just the math: without this test, deleting the
     * {@code applyGrpoReRank(...)} call in {@code recommend()} leaves every other test green.
     *
     * "drama" and "sci-fi" are set up (equal popularity, equal content match, no relevance/genre
     * preference signal) so their post-diversity finalScore ties at 0.302 -- verified empirically,
     * not asserted directly, since the exact float is an implementation detail. With a tie, the
     * incumbent order is the catalog's insertion order (drama, sci-fi), and any non-degenerate
     * GRPO signal decides the re-rank outright regardless of the fixed 0.10 blend weight, because
     * there is no incumbent preference gap to overcome.
     *
     * The two items differ sharply in engagement: drama 5/50 clicks/impressions, sci-fi 40/200.
     * GRPO weights isolate feature index 8 (smoothed CTR = clicks/(impressions+1)), which favors
     * sci-fi (~0.199 vs ~0.098), so `on` mode must swap the order to [sci-fi, drama].
     */
    @Test
    void onModeReRanksTheServedSlateWhenGrpoDisagreesWithTheTiedIncumbent() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        HashOperations<String, Object, Object> hashes = mock(HashOperations.class);
        ListOperations<String, String> lists = mock(ListOperations.class);
        SetOperations<String, String> sets = mock(SetOperations.class);
        ValueOperations<String, String> values = mock(ValueOperations.class);
        ZSetOperations<String, String> sortedSets = mock(ZSetOperations.class);
        when(redis.opsForHash()).thenReturn(hashes);
        when(redis.opsForList()).thenReturn(lists);
        when(redis.opsForSet()).thenReturn(sets);
        when(redis.opsForValue()).thenReturn(values);
        when(redis.opsForZSet()).thenReturn(sortedSets);
        when(sets.size(any())).thenReturn(1L);
        when(sortedSets.reverseRangeWithScores(eq("global:item_popularity"), eq(0L), anyLong()))
            .thenReturn(new LinkedHashSet<>(List.of(
                ZSetOperations.TypedTuple.of("drama", 100.0),
                ZSetOperations.TypedTuple.of("sci-fi", 100.0)
            )));
        when(values.multiGet(any())).thenAnswer(invocation -> {
            List<String> keys = invocation.getArgument(0);
            return keys.stream().map(key -> {
                if (key.startsWith("i2vEmb:")) return "1.0 0.0";
                if (key.equals("bandit:item:drama:impressions")) return "50";
                if (key.equals("bandit:item:drama:clicks")) return "5";
                if (key.equals("bandit:item:sci-fi:impressions")) return "200";
                if (key.equals("bandit:item:sci-fi:clicks")) return "40";
                return "0";
            }).toList();
        });
        String[] weights = new String[GrpoFeatures.DIM];
        Arrays.fill(weights, "0.0");
        weights[8] = "1.0";
        Map<Object, Object> grpoWeights = new LinkedHashMap<>();
        grpoWeights.put("feature_version", GrpoFeatures.VERSION);
        grpoWeights.put("weights", String.join(",", weights));
        when(hashes.entries(eq(GrpoPolicyScorer.WEIGHTS_KEY))).thenReturn(grpoWeights);

        RecommendationProperties properties = new RecommendationProperties();
        properties.getCandidateGeneration().setColdStartPoolSize(2);
        properties.getBandit().setRelevanceWeight(0.0);
        properties.getBandit().setContentWeight(1.0);
        properties.getBandit().setPopularityWeight(0.0);
        properties.getGrpo().setMode("on");
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("drama", movie("drama"));
        catalog.put("sci-fi", movie("sci-fi"));
        properties.setCatalog(catalog);
        FeatureCache featureCache = new FeatureCache(properties);
        HybridRecommendationService service = new HybridRecommendationService(
            redis, properties,
            new OnlineLearningService(redis, properties, featureCache), featureCache,
            List.of());

        RecommendationResult result = service.recommend("u1", 2);

        assertEquals(List.of("sci-fi", "drama"), result.recommendations());
    }

    /**
     * GrpoFeaturesTest.theTwoOverloadsAgreeAfterTheSameRoundingTheRealCallSitesApply rounds inline
     * on both sides of the comparison and never calls a production method -- it proves
     * rounding-then-comparing agrees with rounding-then-comparing, which would stay green even if
     * {@code applyGrpoReRank} stopped rounding entirely. This test instead drives the two REAL call
     * sites on the same unrounded {@code ScoredCandidate}: {@code toServedMovie}, which
     * GrpoImpressionEvents packs training's grpo_x from, and {@code grpoFeaturesFor}, which
     * applyGrpoReRank feeds the re-rank scorer. If either call site's rounding drifted from the
     * other -- including if the rounding were removed entirely -- this comparison would fail on
     * the affected indices, unlike the inline-rounding test above.
     */
    @Test
    void toServedMovieAndGrpoFeaturesForAgreeOnAnUnroundedCandidate() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        RecommendationProperties properties = new RecommendationProperties();
        FeatureCache featureCache = new FeatureCache(properties);
        HybridRecommendationService service = new HybridRecommendationService(
            redis, properties,
            new OnlineLearningService(redis, properties, featureCache),
            featureCache, List.of());

        HybridRecommendationService.ScoredCandidate candidate = new HybridRecommendationService.ScoredCandidate(
            "m1",
            0.654321789, // estimatedReward
            0.0, 0.0, 0.0, // relevanceScore, contentScore, popularityScore -- not GRPO features
            0.333333789, // onlineScore
            0.070707789, // explorationBonus
            0.123456789, // banditScore
            false,       // coldStart
            0.0,         // noveltyScore -- not a GRPO feature
            42L, 7L,     // impressions, clicks
            0.0, 0.0, 0.0, 0.0, // qValue, weightedOutcomeScore, predictionScore, diversityScore
            null, null   // diversityGroupId, primaryGenre
        );

        double[] viaToServedMovie = GrpoFeatures.of(service.toServedMovie(candidate));
        double[] viaGrpoFeaturesFor = service.grpoFeaturesFor(candidate);

        assertArrayEquals(viaToServedMovie, viaGrpoFeaturesFor, 1e-12);
    }

    // Guards the default-off promise: RECSYS_GRPO_EMIT_EVENTS=false must mean nothing new happens
    // at all, including a broken Avro schema resource failing Spring bean creation. Mocks the
    // RecsysEventAvroCodec constructor to throw, simulating a corrupt/missing schema, and proves
    // construction still succeeds with emission disabled — and that the codec was never
    // instantiated in the first place, so no such failure could ever reach it.
    @Test
    void constructionSucceedsWithEmitEventsDisabledEvenIfAvroCodecConstructionFails() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        RecommendationProperties properties = new RecommendationProperties();
        properties.getGrpo().setEmitEvents(false);
        FeatureCache featureCache = new FeatureCache(properties);

        try (MockedConstruction<RecsysEventAvroCodec> codecConstruction = mockConstruction(
                RecsysEventAvroCodec.class,
                (mockCodec, context) -> {
                    throw new IllegalStateException("simulated corrupt/missing Avro schema resource");
                })) {
            assertDoesNotThrow(() -> new HybridRecommendationService(
                redis, properties,
                new OnlineLearningService(redis, properties, featureCache),
                featureCache, List.of()));
            assertTrue(codecConstruction.constructed().isEmpty());
        }
    }

    private static MovieProfile movie(String genre) {
        MovieProfile profile = new MovieProfile();
        profile.setGenres(List.of(genre));
        profile.setTags(List.of(genre));
        return profile;
    }
}
