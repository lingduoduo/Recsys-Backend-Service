package com.demo.retrieval.service;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.UserDemographics;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MovieLensUserFeaturesTest {
    @Test
    void storesOnlyMovieLensBehaviorAndPreferenceSignals() {
        MovieLensUserFeatures features = MovieLensUserFeatures.forUser("u1")
            .withActionSequenceMovieIds(List.of("a"))
            .withRetrievalSequenceMovieIds(List.of("b"))
            .withScoringSequenceMovieIds(List.of("c"))
            .withServedMovieIds(List.of("d"))
            .withImpressedMovieIds(List.of("e"))
            .withCachedMovieIds(List.of("f"), true)
            .withInferredGenres(List.of(1, 2));

        assertEquals(List.of("a"), features.actionSequenceMovieIds());
        assertEquals(List.of("b"), features.retrievalSequenceMovieIds());
        assertEquals(List.of("c"), features.scoringSequenceMovieIds());
        assertEquals(List.of("d"), features.servedMovieIds());
        assertEquals(List.of("e"), features.impressedMovieIds());
        assertEquals(List.of("f"), features.cachedMovieIds());
        assertEquals(List.of(1, 2), features.inferredGenres());
        assertTrue(features.hasCachedMovies());
    }

    @Test
    void preservesBehaviorPreferencesAcrossEveryFeatureCopyMethod() {
        Map<String, Double> genres = new LinkedHashMap<>();
        genres.put(" Drama ", 0.4);
        genres.put("SCI-FI", 0.9);
        genres.put("discarded", 0.0);
        genres.put("not-a-number", Double.NaN);
        genres.put("null-score", null);
        genres.put(null, 0.5);
        Map<String, Double> tags = new LinkedHashMap<>();
        tags.put(" Space ", 0.7);
        tags.put("ignored", Double.NEGATIVE_INFINITY);

        MovieLensUserFeatures features = MovieLensUserFeatures.forUser("u1")
            .withBehaviorPreferences(genres, tags)
            .withActionSequenceMovieIds(List.of("a"))
            .withRetrievalSequenceMovieIds(List.of("b"))
            .withScoringSequenceMovieIds(List.of("c"))
            .withServedMovieIds(List.of("d"))
            .withPastRequestTimestamps(List.of(1L))
            .withInferredGenres(List.of(1))
            .withInferredGrokTopics(List.of(2))
            .withImpressionBloomFilter(List.of(3L))
            .withImpressedMovieIds(List.of("e"))
            .withCachedMovieIds(List.of("f"), true)
            .withDemographics(new UserDemographics(31, "F", "engineer", "10001"));

        assertEquals(Map.of("sci-fi", 0.9, "drama", 0.4), features.genrePreferences());
        assertEquals(Map.of("space", 0.7), features.tagPreferences());
        assertEquals(List.of("sci-fi", "drama"), List.copyOf(features.genrePreferences().keySet()));
        assertEquals(List.of("space"), List.copyOf(features.tagPreferences().keySet()));
        assertThrows(UnsupportedOperationException.class, () -> features.genrePreferences().put("new", 0.1));
    }
}
