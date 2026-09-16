package com.demo.retrieval.service.content;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatalogContentScoringTest {

    private CatalogContentScoring scoringFor(MovieProfile profile) {
        RecommendationProperties properties = new RecommendationProperties();
        Map<String, MovieProfile> catalog = new LinkedHashMap<>();
        catalog.put("m1", profile);
        properties.setCatalog(catalog);
        return new CatalogContentScoring(properties);
    }

    private static MovieProfile movie(List<String> genres, List<String> tags, boolean newRelease) {
        MovieProfile p = new MovieProfile();
        p.setGenres(genres);
        p.setTags(tags);
        p.setNewRelease(newRelease);
        return p;
    }

    @Test
    void buildsNormalizedProfileAndCachesByCatalogIdentity() {
        CatalogContentScoring scoring = scoringFor(movie(List.of("Drama"), List.of("Dark"), true));
        NormalizedProfile np = scoring.profileFor("m1");
        assertTrue(np.genres().contains("drama"));
        assertTrue(np.tags().contains("dark"));
        assertTrue(scoring.isNewRelease("m1"));
        assertSame(scoring.normalizedCatalog(), scoring.normalizedCatalog());
    }

    @Test
    void contentScoreIsGenreTagJaccardBlend() {
        CatalogContentScoring scoring = scoringFor(movie(List.of("drama"), List.of("dark"), false));
        NormalizedProfile np = scoring.profileFor("m1");
        double full = scoring.contentScore(np, Set.of("drama"), Set.of("dark"));
        double none = scoring.contentScore(np, Set.of("comedy"), Set.of("light"));
        assertTrue(full > none);
        assertEquals(0.0, none, 1e-9);
    }

    @Test
    void contentScoreWeightsMatchingPreferencesAndIgnoresInvalidWeights() {
        CatalogContentScoring sciFiScoring = scoringFor(movie(List.of("sci-fi"), List.of("space"), false));
        CatalogContentScoring dramaScoring = scoringFor(movie(List.of("drama"), List.of("space"), false));

        double strongGenre = sciFiScoring.contentScore(
            sciFiScoring.profileFor("m1"), Map.of("sci-fi", 0.9, "drama", 0.3), Map.of());
        double weakGenre = dramaScoring.contentScore(
            dramaScoring.profileFor("m1"), Map.of("sci-fi", 0.9, "drama", 0.3), Map.of());
        double genreAndTag = sciFiScoring.contentScore(
            sciFiScoring.profileFor("m1"), Map.of("sci-fi", 0.9, "drama", 0.3), Map.of("space", 0.4, "other", 0.6));
        double invalid = sciFiScoring.contentScore(
            sciFiScoring.profileFor("m1"), Map.of("sci-fi", -1.0), Map.of("space", -1.0));
        double unknown = sciFiScoring.contentScore(
            sciFiScoring.profileFor("m1"), Map.of("unknown", 0.9), Map.of("unmatched", 0.4));

        assertTrue(strongGenre > weakGenre);
        assertTrue(genreAndTag > strongGenre);
        assertEquals(0.0, invalid, 1e-9);
        assertEquals(0.0, unknown, 1e-9);
    }

    @Test
    void setOverloadDelegatesToUnitWeightPreferences() {
        CatalogContentScoring scoring = scoringFor(movie(List.of("drama"), List.of("dark"), false));
        NormalizedProfile profile = scoring.profileFor("m1");

        assertEquals(
            scoring.contentScore(profile, Map.of("drama", 1.0), Map.of("dark", 1.0)),
            scoring.contentScore(profile, Set.of("drama"), Set.of("dark")),
            1e-9);
    }
}
