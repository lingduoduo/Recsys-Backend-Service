package com.demo.retrieval.service.content;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.config.RecommendationProperties.MovieProfile;
import com.demo.retrieval.service.RecommendationConstants;
import com.demo.retrieval.service.text.TextNormalization;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Shared normalized-catalog + content-scoring core, used by both the candidate retriever and the
 * scorer. Owns the normalized-catalog cache (keyed on catalog-map identity) and the genre/tag
 * Weighted genre/tag content score. Extracted from HybridRecommendationService.
 */
public class CatalogContentScoring {

    private final RecommendationProperties properties;
    private volatile CatalogCache catalogCache;

    public CatalogContentScoring(RecommendationProperties properties) {
        this.properties = properties;
    }

    public Map<String, NormalizedProfile> normalizedCatalog() {
        Map<String, MovieProfile> catalog = properties.getCatalog();
        CatalogCache cache = catalogCache;
        if (cache != null && cache.source() == catalog) {
            return cache.normalized();
        }
        Map<String, NormalizedProfile> built = new HashMap<>(catalog.size() * 4 / 3 + 1);
        catalog.forEach((id, p) -> {
            Set<String> normalizedTags = TextNormalization.normalize(p.getTags());
            Set<String> allKeywords = new HashSet<>(normalizedTags);
            allKeywords.addAll(TextNormalization.normalize(p.getKeywords()));
            built.put(id, new NormalizedProfile(
                TextNormalization.normalizeValue(p.getProductType()),
                TextNormalization.normalize(p.getGenres()),
                Collections.unmodifiableSet(normalizedTags),
                Collections.unmodifiableSet(allKeywords),
                TextNormalization.normalizeValue(p.getTitle()),
                p.isNewRelease(),
                p.getExpiresAtEpochMillis()
            ));
        });
        synchronized (this) {
            CatalogCache c2 = catalogCache;
            if (c2 != null && c2.source() == catalog) {
                return c2.normalized();
            }
            CatalogCache newCache = new CatalogCache(catalog, Collections.unmodifiableMap(built));
            catalogCache = newCache;
            return newCache.normalized();
        }
    }

    public NormalizedProfile profileFor(String itemId) {
        return normalizedCatalog().get(itemId);
    }

    public boolean isNewRelease(String itemId) {
        NormalizedProfile profile = profileFor(itemId);
        return profile != null && profile.newRelease();
    }

    public double contentScore(NormalizedProfile profile, Set<String> userGenres, Set<String> userTags) {
        return contentScore(profile, unitWeights(userGenres), unitWeights(userTags));
    }

    public double contentScore(
        NormalizedProfile profile,
        Map<String, Double> genrePreferences,
        Map<String, Double> tagPreferences
    ) {
        return RecommendationConstants.clamp(
            (weightedOverlapRatio(genrePreferences, profile.genres()) * RecommendationConstants.CONTENT_GENRE_WEIGHT)
            + (weightedOverlapRatio(tagPreferences, profile.tags()) * RecommendationConstants.CONTENT_TAG_WEIGHT));
    }

    private Map<String, Double> unitWeights(Set<String> preferences) {
        if (preferences == null || preferences.isEmpty()) {
            return Map.of();
        }
        Map<String, Double> weights = new LinkedHashMap<>();
        for (String preference : preferences) {
            if (preference != null) {
                weights.put(preference, 1.0);
            }
        }
        return weights;
    }

    private double weightedOverlapRatio(Map<String, Double> preferences, Set<String> catalogValues) {
        if (preferences == null || preferences.isEmpty() || catalogValues == null || catalogValues.isEmpty()) {
            return 0.0;
        }
        double matchingWeight = 0.0;
        double totalWeight = 0.0;
        for (Map.Entry<String, Double> entry : preferences.entrySet()) {
            Double rawWeight = entry.getValue();
            if (entry.getKey() == null || rawWeight == null || !Double.isFinite(rawWeight) || rawWeight <= 0.0) {
                continue;
            }
            double weight = Math.min(rawWeight, 1.0);
            totalWeight += weight;
            if (catalogValues.contains(entry.getKey())) {
                matchingWeight += weight;
            }
        }
        return totalWeight == 0.0 ? 0.0 : matchingWeight / totalWeight;
    }

    private record CatalogCache(
        Map<String, MovieProfile> source,
        Map<String, NormalizedProfile> normalized) {
    }
}
