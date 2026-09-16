package com.demo.retrieval.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/** Read model for the versioned user-behavior profile emitted by the Spark profile job. */
public record UserBehaviorProfile(
    @JsonProperty("user_id") String userId,
    @JsonProperty("profile_version") int profileVersion,
    @JsonProperty("run_id") String runId,
    @JsonProperty("generated_at") String generatedAt,
    @JsonProperty("source_window") SourceWindow sourceWindow,
    @JsonProperty("evidence_count") long evidenceCount,
    Preferences preferences,
    @JsonProperty("behavioral_features") BehavioralFeatures behavioralFeatures,
    List<Persona> personas
) {
    public UserBehaviorProfile {
        personas = personas == null ? List.of() : List.copyOf(personas);
    }

    public record SourceWindow(String start, String end) {
    }

    public record Preferences(List<Preference> genres, List<Preference> tags) {
        public Preferences {
            genres = genres == null ? List.of() : List.copyOf(genres);
            tags = tags == null ? List.of() : List.copyOf(tags);
        }
    }

    public record Preference(String value, double score, @JsonProperty("evidence_count") long evidenceCount) {
    }

    public record BehavioralFeatures(
        @JsonProperty("engagement_rate") Double engagementRate,
        @JsonProperty("conversion_rate") Double conversionRate,
        @JsonProperty("genre_diversity") Double genreDiversity,
        @JsonProperty("preference_concentration") Double preferenceConcentration,
        @JsonProperty("recent_release_affinity") Double recentReleaseAffinity,
        @JsonProperty("average_rating") Double averageRating,
        @JsonProperty("activity_level") String activityLevel
    ) {
    }

    public record Persona(String type, String label, double confidence, Map<String, Double> evidence) {
        public Persona {
            evidence = evidence == null ? Map.of() : Map.copyOf(evidence);
        }
    }
}
