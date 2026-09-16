package com.demo.retrieval.service.audit;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.util.List;
import java.util.Map;

/** Response of GET /actuator/profile-audit. Null fields are omitted from JSON. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"status", "active_run", "generated_at", "elapsed_ms", "truncated", "summary", "users"})
public record ProfileAuditReport(
    String status,
    @JsonProperty("active_run") String activeRun,
    @JsonProperty("generated_at") String generatedAt,
    @JsonProperty("elapsed_ms") long elapsedMs,
    boolean truncated,
    Summary summary,
    List<UserRow> users
) {
    public static final String STATUS_OK = "ok";
    public static final String STATUS_MISSING_ACTIVE_RUN = "missing_active_run";

    public static final String FINDING_NO_PROFILE = "no_profile";
    public static final String FINDING_NEW_OR_UNKNOWN = "new_or_unknown";
    public static final String FINDING_EMPTY_PREFERENCES = "empty_preferences";
    public static final String FINDING_PREFERENCE_WITHOUT_CONTENT = "preference_without_content";

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Summary(
        @JsonProperty("users_scanned") int usersScanned,
        @JsonProperty("users_with_profile") int usersWithProfile,
        @JsonProperty("users_healthy") int usersHealthy,
        @JsonProperty("no_profile_by_reason") Map<String, Integer> noProfileByReason,
        @JsonProperty("findings_by_type") Map<String, Integer> findingsByType,
        @JsonProperty("unmatched_preferences") Map<String, Map<String, Integer>> unmatchedPreferences,
        @JsonProperty("min_profile_ttl_seconds") Long minProfileTtlSeconds,
        @JsonProperty("catalog_size") int catalogSize
    ) {
    }

    /** One user with at least one finding. {@code ttlSeconds} and {@code preferences} are present only for valid profiles. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonPropertyOrder({"user_id", "has_profile", "ttl_seconds", "findings", "preferences"})
    public record UserRow(
        @JsonProperty("user_id") String userId,
        @JsonProperty("has_profile") boolean hasProfile,
        @JsonProperty("ttl_seconds") Long ttlSeconds,
        List<Finding> findings,
        List<PreferenceMatch> preferences
    ) {
    }

    /** {@code reason} only for no_profile; {@code preferences} only for preference_without_content. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record Finding(String type, String reason, List<PreferenceRef> preferences) {
    }

    public record PreferenceRef(String kind, String value) {
    }

    public record PreferenceMatch(
        String kind,
        String value,
        double score,
        @JsonProperty("evidence_count") long evidenceCount,
        @JsonProperty("matched_items") int matchedItems,
        @JsonProperty("sample_items") List<String> sampleItems
    ) {
    }
}
