package com.recsys.retrieval.model;

import java.util.List;
import java.util.Map;

/**
 * @param degraded true when a backing-store failure (Redis) forced an empty answer. It distinguishes an outage
 *                 from a user who genuinely has nothing to recommend, which otherwise look identical.
 */
public record RecommendationResult(
    String user,
    List<String> recent,
    List<String> recommendations,
    List<Map<String, Object>> candidateDiagnostics,
    Map<String, Object> metrics,
    boolean degraded
) {
    public RecommendationResult(String user,
                                List<String> recent,
                                List<String> recommendations,
                                List<Map<String, Object>> candidateDiagnostics,
                                Map<String, Object> metrics) {
        this(user, recent, recommendations, candidateDiagnostics, metrics, false);
    }

    /** An empty answer forced by a backing-store failure. */
    public static RecommendationResult degraded(String user) {
        return new RecommendationResult(user, List.of(), List.of(), List.of(), Map.of(), true);
    }
}
