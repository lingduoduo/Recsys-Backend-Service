package com.demo.retrieval.model;

import java.util.List;
import java.util.Map;

public record RecommendationResult(
    String user,
    List<String> recent,
    List<String> recommendations,
    List<Map<String, Object>> candidateDiagnostics,
    Map<String, Object> metrics
) {
}
