package com.demo.retrieval.measurement;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Collections;

/** Versioned, aggregate-only view suitable for the live metrics endpoint. */
public record MeasurementSnapshot(
    String schemaVersion,
    Map<String, Object> latency,
    Map<String, Object> throughput,
    Map<String, Object> cache,
    Map<String, Object> freshness,
    Map<String, Object> safety,
    Map<String, Object> feedbackCoverage
) {
    public MeasurementSnapshot {
        latency = immutableCopy(latency);
        throughput = immutableCopy(throughput);
        cache = immutableCopy(cache);
        freshness = immutableCopy(freshness);
        safety = immutableCopy(safety);
        feedbackCoverage = immutableCopy(feedbackCoverage);
    }

    public Map<String, Object> asMap() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("schemaVersion", schemaVersion);
        result.put("latency", latency);
        result.put("throughput", throughput);
        result.put("cache", cache);
        result.put("freshness", freshness);
        result.put("safety", safety);
        result.put("feedbackCoverage", feedbackCoverage);
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, Object> immutableCopy(Map<String, Object> values) {
        return Collections.unmodifiableMap(new LinkedHashMap<>(values));
    }
}
