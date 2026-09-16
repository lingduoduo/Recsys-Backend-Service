package com.demo.retrieval.service.content;

import java.util.Set;

public record NormalizedProfile(
    String productType,
    Set<String> genres,
    Set<String> tags,
    Set<String> allKeywords,
    String title,
    boolean newRelease,
    long expiresAtEpochMillis) {
}
