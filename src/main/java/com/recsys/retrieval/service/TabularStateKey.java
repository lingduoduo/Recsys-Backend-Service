package com.demo.retrieval.service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Derives the tabular Q-learning/SARSA state key from the user's coarse taste
 * profile (genre + tag sets) only. Raw recent item-IDs are deliberately excluded:
 * they are high-cardinality, so including them makes almost every state unique and
 * the Bellman next-state bootstrap degenerates to a running average of immediate
 * reward. Hashing the order-independent genre/tag signature lets states recur across
 * users and sessions, restoring temporal credit.
 */
final class TabularStateKey {

    private TabularStateKey() {
    }

    /** base64url(SHA-256) of the canonical, order-independent genre/tag signature. */
    static String hash(Object genres, Object tags) {
        String canonical = "g:" + normalize(genres) + "|t:" + normalize(tags);
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(canonical.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 not available", e);
        }
    }

    private static String normalize(Object raw) {
        if (!(raw instanceof Collection<?> values)) {
            return "";
        }
        return values.stream()
            .filter(v -> v != null)
            .map(String::valueOf)
            .sorted()
            .distinct()
            .collect(Collectors.joining(","));
    }
}
