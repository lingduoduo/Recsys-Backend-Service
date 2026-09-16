package com.demo.retrieval.service.audit;

import java.util.List;
import java.util.Optional;

/**
 * The only seam between the profile audit and Redis. Mirrors the Spark side's RedisProfileStore:
 * everything above this interface is pure and tested with an in-memory fake.
 */
public interface ProfileAuditStore {

    /** {@code truncated} is true when {@code limit} stopped the scan before the cursor was exhausted. */
    record ScanResult(List<String> userIds, boolean truncated) {
    }

    /**
     * One user's profile blob as stored. {@code json} is null when the key is absent.
     * {@code ttlSeconds} follows Redis TTL semantics: -2 absent, -1 no expiry, else seconds left.
     */
    record RawProfile(String userId, String json, long ttlSeconds) {
    }

    /** Level 0: distinct user ids whose key matches {@code pattern}, at most {@code limit}. */
    ScanResult scanUserIds(String pattern, int limit);

    /** Level 1: the active-run pointer, empty when unset or blank. */
    Optional<String> activeRun();

    /** Level 1: profile blob + TTL for every user in one round trip, in the same order as {@code userIds}. */
    List<RawProfile> readProfiles(String run, List<String> userIds);
}
