package com.demo.retrieval.service.audit;

import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.service.audit.ProfileAuditReport.Finding;
import com.demo.retrieval.service.audit.ProfileAuditReport.PreferenceRef;
import com.demo.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.demo.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.demo.retrieval.service.content.NormalizedProfile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UserAuditClassifierTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final CatalogPreferenceIndex INDEX = CatalogPreferenceIndex.build(Map.of(
        "item1", new NormalizedProfile("", Set.of("sci-fi"), Set.of("space"), Set.of("space"), "", false, 0L),
        "item2", new NormalizedProfile("", Set.of("sci-fi"), Set.of(), Set.of(), "", false, 0L),
        "item3", new NormalizedProfile("", Set.of("drama"), Set.of(), Set.of(), "", false, 0L)));

    /** Builds a profile JSON for {@code user}/{@code run} using the real record so the shape cannot drift. */
    static String profileJson(String user, String run, List<String> genres, List<String> tags, boolean newOrUnknown) {
        List<UserBehaviorProfile.Preference> g = genres.stream()
            .map(v -> new UserBehaviorProfile.Preference(v, 0.7, 8L)).toList();
        List<UserBehaviorProfile.Preference> t = tags.stream()
            .map(v -> new UserBehaviorProfile.Preference(v, 0.5, 3L)).toList();
        List<UserBehaviorProfile.Persona> personas = newOrUnknown
            ? List.of(new UserBehaviorProfile.Persona("new_or_unknown", "New or unknown", 1.0, Map.of()))
            : List.of();
        UserBehaviorProfile profile = new UserBehaviorProfile(
            user, 1, run, "2026-09-07T00:00:00Z",
            new UserBehaviorProfile.SourceWindow("2026-08-07T00:00:00Z", "2026-09-07T00:00:00Z"),
            12L, new UserBehaviorProfile.Preferences(g, t),
            new UserBehaviorProfile.BehavioralFeatures(0.5, 0.1, 0.4, 0.7, 0.6, null, "medium"),
            personas);
        try {
            return MAPPER.writeValueAsString(profile);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static UserRow classify(RawProfile raw) {
        return UserAuditClassifier.classify(raw, "run-7", INDEX, MAPPER, 1);
    }

    @Test
    void missingBlobIsNoProfileWithReason() {
        UserRow row = classify(new RawProfile("u1", null, -2L));

        assertFalse(row.hasProfile());
        assertNull(row.ttlSeconds());
        assertNull(row.preferences());
        assertEquals(List.of(new Finding("no_profile", "missing_profile", null)), row.findings());
    }

    @Test
    void wrongRunIsNoProfileWithRunMismatch() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-6", List.of("sci-fi"), List.of(), false), 100L));

        assertEquals("run_mismatch", row.findings().get(0).reason());
    }

    @Test
    void healthyUserHasNoFindingsButKeepsMatchesAndTtl() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-7", List.of("Sci-Fi"), List.of("space"), false), 3400L));

        assertTrue(row.hasProfile());
        assertEquals(3400L, row.ttlSeconds());
        assertTrue(row.findings().isEmpty());
        assertEquals(2, row.preferences().size());
        assertEquals("genre", row.preferences().get(0).kind());
        assertEquals("sci-fi", row.preferences().get(0).value());
        assertEquals(2, row.preferences().get(0).matchedItems());
        assertEquals(List.of("item1"), row.preferences().get(0).sampleItems());   // sampleItems = 1
        assertEquals("tag", row.preferences().get(1).kind());
        assertEquals(1, row.preferences().get(1).matchedItems());
    }

    @Test
    void lowEvidencePersonaIsAFinding() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-7", List.of("sci-fi"), List.of(), true), 100L));

        assertEquals(List.of(new Finding("new_or_unknown", null, null)), row.findings());
    }

    @Test
    void noPositivePreferenceIsEmptyPreferences() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-7", List.of(), List.of(), false), 100L));

        assertEquals(List.of(new Finding("empty_preferences", null, null)), row.findings());
        assertTrue(row.preferences().isEmpty());
    }

    @Test
    void unmatchedPreferencesAreListedInOneFinding() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-7", List.of("sci-fi", "film-noir"), List.of("robots"), false), 100L));

        assertEquals(1, row.findings().size());
        Finding finding = row.findings().get(0);
        assertEquals("preference_without_content", finding.type());
        assertEquals(List.of(new PreferenceRef("genre", "film-noir"), new PreferenceRef("tag", "robots")), finding.preferences());
        assertEquals(0, row.preferences().get(1).matchedItems());
    }

    @Test
    void noExpiryTtlIsReportedAsNull() {
        UserRow row = classify(new RawProfile("u1", profileJson("u1", "run-7", List.of("sci-fi"), List.of(), false), -1L));

        assertNull(row.ttlSeconds());
    }

    @Test
    void nullActiveRunIsReportedAsMissingActiveRun() {
        UserRow row = UserAuditClassifier.classify(
            new RawProfile("u1", profileJson("u1", "run-7", List.of("sci-fi"), List.of(), false), 100L),
            null, INDEX, MAPPER, 1);

        assertFalse(row.hasProfile());
        assertNull(row.ttlSeconds());
        assertNull(row.preferences());
        assertEquals(List.of(new Finding("no_profile", "missing_active_run", null)), row.findings());
    }
}
