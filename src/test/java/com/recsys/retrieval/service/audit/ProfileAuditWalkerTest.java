package com.recsys.retrieval.service.audit;

import com.recsys.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.recsys.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.recsys.retrieval.service.content.NormalizedProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.recsys.retrieval.service.audit.UserAuditClassifierTest.profileJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileAuditWalkerTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final CatalogPreferenceIndex INDEX = CatalogPreferenceIndex.build(Map.of(
        "item1", new NormalizedProfile("", Set.of("sci-fi"), Set.of("space"), Set.of("space"), "", false, 0L)));

    /** Records every readProfiles batch so the test can assert one round trip per walk. */
    private static class RecordingStore implements ProfileAuditStore {
        final List<List<String>> batches = new ArrayList<>();
        final Map<String, RawProfile> profiles;

        RecordingStore(Map<String, RawProfile> profiles) {
            this.profiles = profiles;
        }

        @Override
        public ScanResult scanUserIds(String pattern, int limit) {
            throw new UnsupportedOperationException("the walker never scans");
        }

        @Override
        public Optional<String> activeRun() {
            throw new UnsupportedOperationException("the walker never reads the pointer");
        }

        @Override
        public List<RawProfile> readProfiles(String run, List<String> userIds) {
            batches.add(List.copyOf(userIds));
            return userIds.stream().map(profiles::get).toList();
        }
    }

    private static RawProfile healthy(String user) {
        return new RawProfile(user, profileJson(user, "run-7", List.of("sci-fi"), List.of("space"), false), 3400L);
    }

    @Test
    void readsTheWholeBatchInOneRoundTripAndKeepsInputOrder() {
        RecordingStore store = new RecordingStore(Map.of(
            "u1", healthy("u1"), "u2", new RawProfile("u2", null, -2L), "u3", healthy("u3")));
        ProfileAuditWalker walker = new ProfileAuditWalker(store, MAPPER, INDEX, 5);

        List<UserRow> rows = walker.walk("run-7", List.of("u1", "u2", "u3"));

        assertEquals(1, store.batches.size());
        assertEquals(List.of("u1", "u2", "u3"), store.batches.get(0));
        assertEquals(List.of("u1", "u2", "u3"), rows.stream().map(UserRow::userId).toList());
        assertTrue(rows.get(0).findings().isEmpty());
        assertEquals("missing_profile", rows.get(1).findings().get(0).reason());
    }

    @Test
    void aNullActiveRunProducesMissingActiveRunRowsWithoutReadingTheStore() {
        RecordingStore store = new RecordingStore(Map.of());
        ProfileAuditWalker walker = new ProfileAuditWalker(store, MAPPER, INDEX, 5);

        List<UserRow> rows = walker.walk(null, List.of("u1", "u2"));

        assertTrue(store.batches.isEmpty());
        assertEquals(List.of("u1", "u2"), rows.stream().map(UserRow::userId).toList());
        rows.forEach(row -> {
            assertFalse(row.hasProfile());
            assertEquals("missing_active_run", row.findings().get(0).reason());
        });
    }

    @Test
    void anEmptyBatchTouchesTheStoreNotAtAll() {
        RecordingStore store = new RecordingStore(Map.of());
        ProfileAuditWalker walker = new ProfileAuditWalker(store, MAPPER, INDEX, 5);

        assertTrue(walker.walk("run-7", List.of()).isEmpty());
        assertTrue(store.batches.isEmpty());
    }

    @Test
    void aStoreFailurePropagatesUnwrapped() {
        ProfileAuditStore failing = new RecordingStore(Map.of()) {
            @Override
            public List<RawProfile> readProfiles(String run, List<String> userIds) {
                throw new IllegalStateException("redis unavailable");
            }
        };
        ProfileAuditWalker walker = new ProfileAuditWalker(failing, MAPPER, INDEX, 5);

        IllegalStateException e = org.junit.jupiter.api.Assertions.assertThrows(
            IllegalStateException.class, () -> walker.walk("run-7", List.of("u1")));
        assertEquals("redis unavailable", e.getMessage());
    }
}
