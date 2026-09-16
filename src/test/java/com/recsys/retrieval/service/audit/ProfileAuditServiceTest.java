package com.recsys.retrieval.service.audit;

import com.recsys.retrieval.config.RecommendationProperties;
import com.recsys.retrieval.config.RecommendationProperties.MovieProfile;
import com.recsys.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.recsys.retrieval.service.audit.UserAuditClassifierTest.profileJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProfileAuditServiceTest {

    /** In-memory store: users in insertion order, per-user (json, ttl), optional failure and blocking. */
    static final class FakeStore implements ProfileAuditStore {
        final List<String> users = new ArrayList<>();
        final Map<String, RawProfile> profiles = new LinkedHashMap<>();
        Optional<String> activeRun = Optional.of("run-7");
        boolean truncated = false;
        String failOnUser = null;
        CountDownLatch blockReads = null;
        final List<List<String>> chunksSeen = new CopyOnWriteArrayList<>();
        long readDelayMillis = 0L;

        FakeStore user(String id, String json, long ttl) {
            users.add(id);
            profiles.put(id, new RawProfile(id, json, ttl));
            return this;
        }

        @Override
        public ScanResult scanUserIds(String pattern, int limit) {
            return new ScanResult(users.subList(0, Math.min(limit, users.size())), truncated);
        }

        @Override
        public Optional<String> activeRun() {
            return activeRun;
        }

        @Override
        public List<RawProfile> readProfiles(String run, List<String> userIds) {
            chunksSeen.add(userIds);
            if (blockReads != null) {
                try { blockReads.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            if (readDelayMillis > 0L) {
                try { Thread.sleep(readDelayMillis); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            if (failOnUser != null && userIds.contains(failOnUser)) {
                throw new IllegalStateException("redis unavailable");
            }
            return userIds.stream().map(profiles::get).toList();
        }
    }

    /** Counts tasks submitted but not yet finished — the quantity a fixed pool hides. */
    static final class CountingPool extends java.util.concurrent.ThreadPoolExecutor {
        final java.util.concurrent.atomic.AtomicInteger outstanding = new java.util.concurrent.atomic.AtomicInteger();
        final java.util.concurrent.atomic.AtomicInteger maxOutstanding = new java.util.concurrent.atomic.AtomicInteger();

        CountingPool(int threads) {
            super(threads, threads, 0L, java.util.concurrent.TimeUnit.MILLISECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>());
        }

        @Override
        public void execute(Runnable command) {
            maxOutstanding.accumulateAndGet(outstanding.incrementAndGet(), Math::max);
            super.execute(command);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            super.afterExecute(r, t);
            outstanding.decrementAndGet();
        }
    }

    private static RecommendationProperties properties(int chunkSize, int parallelism) {
        RecommendationProperties properties = new RecommendationProperties();
        MovieProfile item1 = new MovieProfile();
        item1.setGenres(List.of("Sci-Fi"));
        item1.setTags(List.of("space"));
        MovieProfile item2 = new MovieProfile();
        item2.setGenres(List.of("drama"));
        properties.getCatalog().put("item1", item1);
        properties.getCatalog().put("item2", item2);
        properties.getProfileAudit().setChunkSize(chunkSize);
        properties.getProfileAudit().setParallelism(parallelism);
        properties.getProfileAudit().setMaxUsers(100);
        return properties;
    }

    private ProfileAuditService service;

    private ProfileAuditService service(FakeStore store, int chunkSize, int parallelism) {
        service = new ProfileAuditService(store, properties(chunkSize, parallelism), new ObjectMapper());
        return service;
    }

    @AfterEach
    void shutdown() {
        if (service != null) {
            service.shutdown();
        }
    }

    @Test
    void listsOnlyUsersWithFindingsAndCountsTheRest() {
        FakeStore store = new FakeStore()
            .user("healthy", profileJson("healthy", "run-7", List.of("sci-fi"), List.of("space"), false), 3400L)
            .user("missing", null, -2L)
            .user("stale", profileJson("stale", "run-6", List.of("sci-fi"), List.of(), false), 100L)
            .user("newbie", profileJson("newbie", "run-7", List.of("sci-fi"), List.of(), true), 900L)
            .user("empty", profileJson("empty", "run-7", List.of(), List.of(), false), 2000L)
            .user("gap", profileJson("gap", "run-7", List.of("film-noir"), List.of("space"), false), 1200L);

        ProfileAuditReport report = service(store, 500, 1).audit(null);

        assertEquals("ok", report.status());
        assertEquals("run-7", report.activeRun());
        assertFalse(report.truncated());
        assertEquals(6, report.summary().usersScanned());
        assertEquals(4, report.summary().usersWithProfile());
        assertEquals(1, report.summary().usersHealthy());
        assertEquals(Map.of("missing_profile", 1, "run_mismatch", 1), report.summary().noProfileByReason());
        assertEquals(Map.of("new_or_unknown", 1, "empty_preferences", 1, "preference_without_content", 1),
            report.summary().findingsByType());
        assertEquals(Map.of("genre", Map.of("film-noir", 1)), report.summary().unmatchedPreferences());
        assertEquals(900L, report.summary().minProfileTtlSeconds());
        assertEquals(2, report.summary().catalogSize());
        assertEquals(List.of("missing", "stale", "newbie", "empty", "gap"),
            report.users().stream().map(UserRow::userId).toList());
    }

    @Test
    void unmatchedPreferenceRepeatedInOneProfileIsCountedOncePerUser() {
        FakeStore store = new FakeStore()
            .user("dup", profileJson("dup", "run-7", List.of("film-noir", "film-noir"), List.of(), false), 1200L);

        ProfileAuditReport report = service(store, 500, 1).audit(null);

        assertEquals(Map.of("genre", Map.of("film-noir", 1)), report.summary().unmatchedPreferences());
    }

    @Test
    void missingActiveRunShortCircuitsWithoutPerUserRows() {
        FakeStore store = new FakeStore().user("u1", null, -2L).user("u2", null, -2L);
        store.activeRun = Optional.empty();

        ProfileAuditReport report = service(store, 500, 1).audit(null);

        assertEquals("missing_active_run", report.status());
        assertNull(report.activeRun());
        assertEquals(2, report.summary().usersScanned());
        assertEquals(0, report.summary().usersWithProfile());
        assertEquals(Map.of("missing_active_run", 2), report.summary().noProfileByReason());
        assertTrue(report.users().isEmpty());
        assertTrue(store.chunksSeen.isEmpty());
    }

    @Test
    void chunksFanOutInParallelAndRowsKeepScanOrder() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 7; i++) {
            store.user("u" + i, null, -2L);
        }

        ProfileAuditReport report = service(store, 2, 3).audit(null);

        assertEquals(4, store.chunksSeen.size());
        assertEquals(List.of("u0", "u1", "u2", "u3", "u4", "u5", "u6"),
            report.users().stream().map(UserRow::userId).toList());
    }

    @Test
    void truncationAndLimitArePassedThrough() {
        FakeStore store = new FakeStore().user("u1", null, -2L).user("u2", null, -2L).user("u3", null, -2L);
        store.truncated = true;

        ProfileAuditReport report = service(store, 500, 1).audit(2);

        assertTrue(report.truncated());
        assertEquals(2, report.summary().usersScanned());
    }

    @Test
    void limitOutsideBoundsIsRejected() {
        ProfileAuditService s = service(new FakeStore(), 500, 1);

        assertThrows(IllegalArgumentException.class, () -> s.audit(0));
        assertThrows(IllegalArgumentException.class, () -> s.audit(101));
    }

    @Test
    void oneFailingChunkFailsTheWholeAudit() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 4; i++) {
            store.user("u" + i, null, -2L);
        }
        store.failOnUser = "u2";

        ProfileAuditService.ProfileAuditFailedException e = assertThrows(
            ProfileAuditService.ProfileAuditFailedException.class, () -> service(store, 2, 2).audit(null));

        assertEquals("redis unavailable", e.getMessage());
        assertEquals(2, store.chunksSeen.size());
    }

    @Test
    void secondConcurrentAuditIsRejectedAsBusy() throws Exception {
        FakeStore store = new FakeStore().user("u1", null, -2L);
        store.blockReads = new CountDownLatch(1);
        ProfileAuditService s = service(store, 500, 1);
        ExecutorService runner = Executors.newSingleThreadExecutor();
        try {
            Future<ProfileAuditReport> first = runner.submit(() -> s.audit(null));
            while (store.chunksSeen.isEmpty()) {
                Thread.sleep(5);
            }

            assertThrows(ProfileAuditService.AuditBusyException.class, () -> s.audit(null));

            store.blockReads.countDown();
            assertEquals(1, first.get().summary().usersScanned());
            assertEquals(1, s.audit(null).summary().usersScanned());   // guard released
        } finally {
            runner.shutdownNow();
        }
    }

    @Test
    void neverKeepsMoreThanParallelismChunksInFlight() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 18; i++) {
            store.user("u" + i, null, -2L);
        }
        store.readDelayMillis = 20L;
        CountingPool pool = new CountingPool(2);
        ProfileAuditService s = new ProfileAuditService(store, properties(2, 2), new ObjectMapper(), pool);
        try {
            ProfileAuditReport report = s.audit(null);

            assertEquals(9, store.chunksSeen.size());
            assertEquals(18, report.summary().usersScanned());
            // afterExecute runs after the future has completed and unparked the joining thread, so
            // the main thread can submit the next chunk before the worker decrements outstanding —
            // one unit of slack above parallelism (2+1=3). The reverted batch-submit implementation
            // measured 9 here, so the bound still discriminates a real regression.
            assertTrue(pool.maxOutstanding.get() <= 3, "max outstanding was " + pool.maxOutstanding.get());
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void streamingKeepsRowsInScanOrderAcrossManySmallChunks() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 7; i++) {
            store.user("u" + i, null, -2L);
        }

        ProfileAuditReport report = service(store, 1, 3).audit(null);

        assertEquals(List.of("u0", "u1", "u2", "u3", "u4", "u5", "u6"),
            report.users().stream().map(UserRow::userId).toList());
    }

    @Test
    void aFailureInTheLastChunkStillFailsTheWholeAudit() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 6; i++) {
            store.user("u" + i, null, -2L);
        }
        store.failOnUser = "u5";

        ProfileAuditService.ProfileAuditFailedException e = assertThrows(
            ProfileAuditService.ProfileAuditFailedException.class, () -> service(store, 2, 2).audit(null));

        assertEquals("redis unavailable", e.getMessage());
        assertEquals(3, store.chunksSeen.size());
    }

    @Test
    void stopsSubmittingChunksOnceOneFails() {
        FakeStore store = new FakeStore();
        for (int i = 0; i < 6; i++) {
            store.user("u" + i, null, -2L);
        }
        store.failOnUser = "u0";

        assertThrows(ProfileAuditService.ProfileAuditFailedException.class,
            () -> service(store, 2, 2).audit(null));

        assertEquals(2, store.chunksSeen.size(), "chunks seen: " + store.chunksSeen);
    }

    @Test
    void auditAccountReturnsTheRowForOneHealthyAccount() {
        FakeStore store = new FakeStore()
            .user("u1", profileJson("u1", "run-7", List.of("sci-fi"), List.of("space"), false), 3400L)
            .user("u2", null, -2L);

        AccountAuditReport report = service(store, 500, 4).auditAccount("u1");

        assertEquals("ok", report.status());
        assertEquals("run-7", report.activeRun());
        assertEquals(2, report.catalogSize());
        assertEquals("u1", report.user().userId());
        assertTrue(report.user().hasProfile());
        assertTrue(report.user().findings().isEmpty());
        assertEquals(2, report.user().preferences().size());
        assertEquals(List.of(List.of("u1")), store.chunksSeen);
    }

    @Test
    void auditAccountReportsMissingActiveRun() {
        FakeStore store = new FakeStore().user("u1", null, -2L);
        store.activeRun = Optional.empty();

        AccountAuditReport report = service(store, 500, 4).auditAccount("u1");

        assertEquals("missing_active_run", report.status());
        assertNull(report.activeRun());
        assertEquals("missing_active_run", report.user().findings().get(0).reason());
        assertTrue(store.chunksSeen.isEmpty());
    }

    @Test
    void auditAccountWrapsAStoreFailure() {
        FakeStore store = new FakeStore().user("u1", null, -2L);
        store.failOnUser = "u1";

        ProfileAuditService.ProfileAuditFailedException e = assertThrows(
            ProfileAuditService.ProfileAuditFailedException.class, () -> service(store, 500, 4).auditAccount("u1"));

        assertEquals("redis unavailable", e.getMessage());
    }

    @Test
    @Timeout(10)
    void auditAccountDoesNotQueueBehindABusyBulkAudit() throws Exception {
        FakeStore store = new FakeStore().user("u1", null, -2L).user("u2", null, -2L);
        store.blockReads = new CountDownLatch(1);
        ProfileAuditService s = service(store, 500, 1);
        ExecutorService bulkRunner = Executors.newSingleThreadExecutor();
        ExecutorService accountRunner = Executors.newSingleThreadExecutor();
        try {
            Future<ProfileAuditReport> bulk = bulkRunner.submit(() -> s.audit(null));
            while (store.chunksSeen.isEmpty()) {
                Thread.sleep(5);
            }

            // auditAccount is deliberately unguarded: it must reach the store (proving it was not
            // rejected by the single-flight guard) even while the bulk audit above is still
            // in flight and holding running == true.
            Future<AccountAuditReport> account = accountRunner.submit(() -> s.auditAccount("u2"));
            while (store.chunksSeen.size() < 2) {
                Thread.sleep(5);
            }

            store.blockReads.countDown();

            AccountAuditReport report = account.get();
            assertEquals("ok", report.status());
            assertEquals("u2", report.user().userId());
            assertEquals(2, bulk.get().summary().usersScanned());
        } finally {
            store.blockReads.countDown();
            bulkRunner.shutdownNow();
            accountRunner.shutdownNow();
        }
    }

    @Test
    void reusesThePreferenceIndexUntilTheCatalogIsReplaced() {
        RecommendationProperties properties = properties(500, 4);
        ProfileAuditService s = new ProfileAuditService(new FakeStore(), properties, new ObjectMapper());
        service = s;

        CatalogPreferenceIndex first = s.preferenceIndex();
        assertSame(first, s.preferenceIndex());

        MovieProfile item3 = new MovieProfile();
        item3.setGenres(List.of("comedy"));
        Map<String, MovieProfile> replacement = new LinkedHashMap<>(properties.getCatalog());
        replacement.put("item3", item3);
        properties.setCatalog(replacement);

        CatalogPreferenceIndex rebuilt = s.preferenceIndex();
        assertNotSame(first, rebuilt);
        assertEquals(3, rebuilt.catalogSize());
    }
}
