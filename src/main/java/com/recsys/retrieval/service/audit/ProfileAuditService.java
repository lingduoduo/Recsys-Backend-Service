package com.recsys.retrieval.service.audit;

import com.recsys.retrieval.config.RecommendationProperties;
import com.recsys.retrieval.service.audit.ProfileAuditReport.Finding;
import com.recsys.retrieval.service.audit.ProfileAuditReport.PreferenceRef;
import com.recsys.retrieval.service.audit.ProfileAuditReport.Summary;
import com.recsys.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.recsys.retrieval.service.audit.ProfileAuditStore.ScanResult;
import com.recsys.retrieval.service.content.CatalogContentScoring;
import com.recsys.retrieval.service.content.NormalizedProfile;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Walks user → has_profile → preferences → catalog content, breadth-first by level, and returns
 * a findings-only report. One audit at a time; level-1 chunks fan out over a small dedicated
 * executor where each task owns exactly one pipelined round trip.
 */
@Component
public class ProfileAuditService {

    public static final class AuditBusyException extends RuntimeException {
        public AuditBusyException() {
            super("a profile audit is already running");
        }
    }

    public static final class ProfileAuditFailedException extends RuntimeException {
        public ProfileAuditFailedException(Throwable cause) {
            super(cause.getMessage(), cause);
        }
    }

    private final ProfileAuditStore store;
    private final RecommendationProperties.ProfileAudit config;
    private final CatalogContentScoring catalogScoring;
    private final ObjectMapper objectMapper;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile IndexCache indexCache;

    @Autowired
    public ProfileAuditService(ProfileAuditStore store, RecommendationProperties properties, ObjectMapper objectMapper) {
        this(store, properties, objectMapper, Executors.newFixedThreadPool(properties.getProfileAudit().getParallelism()));
    }

    /**
     * Package-private seam: production always uses the public constructor, which owns its pool.
     * A test injects its own executor to observe how many chunk tasks are submitted — a quantity
     * a fixed pool hides, since it bounds concurrent execution and queues the surplus. Note that
     * {@code shutdown()} will {@code shutdownNow()} whatever executor is passed in.
     */
    ProfileAuditService(ProfileAuditStore store, RecommendationProperties properties, ObjectMapper objectMapper, ExecutorService executor) {
        this.store = store;
        this.config = properties.getProfileAudit();
        this.catalogScoring = new CatalogContentScoring(properties);
        this.objectMapper = objectMapper;
        this.executor = executor;
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
    }

    /**
     * The index is derived from the normalized catalog, which is itself cached on catalog identity,
     * so rebuilding it per request would make a single-account lookup scale with catalog size.
     * Two threads racing here both build a correct index and the loser's is discarded — cheaper
     * than locking a read path that changes only when the catalog is replaced.
     */
    CatalogPreferenceIndex preferenceIndex() {
        Map<String, NormalizedProfile> normalized = catalogScoring.normalizedCatalog();
        IndexCache cache = indexCache;
        if (cache != null && cache.source() == normalized) {
            return cache.index();
        }
        CatalogPreferenceIndex built = CatalogPreferenceIndex.build(normalized);
        indexCache = new IndexCache(normalized, built);
        return built;
    }

    private record IndexCache(Map<String, NormalizedProfile> source, CatalogPreferenceIndex index) {
    }

    /**
     * One account, one round trip. Deliberately unguarded and executor-free: this path is a
     * bounded neighbor lookup, so it must not queue behind a full audit.
     */
    public AccountAuditReport auditAccount(String userId) {
        long started = System.nanoTime();
        try {
            CatalogPreferenceIndex index = preferenceIndex();
            String activeRun = store.activeRun().orElse(null);
            UserRow row = new ProfileAuditWalker(store, objectMapper, index, config.getSampleItems())
                .walk(activeRun, List.of(userId)).get(0);
            return new AccountAuditReport(
                activeRun == null ? ProfileAuditReport.STATUS_MISSING_ACTIVE_RUN : ProfileAuditReport.STATUS_OK,
                activeRun,
                Instant.now().toString(),
                (System.nanoTime() - started) / 1_000_000L,
                index.catalogSize(),
                row);
        } catch (RuntimeException e) {
            if (e instanceof ProfileAuditFailedException) {
                throw e;
            }
            throw new ProfileAuditFailedException(e);
        }
    }

    /** {@code limit} null means {@code max-users}; otherwise it must lie in 1..max-users. */
    public ProfileAuditReport audit(Integer limit) {
        int maxUsers = config.getMaxUsers();
        int effectiveLimit = limit == null ? maxUsers : limit;
        if (effectiveLimit < 1 || effectiveLimit > maxUsers) {
            throw new IllegalArgumentException("limit must be between 1 and " + maxUsers);
        }
        if (!running.compareAndSet(false, true)) {
            throw new AuditBusyException();
        }
        long started = System.nanoTime();
        try {
            CatalogPreferenceIndex index = preferenceIndex();
            ScanResult scan = store.scanUserIds(config.getUserKeyPattern(), effectiveLimit);
            String activeRun = store.activeRun().orElse(null);
            Aggregation aggregation = new Aggregation();

            if (activeRun == null) {
                aggregation.noProfileByReason.put(ProfileAuditReport.STATUS_MISSING_ACTIVE_RUN, scan.userIds().size());
            } else {
                streamRows(activeRun, scan.userIds(), index, aggregation);
            }

            return new ProfileAuditReport(
                activeRun == null ? ProfileAuditReport.STATUS_MISSING_ACTIVE_RUN : ProfileAuditReport.STATUS_OK,
                activeRun,
                Instant.now().toString(),
                (System.nanoTime() - started) / 1_000_000L,
                scan.truncated(),
                aggregation.summary(scan.userIds().size(), index.catalogSize()),
                aggregation.rows
            );
        } catch (RuntimeException e) {
            if (e instanceof ProfileAuditFailedException) {
                throw e;
            }
            throw new ProfileAuditFailedException(e);
        } finally {
            running.set(false);
        }
    }

    /**
     * Streams chunks through the walker with at most {@code parallelism} futures in flight,
     * joining the oldest first so rows stay in scan order and only a bounded number of chunks is
     * ever resident. Failure semantics are unchanged: every in-flight read settles, then the
     * failure hit first in chunk order is rethrown and the whole report is discarded.
     */
    private void streamRows(String activeRun, List<String> userIds, CatalogPreferenceIndex index, Aggregation aggregation) {
        ProfileAuditWalker walker = new ProfileAuditWalker(store, objectMapper, index, config.getSampleItems());
        Deque<CompletableFuture<List<UserRow>>> inFlight = new ArrayDeque<>();
        try {
            for (int from = 0; from < userIds.size(); from += config.getChunkSize()) {
                if (inFlight.size() >= config.getParallelism()) {
                    drainOldest(inFlight, aggregation);
                }
                List<String> chunk = userIds.subList(from, Math.min(from + config.getChunkSize(), userIds.size()));
                inFlight.add(CompletableFuture.supplyAsync(() -> walker.walk(activeRun, chunk), executor));
            }
            while (!inFlight.isEmpty()) {
                drainOldest(inFlight, aggregation);
            }
        } catch (ProfileAuditFailedException e) {
            CompletableFuture.allOf(inFlight.toArray(CompletableFuture[]::new)).exceptionally(t -> null).join();
            throw e;
        }
    }

    private static void drainOldest(Deque<CompletableFuture<List<UserRow>>> inFlight, Aggregation aggregation) {
        CompletableFuture<List<UserRow>> oldest = inFlight.poll();
        try {
            oldest.join().forEach(aggregation::add);
        } catch (CompletionException e) {
            throw new ProfileAuditFailedException(e.getCause() == null ? e : e.getCause());
        }
    }

    private static final class Aggregation {
        final List<UserRow> rows = new ArrayList<>();
        final Map<String, Integer> noProfileByReason = new TreeMap<>();
        final Map<String, Integer> findingsByType = new TreeMap<>();
        final Map<String, Map<String, Integer>> unmatchedPreferences = new TreeMap<>();
        int usersWithProfile;
        int usersHealthy;
        Long minTtl;

        void add(UserRow row) {
            if (row.hasProfile()) {
                usersWithProfile++;
                if (row.ttlSeconds() != null && (minTtl == null || row.ttlSeconds() < minTtl)) {
                    minTtl = row.ttlSeconds();
                }
            }
            if (row.findings().isEmpty()) {
                usersHealthy++;
                return;
            }
            rows.add(row);
            for (Finding finding : row.findings()) {
                if (ProfileAuditReport.FINDING_NO_PROFILE.equals(finding.type())) {
                    noProfileByReason.merge(finding.reason(), 1, Integer::sum);
                    continue;
                }
                findingsByType.merge(finding.type(), 1, Integer::sum);
                if (finding.preferences() != null) {
                    for (PreferenceRef ref : new LinkedHashSet<>(finding.preferences())) {
                        unmatchedPreferences.computeIfAbsent(ref.kind(), k -> new TreeMap<>()).merge(ref.value(), 1, Integer::sum);
                    }
                }
            }
        }

        Summary summary(int usersScanned, int catalogSize) {
            return new Summary(usersScanned, usersWithProfile, usersHealthy,
                noProfileByReason, findingsByType, unmatchedPreferences, minTtl, catalogSize);
        }
    }
}
