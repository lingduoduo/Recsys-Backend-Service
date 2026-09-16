package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.sequence.SequenceClient;
import com.demo.retrieval.service.sequence.SequenceSchemaConstants;
import com.demo.retrieval.service.sequence.SequenceSlice;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BehaviorSequencesQueryHydratorTest {

    /** Serves aligned item/ts/action columns and records what was asked of it. */
    private static final class RecordingSequenceClient implements SequenceClient {
        Set<String> requestedColumns;
        String requestedKind;
        int requestedMaxRows;
        Duration requestedLookback;
        final List<String> itemIds;
        final List<String> actions;

        RecordingSequenceClient(List<String> itemIds, List<String> actions) {
            this.itemIds = itemIds;
            this.actions = actions;
        }

        @Override
        public SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback) {
            this.requestedColumns = columns;
            this.requestedKind = kind;
            this.requestedMaxRows = maxRows;
            this.requestedLookback = lookback;
            int rows = Math.min(maxRows, itemIds.size());
            List<String> timestamps = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                timestamps.add(String.valueOf(1_000_000L - i));
            }
            Map<String, List<String>> columnMap = new LinkedHashMap<>();
            columnMap.put(SequenceSchemaConstants.COL_ITEM_ID, itemIds.subList(0, rows));
            columnMap.put(SequenceSchemaConstants.COL_TS, timestamps);
            columnMap.put(SequenceSchemaConstants.COL_ACTION, actions.subList(0, rows));
            return new SequenceSlice(columnMap, rows);
        }
    }

    private static final class FailingSequenceClient implements SequenceClient {
        @Override
        public SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback) {
            throw new IllegalStateException("redis is down");
        }
    }

    /** Newest first: a click on m2, a duplicate click on m1, the search that found them, a detail view. */
    private static RecordingSequenceClient behaviorClient() {
        return new RecordingSequenceClient(
            List.of("m2", "m1", "", "m1"),
            List.of("click", "click", "search", "detail_view"));
    }

    private static ScoredMoviesQuery queryWithWatched(String... watched) {
        ScoredMoviesQuery base = ScoredMoviesQuery.forUser("u1");
        return new ScoredMoviesQuery(
            base.userId(), base.userFeatures(), List.of(watched), List.of(), List.of());
    }

    @Test
    void onModePrependsBehaviorItemsAheadOfLegacyWatchedHistory() {
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(behaviorClient(), "on", 90, 100);

        assertEquals(List.of("m2", "m1", "legacy"),
            hydrator.hydrate(queryWithWatched("legacy")).watchedMovieIds());
    }

    @Test
    void offModeNeverReadsTheSequenceStore() {
        RecordingSequenceClient client = behaviorClient();
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(client, "off", 90, 100);

        assertEquals(List.of("legacy"), hydrator.hydrate(queryWithWatched("legacy")).watchedMovieIds());
        assertNull(client.requestedColumns, "sequence store must not be read in off mode");
    }

    @Test
    void shadowModeReadsTheStoreButStillServesLegacyHistory() {
        RecordingSequenceClient client = behaviorClient();
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(client, "shadow", 90, 100);

        assertEquals(List.of("legacy"), hydrator.hydrate(queryWithWatched("legacy")).watchedMovieIds());
        assertEquals(SequenceSchemaConstants.KIND_BEHAVIOR, client.requestedKind);
    }

    @Test
    void readsOnlyTheColumnsItActuallyUses() {
        RecordingSequenceClient client = behaviorClient();

        new BehaviorSequencesQueryHydrator(client, "on", 30, 100).hydrate(queryWithWatched());

        assertEquals(
            Set.of(SequenceSchemaConstants.COL_ITEM_ID, SequenceSchemaConstants.COL_TS,
                SequenceSchemaConstants.COL_ACTION),
            client.requestedColumns);
        assertEquals(Duration.ofDays(30), client.requestedLookback);
        assertEquals(100 * BehaviorSequencesQueryHydrator.ROW_OVERSCAN, client.requestedMaxRows);
    }

    @Test
    void onModeDropsTheSearchSentinelAndKeepsNewestFirstOrder() {
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(behaviorClient(), "on", 90, 100);

        List<String> watched = hydrator.hydrate(queryWithWatched()).watchedMovieIds();

        assertEquals(List.of("m2", "m1"), watched);
        assertTrue(watched.stream().noneMatch(String::isBlank));
    }

    @Test
    void onModeIgnoresActionsThatCarryNoItemIntent() {
        RecordingSequenceClient client = new RecordingSequenceClient(
            List.of("m1", "m2", "m3"),
            List.of("detail_view", "impression", "abandon"));
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(client, "on", 90, 100);

        assertEquals(List.of("m1"), hydrator.hydrate(queryWithWatched()).watchedMovieIds());
    }

    @Test
    void aResultViewIsNotWatchHistory() {
        // watchedMovieIds is a hard exclusion set in PreviouslySeenMoviesFilter. An item the
        // user merely saw in a search slate must not become permanently unrecommendable.
        RecordingSequenceClient client = new RecordingSequenceClient(
            List.of("seen-only", "opened", "clicked"),
            List.of("result_view", "detail_view", "click"));
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(client, "on", 90, 100);

        assertEquals(List.of("opened", "clicked"),
            hydrator.hydrate(queryWithWatched()).watchedMovieIds());
    }

    @Test
    void behaviorNeverEvictsLegacyWatchedHistory() {
        // Truncating behavior ++ legacy to maxItems would let a burst of recent clicks push
        // genuinely watched movies out of the exclusion set and back into recommendations.
        RecordingSequenceClient client = new RecordingSequenceClient(
            List.of("b1", "b2", "b3"), List.of("click", "click", "click"));
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(client, "on", 90, 2);

        assertEquals(List.of("b1", "b2", "legacy1", "legacy2"),
            hydrator.hydrate(queryWithWatched("legacy1", "legacy2")).watchedMovieIds());
    }

    @Test
    void overscansTheReadSoSearchRowsDoNotStarveTheItemBudget() {
        // Only 2 of the 4 behavioral actions count as engagement, so asking for exactly maxItems
        // rows would return a fraction of maxItems items whenever the window holds searches.
        RecordingSequenceClient client = behaviorClient();

        new BehaviorSequencesQueryHydrator(client, "on", 90, 25).hydrate(queryWithWatched());

        assertEquals(25 * BehaviorSequencesQueryHydrator.ROW_OVERSCAN, client.requestedMaxRows);
    }

    @Test
    void fillsTheItemBudgetFromASearchHeavyWindow() {
        List<String> itemIds = new ArrayList<>();
        List<String> actions = new ArrayList<>();
        for (int journey = 0; journey < 20; journey++) {
            itemIds.add("");
            actions.add("search");
            itemIds.add("m" + journey);
            actions.add("result_view");
            itemIds.add("m" + journey);
            actions.add("click");
        }
        BehaviorSequencesQueryHydrator hydrator = new BehaviorSequencesQueryHydrator(
            new RecordingSequenceClient(itemIds, actions), "on", 90, 20);

        assertEquals(20, hydrator.hydrate(queryWithWatched()).watchedMovieIds().size());
    }

    @Test
    void shadowModeReportsWhatFlippingToOnWouldActuallyChange() {
        // The rollout plan is "enable shadow, compare, then enable on". Two lengths cannot answer
        // that question; the operator needs to know how much new history would be prepended.
        RecordingSequenceClient client = new RecordingSequenceClient(
            List.of("new1", "already", "new2"),
            List.of("click", "click", "detail_view"));
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(client, "shadow", 90, 100);

        BehaviorSequencesQueryHydrator.ShadowDiff diff =
            hydrator.shadowDiff(List.of("new1", "already", "new2"), List.of("already", "legacy"));

        assertEquals(2, diff.legacyLen());
        assertEquals(3, diff.behaviorLen());
        assertEquals(2, diff.newItems());
        assertEquals(1, diff.overlap());
        assertEquals(4, diff.mergedLen());
    }

    @Test
    void shadowDiffReportsNoChangeWhenBehaviorAddsNothingNew() {
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(behaviorClient(), "shadow", 90, 100);

        BehaviorSequencesQueryHydrator.ShadowDiff diff =
            hydrator.shadowDiff(List.of("a", "b"), List.of("a", "b", "c"));

        assertEquals(0, diff.newItems());
        assertEquals(2, diff.overlap());
        assertEquals(3, diff.mergedLen());
    }

    @Test
    void aFailedReadRetainsLegacyHistory() {
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(new FailingSequenceClient(), "on", 90, 100);

        assertEquals(List.of("legacy"), hydrator.hydrate(queryWithWatched("legacy")).watchedMovieIds());
    }

    @Test
    void onModeBoundsTheBehaviorContributionToTheConfiguredMaximum() {
        RecordingSequenceClient client = new RecordingSequenceClient(
            List.of("m1", "m2", "m3"), List.of("click", "click", "click"));
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(client, "on", 90, 2);

        assertEquals(List.of("m1", "m2", "legacy"),
            hydrator.hydrate(queryWithWatched("legacy")).watchedMovieIds());
    }

    @Test
    void updateCarriesOnlyTheWatchedHistoryBack() {
        BehaviorSequencesQueryHydrator hydrator =
            new BehaviorSequencesQueryHydrator(behaviorClient(), "on", 90, 100);
        ScoredMoviesQuery original = queryWithWatched("legacy");
        ScoredMoviesQuery hydrated = hydrator.hydrate(original);

        ScoredMoviesQuery updated = hydrator.update(original, hydrated);

        assertEquals(hydrated.watchedMovieIds(), updated.watchedMovieIds());
        assertEquals(original.userFeatures(), updated.userFeatures());
        assertEquals(original.ratedMovieIds(), updated.ratedMovieIds());
        assertEquals(original.candidateMovieIds(), updated.candidateMovieIds());
    }
}
