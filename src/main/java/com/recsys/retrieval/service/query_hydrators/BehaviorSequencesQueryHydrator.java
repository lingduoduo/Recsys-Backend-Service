package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.sequence.SequenceClient;
import com.demo.retrieval.service.sequence.SequenceSchemaConstants;
import com.demo.retrieval.service.sequence.SequenceSlice;
import org.springframework.core.Ordered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Turns the unified {@code behavior} sequence into recent watched history.
 *
 * The sequence holds every behavioral action a user took — searches included — so this
 * adapter keeps only the ones that name an item the user showed intent toward, and drops the
 * empty sentinel a search occupies to keep the columnar rows aligned.
 *
 * The first slice deliberately writes into the existing {@code watchedMovieIds} view rather
 * than adding a field to the public query, so nothing downstream has to change to benefit.
 * Selected by {@code recsys.sequence.mode}, the same switch {@link RatingSequencesQueryHydrator}
 * reads:
 *   off    — legacy watched history only, no read at all
 *   shadow — read and log the comparison, still serve legacy
 *   on     — behavior history first, then legacy, de-duplicated and bounded
 */
public class BehaviorSequencesQueryHydrator implements QueryHydrator<ScoredMoviesQuery>, Ordered {

    public static final String MODE_OFF = "off";
    public static final String MODE_SHADOW = "shadow";
    public static final String MODE_ON = "on";

    /**
     * Actions that count as watch history.
     *
     * A search names a query rather than a movie, so it is out. So is {@code result_view}, which
     * is an impression: {@code watchedMovieIds} is a hard exclusion set in
     * {@code PreviouslySeenMoviesFilter}, so treating "appeared in a slate" as "watched" would
     * permanently suppress every item a search ever surfaced. Impression suppression already has
     * its own home in {@code ImpressionBloomFilterQueryHydrator}.
     */
    private static final Set<String> ENGAGEMENT_ACTIONS = Set.of("detail_view", "click");

    /**
     * How many sequence rows to read per item wanted.
     *
     * The behavior sequence interleaves four actions and only two of them are engagement, so a
     * read of exactly {@code maxItems} rows comes back holding a fraction of {@code maxItems}
     * items. One search journey is a search, its result views, a detail view and a click — a
     * little over four rows per engagement item — so this reads four times the budget and still
     * lets the item bound, not the row bound, decide what is returned.
     */
    public static final int ROW_OVERSCAN = 4;

    private static final Logger log = LoggerFactory.getLogger(BehaviorSequencesQueryHydrator.class);
    private static final Set<String> READ_COLUMNS = Set.of(
        SequenceSchemaConstants.COL_ITEM_ID,
        SequenceSchemaConstants.COL_TS,
        SequenceSchemaConstants.COL_ACTION);

    private final SequenceClient sequenceClient;
    private final String mode;
    private final Duration lookback;
    private final int maxItems;

    public BehaviorSequencesQueryHydrator(
        SequenceClient sequenceClient,
        String mode,
        int lookbackDays,
        int maxItems
    ) {
        this.sequenceClient = sequenceClient;
        this.mode = resolveMode(mode);
        this.lookback = Duration.ofDays(Math.max(1, lookbackDays));
        this.maxItems = Math.max(1, maxItems);
    }

    private static String resolveMode(String mode) {
        String normalized = mode == null ? MODE_OFF : mode.trim().toLowerCase();
        if (MODE_OFF.equals(normalized) || MODE_SHADOW.equals(normalized) || MODE_ON.equals(normalized)) {
            return normalized;
        }
        log.warn("Unrecognized recsys.sequence.behavior-mode '{}', treating as '{}'", mode, MODE_OFF);
        return MODE_OFF;
    }

    /**
     * Last in the hydration chain.
     *
     * {@code MovieLensUserHistoryQueryHydrator.update} *replaces* {@code watchedMovieIds} rather
     * than merging into it, so a behavior merge performed before it runs is silently discarded.
     * Ordering otherwise falls out of Spring's registration order, which is not something to
     * leave a feature's correctness resting on. Declared on the class rather than on the
     * {@code @Bean} method so the guarantee travels with the object.
     */
    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        List<String> watched = query.watchedMovieIds();

        if (!MODE_OFF.equals(mode) && sequenceClient != null) {
            try {
                List<String> fromStore = readBehaviorItems(query.userId());
                if (MODE_SHADOW.equals(mode)) {
                    ShadowDiff diff = shadowDiff(fromStore, query.watchedMovieIds());
                    log.info("behavior-shadow user={} legacyLen={} behaviorLen={} newItems={} "
                            + "overlap={} mergedLen={}",
                        query.userId(), diff.legacyLen(), diff.behaviorLen(), diff.newItems(),
                        diff.overlap(), diff.mergedLen());
                } else {
                    watched = merge(fromStore, query.watchedMovieIds());
                }
            } catch (RuntimeException e) {
                log.warn("Behavior sequence read failed for user {}, using legacy history: {}",
                    query.userId(), e.getMessage());
            }
        }

        return withWatched(query, watched);
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return withWatched(query, hydrated.watchedMovieIds());
    }

    private static ScoredMoviesQuery withWatched(ScoredMoviesQuery query, List<String> watched) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures(),
            watched,
            query.ratedMovieIds(),
            query.candidateMovieIds(),
            query.genreIds(),
            query.excludedGenreIds(),
            query.bulkTopicRequest(),
            query.excludeVideos()
        );
    }

    /**
     * The item ids of engagement actions, newest first.
     *
     * The item and action columns are positionally aligned, so a slice whose columns disagree
     * in length is read only as far as the shorter one rather than throwing on the first row
     * that has no partner.
     */
    private List<String> readBehaviorItems(String userId) {
        SequenceSlice slice = sequenceClient.read(
            userId, SequenceSchemaConstants.KIND_BEHAVIOR, READ_COLUMNS,
            maxItems * ROW_OVERSCAN, lookback);
        List<String> itemIds = slice.itemIds();
        List<String> actions = slice.column(SequenceSchemaConstants.COL_ACTION);

        List<String> out = new ArrayList<>(Math.min(itemIds.size(), actions.size()));
        for (int i = 0; i < itemIds.size() && i < actions.size(); i++) {
            String itemId = itemIds.get(i);
            if (itemId != null && !itemId.isBlank() && ENGAGEMENT_ACTIONS.contains(actions.get(i))) {
                out.add(itemId);
            }
        }
        return out;
    }

    /**
     * What flipping this hydrator to {@code on} would change, for one user.
     *
     * The rollout is "enable shadow, compare, then enable on", and a pair of lengths cannot
     * answer that: behavior history is merged *in front of* legacy rather than replacing it, so
     * what an operator needs is how much genuinely new history would be prepended and how much
     * the two sources already agree on. Split out from the log call so it can be asserted
     * directly rather than through a captured log line.
     */
    public record ShadowDiff(int legacyLen, int behaviorLen, int newItems, int overlap, int mergedLen) {
    }

    ShadowDiff shadowDiff(List<String> behavior, List<String> legacy) {
        List<String> merged = merge(behavior, legacy);
        Set<String> legacyItems = Set.copyOf(legacy);
        int overlap = 0;
        for (String item : new LinkedHashSet<>(behavior)) {
            if (legacyItems.contains(item)) {
                overlap++;
            }
        }
        return new ShadowDiff(
            legacy.size(), behavior.size(), merged.size() - legacy.size(), overlap, merged.size());
    }

    /**
     * Behavior first, then whatever legacy history the query already carried.
     *
     * {@code maxItems} bounds the behavior contribution, not the merged total. Bounding the total
     * would let a burst of recent clicks evict the legacy watched history entirely — and since
     * this list is an exclusion set, evicting it puts genuinely watched movies back into the
     * recommendations they were being kept out of.
     */
    private List<String> merge(List<String> behavior, List<String> legacy) {
        LinkedHashSet<String> merged = new LinkedHashSet<>(
            behavior.size() > maxItems ? behavior.subList(0, maxItems) : behavior);
        merged.addAll(legacy);
        return List.copyOf(merged);
    }
}
