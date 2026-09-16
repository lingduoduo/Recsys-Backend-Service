package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.clients.MovieLensFeatureClient;
import com.demo.retrieval.service.sequence.SequenceClient;
import com.demo.retrieval.service.sequence.SequenceSchemaConstants;
import com.demo.retrieval.service.sequence.SequenceSlice;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Hydrates all three rating-sequence fields in a single feature-store read.
 *
 * The three sequences are length views over one deduped rating sequence:
 *
 *   actionSequenceMovieIds    — 50 items   (sequential model input)
 *   retrievalSequenceMovieIds — 100 items  (ANN candidate retrieval)
 *   scoringSequenceMovieIds   — 20 items   (ranking model input)
 *
 * The source is either the legacy {@code user:{id}:features} CSV blob or the columnar
 * sequence store, selected by {@code recsys.sequence.mode}:
 *   off    — legacy only
 *   shadow — read both, serve legacy, log the diff
 *   on     — serve the sequence store, falling back to legacy only on error
 */
public class RatingSequencesQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MAX_ACTION_SEQ_LENGTH = 50;
    public static final int MAX_RETRIEVAL_SEQ_LENGTH = 100;
    public static final int MAX_SCORING_SEQ_LENGTH = 20;

    public static final String MODE_OFF = "off";
    public static final String MODE_SHADOW = "shadow";
    public static final String MODE_ON = "on";

    private static final Logger log = LoggerFactory.getLogger(RatingSequencesQueryHydrator.class);
    private static final Set<String> READ_COLUMNS =
        Set.of(SequenceSchemaConstants.COL_ITEM_ID, SequenceSchemaConstants.COL_TS);

    private final MovieLensFeatureClient featureClient;
    private final SequenceClient sequenceClient;
    private final String mode;
    private final Duration lookback;

    public RatingSequencesQueryHydrator(MovieLensFeatureClient featureClient) {
        this(featureClient, null, MODE_OFF, 90);
    }

    public RatingSequencesQueryHydrator(
        MovieLensFeatureClient featureClient,
        SequenceClient sequenceClient,
        String mode,
        int lookbackDays
    ) {
        this.featureClient = featureClient;
        this.sequenceClient = sequenceClient;
        this.mode = resolveMode(mode);
        this.lookback = Duration.ofDays(Math.max(1, lookbackDays));
    }

    private static String resolveMode(String mode) {
        String normalized = mode == null ? MODE_OFF : mode.trim().toLowerCase();
        if (MODE_OFF.equals(normalized) || MODE_SHADOW.equals(normalized) || MODE_ON.equals(normalized)) {
            return normalized;
        }
        log.warn("Unrecognized recsys.sequence.mode '{}', treating as '{}'", mode, MODE_OFF);
        return MODE_OFF;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        List<String> legacy = dedupe(featureClient.getUserFeatures(query.userId())
            .map(MovieLensUserFeatures::recentlyRatedMovieIds)
            .orElseGet(List::of));

        List<String> source = legacy;
        if (!MODE_OFF.equals(mode) && sequenceClient != null) {
            try {
                List<String> fromStore = dedupe(readSequence(query.userId()));
                if (MODE_SHADOW.equals(mode)) {
                    logDiff(query.userId(), legacy, fromStore);
                } else {
                    source = fromStore;
                }
            } catch (RuntimeException e) {
                log.warn("Sequence store read failed for user {}, using legacy path: {}",
                    query.userId(), e.getMessage());
            }
        }

        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures()
                .withActionSequenceMovieIds(truncate(source, MAX_ACTION_SEQ_LENGTH))
                .withRetrievalSequenceMovieIds(truncate(source, MAX_RETRIEVAL_SEQ_LENGTH))
                .withScoringSequenceMovieIds(truncate(source, MAX_SCORING_SEQ_LENGTH)),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        MovieLensUserFeatures hf = hydrated.userFeatures();
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures()
                .withActionSequenceMovieIds(hf.actionSequenceMovieIds())
                .withRetrievalSequenceMovieIds(hf.retrievalSequenceMovieIds())
                .withScoringSequenceMovieIds(hf.scoringSequenceMovieIds()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    private List<String> readSequence(String userId) {
        SequenceSlice slice = sequenceClient.read(
            userId, SequenceSchemaConstants.KIND_RATING, READ_COLUMNS, MAX_RETRIEVAL_SEQ_LENGTH, lookback
        );
        return slice.itemIds();
    }

    private void logDiff(String userId, List<String> legacy, List<String> fromStore) {
        int prefix = 0;
        while (prefix < legacy.size() && prefix < fromStore.size()
            && legacy.get(prefix).equals(fromStore.get(prefix))) {
            prefix++;
        }
        log.info("sequence-shadow user={} legacyLen={} storeLen={} prefixAgreement={} firstDivergence={}",
            userId, legacy.size(), fromStore.size(), prefix,
            prefix == Math.min(legacy.size(), fromStore.size()) ? -1 : prefix);
    }

    private static List<String> dedupe(List<String> items) {
        return List.copyOf(new LinkedHashSet<>(items));
    }

    private static List<String> truncate(List<String> items, int maxLen) {
        return items.size() > maxLen ? List.copyOf(items.subList(0, maxLen)) : items;
    }
}
