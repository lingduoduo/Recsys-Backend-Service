package com.demo.retrieval.config;

import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.query_hydrators.BehaviorSequencesQueryHydrator;
import com.demo.retrieval.service.query_hydrators.MovieLensUserHistoryQueryHydrator;
import com.demo.retrieval.service.query_hydrators.QueryHydrator;
import com.demo.retrieval.service.sequence.SequenceClient;
import com.demo.retrieval.service.sequence.SequenceSlice;
import org.junit.jupiter.api.Test;
import org.springframework.core.annotation.AnnotationAwareOrderComparator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SequenceConfigTest {

    /** Any read at all is a failure of the flag under test, so make one impossible to miss. */
    private static final SequenceClient THROWING = new SequenceClient() {
        @Override
        public SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback) {
            throw new AssertionError("behavior hydration must not read while its own mode is off");
        }
    };

    private static final SequenceClient EMPTY = new SequenceClient() {
        @Override
        public SequenceSlice read(String userId, String kind, Set<String> columns, int maxRows, Duration lookback) {
            return SequenceSlice.empty();
        }
    };

    /**
     * MovieLensUserHistoryQueryHydrator.update REPLACES watchedMovieIds rather than merging, so a
     * behavior hydrator that ran before it would have its merge silently discarded. Ordering comes
     * from Spring's List injection, which honours @Order — this pins that the annotation is
     * actually there and actually sorts last.
     */
    @Test
    void theBehaviorHydratorSortsAfterTheHydratorThatReplacesWatchedHistory() {
        QueryHydrator<ScoredMoviesQuery> behavior =
            new SequenceConfig().behaviorSequencesQueryHydrator(EMPTY, new RecommendationProperties());
        QueryHydrator<ScoredMoviesQuery> history =
            new MovieLensUserHistoryQueryHydrator(userId -> null);

        List<QueryHydrator<ScoredMoviesQuery>> hydrators = new ArrayList<>(List.of(behavior, history));
        AnnotationAwareOrderComparator.sort(hydrators);

        assertEquals(history, hydrators.get(0));
        assertEquals(behavior, hydrators.get(1));
    }

    @Test
    void theBehaviorHydratorDefaultsToOffSoItReadsNothingUntilTheRolloutSaysOtherwise() {
        RecommendationProperties properties = new RecommendationProperties();

        assertTrue(new SequenceConfig()
            .behaviorSequencesQueryHydrator(EMPTY, properties) instanceof BehaviorSequencesQueryHydrator);
        assertEquals("off", properties.getSequence().getBehaviorMode());
    }

    /**
     * Advancing the pre-existing rating rollout must not switch behavior hydration on behind it.
     * The two read different sequences into different fields and carry different risk.
     */
    @Test
    void theRatingRolloutDoesNotDragBehaviorHydrationAlongWithIt() {
        RecommendationProperties properties = new RecommendationProperties();
        properties.getSequence().setMode("on");

        assertEquals("off", properties.getSequence().getBehaviorMode());
        assertEquals(List.of(),
            new SequenceConfig().behaviorSequencesQueryHydrator(THROWING, properties)
                .hydrate(ScoredMoviesQuery.forUser("u1")).watchedMovieIds());
    }
}
