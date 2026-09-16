package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.service.clients.UserProfileClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class UserBehaviorProfileQueryHydratorTest {
    @Test
    void hydratesPositiveFinitePreferencesAsNormalizedScoreOrderedWeights() {
        UserBehaviorProfileQueryHydrator hydrator = new UserBehaviorProfileQueryHydrator(
            userId -> Optional.of(profile(userId))
        );

        MovieLensUserFeatures features = hydrator.hydrate(ScoredMoviesQuery.forUser("u1")).userFeatures();

        assertEquals(Map.of("sci-fi", 1.0, "comedy", 0.5, "drama", 0.5), features.genrePreferences());
        assertEquals(List.of("sci-fi", "comedy", "drama"), List.copyOf(features.genrePreferences().keySet()));
        assertEquals(Map.of("space opera", 0.75, "cult", 0.25), features.tagPreferences());
        assertEquals(List.of("space opera", "cult"), List.copyOf(features.tagPreferences().keySet()));
    }

    @Test
    void missingProfileLeavesTheQueryUnchanged() {
        UserProfileClient missingProfile = userId -> Optional.empty();
        UserBehaviorProfileQueryHydrator hydrator = new UserBehaviorProfileQueryHydrator(missingProfile);
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1").withActionSequenceMovieIds(List.of("m1")),
            List.of("watched"), List.of("rated"), List.of("candidate"),
            List.of(1), List.of(2), true, true
        );

        assertSame(query, hydrator.hydrate(query));
    }

    @Test
    void updateMergesBehaviorPreferencesWithoutReplacingOtherHydratedFeatures() {
        UserBehaviorProfileQueryHydrator hydrator = new UserBehaviorProfileQueryHydrator(
            userId -> Optional.of(profile(userId))
        );
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1").withActionSequenceMovieIds(List.of("m1")),
            List.of(), List.of(), List.of(), List.of(1), List.of(), true, false
        );
        ScoredMoviesQuery hydrated = hydrator.hydrate(query);

        ScoredMoviesQuery updated = hydrator.update(query, hydrated);

        assertEquals(List.of("m1"), updated.userFeatures().actionSequenceMovieIds());
        assertEquals(Map.of("sci-fi", 1.0, "comedy", 0.5, "drama", 0.5), updated.userFeatures().genrePreferences());
        assertEquals(List.of(1), updated.genreIds());
        assertEquals(true, updated.bulkTopicRequest());
    }

    private static UserBehaviorProfile profile(String userId) {
        return new UserBehaviorProfile(
            userId, 1, "run-1", "2026-08-06T00:00:00Z",
            new UserBehaviorProfile.SourceWindow("2026-07-01T00:00:00Z", "2026-08-06T00:00:00Z"),
            10,
            new UserBehaviorProfile.Preferences(
                List.of(
                    new UserBehaviorProfile.Preference(" Drama ", 0.5, 2),
                    new UserBehaviorProfile.Preference("SCI-FI", 1.5, 3),
                    new UserBehaviorProfile.Preference(" comedy ", 0.5, 1),
                    new UserBehaviorProfile.Preference("ignored-zero", 0.0, 1),
                    new UserBehaviorProfile.Preference("ignored-nan", Double.NaN, 1),
                    new UserBehaviorProfile.Preference("ignored-infinity", Double.POSITIVE_INFINITY, 1)
                ),
                List.of(
                    new UserBehaviorProfile.Preference(" Space Opera ", 0.75, 2),
                    new UserBehaviorProfile.Preference("cult", 0.25, 1),
                    new UserBehaviorProfile.Preference("ignored-negative", -0.2, 1)
                )
            ),
            null,
            List.of()
        );
    }
}
