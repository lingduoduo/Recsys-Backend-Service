package com.demo.retrieval.service;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.query_hydrators.UserMovieFeaturesQueryHydrator;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class UserMovieFeaturesQueryHydratorTest {

    @Test
    void hydrateAddsFetchedUserFeatures() {
        MovieLensUserFeatures fetched = new MovieLensUserFeatures(
            "u1",
            List.of("sci-fi", "adventure"),
            4.25,
            12,
            List.of("m1", "m2")
        );
        UserMovieFeaturesQueryHydrator hydrator = new UserMovieFeaturesQueryHydrator(
            userId -> Optional.of(fetched)
        );
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            MovieLensUserFeatures.forUser("u1"),
            List.of("watched"),
            List.of("rated"),
            List.of("candidate")
        );

        ScoredMoviesQuery hydrated = hydrator.hydrate(query);

        assertEquals(fetched, hydrated.userFeatures());
        assertEquals(query.watchedMovieIds(), hydrated.watchedMovieIds());
        assertEquals(query.ratedMovieIds(), hydrated.ratedMovieIds());
        assertEquals(query.candidateMovieIds(), hydrated.candidateMovieIds());
    }

    @Test
    void hydrateFallsBackToEmptyFeaturesWhenClientMisses() {
        UserMovieFeaturesQueryHydrator hydrator = new UserMovieFeaturesQueryHydrator(
            userId -> Optional.empty()
        );
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser("new-user");

        ScoredMoviesQuery hydrated = hydrator.hydrate(query);

        assertEquals(MovieLensUserFeatures.forUser("new-user"), hydrated.userFeatures());
    }

    @Test
    void updateCopiesOnlyHydratedFeatures() {
        MovieLensUserFeatures originalFeatures = new MovieLensUserFeatures(
            "u1",
            List.of("drama"),
            3.5,
            3,
            List.of("old")
        );
        MovieLensUserFeatures hydratedFeatures = new MovieLensUserFeatures(
            "u1",
            List.of("comedy"),
            4.8,
            20,
            List.of("new")
        );
        UserMovieFeaturesQueryHydrator hydrator = new UserMovieFeaturesQueryHydrator(
            userId -> Optional.of(hydratedFeatures)
        );
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1",
            originalFeatures,
            List.of("watched"),
            List.of("rated"),
            List.of("candidate")
        );
        ScoredMoviesQuery hydrated = new ScoredMoviesQuery(
            "u1",
            hydratedFeatures,
            List.of("other-watched"),
            List.of("other-rated"),
            List.of("other-candidate")
        );

        ScoredMoviesQuery updated = hydrator.update(query, hydrated);

        assertEquals(hydratedFeatures, updated.userFeatures());
        assertEquals(query.watchedMovieIds(), updated.watchedMovieIds());
        assertEquals(query.ratedMovieIds(), updated.ratedMovieIds());
        assertEquals(query.candidateMovieIds(), updated.candidateMovieIds());
    }
}
