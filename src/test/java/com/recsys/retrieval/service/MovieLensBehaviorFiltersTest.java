package com.demo.retrieval.service;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.retrieval.MovieCandidate;
import com.demo.retrieval.service.filters.CandidateFilterResult;
import com.demo.retrieval.service.filters.PreviouslySeenMoviesBackupFilter;
import com.demo.retrieval.service.filters.PreviouslySeenMoviesFilter;
import com.demo.retrieval.service.filters.PreviouslyServedMoviesFilter;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MovieLensBehaviorFiltersTest {
    @Test
    void filtersMoviesFromRatingWatchAndActionHistory() {
        MovieLensUserFeatures features = MovieLensUserFeatures.forUser("u1")
            .withActionSequenceMovieIds(List.of("action"));
        ScoredMoviesQuery query = new ScoredMoviesQuery(
            "u1", features, List.of("watched"), List.of("rated"), List.of()
        );

        CandidateFilterResult result = new PreviouslySeenMoviesFilter().filter(
            query,
            candidates("watched", "rated", "action", "fresh")
        );

        assertEquals(List.of("fresh"), ids(result.kept()));
        assertEquals(List.of("watched", "rated", "action"), ids(result.removed()));
    }

    @Test
    void filtersImpressedAndPreviouslyServedMovies() {
        MovieLensUserFeatures features = MovieLensUserFeatures.forUser("u1")
            .withImpressedMovieIds(List.of("impressed"))
            .withServedMovieIds(List.of("served"));
        ScoredMoviesQuery query = new ScoredMoviesQuery("u1", features, List.of(), List.of(), List.of());

        CandidateFilterResult impressed = new PreviouslySeenMoviesBackupFilter()
            .filter(query, candidates("impressed", "served", "fresh"));
        CandidateFilterResult served = new PreviouslyServedMoviesFilter().filter(query, impressed.kept());

        assertEquals(List.of("fresh"), ids(served.kept()));
        assertEquals(List.of("impressed"), ids(impressed.removed()));
        assertEquals(List.of("served"), ids(served.removed()));
    }

    private static List<MovieCandidate> candidates(String... ids) {
        return java.util.Arrays.stream(ids)
            .map(id -> new MovieCandidate(id, 0.0, 0.0, false))
            .toList();
    }

    private static List<String> ids(List<MovieCandidate> candidates) {
        return candidates.stream().map(MovieCandidate::movieId).toList();
    }
}
