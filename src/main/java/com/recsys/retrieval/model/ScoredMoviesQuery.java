package com.demo.retrieval.model;

import java.util.List;

public record ScoredMoviesQuery(
    String userId,
    MovieLensUserFeatures userFeatures,
    List<String> watchedMovieIds,
    List<String> ratedMovieIds,
    List<String> candidateMovieIds,
    List<Integer> genreIds,
    List<Integer> excludedGenreIds,
    boolean bulkTopicRequest,
    boolean excludeVideos
) {
    public ScoredMoviesQuery(
        String userId,
        MovieLensUserFeatures userFeatures,
        List<String> watchedMovieIds,
        List<String> ratedMovieIds,
        List<String> candidateMovieIds
    ) {
        this(userId, userFeatures, watchedMovieIds, ratedMovieIds, candidateMovieIds, List.of(), List.of(), false, false);
    }

    public ScoredMoviesQuery {
        userFeatures = userFeatures == null ? MovieLensUserFeatures.forUser(userId) : userFeatures;
        watchedMovieIds = watchedMovieIds == null ? List.of() : List.copyOf(watchedMovieIds);
        ratedMovieIds = ratedMovieIds == null ? List.of() : List.copyOf(ratedMovieIds);
        candidateMovieIds = candidateMovieIds == null ? List.of() : List.copyOf(candidateMovieIds);
        genreIds = genreIds == null ? List.of() : List.copyOf(genreIds);
        excludedGenreIds = excludedGenreIds == null ? List.of() : List.copyOf(excludedGenreIds);
    }

    public static ScoredMoviesQuery forUser(String userId) {
        return new ScoredMoviesQuery(userId, MovieLensUserFeatures.forUser(userId), List.of(), List.of(), List.of());
    }

    public boolean isGenreRequest() {
        return !genreIds.isEmpty();
    }

    public boolean hasExcludedGenres() {
        return !excludedGenreIds.isEmpty();
    }
}
