package com.demo.retrieval.service.retrieval;

public record MovieCandidate(
    String movieId,
    double popularityScore,
    double contentScore,
    boolean coldStartSource
) {
    /** Merge two candidates for the same movie: keep the stronger signals and any cold-start flag. */
    public static MovieCandidate merge(MovieCandidate left, MovieCandidate right) {
        return new MovieCandidate(
            left.movieId(),
            Math.max(left.popularityScore(), right.popularityScore()),
            Math.max(left.contentScore(), right.contentScore()),
            left.coldStartSource() || right.coldStartSource()
        );
    }
}
