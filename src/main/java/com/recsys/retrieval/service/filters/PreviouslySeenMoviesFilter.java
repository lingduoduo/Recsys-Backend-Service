package com.demo.retrieval.service.filters;

import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.retrieval.MovieCandidate;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PreviouslySeenMoviesFilter implements CandidateFilter {
    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Set<String> seen = new HashSet<>(query.watchedMovieIds());
        seen.addAll(query.ratedMovieIds());
        seen.addAll(query.userFeatures().recentlyRatedMovieIds());
        seen.addAll(query.userFeatures().actionSequenceMovieIds());
        seen.addAll(query.userFeatures().cachedMovieIds());

        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate -> seen.contains(candidate.movieId())));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.FALSE, List.of()),
            partitioned.getOrDefault(Boolean.TRUE, List.of())
        );
    }
}
