package com.demo.retrieval.service.filters;

import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.retrieval.MovieCandidate;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class PreviouslyServedMoviesFilter implements CandidateFilter {
    @Override
    public CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates) {
        Set<String> served = new HashSet<>(query.userFeatures().servedMovieIds());
        if (served.isEmpty()) {
            return new CandidateFilterResult(candidates, List.of());
        }

        Map<Boolean, List<MovieCandidate>> partitioned = candidates.stream()
            .collect(Collectors.partitioningBy(candidate -> served.contains(candidate.movieId())));
        return new CandidateFilterResult(
            partitioned.getOrDefault(Boolean.FALSE, List.of()),
            partitioned.getOrDefault(Boolean.TRUE, List.of())
        );
    }
}
