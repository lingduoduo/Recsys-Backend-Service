package com.recsys.retrieval.service.filters;

import com.recsys.retrieval.model.ScoredMoviesQuery;
import com.recsys.retrieval.service.retrieval.MovieCandidate;

import java.util.List;

@FunctionalInterface
public interface CandidateFilter {
    CandidateFilterResult filter(ScoredMoviesQuery query, List<MovieCandidate> candidates);

    default boolean enable(ScoredMoviesQuery query) {
        return true;
    }
}
