package com.recsys.retrieval.service.filters;

import com.recsys.retrieval.service.retrieval.MovieCandidate;

import java.util.List;

public record CandidateFilterResult(List<MovieCandidate> kept, List<MovieCandidate> removed) {
    public CandidateFilterResult {
        kept = kept == null ? List.of() : List.copyOf(kept);
        removed = removed == null ? List.of() : List.copyOf(removed);
    }
}
