package com.demo.retrieval.service.retrieval;

import com.demo.retrieval.measurement.FilterDecision;
import java.util.List;

public record RetrievalOutcome(
    List<MovieCandidate> retrievedCandidates,
    List<MovieCandidate> filteredCandidates,
    List<MovieCandidate> scoredCandidates,
    List<MovieCandidate> selectedCandidates,
    List<FilterDecision> filterDecisions,
    int evaluatedCandidateCount) {
}
