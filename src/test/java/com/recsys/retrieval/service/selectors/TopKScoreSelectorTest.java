package com.demo.retrieval.service.selectors;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TopKScoreSelectorTest {
    private final TopKScoreSelector<Candidate> selector = new TopKScoreSelector<>();

    @Test
    void selectsTopKByFinalScoreDescending() {
        TopKScoreSelector.SelectionResult<Candidate> result = selector.select(List.of(
            new Candidate("low", 0.1),
            new Candidate("high", 0.9),
            new Candidate("mid", 0.5)
        ), 2);

        assertEquals(List.of("high", "mid"), result.selected().stream().map(Candidate::id).toList());
        assertEquals(List.of("low"), result.nonSelected().stream().map(Candidate::id).toList());
    }

    @Test
    void handlesEmptyAndOversizedSelections() {
        assertEquals(List.of(), selector.select(List.<Candidate>of(), 3).selected());

        TopKScoreSelector.SelectionResult<Candidate> result = selector.select(List.of(
            new Candidate("one", 1.0)
        ), 5);

        assertEquals(List.of("one"), result.selected().stream().map(Candidate::id).toList());
        assertEquals(List.of(), result.nonSelected());
    }

    private record Candidate(String id, double finalScore) implements TopKScoreSelector.Scored {
    }
}
