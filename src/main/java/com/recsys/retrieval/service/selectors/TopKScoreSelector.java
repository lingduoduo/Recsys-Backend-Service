package com.demo.retrieval.service.selectors;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class TopKScoreSelector<T extends TopKScoreSelector.Scored> {
    public SelectionResult<T> select(List<T> candidates, int size) {
        if (candidates == null || candidates.isEmpty() || size <= 0) {
            return new SelectionResult<>(List.of(), candidates == null ? List.of() : List.copyOf(candidates));
        }

        List<T> ranked = new ArrayList<>(candidates);
        ranked.sort(Comparator.comparingDouble(Scored::finalScore).reversed());

        int selectedSize = Math.min(size, ranked.size());
        return new SelectionResult<>(
            List.copyOf(ranked.subList(0, selectedSize)),
            List.copyOf(ranked.subList(selectedSize, ranked.size()))
        );
    }

    public interface Scored {
        double finalScore();
    }

    public record SelectionResult<T>(
        List<T> selected,
        List<T> nonSelected
    ) {
        public SelectionResult {
            selected = selected == null ? List.of() : List.copyOf(selected);
            nonSelected = nonSelected == null ? List.of() : List.copyOf(nonSelected);
        }
    }
}
