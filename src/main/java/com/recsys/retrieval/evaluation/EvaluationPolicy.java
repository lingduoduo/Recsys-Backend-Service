package com.demo.retrieval.evaluation;

import java.util.Comparator;
import java.util.Objects;
import java.util.Random;

/** Selects one available action for an offline evaluation state. */
@FunctionalInterface
public interface EvaluationPolicy {
    String select(FiniteHorizonEnvironment.State state, Random random);

    static EvaluationPolicy uniform() {
        return (state, random) -> {
            Objects.requireNonNull(state, "state");
            Objects.requireNonNull(random, "random");
            if (state.availableActions().isEmpty()) {
                throw new IllegalArgumentException("Cannot select from an empty action list");
            }
            return state.availableActions().get(random.nextInt(state.availableActions().size()));
        };
    }

    static EvaluationPolicy greedy(MovieLensDataset dataset) {
        Objects.requireNonNull(dataset, "dataset");
        return (state, random) -> {
            Objects.requireNonNull(state, "state");
            if (state.availableActions().isEmpty()) {
                throw new IllegalArgumentException("Cannot select from an empty action list");
            }
            return state.availableActions().stream()
                    .min(Comparator
                            .<String>comparingDouble(movieId ->
                                    dataset.scoreExcludingUser(state.userId(), movieId))
                            .reversed()
                            .thenComparing(Comparator.naturalOrder()))
                    .orElseThrow();
        };
    }
}
