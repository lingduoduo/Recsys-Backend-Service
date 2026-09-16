package com.demo.retrieval.evaluation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class EvaluationPolicyTest {
    @TempDir Path tempDir;

    @Test
    void uniformSelectionIsSeededAndAlwaysAvailable() {
        EvaluationPolicy policy = EvaluationPolicy.uniform();
        FiniteHorizonEnvironment.State state =
                new FiniteHorizonEnvironment.State("1", List.of("10", "20", "30"), 0);
        Random first = new Random(83);
        Random second = new Random(83);

        for (int i = 0; i < 20; i++) {
            String selected = policy.select(state, first);
            assertThat(selected).isEqualTo(policy.select(state, second));
            assertThat(state.availableActions()).contains(selected);
        }
    }

    @Test
    void greedySelectsHighestLeaveOneUserOutScore() throws Exception {
        MovieLensDataset data = dataset("""
                userId,movieId,rating,timestamp
                1,10,5,0
                1,20,1,0
                2,10,1,0
                2,20,5,0
                """);

        String selected = EvaluationPolicy.greedy(data).select(
                new FiniteHorizonEnvironment.State("1", List.of("10", "20"), 0), new Random(1));

        assertThat(selected).isEqualTo("20");
    }

    @Test
    void greedyBreaksScoreTiesByLowerMovieId() throws Exception {
        MovieLensDataset data = dataset("""
                userId,movieId,rating,timestamp
                1,10,5,0
                1,2,5,0
                2,10,3,0
                2,2,3,0
                """);

        String selected = EvaluationPolicy.greedy(data).select(
                new FiniteHorizonEnvironment.State("1", List.of("2", "10"), 0), new Random(1));

        assertThat(selected).isEqualTo("10");
    }

    @Test
    void policiesRejectEmptyActions() throws Exception {
        MovieLensDataset data = dataset("""
                userId,movieId,rating,timestamp
                1,10,5,0
                """);
        FiniteHorizonEnvironment.State empty =
                new FiniteHorizonEnvironment.State("1", List.of(), 0);

        assertThatThrownBy(() -> EvaluationPolicy.uniform().select(empty, new Random()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> EvaluationPolicy.greedy(data).select(empty, new Random()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private MovieLensDataset dataset(String csv) throws Exception {
        Path path = tempDir.resolve("ratings.csv");
        Files.writeString(path, csv);
        return MovieLensDataset.load(path, 0, 0);
    }
}
