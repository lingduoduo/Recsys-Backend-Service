package com.demo.retrieval.evaluation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FiniteHorizonEnvironmentTest {
    @TempDir Path tempDir;

    @Test
    void seededCandidatePoolsAreReproducibleBoundedAndImmutable() throws Exception {
        MovieLensDataset data = dataset("""
                userId,movieId,rating,timestamp
                1,10,5,0
                1,20,4,0
                1,30,3,0
                2,20,5,0
                2,40,4,0
                3,40,3,0
                """);
        FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(data, 3, 2, -1.0);

        FiniteHorizonEnvironment.State first = environment.initialState("1", new Random(17));
        FiniteHorizonEnvironment.State second = environment.initialState("1", new Random(17));

        assertThat(first).isEqualTo(second);
        assertThat(first.availableActions()).hasSizeLessThanOrEqualTo(3).doesNotHaveDuplicates();
        assertThatThrownBy(() -> first.availableActions().add("99"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void candidatePoolReservesSpaceForBothRatedAndUnseenMovies() throws Exception {
        MovieLensDataset data = dataset(popularityWindowCsv());
        FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(data, 4, 2, -1.0);

        List<String> actions = environment.initialState("1", new Random(17)).availableActions();

        assertThat(actions).anyMatch(data.ratingsFor("1")::containsKey);
        assertThat(actions).anyMatch(movieId -> !data.ratingsFor("1").containsKey(movieId));
    }

    @Test
    void unseenSamplingIsReproducibleSeedSensitiveAndLimitedToTopPopularityWindow()
            throws Exception {
        MovieLensDataset data = dataset(popularityWindowCsv());
        FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(data, 4, 2, -1.0);

        FiniteHorizonEnvironment.State first = environment.initialState("1", new Random(17));
        FiniteHorizonEnvironment.State repeated = environment.initialState("1", new Random(17));
        Set<Set<String>> unseenSelections = IntStream.range(0, 30)
                .mapToObj(seed -> environment.initialState("1", new Random(seed)).availableActions())
                .map(actions -> actions.stream()
                        .filter(movieId -> !data.ratingsFor("1").containsKey(movieId))
                        .collect(Collectors.toSet()))
                .collect(Collectors.toSet());

        assertThat(first).isEqualTo(repeated);
        assertThat(unseenSelections).hasSizeGreaterThan(1);
        assertThat(unseenSelections).allSatisfy(selection ->
                assertThat(selection).hasSize(2).isSubsetOf("20", "21", "22", "23"));
    }

    @Test
    void transitionRemovesOnlySelectedActionAndIncrementsStep() throws Exception {
        FiniteHorizonEnvironment environment = environment(3, 3, -1.0);
        FiniteHorizonEnvironment.State state =
                new FiniteHorizonEnvironment.State("1", List.of("10", "20", "30"), 0);

        FiniteHorizonEnvironment.Step result = environment.step(state, "20");

        assertThat(result.nextState().availableActions()).containsExactly("10", "30");
        assertThat(result.nextState().step()).isEqualTo(1);
        assertThat(result.done()).isFalse();
    }

    @Test
    void unavailableActionFails() throws Exception {
        FiniteHorizonEnvironment environment = environment(3, 3, -1.0);
        FiniteHorizonEnvironment.State state =
                new FiniteHorizonEnvironment.State("1", List.of("10"), 0);

        assertThatThrownBy(() -> environment.step(state, "20"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("available");
    }

    @Test
    void ratedRewardIsCenteredAndUnratedRewardRepresentsUnknownFeedback() throws Exception {
        FiniteHorizonEnvironment environment = environment(3, 3, -0.25);

        assertThat(environment.step(new FiniteHorizonEnvironment.State("1", List.of("10"), 0), "10")
                .reward()).isEqualTo(2.0);
        assertThat(environment.step(new FiniteHorizonEnvironment.State("1", List.of("30"), 0), "30")
                .reward()).isEqualTo(-0.25);
    }

    @Test
    void terminatesAtSlateSizeOrCandidateExhaustion() throws Exception {
        FiniteHorizonEnvironment slateLimited = environment(3, 1, -1.0);
        FiniteHorizonEnvironment exhausted = environment(3, 3, -1.0);

        assertThat(slateLimited.step(
                new FiniteHorizonEnvironment.State("1", List.of("10", "20"), 0), "10").done()).isTrue();
        assertThat(exhausted.step(
                new FiniteHorizonEnvironment.State("1", List.of("10"), 0), "10").done()).isTrue();
    }

    @Test
    void rolloutUsesExactDiscountPowers() throws Exception {
        MovieLensDataset data = dataset("""
                userId,movieId,rating,timestamp
                1,10,5,0
                1,20,4,0
                """);
        FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(data, 2, 2, -1.0);
        EvaluationPolicy ordered = (state, random) -> state.availableActions().stream()
                .min(String::compareTo).orElseThrow();

        FiniteHorizonEnvironment.Rollout result =
                environment.rollout("1", ordered, new Random(4), 0.5);

        assertThat(result.discountedReturn()).isEqualTo(2.5);
        assertThat(result.steps()).isEqualTo(2);
    }

    @Test
    void rolloutKeepsEnvironmentAndPolicyRandomStreamsIndependent() throws Exception {
        MovieLensDataset data = dataset(popularityWindowCsv());
        FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(data, 4, 2, -1.0);
        List<List<String>> observedCandidates = new java.util.ArrayList<>();
        EvaluationPolicy noExtraDraws = (state, random) -> {
            observedCandidates.add(state.availableActions());
            return state.availableActions().get(0);
        };
        EvaluationPolicy manyExtraDraws = (state, random) -> {
            observedCandidates.add(state.availableActions());
            for (int draw = 0; draw < 100; draw++) random.nextLong();
            return state.availableActions().get(0);
        };

        environment.rollout("1", noExtraDraws, new Random(19), new Random(23), 0.9);
        List<List<String>> withoutExtraDraws = List.copyOf(observedCandidates);
        observedCandidates.clear();
        environment.rollout("1", manyExtraDraws, new Random(19), new Random(23), 0.9);

        assertThat(observedCandidates).isEqualTo(withoutExtraDraws);
    }

    @Test
    void rejectsInvalidConfigurationAndDiscount() throws Exception {
        MovieLensDataset data = dataset(baseCsv());
        assertThatThrownBy(() -> new FiniteHorizonEnvironment(data, 0, 1, -1.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FiniteHorizonEnvironment(data, 1, 0, -1.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FiniteHorizonEnvironment(data, -1, 1, -1.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FiniteHorizonEnvironment(data, 1, -1, -1.0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FiniteHorizonEnvironment(data, 1, 1, Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
        FiniteHorizonEnvironment environment = new FiniteHorizonEnvironment(data, 2, 2, -1.0);
        assertThatThrownBy(() -> environment.rollout("1", EvaluationPolicy.uniform(), new Random(), 1.01))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> environment.rollout("1", EvaluationPolicy.uniform(), new Random(), -0.01))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> environment.rollout("1", EvaluationPolicy.uniform(), new Random(), Double.NaN))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void stateRejectsBlankIdentifiers() {
        assertThatThrownBy(() -> new FiniteHorizonEnvironment.State(" ", List.of("10"), 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new FiniteHorizonEnvironment.State("1", List.of(" "), 0))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private FiniteHorizonEnvironment environment(int poolSize, int slateSize, double unratedReward)
            throws Exception {
        return new FiniteHorizonEnvironment(dataset(baseCsv()), poolSize, slateSize, unratedReward);
    }

    private String baseCsv() {
        return """
                userId,movieId,rating,timestamp
                1,10,5,0
                1,20,4,0
                2,10,3,0
                2,20,2,0
                2,30,5,0
                """;
    }

    private String popularityWindowCsv() {
        return """
                userId,movieId,rating,timestamp
                1,10,5,0
                1,11,4,0
                1,12,3,0
                1,13,2,0
                2,20,4,0
                2,21,4,0
                2,22,4,0
                2,23,4,0
                2,24,4,0
                3,20,4,0
                3,21,4,0
                3,22,4,0
                3,23,4,0
                4,20,4,0
                4,21,4,0
                4,22,4,0
                5,20,4,0
                5,21,4,0
                6,20,4,0
                """;
    }

    private MovieLensDataset dataset(String csv) throws Exception {
        Path path = tempDir.resolve("ratings-" + Math.abs(csv.hashCode()) + ".csv");
        Files.writeString(path, csv);
        return MovieLensDataset.load(path, 0, 0);
    }
}
