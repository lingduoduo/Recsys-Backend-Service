package com.demo.retrieval.evaluation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/** Finite-horizon MovieLens environment with shrinking candidate sets. */
public final class FiniteHorizonEnvironment {
    private final MovieLensDataset dataset;
    private final int candidatePoolSize;
    private final int slateSize;
    private final double unratedReward;

    public FiniteHorizonEnvironment(MovieLensDataset dataset, int candidatePoolSize,
                                    int slateSize, double unratedReward) {
        this.dataset = Objects.requireNonNull(dataset, "dataset");
        if (candidatePoolSize <= 0) {
            throw new IllegalArgumentException("Candidate pool size must be positive");
        }
        if (slateSize <= 0) {
            throw new IllegalArgumentException("Slate size must be positive");
        }
        if (!Double.isFinite(unratedReward)) {
            throw new IllegalArgumentException("Unrated reward must be finite");
        }
        this.candidatePoolSize = candidatePoolSize;
        this.slateSize = slateSize;
        this.unratedReward = unratedReward;
    }

    public State initialState(String userId, Random random) {
        Objects.requireNonNull(random, "random");
        Map<String, Double> ratings = dataset.ratingsFor(userId);
        if (ratings.isEmpty()) {
            throw new IllegalArgumentException("Unknown user ID: " + userId);
        }

        List<String> rated = new ArrayList<>(ratings.keySet());
        Collections.shuffle(rated, random);
        List<String> unseen = dataset.movieIdsByPopularity().stream()
                .filter(movieId -> !ratings.containsKey(movieId))
                .toList();
        int targetSize = Math.min(candidatePoolSize, rated.size() + unseen.size());
        int unseenSlots = unseen.isEmpty() ? 0
                : Math.min(unseen.size(), Math.max(1, targetSize / 2));
        int ratedSlots = Math.min(rated.size(), targetSize - unseenSlots);
        unseenSlots = Math.min(unseen.size(), targetSize - ratedSlots);

        List<String> candidates = new ArrayList<>(targetSize);
        rated.stream().limit(ratedSlots).forEach(candidates::add);
        if (unseenSlots > 0) {
            int windowSize = Math.min(unseen.size(), unseenSlots * 2);
            List<String> popularityWindow = new ArrayList<>(unseen.subList(0, windowSize));
            Collections.shuffle(popularityWindow, random);
            popularityWindow.stream().limit(unseenSlots).forEach(candidates::add);
        }
        Collections.shuffle(candidates, random);
        return new State(userId, candidates, 0);
    }

    public Step step(State state, String action) {
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(action, "action");
        if (!state.availableActions().contains(action)) {
            throw new IllegalArgumentException("Action is not available: " + action);
        }
        List<String> remaining = new ArrayList<>(state.availableActions());
        remaining.remove(action);
        State next = new State(state.userId(), remaining, state.step() + 1);
        Double rating = dataset.ratingsFor(state.userId()).get(action);
        double reward = rating == null ? unratedReward : rating - 3.0;
        boolean done = next.step() >= slateSize || next.availableActions().isEmpty();
        return new Step(next, reward, done);
    }

    public Rollout rollout(String userId, EvaluationPolicy policy, Random random, double discount) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(random, "random");
        return rollout(userId, policy, new Random(random.nextLong()),
                new Random(random.nextLong()), discount);
    }

    public Rollout rollout(String userId, EvaluationPolicy policy, Random environmentRandom,
                           Random policyRandom, double discount) {
        Objects.requireNonNull(policy, "policy");
        Objects.requireNonNull(environmentRandom, "environmentRandom");
        Objects.requireNonNull(policyRandom, "policyRandom");
        if (!Double.isFinite(discount) || discount < 0.0 || discount > 1.0) {
            throw new IllegalArgumentException("Discount must be in [0, 1]");
        }

        State state = initialState(userId, environmentRandom);
        double discountedReturn = 0.0;
        double discountPower = 1.0;
        int steps = 0;
        while (!state.availableActions().isEmpty() && state.step() < slateSize) {
            String action = policy.select(state, policyRandom);
            Step transition = step(state, action);
            discountedReturn += discountPower * transition.reward();
            discountPower *= discount;
            steps++;
            state = transition.nextState();
            if (transition.done()) {
                break;
            }
        }
        return new Rollout(discountedReturn, steps);
    }

    public record State(String userId, List<String> availableActions, int step) {
        public State {
            Objects.requireNonNull(userId, "userId");
            if (userId.isBlank()) {
                throw new IllegalArgumentException("User ID must be nonblank");
            }
            availableActions = List.copyOf(availableActions);
            if (availableActions.stream().anyMatch(String::isBlank)) {
                throw new IllegalArgumentException("Movie IDs must be nonblank");
            }
            if (step < 0) {
                throw new IllegalArgumentException("Step must be nonnegative");
            }
        }
    }

    public record Step(State nextState, double reward, boolean done) {
        public Step {
            Objects.requireNonNull(nextState, "nextState");
        }
    }

    public record Rollout(double discountedReturn, int steps) {
        public Rollout {
            if (steps < 0) {
                throw new IllegalArgumentException("Steps must be nonnegative");
            }
        }
    }
}
