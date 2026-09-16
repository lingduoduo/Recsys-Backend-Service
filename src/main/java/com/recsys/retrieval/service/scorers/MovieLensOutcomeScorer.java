package com.demo.retrieval.service.scorers;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MovieLensOutcomeScorer {
    private static final double DIVERSITY_DECAY = 0.72;
    private static final double DIVERSITY_FLOOR = 0.55;
    private static final double NO_DIVERSITY_MULTIPLIER = 1.0;

    // Exploitation blend applied to the ranking score in score() (weights sum to 0.85).
    // They summed to 1.0 only on paper: the fourth term was a deep-learning score that was always
    // 0.0 at runtime, so the reachable maximum has always been 0.85. The remaining weights are
    // deliberately NOT renormalized -- renormalizing would change every score in the service.
    static final double EXPLOITATION_BANDIT_WEIGHT = 0.55;
    static final double EXPLOITATION_OUTCOME_WEIGHT = 0.25;
    static final double EXPLOITATION_Q_WEIGHT = 0.05;

    // Estimated-reward blend in score() (weights sum to 1.0).
    static final double ESTIMATED_REWARD_POSTERIOR_WEIGHT = 0.65;
    static final double ESTIMATED_REWARD_OUTCOME_WEIGHT = 0.35;

    // Weighted-outcome blend in weightedOutcome(): positive weights sum to 1.0;
    // negative feedback is a subtractive penalty, not part of the convex sum.
    static final double OUTCOME_POSITIVE_RATING_WEIGHT = 0.30;
    static final double OUTCOME_PREFERENCE_WEIGHT = 0.22;
    static final double OUTCOME_CLICK_WEIGHT = 0.18;
    static final double OUTCOME_WATCH_WEIGHT = 0.22;
    static final double OUTCOME_NOVEL_DISCOVERY_WEIGHT = 0.08;
    static final double OUTCOME_NEGATIVE_FEEDBACK_PENALTY = 0.35;

    public ScoringResult score(ScoringInput input) {
        MovieLensOutcomeProbabilities probabilities = movieLensOutcomeProbabilities(input);
        double weightedOutcome = weightedOutcome(probabilities);
        double exploitation = EXPLOITATION_BANDIT_WEIGHT * input.banditRankingScore()
            + EXPLOITATION_OUTCOME_WEIGHT * weightedOutcome
            + EXPLOITATION_Q_WEIGHT * clamp(input.qValue());
        double predictionScore = clamp(exploitation + input.explorationBonus());
        double estimatedReward = clamp(ESTIMATED_REWARD_POSTERIOR_WEIGHT * input.posteriorMean()
            + ESTIMATED_REWARD_OUTCOME_WEIGHT * weightedOutcome);
        return new ScoringResult(
            estimatedReward,
            weightedOutcome,
            predictionScore,
            NO_DIVERSITY_MULTIPLIER,
            predictionScore
        );
    }

    public <T extends DiversityCandidate> List<T> applyDiversity(List<T> candidates) {
        if (candidates.size() < 2) {
            return candidates;
        }

        List<T> ordered = new ArrayList<>(candidates);
        ordered.sort(Comparator.comparingDouble(DiversityCandidate::preDiversityScore).reversed());

        Map<String, Integer> diversityCounts = new HashMap<>();
        List<T> adjusted = new ArrayList<>(ordered.size());
        for (T candidate : ordered) {
            String key = diversityKey(candidate);
            int position = diversityCounts.getOrDefault(key, 0);
            diversityCounts.put(key, position + 1);

            double multiplier = diversityMultiplier(position);
            @SuppressWarnings("unchecked")
            T updated = (T) candidate.withDiversity(multiplier, candidate.preDiversityScore() * multiplier);
            adjusted.add(updated);
        }

        adjusted.sort(Comparator.comparingDouble(DiversityCandidate::finalScore).reversed());
        return List.copyOf(adjusted);
    }

    private MovieLensOutcomeProbabilities movieLensOutcomeProbabilities(ScoringInput input) {
        double ctr = input.impressions() <= 0 ? 0.0 : (double) input.clicks() / input.impressions();
        double smoothedCtr = (input.clicks() + 1.0) / (input.impressions() + 4.0);
        double novelty = input.noveltyScore();

        double base = -1.15
            + 1.20 * clamp(input.relevance())
            + 0.85 * clamp(input.content())
            + 0.65 * clamp(input.popularity())
            + 0.55 * smoothedCtr
            + 0.20 * novelty;

        double click = sigmoid(base + 0.35 * ctr);
        double positiveRating = sigmoid(base - 0.05 + 0.20 * input.relevance());
        double preference = sigmoid(base - 0.15 + 0.15 * input.content());
        double watch = sigmoid(base + 0.10 + 0.25 * input.content());
        double novelDiscovery = sigmoid(base - 0.70 + 0.20 * novelty);
        double negativeFeedback = sigmoid(-base - 0.40 + 0.25 * Math.max(0.0, 0.08 - smoothedCtr));

        return new MovieLensOutcomeProbabilities(
            positiveRating,
            preference,
            click,
            watch,
            novelDiscovery,
            negativeFeedback
        );
    }

    private double weightedOutcome(MovieLensOutcomeProbabilities p) {
        double positive = OUTCOME_POSITIVE_RATING_WEIGHT * p.positiveRating()
            + OUTCOME_PREFERENCE_WEIGHT * p.preference()
            + OUTCOME_CLICK_WEIGHT * p.click()
            + OUTCOME_WATCH_WEIGHT * p.watch()
            + OUTCOME_NOVEL_DISCOVERY_WEIGHT * p.novelDiscovery();
        return clamp(positive - OUTCOME_NEGATIVE_FEEDBACK_PENALTY * p.negativeFeedback());
    }

    private String diversityKey(DiversityCandidate candidate) {
        String diversityGroup = candidate.diversityGroupId();
        if (diversityGroup != null && !diversityGroup.isBlank()) {
            return "group:" + diversityGroup;
        }
        String genre = candidate.primaryGenre();
        if (genre != null && !genre.isBlank()) {
            return "genre:" + genre;
        }
        return "item:" + candidate.itemId();
    }

    private double diversityMultiplier(int position) {
        return (1.0 - DIVERSITY_FLOOR) * Math.pow(DIVERSITY_DECAY, position) + DIVERSITY_FLOOR;
    }

    private static double sigmoid(double value) {
        return 1.0 / (1.0 + Math.exp(-value));
    }

    private static double clamp(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return 0.0;
        }
        return Math.max(0.0, Math.min(1.0, value));
    }

    public record ScoringInput(
        String itemId,
        double relevance,
        double content,
        double popularity,
        double posteriorMean,
        double banditRankingScore,
        double explorationBonus,
        double noveltyScore,
        double qValue,
        long impressions,
        long clicks
    ) {
    }

    public record MovieLensOutcomeProbabilities(
        double positiveRating,
        double preference,
        double click,
        double watch,
        double novelDiscovery,
        double negativeFeedback
    ) {
    }

    public record ScoringResult(
        double estimatedReward,
        double weightedOutcomeScore,
        double predictionScore,
        double diversityScore,
        double finalScore
    ) {
    }

    public interface DiversityCandidate {
        String itemId();

        String diversityGroupId();

        String primaryGenre();

        double preDiversityScore();

        double finalScore();

        DiversityCandidate withDiversity(double diversityScore, double finalScore);
    }
}
