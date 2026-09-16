package com.demo.retrieval.service.grpo;

import com.demo.retrieval.config.RecommendationProperties;
import com.demo.retrieval.model.FeatureCache;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

/**
 * Serves the GRPO policy score, off by default.
 *
 *   off    — not computed, no Redis read
 *   shadow — computed and logged at blend weight 0.0
 *   on     — re-ranks the post-diversity, post-selection slate (see {@link #reRankOrder})
 *
 * Mirrors recsys.sequence.behavior-mode so operators meet a rollout shape they already know.
 * Every failure path scores 0.0 / re-ranks nothing: a missing, stale or wrongly versioned weight
 * vector must leave recommendations exactly as they were, never fail a request.
 *
 * Constructed directly by MovieLensServingSideEffects' owner rather than injected, matching
 * GrpoEventPublisher: both are serving-path side effects, not collaborators anything else
 * resolves from the context.
 */
public class GrpoPolicyScorer {

    private static final Logger log = LoggerFactory.getLogger(GrpoPolicyScorer.class);

    public static final String MODE_OFF = "off";
    public static final String MODE_SHADOW = "shadow";
    public static final String MODE_ON = "on";
    public static final String WEIGHTS_KEY = "grpo:policy:weights";

    /**
     * Weight GRPO's min-max-normalized score carries in the post-diversity ranking blend that
     * {@link #reRankOrder} applies: {@code adjusted = (1 - w) * finalScore + w * grpoNorm}.
     *
     * The original design instead let GRPO claim part of the 0.15 that MovieLensOutcomeScorer's
     * exploitation weights leave unclaimed, blended inside that scorer. That was abandoned as
     * circular: index 2 of the GRPO feature vector is estimatedReward, which is an OUTPUT of
     * MovieLensOutcomeScorer.score() -- an in-blend term there would feed the scorer its own
     * output. The training data's grpo_x is also recorded after scoring runs, so serving must
     * score after scoring (and diversity) too, or reintroduce the train/serve mismatch the v2
     * feature change removed. Do not move this back inside MovieLensOutcomeScorer.
     */
    public static final double ON_BLEND_WEIGHT = 0.10;

    private final StringRedisTemplate redis;
    private final FeatureCache featureCache;
    private final String mode;

    public GrpoPolicyScorer(StringRedisTemplate redis, RecommendationProperties properties, FeatureCache featureCache) {
        this.redis = redis;
        this.featureCache = featureCache;
        String configured = properties.getGrpo().getMode();
        String normalized = configured == null ? MODE_OFF : configured.trim().toLowerCase(Locale.ROOT);
        if (!MODE_OFF.equals(normalized) && !MODE_SHADOW.equals(normalized) && !MODE_ON.equals(normalized)) {
            log.warn("Unrecognized recsys.grpo.mode '{}', treating as '{}'", configured, MODE_OFF);
            normalized = MODE_OFF;
        }
        this.mode = normalized;
    }

    public double blendWeight() {
        return MODE_ON.equals(mode) ? ON_BLEND_WEIGHT : 0.0;
    }

    private static double dot(double[] w, double[] x) {
        double sum = 0.0;
        for (int i = 0; i < GrpoFeatures.DIM; i++) {
            sum += w[i] * x[i];
        }
        return sum;
    }

    /**
     * New order for the candidates, or empty when GRPO should not re-rank.
     *
     * Called by HybridRecommendationService after MovieLensOutcomeScorer.applyDiversity and
     * top-K selection have both run, on the final served slate -- not earlier, because both of
     * those stages independently re-sort by their own score field, which would silently undo a
     * reorder applied to anything upstream of them.
     *
     * featureVectors and finalScores must be the same length and index-aligned: featureVectors[i]
     * is candidate i's GRPO feature vector (built via {@link GrpoFeatures#of}) and finalScores[i]
     * is that same candidate's current (post-diversity) ranking score, clamped to [0, 1].
     *
     * The blend is adjusted[i] = (1 - w) * finalScores[i] + w * grpoNorm[i], where w is
     * {@link #blendWeight()} and grpoNorm is w·x min-max normalized across the candidate set.
     * Normalizing is not optional: w·x is unbounded while finalScore is clamped to [0, 1], so
     * adding them unnormalized would let GRPO dominate regardless of the configured weight.
     *
     * Same failure contract as {@link #recordShadowSlate}: an operator error in Redis (e.g. the
     * weights key holding the wrong type) must degrade to "don't re-rank," never fail the request
     * that every other candidate's score already succeeded on.
     */
    public Optional<int[]> reRankOrder(List<double[]> featureVectors, double[] finalScores) {
        if (blendWeight() <= 0.0) {
            // off scores 0.0 and never reads Redis; shadow computes but must never move an order.
            return Optional.empty();
        }
        int n = featureVectors.size();
        if (n < 2) {
            // Nothing to reorder.
            return Optional.empty();
        }
        try {
            Optional<double[]> weights = readWeights();
            if (weights.isEmpty()) {
                return Optional.empty();
            }
            double[] w = weights.get();
            double[] raw = new double[n];
            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;
            for (int i = 0; i < n; i++) {
                raw[i] = dot(w, featureVectors.get(i));
                min = Math.min(min, raw[i]);
                max = Math.max(max, raw[i]);
            }
            double range = max - min;
            if (range < 1e-12) {
                // Every candidate scores identically: min-max normalization is degenerate and
                // there is no preference for GRPO to express, so re-ranking would be an arbitrary
                // tie-break.
                return Optional.empty();
            }
            double blend = blendWeight();
            double[] adjusted = new double[n];
            for (int i = 0; i < n; i++) {
                double norm = (raw[i] - min) / range;
                adjusted[i] = (1.0 - blend) * finalScores[i] + blend * norm;
            }
            // Stable, descending: ties keep their incumbent order, as Arrays.sort did before.
            int[] order = IntStream.range(0, n).boxed()
                .sorted((a, b) -> Double.compare(adjusted[b], adjusted[a]))
                .mapToInt(Integer::intValue)
                .toArray();
            return Optional.of(order);
        } catch (Exception e) {
            // Same contract as GrpoEventPublisher.publish and recordShadowSlate: a Redis-side
            // surprise (e.g. the weights key holding the wrong type) must never turn into a
            // failed recommendation.
            log.warn("Failed to compute GRPO re-rank order; leaving the incumbent order", e);
            return Optional.empty();
        }
    }

    /**
     * Logs one line per slate describing how the GRPO policy would have ordered it.
     *
     * Called from the serving side effects because that is the only place a slate position exists:
     * modelPredictions — the obvious alternative home for the score — is built before selection
     * assigns positions. Putting grpoScore there would train on a position the serving side never
     * had, so the score is logged and deliberately not carried.
     *
     * Rank agreement is reported as pairwise concordance: over every ordered pair (i &lt; j) of
     * served positions, the fraction the GRPO score ranks the same way serving did. Top-N overlap
     * was the alternative; concordance wins because it uses the whole slate and moves continuously,
     * so the spec's flip criterion reads a trend rather than a step function whose value depends on
     * a chosen N and collapses on slates shorter than it.
     *
     * Shadow only. In `on` mode the score already steers the order it would be compared against, so
     * the measure would grade the policy on its own output; in `off` mode nothing runs at all — not
     * even the Redis read.
     */
    public void recordShadowSlate(ServingSideEffectRequest request) {
        if (!MODE_SHADOW.equals(mode)) {
            return;
        }
        try {
            List<ServedMovie> served = request.selected();
            int slateSize = served.size();
            // A single-item slate has no pair to be concordant on; concordance would always read
            // 0.0 and look like total disagreement.
            if (slateSize < 2) {
                return;
            }
            // One Redis read for the whole slate, not one per item.
            Optional<double[]> weights = readWeights();
            if (weights.isEmpty()) {
                return;
            }
            double[] w = weights.get();
            double[] scores = new double[slateSize];
            for (int i = 0; i < slateSize; i++) {
                scores[i] = dot(w, GrpoFeatures.of(served.get(i)));
            }
            log.info("GRPO shadow slate requestId={} slateSize={} pairwiseConcordance={}",
                request.requestId(), slateSize, pairwiseConcordance(scores));
        } catch (Exception e) {
            // Same contract as GrpoEventPublisher.publish: shadow observability must never
            // turn into a failed recommendation.
            log.warn("Failed to log GRPO shadow slate for request {}", request.requestId(), e);
        }
    }

    /**
     * Fraction of served position pairs the scores order the same way, in [0, 1].
     *
     * A tie counts as disagreement: the served order committed to one of them, and a policy that
     * cannot separate two items did not reproduce that decision.
     */
    static double pairwiseConcordance(double[] scores) {
        long pairs = 0L;
        long concordant = 0L;
        for (int i = 0; i < scores.length; i++) {
            for (int j = i + 1; j < scores.length; j++) {
                pairs++;
                if (scores[i] > scores[j]) {
                    concordant++;
                }
            }
        }
        return pairs == 0L ? 0.0 : (double) concordant / pairs;
    }

    /**
     * The usable weight vector, read through the feature cache: the vector changes once per
     * training micro-batch, so it is re-read from Redis at most once per
     * recsys.cache.grpo-weights-ttl-seconds. An absent or rejected vector is cached as an empty
     * array for the same window rather than re-read on every request.
     */
    private Optional<double[]> readWeights() {
        double[] cached = featureCache.getGrpoWeights(WEIGHTS_KEY);
        if (cached == null) {
            cached = parseWeights(redis.opsForHash().entries(WEIGHTS_KEY)).orElseGet(() -> new double[0]);
            featureCache.putGrpoWeights(WEIGHTS_KEY, cached);
        }
        return cached.length == 0 ? Optional.empty() : Optional.of(cached);
    }

    private static Optional<double[]> parseWeights(Map<Object, Object> raw) {
        if (raw == null || raw.isEmpty()) {
            return Optional.empty();
        }
        Object version = raw.get("feature_version");
        if (version == null || !GrpoFeatures.VERSION.equals(version.toString())) {
            log.warn("GRPO weights carry feature_version '{}', expected '{}' — ignoring",
                version, GrpoFeatures.VERSION);
            return Optional.empty();
        }
        Object packed = raw.get("weights");
        if (packed == null) {
            return Optional.empty();
        }
        String[] parts = packed.toString().split(",");
        if (parts.length != GrpoFeatures.DIM) {
            return Optional.empty();
        }
        double[] w = new double[GrpoFeatures.DIM];
        try {
            for (int i = 0; i < parts.length; i++) {
                w[i] = Double.parseDouble(parts[i]);
                // Double.parseDouble accepts "NaN" and "Infinity" — a diverged training run would
                // otherwise be read back as a valid vector and every score would come out NaN,
                // which sorts unpredictably instead of degrading to the no-weights case.
                if (!Double.isFinite(w[i])) {
                    log.warn("GRPO weight {} is not finite ('{}') — ignoring the weight vector",
                        i, parts[i]);
                    return Optional.empty();
                }
            }
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
        return Optional.of(w);
    }
}
