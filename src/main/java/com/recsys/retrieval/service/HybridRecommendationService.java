package com.recsys.retrieval.service;

import com.recsys.retrieval.model.FeatureCache;
import com.recsys.retrieval.kafka.config.KafkaConfig;
import com.recsys.retrieval.kafka.core.KafkaProducer;
import com.recsys.retrieval.kafka.core.KafkaUtils;
import com.recsys.retrieval.measurement.RecommendationMeasurementService;
import com.recsys.retrieval.model.FeedbackRequest;
import com.recsys.retrieval.model.MovieLensUserFeatures;
import com.recsys.retrieval.model.RecommendationResult;
import com.recsys.retrieval.model.ScoredMoviesQuery;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.recsys.retrieval.config.RecommendationProperties;
import com.recsys.retrieval.config.RecommendationProperties.Filtering;
import com.recsys.retrieval.config.RecommendationProperties.MovieProfile;
import com.recsys.retrieval.service.content.CatalogContentScoring;
import com.recsys.retrieval.service.content.NormalizedProfile;
import com.recsys.retrieval.service.filters.FilterContext;
import com.recsys.retrieval.event.RecsysEventAvroCodec;
import com.recsys.retrieval.service.grpo.GrpoEventPublisher;
import com.recsys.retrieval.service.grpo.GrpoFeatures;
import com.recsys.retrieval.service.grpo.GrpoPolicyScorer;
import com.recsys.retrieval.service.retrieval.ContentCandidateRetriever;
import com.recsys.retrieval.service.retrieval.MovieCandidate;
import com.recsys.retrieval.service.replay.ReplayEvent;
import com.recsys.retrieval.service.retrieval.RetrievalOutcome;
import com.recsys.retrieval.service.text.TextNormalization;
import com.recsys.retrieval.service.query_hydrators.QueryHydrator;
import com.recsys.retrieval.service.scorers.MovieLensOutcomeScorer;
import com.recsys.retrieval.service.scorers.MovieLensOutcomeScorer.ScoringInput;
import com.recsys.retrieval.service.scorers.MovieLensOutcomeScorer.ScoringResult;
import com.recsys.retrieval.service.selectors.TopKScoreSelector;
import com.recsys.retrieval.service.selectors.TopKScoreSelector.SelectionResult;
import com.recsys.retrieval.service.side_effects.MovieLensServingSideEffects;
import com.recsys.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.recsys.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisOperations;
import org.springframework.data.redis.core.SessionCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class HybridRecommendationService {
    private static final Logger log = LoggerFactory.getLogger(HybridRecommendationService.class);
    private static final String GLOBAL_POPULARITY_KEY = "global:item_popularity";
    private static final String METRICS_HASH_KEY = "bandit:metrics";
    private static final String REPLAY_BUFFER_KEY = "replay:recommendations";
    private static final List<String> SUPPORTED_ALGORITHMS = List.of("ucb", "thompson", "q-learning", "sarsa");
    private static final double WARM_PRIOR_STRENGTH = RecommendationConstants.WARM_PRIOR_STRENGTH;
    // Atomic tabular-RL update: q = HGET; updated = q + alpha*(reward + gamma*nextValue - q); HSET.
    // ARGV = [action, reward, alpha, gamma, nextValue]; returns {updated, tdError} as strings.
    private static final RedisScript<List> Q_UPDATE_SCRIPT = new DefaultRedisScript<>(
        "local q = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')\n"
      + "local tdError = (tonumber(ARGV[2]) + tonumber(ARGV[4]) * tonumber(ARGV[5])) - q\n"
      + "local updated = q + tonumber(ARGV[3]) * tdError\n"
      + "redis.call('HSET', KEYS[1], ARGV[1], tostring(updated))\n"
      + "return {tostring(updated), tostring(tdError)}",
        List.class);
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");

    // Per-item data normalized once per catalog lifetime (productType, title lowercased+trimmed;
    // tags stored separately for content scoring; tags+keywords merged into allKeywords for
    // keyword-mute checks so no per-call set allocation is needed in the hot path).

    private final StringRedisTemplate redis;
    private final RecommendationProperties properties;
    private final OnlineLearningService onlineLearningService;
    private final FeatureCache featureCache;
    private final List<QueryHydrator<ScoredMoviesQuery>> queryHydrators;
    private final MovieLensOutcomeScorer movieLensOutcomeScorer = new MovieLensOutcomeScorer();
    private final TopKScoreSelector<ScoredCandidate> topKScoreSelector = new TopKScoreSelector<>();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final MovieLensServingSideEffects servingSideEffects;
    private final GrpoEventPublisher grpoPublisher;
    private final GrpoPolicyScorer grpoPolicyScorer;
    private final CatalogContentScoring catalogContentScoring;
    private final ContentCandidateRetriever contentCandidateRetriever;
    private final RecommendationMeasurementService measurementService;

    public HybridRecommendationService(
        StringRedisTemplate redis,
        RecommendationProperties properties,
        OnlineLearningService onlineLearningService,
        FeatureCache featureCache,
        List<QueryHydrator<ScoredMoviesQuery>> queryHydrators
    ) {
        this(
            redis, properties, onlineLearningService, featureCache, queryHydrators,
            RecommendationMeasurementService.noOp());
    }

    @Autowired
    public HybridRecommendationService(
        StringRedisTemplate redis,
        RecommendationProperties properties,
        OnlineLearningService onlineLearningService,
        FeatureCache featureCache,
        List<QueryHydrator<ScoredMoviesQuery>> queryHydrators,
        RecommendationMeasurementService measurementService
    ) {
        this.redis = redis;
        this.properties = properties;
        this.onlineLearningService = onlineLearningService;
        this.featureCache = featureCache;
        this.queryHydrators = List.copyOf(queryHydrators);
        this.grpoPublisher = new GrpoEventPublisher(
            createGrpoCodec(properties), createGrpoSender(properties), properties.getGrpo().isEmitEvents());
        this.grpoPolicyScorer = new GrpoPolicyScorer(redis, properties, featureCache);
        this.servingSideEffects = new MovieLensServingSideEffects(
            redis, objectMapper, properties.getReplayBuffer().getPendingTtl(),
            this.grpoPublisher,
            this.grpoPolicyScorer);
        this.catalogContentScoring = new CatalogContentScoring(properties);
        this.contentCandidateRetriever = new ContentCandidateRetriever(redis, properties, catalogContentScoring);
        this.measurementService = measurementService;
    }

    /**
     * The topic serving publishes its impression events to.
     *
     * Read from ONLINE_JOINER_INPUT_TOPIC — the same variable OnlineJoinerStreamingJob subscribes
     * with, and the same default — because these events are only worth emitting if that job
     * consumes them. Naming a topic of our own here (the previous behaviour) meant the two agreed
     * only when both happened to be left at their defaults, and the sim scripts, which point the
     * joiner at a per-run topic, are exactly the case where they do not.
     */
    static String grpoOutputTopic() {
        return System.getenv().getOrDefault("ONLINE_JOINER_INPUT_TOPIC", "recsys_events");
    }

    // Guarded the same way createGrpoSender below is: RecsysEventAvroCodec parses a bundled schema
    // resource at construction, so building it unconditionally would let a missing or corrupt
    // schema fail Spring bean creation even with emit-events off, breaking the promise that the
    // flag being off means nothing new happens at all.
    private static RecsysEventAvroCodec createGrpoCodec(RecommendationProperties properties) {
        if (!properties.getGrpo().isEmitEvents()) {
            return null;
        }
        return new RecsysEventAvroCodec();
    }

    // No KafkaConfig bean is reachable from here — this service has never published to Kafka
    // before. Off by default (emit-events=false), so the live path on every existing deployment
    // never opens a producer connection. When enabled, a broker that can't be reached at startup
    // must not fail startup: publishing is a logging side effect, never a serving dependency.
    private static GrpoEventPublisher.GrpoSender createGrpoSender(RecommendationProperties properties) {
        if (!properties.getGrpo().isEmitEvents()) {
            return null;
        }
        try {
            String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
            String topic = grpoOutputTopic();
            KafkaConfig base = KafkaUtils.kafkaConfig(bootstrapServers, topic, null);
            KafkaProducer producer = new KafkaProducer(KafkaUtils.producerConfig(base));
            log.info("GRPO impression events will be published to topic {}", topic);
            return (key, payload) -> producer.send(topic, payload);
        } catch (Exception e) {
            log.warn("Failed to construct GRPO Kafka producer; GRPO impression events will not be published", e);
            return null;
        }
    }

    public RecommendationResult recommend(String user, int limit) {
        String algorithm = currentAlgorithm();
        boolean tabularRl = "q-learning".equals(algorithm) || "sarsa".equals(algorithm);
        FilterContext filterCtx = buildFilterContext();
        ScoredMoviesQuery query = ScoredMoviesQuery.forUser(user);
        ScoredMoviesQuery hydratedQuery;
        try {
            hydratedQuery = measurementService.timeStage("hydration", () -> hydrateQuery(query));
        } catch (Exception e) {
            log.error("Recommendation fetch failed for user {}", user, e);
            return new RecommendationResult(user, List.of(), List.of(), List.of(), Map.of());
        }
        List<String> recent = hydratedQuery.watchedMovieIds();
        List<String> rated = hydratedQuery.ratedMovieIds();
        MovieLensUserFeatures userFeatures = hydratedQuery.userFeatures();

        RedisFetchOutputs redisFetch;
        try {
            redisFetch = measurementService.timeStage("redis_fetch", () -> {
            int fetchSize = Math.max(limit * properties.getCandidateGeneration().getPopularityFetchMultiplier(), limit);
            Set<ZSetOperations.TypedTuple<String>> popularWithScores;
            try {
                popularWithScores = Optional.ofNullable(
                    redis.opsForZSet().reverseRangeWithScores(GLOBAL_POPULARITY_KEY, 0, fetchSize - 1L)
                ).orElseGet(Set::of);
            } catch (Exception e) {
                throw new PopularityFetchException(e);
            }
            Map<String, Double> popularityMap = popularWithScores.stream()
                .filter(t -> t.getValue() != null && t.getScore() != null)
                .collect(Collectors.toMap(
                    ZSetOperations.TypedTuple::getValue,
                    t -> Math.log1p(t.getScore()),
                    Math::max,
                    LinkedHashMap::new
                ));
            double maxPopularity = popularityMap.isEmpty() ? 0.0 : popularityMap.values().iterator().next();
            Set<String> historySet = new HashSet<>(recent);
            historySet.addAll(rated);
            historySet.addAll(userFeatures.recentlyRatedMovieIds());
            historySet.addAll(userFeatures.actionSequenceMovieIds());
            historySet.addAll(userFeatures.servedMovieIds());
            historySet.addAll(userFeatures.impressedMovieIds());
            TasteProfile tasteProfile = deriveTasteProfile(recent, rated, userFeatures, popularityMap.keySet());
            Map<String, Double> genrePreferences = tasteProfile.genres();
            Map<String, Double> tagPreferences = tasteProfile.tags();
            Map<String, Object> state = buildState(recent, genrePreferences, tagPreferences);
            String stateKey = stateKey(state);
            RetrievalOutcome candidateGeneration = generateCandidates(
                hydratedQuery, popularityMap, historySet, genrePreferences, tagPreferences, filterCtx, limit);
            measurementService.recordFilterDecisions(
                candidateGeneration.evaluatedCandidateCount(), candidateGeneration.filterDecisions());
            Map<String, MovieCandidate> candidateById = candidateGeneration.selectedCandidates().stream()
                .collect(Collectors.toMap(MovieCandidate::movieId, candidate -> candidate, MovieCandidate::merge, LinkedHashMap::new));
            List<String> eligibleList = candidateGeneration.selectedCandidates().stream()
                .map(MovieCandidate::movieId)
                .toList();
            batchWarmItemVectors(eligibleList, Stream.concat(recent.stream(), rated.stream()).distinct().toList());
            long totalImpressions = resolveLongMetric("recommendations_served");
            Map<String, long[]> counters = batchFetchCounters(eligibleList);
            Map<String, Double> qValues = tabularRl ? batchFetchQValues(stateKey, eligibleList) : Map.of();
            onlineLearningService.batchWarmRewardStats(eligibleList, properties.getCatalog());
            return new RedisFetchOutputs(
                popularityMap, maxPopularity, genrePreferences, tagPreferences, state, stateKey, candidateGeneration,
                candidateById, eligibleList, totalImpressions, counters, qValues);
            });
        } catch (PopularityFetchException e) {
            log.error("Recommendation fetch failed for user {}", user, e.getCause());
            return new RecommendationResult(user, List.of(), List.of(), List.of(), Map.of());
        }

        List<ScoredCandidate> scored = measurementService.timeStage("scoring", () -> {
            double[] userVector = resolveUserVector(user, recent, rated);
            Map<String, Double> relevanceScores = batchRelevanceScores(userVector, redisFetch.eligibleList());
            return movieLensOutcomeScorer.applyDiversity(redisFetch.eligibleList().stream()
                .map(item -> {
                    long[] c = redisFetch.counters().getOrDefault(item, new long[]{0L, 0L});
                    return scoreCandidate(
                        redisFetch.candidateById().get(item), item, relevanceScores.getOrDefault(item, 0.0),
                        redisFetch.genrePreferences(), redisFetch.tagPreferences(),
                        redisFetch.popularityMap().getOrDefault(item, 0.0),
                        redisFetch.maxPopularity(), redisFetch.totalImpressions(), c[0], c[1],
                        redisFetch.qValues().get(item), tabularRl);
                })
                .toList());
        });

        if (scored.isEmpty()) {
            return new RecommendationResult(
                user,
                recent,
                List.of(),
                List.of(),
                Map.of("eligibleCandidateCount", 0, "algorithm", algorithm)
            );
        }

        String requestId = UUID.randomUUID().toString();
        SelectionOutputs selectionOutputs = measurementService.timeStage("selection", () -> {
            SelectionResult<ScoredCandidate> selection = topKScoreSelector.select(scored, limit);
            List<ScoredCandidate> candidateSnapshot = topKScoreSelector
                .select(scored, Math.max(limit, properties.getReplayBuffer().getCandidateSnapshotSize()))
                .selected();
            return new SelectionOutputs(selection.selected(), candidateSnapshot);
        });
        List<ScoredCandidate> selected = applyGrpoReRank(selectionOutputs.selected());
        List<ScoredCandidate> candidateSnapshot = selectionOutputs.candidateSnapshot();
        List<String> recommendations = selected.stream().map(ScoredCandidate::itemId).toList();
        measurementService.recordFreshness(recommendations.stream()
            .map(itemId -> properties.getCatalog().get(itemId))
            .toList());
        double pseudoRegret = computePseudoRegret(scored, selected, limit);
        double avgEstimatedReward = selected.stream().mapToDouble(ScoredCandidate::estimatedReward).average().orElse(0.0);
        double avgExplorationBonus = selected.stream().mapToDouble(ScoredCandidate::explorationBonus).average().orElse(0.0);
        long coldStartCount = selected.stream().filter(ScoredCandidate::coldStart).count();
        double coldStartShare = selected.isEmpty() ? 0.0 : (double) coldStartCount / selected.size();
        long exploratoryCount = selected.stream().filter(c -> c.explorationBonus() > 0.0).count();
        double noveltyTotal = selected.stream().mapToDouble(ScoredCandidate::noveltyScore).sum();
        long servedCount = selected.size();
        double estimatedRewardTotal = avgEstimatedReward * servedCount;
        measurementService.timeStage("side_effects", () -> {
            servingSideEffects.recordServed(new ServingSideEffectRequest(
            requestId,
            user,
            algorithm,
            redisFetch.state(),
            candidateSnapshot.stream().map(this::toServedMovie).toList(),
            selected.stream().map(this::toServedMovie).toList(),
            userFeatures.servedMovieIds(),
            userFeatures.impressedMovieIds(),
            coldStartCount,
            exploratoryCount,
            servedCount,
            estimatedRewardTotal,
            pseudoRegret,
                noveltyTotal
            ));
            return null;
        });

        List<Map<String, Object>> diagnostics = selected.stream()
            .map(candidate -> {
                Map<String, Object> row = new LinkedHashMap<>();
                row.put("item", candidate.itemId());
                row.put("estimatedReward", round(candidate.estimatedReward()));
                row.put("relevanceScore", round(candidate.relevanceScore()));
                row.put("contentScore", round(candidate.contentScore()));
                row.put("qValue", round(candidate.qValue()));
                row.put("rewardModelScore", round(candidate.onlineScore()));
                row.put("weightedOutcomeScore", round(candidate.weightedOutcomeScore()));
                row.put("predictionScore", round(candidate.predictionScore()));
                row.put("diversityScore", round(candidate.diversityScore()));
                row.put("explorationBonus", round(candidate.explorationBonus()));
                row.put("banditScore", round(candidate.banditScore()));
                row.put("coldStart", candidate.coldStart());
                row.put("impressions", candidate.impressions());
                row.put("clicks", candidate.clicks());
                return row;
            })
            .toList();

        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("algorithm", algorithm);
        metrics.put("retrievedCandidateCount", redisFetch.candidateGeneration().retrievedCandidates().size());
        metrics.put("filteredCandidateCount", redisFetch.candidateGeneration().filteredCandidates().size());
        metrics.put("scoredCandidateCount", redisFetch.candidateGeneration().scoredCandidates().size());
        metrics.put("eligibleCandidateCount", redisFetch.eligibleList().size());
        metrics.put("selectionStrategy", "top_k_final_score");
        metrics.put("pseudoRegret", round(pseudoRegret));
        metrics.put("avgEstimatedReward", round(avgEstimatedReward));
        metrics.put("avgExplorationBonus", round(avgExplorationBonus));
        metrics.put("coldStartShare", round(coldStartShare));
        metrics.put("catalogCoverage", round(resolveCatalogCoverage()));

        return new RecommendationResult(user, recent, recommendations, diagnostics, metrics);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public Map<String, Object> recordFeedback(FeedbackRequest request) {
        String itemId = request.item();
        if (itemId == null || itemId.isBlank()) {
            return Map.of("status", "ignored", "reason", "missing_item");
        }

        double reward = request.reward();
        MovieProfile profile = properties.getCatalog().get(itemId);
        String algorithm = currentAlgorithm();

        // Read-side: GET pending replay context before the pipeline (reads can't be pipelined with writes).
        Map<String, Object> replayEvent = readPendingReplayContext(request.user(), request.item()).orElseGet(LinkedHashMap::new);
        TabularRlUpdate tabularRlUpdate = isTabularRl()
            ? buildTabularRlUpdate(request, replayEvent)
            : null;
        // Apply the Q-update atomically (Lua) before the pipeline; the returned tdError feeds metrics.
        final boolean qApplied = tabularRlUpdate != null;
        final double qTdError = qApplied ? applyAtomicQUpdate(tabularRlUpdate)[1] : 0.0;
        String replayPayload = buildReplayPayload(request, replayEvent);

        // Collect keys invalidated by the pipeline so the in-memory cache is purged after the flush.
        List<String> cacheKeysToInvalidate = new ArrayList<>();

        // Batch ALL feedback writes into one pipeline: ~22+ round-trips → 1.
        measurementService.recordFeedbackCoverage(request);
        redis.executePipelined(new SessionCallback<Object>() {
            @Override
            public Object execute(RedisOperations operations) {
                operations.delete(pendingReplayKey(request.user(), request.item()));
                if (request.clicked()) {
                    operations.opsForHash().increment(METRICS_HASH_KEY, "clicks", 1L);
                    operations.opsForHash().increment(metricsHashKey(algorithm), "clicks", 1L);
                    operations.opsForValue().increment("bandit:item:" + itemId + ":clicks", 1);
                }
                operations.opsForHash().increment(METRICS_HASH_KEY, "reward_total", reward);
                operations.opsForHash().increment(metricsHashKey(algorithm), "reward_total", reward);
                onlineLearningService.pipelineUpdate(operations, itemId, reward, profile, cacheKeysToInvalidate);
                if (qApplied) {
                    operations.opsForHash().increment(metricsHashKey(algorithm), "q_updates", 1L);
                    operations.opsForHash().increment(metricsHashKey(algorithm), "q_td_error_total", qTdError);
                }
                if (replayPayload != null) {
                    operations.opsForList().rightPush(REPLAY_BUFFER_KEY, replayPayload);
                    operations.opsForList().trim(REPLAY_BUFFER_KEY, -(long) Math.max(1, properties.getReplayBuffer().getMaxSize()), -1L);
                }
                return null;
            }
        });

        cacheKeysToInvalidate.forEach(featureCache::invalidateRewardStats);

        // Serving's own requestId first, since that's what the impression event carries; the
        // replay context's requestId (read above, before the pipeline) is the fallback for a
        // client that didn't send one. Without either, the event can't join to its impression,
        // and inventing an id would silently corrupt the join rather than just dropping the row.
        String grpoRequestId = request.requestId() != null && !request.requestId().isBlank()
            ? request.requestId()
            : asNonBlankString(replayEvent.get(ReplayEvent.REQUEST_ID));
        if (grpoRequestId != null) {
            grpoPublisher.publishFeedback(request, grpoRequestId, System.currentTimeMillis());
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "ok");
        response.put("item", itemId);
        response.put("clicked", request.clicked());
        response.put("reward", reward);
        response.put("metrics", getAggregateMetrics());
        return response;
    }

    public Map<String, Object> getAggregateMetrics() {
        String algorithm = currentAlgorithm();
        double catalogCoverage = resolveCatalogCoverage();
        Map<String, Object> metrics = buildAggregateMetricsForAlgorithm(algorithm, catalogCoverage);

        Map<String, Object> allAlgorithms = new LinkedHashMap<>();
        for (String candidate : supportedAlgorithms()) {
            Map<String, Object> candidateMetrics = buildAggregateMetricsForAlgorithm(candidate, catalogCoverage);
            if (hasRecordedActivity(candidateMetrics) || candidate.equals(algorithm)) {
                allAlgorithms.put(candidate, candidateMetrics);
            }
        }

        metrics.put("allAlgorithms", allAlgorithms);
        metrics.put("global", buildAggregateMetricsFromHash(METRICS_HASH_KEY, "all", catalogCoverage));
        return metrics;
    }

    private ScoredMoviesQuery hydrateQuery(ScoredMoviesQuery query) {
        ScoredMoviesQuery current = query;
        for (QueryHydrator<ScoredMoviesQuery> hydrator : queryHydrators) {
            if (!hydrator.enable(query)) continue;
            ScoredMoviesQuery hydrated = hydrator.hydrate(current);
            current = hydrator.update(current, hydrated);
        }
        return current;
    }

    private RetrievalOutcome generateCandidates(
        ScoredMoviesQuery query,
        Map<String, Double> popularityMap,
        Set<String> excludedItems,
        Map<String, Double> genrePreferences,
        Map<String, Double> tagPreferences,
        FilterContext filterCtx,
        int limit
    ) {
        return contentCandidateRetriever.retrieve(
            query, popularityMap, excludedItems, genrePreferences, tagPreferences, filterCtx, limit);
    }


    private FilterContext buildFilterContext() {
        Filtering f = properties.getFiltering();
        if (!f.isEnabled()) {
            return FilterContext.empty();
        }
        return new FilterContext(
            TextNormalization.normalize(f.getMutedProductTypes()),
            TextNormalization.normalize(f.getMutedGenres()),
            TextNormalization.normalize(f.getMutedKeywords())
        );
    }


    /**
     * Re-ranks the already-diversified, already-selected slate with GRPO's policy score, if the
     * mode is `on` and a usable weight vector is available. Deliberately placed after both
     * MovieLensOutcomeScorer.applyDiversity and TopKScoreSelector.select have already sorted by
     * their own score field: reordering anything upstream of either would be silently undone by
     * the next of their own re-sorts, and this is also the exact slate whose position
     * servingSideEffects then records as served.
     *
     * `off` and `shadow` cost nothing beyond this blendWeight() check -- no feature vectors are
     * built and no Redis is touched -- which is how default-off stays byte-identical to before
     * this method existed, and shadow (which never re-ranks) does no wasted work either.
     *
     * Features are rounded to 3dp with the same {@link #round} helper toServedMovie uses on the
     * ServedMovie that GrpoImpressionEvents later packs into training's grpo_x: serving must score
     * the identical rounded values training records, or this reintroduces exactly the train/serve
     * mismatch the v2 feature change removed.
     */
    private List<ScoredCandidate> applyGrpoReRank(List<ScoredCandidate> selected) {
        if (grpoPolicyScorer.blendWeight() <= 0.0) {
            return selected;
        }
        List<double[]> featureVectors = selected.stream().map(this::grpoFeaturesFor).toList();
        double[] finalScores = selected.stream().mapToDouble(ScoredCandidate::finalScore).toArray();
        Optional<int[]> reRankOrder = grpoPolicyScorer.reRankOrder(featureVectors, finalScores);
        if (reRankOrder.isEmpty()) {
            return selected;
        }
        List<ScoredCandidate> reordered = new ArrayList<>(selected.size());
        for (int index : reRankOrder.get()) {
            reordered.add(selected.get(index));
        }
        return List.copyOf(reordered);
    }

    /**
     * Package-private (not private) so a test can drive this exact call site directly, alongside
     * {@link #toServedMovie}, on the same candidate -- the only way to prove the rounding here
     * actually matches training's rounding without both sides of the test rounding independently
     * and comparing rounded-to-rounded, which would pass even if this method stopped rounding.
     */
    double[] grpoFeaturesFor(ScoredCandidate candidate) {
        return GrpoFeatures.of(
            round(candidate.banditScore()), round(candidate.estimatedReward()), round(candidate.onlineScore()),
            round(candidate.explorationBonus()), candidate.coldStart(), candidate.impressions(), candidate.clicks());
    }

    private ScoredCandidate scoreCandidate(
        MovieCandidate candidate,
        String itemId,
        double relevance,
        Map<String, Double> genrePreferences,
        Map<String, Double> tagPreferences,
        double itemPopularity,
        double maxPopularity,
        long totalImpressions,
        long impressions,
        long clicks,
        Double qValue,
        boolean tabularRl
    ) {
        NormalizedProfile profile = catalogContentScoring.normalizedCatalog().get(itemId);
        MovieProfile movieProfile = properties.getCatalog().get(itemId);

        double content = profile == null ? 0.0 : catalogContentScoring.contentScore(profile, genrePreferences, tagPreferences);
        double popularity = maxPopularity == 0.0 ? 0.0 : itemPopularity / maxPopularity;

        double offlineScore = RecommendationConstants.blendOfflineScore(
            properties.getBandit().getRelevanceWeight(), normalizeScore(relevance),
            properties.getBandit().getContentWeight(), content,
            properties.getBandit().getPopularityWeight(), popularity);
        double onlineScore = onlineLearningService.score(itemId, movieProfile, offlineScore);
        double onlineWeight = clamp(properties.getRewardModel().getWeight());
        double learnedPrior = (offlineScore * (1.0 - onlineWeight)) + (onlineScore * onlineWeight);
        boolean coldStart = impressions < properties.getBandit().getColdStartExposureThreshold() || (profile != null && profile.newRelease());
        BanditArmScore armScore = computeBanditArmScore(learnedPrior, impressions, clicks, totalImpressions, coldStart);
        double qLearningScore = computeTabularRlRankingScore(learnedPrior, qValue);
        double baseRankingScore = tabularRl ? qLearningScore : armScore.rankingScore();
        double novelty = 1.0 / (impressions + 1.0);
        ScoringResult scoring = movieLensOutcomeScorer.score(new ScoringInput(
            itemId,
            normalizeScore(relevance),
            content,
            popularity,
            armScore.posteriorMean(),
            baseRankingScore,
            armScore.explorationBonus(),
            novelty,
            qValue == null ? 0.0 : qValue,
            impressions,
            clicks
        ));
        String diversityGroupId = profile == null || profile.genres().isEmpty()
            ? null
            : profile.genres().iterator().next();
        String primaryGenre = profile == null || profile.genres().isEmpty() ? null : profile.genres().iterator().next();

        return new ScoredCandidate(
            itemId,
            scoring.estimatedReward(),
            relevance,
            content,
            popularity,
            onlineScore,
            armScore.explorationBonus(),
            scoring.finalScore(),
            coldStart,
            novelty,
            impressions,
            clicks,
            qValue == null ? 0.0 : qValue,
            scoring.weightedOutcomeScore(),
            scoring.predictionScore(),
            scoring.diversityScore(),
            diversityGroupId,
            primaryGenre
        );
    }

    // Must run before the write pipeline: reads cannot be issued inside executePipelined.
    private String buildReplayPayload(FeedbackRequest request, Map<String, Object> event) {
        ReplayEvent.applyFeedback(event, request.user(), request.item(), request.clicked(),
            request.reward(), System.currentTimeMillis(), buildCurrentState(request.user()));
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize replay event for user {} item {}", request.user(), request.item(), e);
            return null;
        }
    }

    // Feedback-time state (s') must derive genres/tags the SAME way serve-time does, or the Bellman
    // bootstrap lands in a different Q-bucket than the serving path ever writes. Hydrate the user's
    // features and reuse deriveTasteProfile (empty popular fallback: only reachable for a fully-cold
    // user, who by definition has feedback history so `recent` is non-empty).
    private Map<String, Object> buildCurrentState(String user) {
        ScoredMoviesQuery hydrated = hydrateQuery(ScoredMoviesQuery.forUser(user));
        List<String> recent = hydrated.watchedMovieIds();
        TasteProfile profile = deriveTasteProfile(
            recent, hydrated.ratedMovieIds(), hydrated.userFeatures(), List.of());
        return buildState(recent, profile.genres(), profile.tags());
    }

    record TasteProfile(Map<String, Double> genres, Map<String, Double> tags) {
    }

    TasteProfile deriveTasteProfile(List<String> recent, List<String> rated,
                                    MovieLensUserFeatures features, Collection<String> popular) {
        List<String> seedItems = firstNonEmpty(
            recent,
            features.cachedMovieIds(),
            features.actionSequenceMovieIds(),
            features.retrievalSequenceMovieIds(),
            features.scoringSequenceMovieIds(),
            rated,
            List.of()
        );
        Map<String, Double> genres = seedItems.stream()
            .map(properties.getCatalog()::get)
            .filter(p -> p != null)
            .flatMap(p -> TextNormalization.normalize(p.getGenres()).stream())
            .collect(Collectors.toMap(genre -> genre, genre -> 1.0, (left, right) -> left, LinkedHashMap::new));
        TextNormalization.normalize(features.favoriteGenres()).forEach(genre -> genres.putIfAbsent(genre, 1.0));
        Map<String, Double> tags = seedItems.stream()
            .map(properties.getCatalog()::get)
            .filter(p -> p != null)
            .flatMap(p -> TextNormalization.normalize(p.getTags()).stream())
            .collect(Collectors.toMap(tag -> tag, tag -> 1.0, (left, right) -> left, LinkedHashMap::new));
        features.genrePreferences().forEach(genres::putIfAbsent);
        features.tagPreferences().forEach(tags::putIfAbsent);
        if (genres.isEmpty() && tags.isEmpty()) {
            popular.stream()
                .map(properties.getCatalog()::get)
                .filter(p -> p != null)
                .forEach(profile -> {
                    TextNormalization.normalize(profile.getGenres()).forEach(genre -> genres.putIfAbsent(genre, 1.0));
                    TextNormalization.normalize(profile.getTags()).forEach(tag -> tags.putIfAbsent(tag, 1.0));
                });
        }
        return new TasteProfile(genres, tags);
    }

    private Map<String, Object> buildState(
        List<String> recent, Map<String, Double> genrePreferences, Map<String, Double> tagPreferences) {
        Map<String, Object> state = new LinkedHashMap<>();
        state.put("recent", recent);
        state.put("genres", genrePreferences.keySet().stream().sorted().toList());
        state.put("tags", tagPreferences.keySet().stream().sorted().toList());
        return state;
    }

    private Map<String, Object> modelPredictions(ScoredCandidate candidate) {
        Map<String, Object> predictions = new LinkedHashMap<>();
        predictions.put("qValue", round(candidate.qValue()));
        predictions.put("rewardModelScore", round(candidate.onlineScore()));
        predictions.put("estimatedReward", round(candidate.estimatedReward()));
        predictions.put("weightedOutcomeScore", round(candidate.weightedOutcomeScore()));
        predictions.put("predictionScore", round(candidate.predictionScore()));
        predictions.put("diversityScore", round(candidate.diversityScore()));
        predictions.put("relevanceScore", round(candidate.relevanceScore()));
        predictions.put("contentScore", round(candidate.contentScore()));
        predictions.put("popularityScore", round(candidate.popularityScore()));
        predictions.put("banditScore", round(candidate.banditScore()));
        return predictions;
    }

    // Package-private (not private): tests must drive this exact call site -- the one
    // GrpoImpressionEvents packs training's grpo_x from -- to compare against grpoFeaturesFor's
    // real output on the same candidate.
    ServedMovie toServedMovie(ScoredCandidate candidate) {
        return new ServedMovie(
            candidate.itemId(),
            round(candidate.estimatedReward()),
            round(candidate.onlineScore()),
            round(candidate.explorationBonus()),
            round(candidate.banditScore()),
            candidate.coldStart(),
            candidate.impressions(),
            candidate.clicks(),
            modelPredictions(candidate)
        );
    }

    private TabularRlUpdate buildTabularRlUpdate(FeedbackRequest request, Map<String, Object> replayEvent) {
        Object rawState = replayEvent.get("state");
        if (!(rawState instanceof Map<?, ?> state)) {
            return null;
        }

        String action = request.item();
        String qKey = qKeyForStateMap(state);
        Map<String, Object> nextState = buildCurrentState(request.user());
        NextActionValue nextActionValue = nextActionValue(nextState);
        double alpha = clamp(properties.getBandit().getQLearningAlpha());
        double gamma = clamp(properties.getBandit().getQLearningGamma());
        replayEvent.put("nextAction", nextActionValue.action());
        return new TabularRlUpdate(qKey, action, request.reward(), alpha, gamma, nextActionValue.value());
    }

    // Atomic GET+compute+HSET so concurrent feedback for the same (state, action) cannot lose an
    // update. Returns [updatedQ, tdError]. nextValue is a bootstrap estimate read before the call.
    private double[] applyAtomicQUpdate(TabularRlUpdate u) {
        @SuppressWarnings("unchecked")
        List<Object> result = (List<Object>) redis.execute(
            Q_UPDATE_SCRIPT, List.of(u.qKey()),
            u.action(),
            String.valueOf(u.reward()),
            String.valueOf(u.alpha()),
            String.valueOf(u.gamma()),
            String.valueOf(u.nextValue()));
        if (result == null || result.size() < 2) {
            return new double[]{0.0, 0.0};
        }
        return new double[]{readDouble(result.get(0)), readDouble(result.get(1))};
    }

    private Map<String, Double> batchFetchQValues(String stateKey, List<String> itemIds) {
        if (itemIds.isEmpty()) {
            return Map.of();
        }

        List<Object> fields = new ArrayList<>(itemIds);
        List<Object> values = Optional.ofNullable(redis.opsForHash().multiGet(qKeyForState(stateKey), fields))
            .orElseGet(() -> Collections.nCopies(itemIds.size(), null));

        Map<String, Double> result = new LinkedHashMap<>();
        for (int i = 0; i < itemIds.size(); i++) {
            Object raw = values.get(i);
            if (raw != null) {
                result.put(itemIds.get(i), readDouble(raw));
            }
        }
        return result;
    }

    private NextActionValue nextActionValue(Map<String, Object> nextState) {
        String qKey = qKeyForStateMap(nextState);
        if ("sarsa".equals(currentAlgorithm())) {
            return chooseSarsaNextAction(nextState, qKey);
        }
        return new NextActionValue(null, maxQValue(qKey));
    }

    private NextActionValue chooseSarsaNextAction(Map<String, Object> nextState, String qKey) {
        List<String> actionSpace = nextActionSpace(nextState);
        if (actionSpace.isEmpty()) {
            return new NextActionValue(null, 0.0);
        }

        if (ThreadLocalRandom.current().nextDouble() < clamp(properties.getBandit().getQLearningEpsilon())) {
            String action = actionSpace.get(ThreadLocalRandom.current().nextInt(actionSpace.size()));
            return new NextActionValue(action, readDouble(redis.opsForHash().get(qKey, action)));
        }

        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(qKey)).orElseGet(Map::of);
        String bestAction = actionSpace.get(0);
        double bestValue = readDouble(raw.get(bestAction));
        for (String action : actionSpace) {
            double value = readDouble(raw.get(action));
            if (value > bestValue) {
                bestAction = action;
                bestValue = value;
            }
        }
        return new NextActionValue(bestAction, bestValue);
    }

    private List<String> nextActionSpace(Map<String, Object> nextState) {
        Object rawRecent = nextState.get("recent");
        Set<String> recent = rawRecent instanceof List<?> values
            ? values.stream().map(String::valueOf).collect(Collectors.toSet())
            : Set.of();
        List<String> actions = properties.getCatalog().keySet().stream()
            .filter(item -> !recent.contains(item))
            .toList();
        return actions.isEmpty() ? List.copyOf(properties.getCatalog().keySet()) : actions;
    }

    private double maxQValue(String qKey) {
        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(qKey)).orElseGet(Map::of);
        return raw.values().stream()
            .mapToDouble(this::readDouble)
            .max()
            .orElse(0.0);
    }

    private static String asNonBlankString(Object value) {
        return value instanceof String s && !s.isBlank() ? s : null;
    }

    private Optional<Map<String, Object>> readPendingReplayContext(String user, String item) {
        String raw = redis.opsForValue().get(pendingReplayKey(user, item));
        if (raw == null || raw.isBlank()) {
            return Optional.empty();
        }
        try {
            return Optional.of(objectMapper.readValue(raw, new TypeReference<>() {
            }));
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize pending replay context for user {} item {}", user, item, e);
            return Optional.empty();
        }
    }

    private String pendingReplayKey(String user, String item) {
        return MovieLensServingSideEffects.pendingReplayKey(user, item);
    }

    // Batch-fetch impression + click counters for a list of item IDs in two mget calls
    private Map<String, long[]> batchFetchCounters(List<String> itemIds) {
        if (itemIds.isEmpty()) {
            return Map.of();
        }
        List<String> impressionKeys = itemIds.stream().map(id -> "bandit:item:" + id + ":impressions").toList();
        List<String> clickKeys = itemIds.stream().map(id -> "bandit:item:" + id + ":clicks").toList();

        List<String> allKeys = new ArrayList<>(impressionKeys.size() + clickKeys.size());
        allKeys.addAll(impressionKeys);
        allKeys.addAll(clickKeys);

        List<String> values = Optional.ofNullable(redis.opsForValue().multiGet(allKeys))
            .orElseGet(() -> Collections.nCopies(allKeys.size(), null));

        Map<String, long[]> result = new LinkedHashMap<>();
        for (int i = 0; i < itemIds.size(); i++) {
            long impressions = readLong(values.get(i));
            long clicks = readLong(values.get(itemIds.size() + i));
            result.put(itemIds.get(i), new long[]{impressions, clicks});
        }
        return result;
    }

    // Mirrors the two-tower rating-embedding weighting: rated items carry an explicit
    // engagement signal so they contribute 2× vs watch-only items in the fallback vector.
    private double[] resolveUserVector(String user, List<String> recent, List<String> rated) {
        String key = properties.getEmbeddings().getUserPrefix() + ":" + user;
        double[] userVector = parseVector(redis.opsForValue().get(key));
        if (userVector.length > 0) {
            return userVector;
        }

        Map<String, Double> weightedItems = new LinkedHashMap<>();
        recent.forEach(id -> weightedItems.put(id, 1.0));
        rated.forEach(id -> weightedItems.merge(id, 2.0, Math::max));

        double[] aggregate = null;
        int dimension = 0;
        double totalWeight = 0.0;
        for (Map.Entry<String, Double> entry : weightedItems.entrySet()) {
            double[] vec = resolveItemVector(entry.getKey());
            if (vec.length == 0) continue;
            if (aggregate == null) {
                dimension = vec.length;
                aggregate = new double[dimension];
            }
            double w = entry.getValue();
            for (int i = 0; i < Math.min(dimension, vec.length); i++) {
                aggregate[i] += w * vec[i];
            }
            totalWeight += w;
        }
        if (aggregate == null || totalWeight == 0.0) {
            return new double[0];
        }
        for (int i = 0; i < dimension; i++) {
            aggregate[i] /= totalWeight;
        }
        return aggregate;
    }

    // Ports user_representation @ movie_corpus_embeddings.T from the two-tower retrieval model:
    // user norm is computed once and reused for every item, avoiding N redundant sqrt() calls.
    private Map<String, Double> batchRelevanceScores(double[] userVector, List<String> itemIds) {
        if (userVector.length == 0 || itemIds.isEmpty()) {
            return Map.of();
        }
        double userNormSq = 0.0;
        for (double v : userVector) {
            userNormSq += v * v;
        }
        if (userNormSq == 0.0) {
            return Map.of();
        }
        double userNorm = Math.sqrt(userNormSq);
        Map<String, Double> scores = new LinkedHashMap<>();
        for (String itemId : itemIds) {
            double[] itemVector = resolveItemVector(itemId);
            if (itemVector.length != userVector.length) {
                scores.put(itemId, 0.0);
                continue;
            }
            double dot = 0.0;
            double itemNormSq = 0.0;
            for (int i = 0; i < userVector.length; i++) {
                dot += userVector[i] * itemVector[i];
                itemNormSq += itemVector[i] * itemVector[i];
            }
            double itemNorm = Math.sqrt(itemNormSq);
            scores.put(itemId, itemNorm == 0.0 ? 0.0 : dot / (userNorm * itemNorm));
        }
        return scores;
    }

    private double[] resolveItemVector(String item) {
        double[] cached = featureCache.getItemVector(item);
        if (cached != null) return cached;
        double[] vector = parseVector(redis.opsForValue().get(properties.getEmbeddings().getItemPrefix() + ":" + item));
        featureCache.putItemVector(item, vector);
        return vector;
    }

    private void batchWarmItemVectors(List<String> candidates, List<String> recentHistory) {
        String prefix = properties.getEmbeddings().getItemPrefix() + ":";
        List<String> coldItems = Stream.concat(candidates.stream(), recentHistory.stream())
            .distinct()
            .filter(item -> !featureCache.hasItemVector(item))
            .collect(Collectors.toCollection(ArrayList::new));
        if (coldItems.isEmpty()) return;
        List<String> keys = coldItems.stream().map(item -> prefix + item).toList();
        List<String> raw = Optional.ofNullable(redis.opsForValue().multiGet(keys))
            .orElseGet(() -> Collections.nCopies(coldItems.size(), null));
        for (int i = 0; i < coldItems.size(); i++) {
            featureCache.putItemVector(coldItems.get(i), parseVector(raw.get(i)));
        }
    }

    private double[] parseVector(String raw) {
        if (raw == null || raw.isBlank()) {
            return new double[0];
        }

        String[] parts = WHITESPACE.split(raw.trim());
        double[] vector = new double[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                vector[i] = Double.parseDouble(parts[i]);
            } catch (NumberFormatException e) {
                return new double[0];
            }
        }
        return vector;
    }

    private long resolveLongMetric(String field) {
        Object value = redis.opsForHash().get(metricsHashKey(currentAlgorithm()), field);
        return readLong(value);
    }

    private Map<String, Object> buildAggregateMetricsForAlgorithm(String algorithm, double catalogCoverage) {
        return buildAggregateMetricsFromHash(metricsHashKey(algorithm), algorithm, catalogCoverage);
    }

    private Map<String, Object> buildAggregateMetricsFromHash(String hashKey, String algorithm, double catalogCoverage) {
        Map<Object, Object> raw = Optional.ofNullable(redis.opsForHash().entries(hashKey)).orElseGet(Map::of);
        Map<String, Object> metrics = new LinkedHashMap<>();
        long requests = readLong(raw.get("requests"));
        long served = readLong(raw.get("recommendations_served"));
        long clicks = readLong(raw.get("clicks"));
        double pseudoRegret = readDouble(raw.get("pseudo_regret_total"));
        double rewardTotal = readDouble(raw.get("reward_total"));
        double estimatedRewardTotal = readDouble(raw.get("estimated_reward_total"));
        double noveltyTotal = readDouble(raw.get("novelty_total"));

        metrics.put("algorithm", algorithm);
        metrics.put("requests", requests);
        metrics.put("recommendationsServed", served);
        metrics.put("clicks", clicks);
        metrics.put("ctr", served == 0 ? 0.0 : round((double) clicks / served));
        metrics.put("avgObservedReward", served == 0 ? 0.0 : round(rewardTotal / served));
        metrics.put("avgEstimatedReward", served == 0 ? 0.0 : round(estimatedRewardTotal / served));
        metrics.put("avgPseudoRegret", requests == 0 ? 0.0 : round(pseudoRegret / requests));
        metrics.put("cumulativePseudoRegret", round(pseudoRegret));
        metrics.put("avgNoveltyScore", served == 0 ? 0.0 : round(noveltyTotal / served));
        metrics.put("coldStartImpressions", readLong(raw.get("cold_start_impressions")));
        metrics.put("exploratoryImpressions", readLong(raw.get("exploratory_impressions")));
        metrics.put("catalogCoverage", round(catalogCoverage));
        return metrics;
    }

    private boolean hasRecordedActivity(Map<String, Object> metrics) {
        Object requests = metrics.get("requests");
        return requests instanceof Number number && number.longValue() > 0L;
    }

    private List<String> supportedAlgorithms() {
        String current = currentAlgorithm();
        if (SUPPORTED_ALGORITHMS.contains(current)) {
            return SUPPORTED_ALGORITHMS;
        }

        List<String> algorithms = new ArrayList<>(SUPPORTED_ALGORITHMS);
        algorithms.add(current);
        return algorithms;
    }

    private String currentAlgorithm() {
        String algorithm = properties.getBandit().getAlgorithm();
        if (algorithm == null || algorithm.isBlank()) {
            return "ucb";
        }
        return algorithm.trim().toLowerCase(Locale.ROOT);
    }

    private boolean isTabularRl() {
        return "q-learning".equals(currentAlgorithm()) || "sarsa".equals(currentAlgorithm());
    }

    private String metricsHashKey(String algorithm) {
        return MovieLensServingSideEffects.metricsHashKey(algorithm);
    }

    private String qKeyForStateMap(Map<?, ?> state) {
        return qKeyForState(stateKey(state));
    }

    private String qKeyForState(String stateKey) {
        return currentAlgorithm() + ":q:" + stateKey;
    }

    // Q-table identity keys on the coarse genre/tag signature only (see TabularStateKey);
    // raw recent item-IDs stay in the state payload for nextActionSpace but are excluded here.
    private String stateKey(Object state) {
        if (state instanceof Map<?, ?> map) {
            return TabularStateKey.hash(map.get("genres"), map.get("tags"));
        }
        return TabularStateKey.hash(null, null);
    }

    private double resolveCatalogCoverage() {
        int catalogSize = properties.getCatalog().size();
        if (catalogSize == 0) {
            return 0.0;
        }
        Long exposed = redis.opsForSet().size(MovieLensServingSideEffects.EXPOSED_ITEMS_KEY);
        return exposed == null ? 0.0 : (double) exposed / catalogSize;
    }

    BanditArmScore computeBanditArmScore(
        double baseScore,
        long itemImpressions,
        long clicks,
        long totalImpressions,
        boolean coldStart
    ) {
        long failures = Math.max(itemImpressions - clicks, 0L);
        // Cold-start raises priorStrength, which also raises effectivePulls in ucbExplorationBonus and thus
        // SHRINKS the UCB confidence bonus; coldStartBoost then multiplies that bonus back up, so the two
        // effects nearly cancel — cold-start's net UCB boost is small. Left as-is (tuning it is behavior-changing).
        double priorStrength = coldStart
            ? Math.max(WARM_PRIOR_STRENGTH,
                properties.getBandit().getColdStartBoost() * RecommendationConstants.COLD_START_PRIOR_STRENGTH_MULTIPLIER)
            : WARM_PRIOR_STRENGTH;
        double base = clamp(baseScore);
        double priorAlpha = 1.0 + (base * priorStrength);
        double priorBeta = 1.0 + ((1.0 - base) * priorStrength);
        double posteriorAlpha = priorAlpha + clicks;
        double posteriorBeta = priorBeta + failures;
        double posteriorMean = clamp(posteriorAlpha / (posteriorAlpha + posteriorBeta));

        if ("thompson".equals(currentAlgorithm())) {
            return thompsonArmScore(posteriorAlpha, posteriorBeta, posteriorMean);
        }
        double bonus = ucbExplorationBonus(itemImpressions, totalImpressions, priorStrength, coldStart);
        return new BanditArmScore(posteriorMean, bonus, posteriorMean + bonus);
    }

    // Thompson: the ranking score IS the posterior sample; the reported explorationBonus
    // (|sample - mean|, capped) is a diagnostic only. "mean + bonus" is literal for UCB, not Thompson.
    private BanditArmScore thompsonArmScore(double posteriorAlpha, double posteriorBeta, double posteriorMean) {
        double sampledPosterior = clamp(sampleBeta(posteriorAlpha, posteriorBeta));
        double explorationMagnitude = Math.min(
            Math.abs(sampledPosterior - posteriorMean),
            properties.getBandit().getMaxExplorationBonus()
        );
        return new BanditArmScore(posteriorMean, explorationMagnitude, sampledPosterior);
    }

    private double ucbExplorationBonus(long itemImpressions, long totalImpressions, double priorStrength, boolean coldStart) {
        double effectivePulls = itemImpressions + priorStrength;
        double confidence = Math.sqrt(Math.log(totalImpressions + 2.0) / (2.0 * (effectivePulls + 1.0)));
        double bonus = properties.getBandit().getExplorationAlpha() * confidence;
        if (coldStart) {
            bonus *= properties.getBandit().getColdStartBoost();
        }
        return Math.min(bonus, properties.getBandit().getMaxExplorationBonus());
    }

    private double computeTabularRlRankingScore(double learnedPrior, Double qValue) {
        if (ThreadLocalRandom.current().nextDouble() < clamp(properties.getBandit().getQLearningEpsilon())) {
            return ThreadLocalRandom.current().nextDouble();
        }
        return qValue == null ? clamp(learnedPrior) : clamp(qValue);
    }


    @SafeVarargs
    private final <T> List<T> firstNonEmpty(List<T>... candidates) {
        for (List<T> candidate : candidates) {
            if (candidate != null && !candidate.isEmpty()) {
                return candidate;
            }
        }
        return List.of();
    }


    private double normalizeScore(double raw) {
        return clamp((raw + 1.0) / 2.0);
    }

    private double clamp(double value) {
        return Math.max(0.0, Math.min(1.0, value));
    }

    private long readLong(Object raw) {
        if (raw == null) {
            return 0L;
        }
        try {
            return Long.parseLong(raw.toString());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }


    private double readDouble(Object raw) {
        if (raw == null) {
            return 0.0;
        }
        try {
            return Double.parseDouble(raw.toString());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private double computePseudoRegret(List<ScoredCandidate> ranked, List<ScoredCandidate> selected, int limit) {
        List<ScoredCandidate> oracle = ranked.stream()
            .sorted(Comparator.comparingDouble(ScoredCandidate::estimatedReward).reversed())
            .limit(limit)
            .toList();

        double oracleReward = oracle.stream().mapToDouble(ScoredCandidate::estimatedReward).sum();
        double selectedReward = selected.stream().mapToDouble(ScoredCandidate::estimatedReward).sum();
        return Math.max(0.0, oracleReward - selectedReward);
    }

    private double round(double value) {
        return Math.round(value * 1000.0) / 1000.0;
    }

    private double sampleBeta(double alpha, double beta) {
        double x = sampleGamma(alpha);
        double y = sampleGamma(beta);
        return (x <= 0.0 && y <= 0.0) ? 0.5 : x / (x + y);
    }

    private double sampleGamma(double shape) {
        if (shape <= 0.0) {
            return 0.0;
        }
        if (shape < 1.0) {
            double u = ThreadLocalRandom.current().nextDouble();
            return sampleGamma(shape + 1.0) * Math.pow(u, 1.0 / shape);
        }

        double d = shape - (1.0 / 3.0);
        double c = 1.0 / Math.sqrt(9.0 * d);
        while (true) {
            double x = ThreadLocalRandom.current().nextGaussian();
            double v = 1.0 + (c * x);
            if (v <= 0.0) {
                continue;
            }
            v = v * v * v;
            double u = ThreadLocalRandom.current().nextDouble();
            if (u < 1.0 - (0.0331 * x * x * x * x)) {
                return d * v;
            }
            if (Math.log(u) < (0.5 * x * x) + d * (1.0 - v + Math.log(v))) {
                return d * v;
            }
        }
    }

    private record SelectionOutputs(
        List<ScoredCandidate> selected,
        List<ScoredCandidate> candidateSnapshot
    ) {
    }

    private record RedisFetchOutputs(
        Map<String, Double> popularityMap,
        double maxPopularity,
        Map<String, Double> genrePreferences,
        Map<String, Double> tagPreferences,
        Map<String, Object> state,
        String stateKey,
        RetrievalOutcome candidateGeneration,
        Map<String, MovieCandidate> candidateById,
        List<String> eligibleList,
        long totalImpressions,
        Map<String, long[]> counters,
        Map<String, Double> qValues
    ) {
    }

    private static final class PopularityFetchException extends RuntimeException {
        private PopularityFetchException(Exception cause) {
            super(cause);
        }
    }

    // Package-private (not private): a test constructs one directly to drive toServedMovie and
    // grpoFeaturesFor on an identical candidate without going through the full recommend() path.
    record ScoredCandidate(
        String itemId,
        double estimatedReward,
        double relevanceScore,
        double contentScore,
        double popularityScore,
        double onlineScore,
        double explorationBonus,
        double banditScore,
        boolean coldStart,
        double noveltyScore,
        long impressions,
        long clicks,
        double qValue,
        double weightedOutcomeScore,
        double predictionScore,
        double diversityScore,
        String diversityGroupId,
        String primaryGenre
    ) implements MovieLensOutcomeScorer.DiversityCandidate, TopKScoreSelector.Scored {
        @Override
        public double preDiversityScore() {
            return predictionScore;
        }

        @Override
        public double finalScore() {
            return banditScore;
        }

        @Override
        public ScoredCandidate withDiversity(double diversityScore, double finalScore) {
            return new ScoredCandidate(
                itemId,
                estimatedReward,
                relevanceScore,
                contentScore,
                popularityScore,
                onlineScore,
                explorationBonus,
                finalScore,
                coldStart,
                noveltyScore,
                impressions,
                clicks,
                qValue,
                weightedOutcomeScore,
                predictionScore,
                diversityScore,
                diversityGroupId,
                primaryGenre
            );
        }
    }

    private record TabularRlUpdate(
        String qKey,
        String action,
        double reward,
        double alpha,
        double gamma,
        double nextValue
    ) {
    }

    private record NextActionValue(
        String action,
        double value
    ) {
    }

    record BanditArmScore(
        double posteriorMean,
        double explorationBonus,
        double rankingScore
    ) {
    }

}
