package com.recsys.api.serving;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.recsys.infrastructure.dataloading.DataManager;
import com.recsys.infrastructure.vectordb.EmbeddingStore;
import com.recsys.config.EnvVars;
import com.recsys.infrastructure.vectordb.CoRatedTokenBags;
import com.recsys.infrastructure.vectordb.ExactMultiVectorIndex;
import com.recsys.infrastructure.vectordb.ExactVectorIndex;
import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.domain.item.Movie;
import com.recsys.domain.item.MovieCandidate;
import com.recsys.domain.recommendation.RecommendationQuery;
import com.recsys.domain.recommendation.RecommendationResponse;
import com.recsys.domain.recommendation.RecommendationResult;
import com.recsys.domain.user.User;
import com.recsys.application.recommendation.RecommendationPipeline;
import com.recsys.application.retrieval.multichannel.MultiChannelRecallService;
import com.recsys.application.retrieval.multichannel.RecallResult;
import com.recsys.application.retrieval.multichannel.RecallResult.DegradationOutcome;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Recommendation-family endpoints for the offline serving API: the v1
 * multichannel-recall lookup, the v2 pipeline endpoint, the embedding-based
 * "similar movies" search, and the liveness probe. They share no state but form
 * one cohesive surface, so they live together as nested services.
 */
public final class RecommendationService {

    private RecommendationService() {}

    /** GET /getrecommendation, /recommendation — v1 multichannel recall by user. */
    public static final class V1 extends BaseApiService {

        private static final int RECALL_MULTIPLIER = 3;

        private final DataManager dataManager;
        private final MultiChannelRecallService recallService;

        public V1(DataManager dataManager, MultiChannelRecallService recallService) {
            this.dataManager = dataManager;
            this.recallService = recallService;
        }

        @Override
        protected HttpResponse doGet(ServiceRequestContext ctx, HttpRequest req) {
            return HttpResponse.of(CompletableFuture.supplyAsync(() -> {
                try {
                    int userId = requiredIntParam(ctx, "userId");
                    User user = dataManager.getUserById(userId);
                    if (user == null) return writeError(HttpStatus.NOT_FOUND, "user not found", "userId", userId);

                    int k = optionalIntParam(ctx, "k", 20, 1, 100);
                    Set<String> excludedItemIds = dataManager.getWatchedMovieIds(userId).stream()
                            .map(String::valueOf)
                            .collect(Collectors.toSet());
                    RecommendationQuery query = new RecommendationQuery(
                            String.valueOf(userId), k, excludedItemIds, null);

                    RecallResult recall = recallService.recallDetailed(query, k * RECALL_MULTIPLIER);
                    List<MovieCandidate> candidates = recall.candidates();
                    List<Movie> movies = candidates.stream()
                            .map(c -> {
                                try { return dataManager.getMovieById(Integer.parseInt(c.itemId())); }
                                catch (NumberFormatException e) { return null; }
                            })
                            .filter(Objects::nonNull)
                            .toList();

                    return writeJsonWithRecallDegraded(HttpStatus.OK,
                            new RecommendationResponse(user, movies), recall.degradedChannels(),
                            recall.outcome());

                } catch (BadRequestException | IllegalArgumentException e) {
                    return writeError(HttpStatus.BAD_REQUEST, e.getMessage());
                } catch (Exception e) {
                    log.error("Unexpected error in RecommendationService.V1", e);
                    return writeError(HttpStatus.INTERNAL_SERVER_ERROR, "internal server error");
                }
            }, ctx.blockingTaskExecutor()));
        }
    }

    /** POST /v2/recommend — pipeline-driven recommendation from a JSON query. */
    public static final class V2 extends BaseApiService {

        private final RecommendationPipeline pipeline;

        public V2(RecommendationPipeline pipeline) {
            this.pipeline = pipeline;
        }

        @Override
        protected HttpResponse doPost(ServiceRequestContext ctx, HttpRequest req) {
            return HttpResponse.of(req.aggregate().thenApplyAsync(agg -> {
                try {
                    RecommendationQuery query = readJsonBody(agg, RecommendationQuery.class);
                    RecommendationResult result = pipeline.recommend(query);
                    String degraded = result.trace().get("degradedChannels");
                    Set<String> degradedSet = (degraded == null || degraded.isBlank())
                            ? Set.of()
                            : new LinkedHashSet<>(List.of(degraded.split(",")));
                    String outcomeValue = result.trace().get("degradationOutcome");
                    DegradationOutcome outcome = outcomeValue == null
                            ? DegradationOutcome.HEALTHY
                            : DegradationOutcome.fromWireValue(outcomeValue);
                    return writeJsonWithRecallDegraded(HttpStatus.OK, result, degradedSet, outcome);
                } catch (BadRequestException | IllegalArgumentException e) {
                    return writeError(HttpStatus.BAD_REQUEST, e.getMessage());
                } catch (Exception e) {
                    log.error("Unexpected error in RecommendationService.V2", e);
                    return writeError(HttpStatus.INTERNAL_SERVER_ERROR, "internal server error");
                }
            }, ctx.blockingTaskExecutor()));
        }
    }

    /**
     * GET /similar — nearest-neighbour movies by raw inner product over a co-rated candidate set,
     * or by Sum of MaxSim over token bags when {@link Scoring#SUM_OF_MAXSIM} is configured.
     */
    public static final class Similar extends BaseApiService {

        private static final int LIMIT_PER_GENRE = 50;
        private static final int RECALL_MULTIPLIER = 5;

        // Embeddings can be rewritten by POST /setembedding, so the fresh window is short.
        // Bulk reloads additionally require an explicit CDN invalidation — see
        // docs/runbooks/cdn-operations.md.
        private static final String CACHE_CONTROL = HttpCaching.publicCache(300, 3600);

        private final EmbeddingStore store;
        private final DataManager dataManager;
        private final Scoring scoring;
        private final CoRatedTokenBags tokenBags;

        /**
         * How candidates are scored against the seed. A deployment-level setting, not a query
         * parameter: the CDN cache key for this route whitelists only {@code movieId} and
         * {@code k}, so a per-request switch would let one mode's body be served under the
         * other mode's key. Flipping it needs a CDN invalidation — docs/runbooks/cdn-operations.md.
         */
        public enum Scoring {
            /** One vector per movie, raw inner product — the original behaviour. */
            INNER_PRODUCT,
            /**
             * Sum of MaxSim over token bags built from the Word2Vec vectors: each movie is its own
             * vector plus its co-rated neighbours' vectors ({@link CoRatedTokenBags}), and the seed's
             * bag is the query. Candidate selection, response shape and cache headers are unchanged.
             */
            SUM_OF_MAXSIM;

            public static final String ENV_VAR = "RECSYS_SIMILAR_SCORING";

            /** Reads {@link #ENV_VAR}; blank means {@link #INNER_PRODUCT}, anything unknown fails fast. */
            public static Scoring fromEnv(EnvVars.EnvReader env) {
                String raw = env.get(ENV_VAR);
                if (raw == null || raw.isBlank()) return INNER_PRODUCT;
                try {
                    return valueOf(raw.trim().toUpperCase(Locale.ROOT));
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("env var " + ENV_VAR + " must be one of "
                            + Arrays.toString(values()) + " (case-insensitive), got: " + raw);
                }
            }
        }

        public Similar(EmbeddingStore store) {
            this(store, DataManager.getInstance());
        }

        public Similar(EmbeddingStore store, DataManager dataManager) {
            this(store, dataManager, Scoring.INNER_PRODUCT);
        }

        public Similar(EmbeddingStore store, DataManager dataManager, Scoring scoring) {
            this.store = store;
            this.dataManager = dataManager;
            this.scoring = scoring;
            this.tokenBags = new CoRatedTokenBags(store, dataManager);
        }

        @Override
        protected HttpResponse doGet(ServiceRequestContext ctx, HttpRequest req) {
            return HttpResponse.of(CompletableFuture.supplyAsync(() -> {
                try {
                    // Both are cache-key parameters (the recsys-similar policy whitelists
                    // movieId and k), so canonical spellings only and no clamping: a clamped
                    // k made every value above 200 a distinct key over the k=200 body.
                    int movieId = cacheKeyIntParam(ctx, "movieId");
                    int k = cacheKeyIntParam(ctx, "k", 10, 1, 200);
                    float[] queryVec = store.getEmbedding(movieId);
                    // Load-bearing no-store: 404 is on CloudFront's unconditionally-cached
                    // list, so without it a miss would be pinned at the edge for the 10 s
                    // Error Caching Minimum TTL — and an embedding can be written at any time
                    // by POST /setembedding, so a pinned 404 would outlive the gap.
                    if (queryVec == null)
                        return writeNoStoreJson(HttpStatus.NOT_FOUND, Map.of(
                                "error", "embedding not found for movieId", "movieId", movieId));
                    Set<Integer> candidateIds = selectCandidates(movieId, k);
                    List<ScoredMovie> scored = score(movieId, queryVec, candidateIds, k);
                    return writeCacheableJson(HttpStatus.OK,
                            new SimilarMoviesResult(movieId, scored), CACHE_CONTROL, req);
                } catch (BadRequestException e) {
                    // Defensive, not load-bearing: CloudFront caches a 400 only when the
                    // origin sends max-age/s-maxage. The unconditionally-cached codes are 404,
                    // 414 and 5xx — see the no-store 404 above and writeNoStoreError's javadoc.
                    return writeNoStoreError(HttpStatus.BAD_REQUEST, e.getMessage());
                } catch (Exception e) {
                    log.error("Unexpected error in RecommendationService.Similar", e);
                    return writeNoStoreError(HttpStatus.INTERNAL_SERVER_ERROR, "internal server error");
                }
            }, ctx.blockingTaskExecutor()));
        }

        private Set<Integer> selectCandidates(int movieId, int k) {
            Set<Integer> candidates = new LinkedHashSet<>();
            int max = k * RECALL_MULTIPLIER;
            for (Movie m : dataManager.getSimilarMovies(movieId)) {
                candidates.add(m.id());
                if (candidates.size() >= max) return candidates;
            }
            Movie seed = dataManager.getMovieById(movieId);
            if (seed != null) {
                for (String genre : seed.genres()) {
                    for (Movie m : dataManager.getMoviesByGenre(genre, LIMIT_PER_GENRE)) {
                        if (m.id() != movieId) candidates.add(m.id());
                        if (candidates.size() >= max) return candidates;
                    }
                }
            }
            for (Movie m : dataManager.getTopRatedMovies(max)) {
                if (m.id() != movieId) candidates.add(m.id());
                if (candidates.size() >= max) return candidates;
            }
            return candidates;
        }

        private List<ScoredMovie> score(int movieId, float[] seedVec, Set<Integer> candidateIds, int k) {
            List<SearchResult> hits = switch (scoring) {
                case INNER_PRODUCT -> ExactVectorIndex.search(
                        store.getEmbeddings(candidateIds), seedVec, k, Set.of(movieId));
                case SUM_OF_MAXSIM -> {
                    // The seed's own bag is the query; candidates' bags are the documents. One bulk
                    // read covers seed + candidates + all their neighbours.
                    Set<Integer> withSeed = new LinkedHashSet<>(candidateIds);
                    withSeed.add(movieId);
                    Map<Integer, float[][]> bags = tokenBags.bagsFor(withSeed);
                    float[][] query = bags.get(movieId);
                    // The seed vector was read a moment ago; a bag can only be missing if it was
                    // deleted in between. Fall back to the vector we hold rather than 500.
                    if (query == null) query = new float[][]{seedVec};
                    yield ExactMultiVectorIndex.search(bags, query, k, Set.of(movieId));
                }
            };
            return hits.stream().map(r -> new ScoredMovie(r.id(), r.score())).toList();
        }

        public record ScoredMovie(int movieId, double score) {}
        public record SimilarMoviesResult(int movieId, List<ScoredMovie> similar) {}
    }

    /** GET /health — liveness probe. */
    public static final class Health extends BaseApiService {
        @Override
        protected HttpResponse doGet(ServiceRequestContext ctx, HttpRequest req) {
            return writeJson(HttpStatus.OK, Map.of("ok", true));
        }
    }
}
