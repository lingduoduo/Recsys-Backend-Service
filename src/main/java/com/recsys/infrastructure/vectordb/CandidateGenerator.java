package com.recsys.infrastructure.vectordb;

import com.recsys.infrastructure.dataloading.DataLoader;
import com.recsys.infrastructure.dataloading.DataManager;
import com.recsys.infrastructure.vectordb.spann.SpannConfig;
import com.recsys.infrastructure.vectordb.spann.SpannVectorIndex;
import com.recsys.domain.item.Movie;
import com.recsys.domain.rating.Rating;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CandidateGenerator {

    private static final Logger log = LoggerFactory.getLogger(CandidateGenerator.class);

    private final DataManager dataManager;
    private final Map<Integer, float[]> movieEmbeddings;
    private final EmbeddingStore userEmbeddingStore;
    private final VectorIndex embeddingIndex;
    private final int embeddingDim;

    /** Classpath-only constructor — user embeddings are loaded once from the file system. */
    public CandidateGenerator(DataManager dataManager) {
        this(dataManager, null);
    }

    /**
     * Three-tier constructor.
     *
     * {@code userEmbeddingStore} is a Tier-3 cache backed by Redis (Tier 2) seeded from the
     * file system (Tier 1). Passing {@code null} falls back to classpath-only behaviour.
     */
    public CandidateGenerator(DataManager dataManager, EmbeddingStore userEmbeddingStore) {
        this(dataManager, userEmbeddingStore, null);
    }

    /** Test seam: same as the two-arg constructor but pins the SPANN index directory. */
    CandidateGenerator(DataManager dataManager, EmbeddingStore userEmbeddingStore, Path spannDir) {
        this.dataManager = dataManager;
        this.movieEmbeddings = DataLoader.loadMovieEmbeddings();
        this.userEmbeddingStore = userEmbeddingStore;
        this.embeddingIndex = createEmbeddingIndex(movieEmbeddings, spannDir);
        this.embeddingDim = movieEmbeddings.isEmpty()
                ? 0
                : movieEmbeddings.values().iterator().next().length;
        log.info("Embedding backend={}, movies={}, userStore={}",
                embeddingIndex.name(), movieEmbeddings.size(),
                userEmbeddingStore != null ? "cache+redis" : "classpath");
    }

    /** Name of the vector backend in use ({@code exact}, {@code lsh}, {@code spann}). */
    public String embeddingBackendName() {
        return embeddingIndex.name();
    }

    /** Releases the vector index's resources (the SPANN index file); a no-op for heap backends. */
    public void close() {
        embeddingIndex.close();
    }

    // Genre-based: for each genre on the seed movie, pull top-rated candidates,
    // deduplicate via map, remove the seed itself.
    public List<Movie> byGenre(Movie seed, int limitPerGenre) {
        Map<Integer, Movie> candidates = new LinkedHashMap<>();
        for (String genre : seed.genres()) {
            for (Movie m : dataManager.getMoviesByGenre(genre, limitPerGenre)) {
                candidates.put(m.id(), m);
            }
        }
        candidates.remove(seed.id());
        return List.copyOf(candidates.values());
    }

    // Multi-strategy: genres derived from user history + global top-rated + latest releases,
    // with already-watched movies excluded.
    public List<Movie> byUserHistory(int userId, int limitPerGenre) {
        List<Rating> history = dataManager.getRatingsByUser(userId);
        Set<Integer> watched = dataManager.getWatchedMovieIds(userId);

        Set<String> genres = new HashSet<>();
        for (Rating r : history) {
            Movie m = dataManager.getMovieById(r.movieId());
            if (m != null) genres.addAll(m.genres());
        }

        Map<Integer, Movie> candidates = new LinkedHashMap<>();
        for (String genre : genres) {
            for (Movie m : dataManager.getMoviesByGenre(genre, limitPerGenre)) {
                candidates.put(m.id(), m);
            }
        }
        for (Movie m : dataManager.getTopRatedMovies(100)) candidates.put(m.id(), m);
        for (Movie m : dataManager.getLatestMovies(100)) candidates.put(m.id(), m);
        watched.forEach(candidates::remove);
        return List.copyOf(candidates.values());
    }

    // Embedding-based retrieval delegates vector search to the selected index.
    // Backends are configured with RECSYS_VECTOR_BACKEND or -Drecsys.vector.backend:
    // lsh (default) or exact. FAISS belongs behind this interface for Linux/JNI deployments.
    public List<Movie> byEmbedding(int userId, int k) {
        return byEmbedding(userId, k, false);
    }

    public List<Movie> byEmbeddingPrimary(int userId, int k) {
        return byEmbedding(userId, k, true);
    }

    private List<Movie> byEmbedding(int userId, int k, boolean primary) {
        // Check the three-tier cache first (heap → Redis); fall back to classpath map.
        float[] userVec = userEmbeddingStore != null
                ? (primary ? userEmbeddingStore.getEmbeddingPrimary(userId) : userEmbeddingStore.getEmbedding(userId))
                : null;
        if (userVec == null) return List.of();

        Set<Integer> watched = dataManager.getWatchedMovieIds(userId);

        List<SearchResult> hits = embeddingIndex.search(userVec, k, watched);
        List<Movie> out = new ArrayList<>(hits.size());
        for (SearchResult s : hits) {
            Movie m = dataManager.getMovieById(s.id());
            if (m != null) out.add(m);
        }
        return Collections.unmodifiableList(out);
    }

    // Hot-reload embedding: serialize concurrent writes from /setembedding HTTP requests
    // so they don't interleave at the LshVectorIndex level (three non-atomic steps:
    // embeddings map + lsh buckets + allIds). Reads are concurrent and correct.
    public synchronized void updateEmbedding(int id, float[] vec) {
        if (vec == null) {
            throw new IllegalArgumentException("vector must not be null");
        }
        // The ANN index is built for the seed embedding dimension; a mismatched vector would
        // overrun the hyperplane arrays (ArrayIndexOutOfBounds) deep inside the index. Reject
        // it here with a clear message so the API can surface a 400 instead of a 500.
        if (embeddingDim > 0 && vec.length != embeddingDim) {
            throw new IllegalArgumentException(
                    "vector dimension mismatch: expected " + embeddingDim + ", got " + vec.length);
        }
        embeddingIndex.addOrUpdate(id, vec);
    }

    /** Embedding dimension the index expects, or 0 if no seed embeddings were loaded. */
    public int embeddingDimension() {
        return embeddingDim;
    }

    private static VectorIndex createEmbeddingIndex(Map<Integer, float[]> embeddings, Path spannDir) {
        if (embeddings.isEmpty()) return new ExactVectorIndex(Map.of());

        String backend = System.getProperty("recsys.vector.backend");
        if (backend == null || backend.isBlank()) {
            backend = System.getenv().getOrDefault("RECSYS_VECTOR_BACKEND", "lsh");
        }

        return switch (backend.trim().toLowerCase()) {
            case "exact", "flat" -> new ExactVectorIndex(embeddings);
            case "lsh", "ann" -> new LshVectorIndex(embeddings);
            case "spann" -> {
                SpannConfig cfg = SpannConfig.fromEnv(System::getenv);
                if (spannDir != null) cfg = cfg.withDir(spannDir);
                yield new SpannVectorIndex(embeddings, cfg);
            }
            case "faiss" -> {
                log.warn("FAISS backend requested but not enabled in the portable build; falling back to LSH.");
                yield new LshVectorIndex(embeddings);
            }
            default -> throw new IllegalArgumentException("Unknown vector backend: " + backend);
        };
    }
}
