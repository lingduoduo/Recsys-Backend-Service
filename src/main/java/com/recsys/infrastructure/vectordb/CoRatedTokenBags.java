package com.recsys.infrastructure.vectordb;

import com.recsys.domain.item.Movie;
import com.recsys.infrastructure.dataloading.DataManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Builds a multi-vector "token bag" per item out of the single Word2Vec vectors the system
 * already has: an item's bag is its own vector followed by the vectors of its co-rated
 * neighbours ({@link DataManager#getSimilarMovies}). This gives {@link MultiVectorMath#sumOfMaxSim}
 * something to take a max over without a new model or a new embedding artifact.
 *
 * <p>Vectors are read from the {@link EmbeddingStore}, not the classpath map, so a vector
 * rewritten by {@code POST /setembedding} is picked up; the whole bag set is one bulk read over
 * the union of items and neighbours. An item with no vector of its own is omitted rather than
 * ranked on borrowed neighbour tokens; a neighbour with no vector is simply left out of the bag.
 */
public final class CoRatedTokenBags {

    private final EmbeddingStore store;
    private final DataManager dataManager;

    public CoRatedTokenBags(EmbeddingStore store, DataManager dataManager) {
        this.store = Objects.requireNonNull(store, "store");
        this.dataManager = Objects.requireNonNull(dataManager, "dataManager");
    }

    /** Bags for {@code itemIds}, keyed by item; items lacking their own vector are absent. */
    public Map<Integer, float[][]> bagsFor(Collection<Integer> itemIds) {
        if (itemIds.isEmpty()) return Map.of();

        Map<Integer, List<Integer>> neighbours = new LinkedHashMap<>();
        Set<Integer> toFetch = new LinkedHashSet<>(itemIds);
        for (int id : itemIds) {
            List<Integer> ns = new ArrayList<>();
            for (Movie m : dataManager.getSimilarMovies(id)) ns.add(m.id());
            neighbours.put(id, ns);
            toFetch.addAll(ns);
        }
        Map<Integer, float[]> vectors = store.getEmbeddings(toFetch);

        Map<Integer, float[][]> bags = new LinkedHashMap<>();
        for (int id : itemIds) {
            float[] own = vectors.get(id);
            if (own == null) continue;
            List<float[]> tokens = new ArrayList<>();
            tokens.add(own);
            for (int n : neighbours.get(id)) {
                float[] v = vectors.get(n);
                // A wrong-width neighbour would make sumOfMaxSim score the whole bag -inf (and,
                // on the seed's bag, drop every candidate). Degrade the bag instead; the
                // own-vector-vs-seed comparison still decides this item's own fate.
                if (v != null && v.length == own.length) tokens.add(v);
            }
            bags.put(id, tokens.toArray(new float[0][]));
        }
        return bags;
    }
}
