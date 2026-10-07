package com.recsys.infrastructure.vectordb;

import com.recsys.domain.item.Movie;
import com.recsys.infrastructure.dataloading.DataManager;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ItemEmbeddingRefresherTest {

    private static final float[] A = {1f, 0f};
    private static final float[] B = {0f, 1f};

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final List<String> applied = new ArrayList<>();
    private final ItemEmbeddingSink recording = (id, v) -> applied.add(id + "=" + v[0] + "," + v[1]);

    /** A Redis stand-in: the current values, ids to report as corrupt, and an optional failure. */
    private final Map<Integer, float[]> redis = new HashMap<>();
    private final Set<Integer> corrupt = new java.util.HashSet<>();
    private RuntimeException failure;
    private int reads;
    private final ItemEmbeddingReader reader = (Collection<Integer> ids, IntConsumer onCorrupt) -> {
        reads++;
        if (failure != null) throw failure;
        Map<Integer, float[]> out = new HashMap<>();
        for (int id : ids) {
            if (corrupt.contains(id)) onCorrupt.accept(id);
            else if (redis.containsKey(id)) out.put(id, redis.get(id));
        }
        return out;
    };

    private ItemEmbeddingRefresher refresher(Map<Integer, float[]> initial, ItemEmbeddingSink... sinks) {
        return new ItemEmbeddingRefresher(() -> List.of(1, 2, 3), reader, 2, initial, List.of(sinks), registry);
    }

    private double count(String name, String... tags) {
        var search = registry.find(name);
        if (tags.length > 0) search = search.tags(tags);
        var c = search.counter();
        return c == null ? 0 : c.count();
    }

    @Test
    void changedVectorReachesEverySinkExactlyOnce() {
        List<String> second = new ArrayList<>();
        redis.put(1, B);
        ItemEmbeddingRefresher r = refresher(Map.of(1, A), recording, (id, v) -> second.add("" + id));

        r.refreshOnce();

        assertThat(applied).containsExactly("1=0.0,1.0");
        assertThat(second).containsExactly("1");
        assertThat(count(ItemEmbeddingRefresher.APPLIED)).isEqualTo(1.0);
    }

    @Test
    void unchangedVectorIsNotReappliedOnTheNextPass() {
        // SpannVectorIndex.addOrUpdate appends a posting block per call: re-applying unchanged
        // vectors every interval would grow the index file without bound.
        redis.put(1, A);
        redis.put(2, B);
        ItemEmbeddingRefresher r = refresher(Map.of(1, A), recording);

        r.refreshOnce();   // id 2 is new relative to the seed -> applied once
        r.refreshOnce();   // nothing changed

        assertThat(applied).containsExactly("2=0.0,1.0");
    }

    @Test
    void missingKeyKeepsTheCurrentVector() {
        ItemEmbeddingRefresher r = refresher(Map.of(1, A), recording);   // redis is empty

        r.refreshOnce();

        assertThat(applied).isEmpty();
    }

    @Test
    void wrongDimensionIsSkippedAndCounted() {
        redis.put(1, new float[]{1f, 2f, 3f});
        ItemEmbeddingRefresher r = refresher(Map.of(), recording);

        r.refreshOnce();

        assertThat(applied).isEmpty();
        assertThat(count(ItemEmbeddingRefresher.SKIPPED, "reason", "dimension")).isEqualTo(1.0);
    }

    @Test
    void corruptValueIsSkippedAndCountedOthersStillApply() {
        corrupt.add(1);
        redis.put(2, B);
        ItemEmbeddingRefresher r = refresher(Map.of(), recording);

        r.refreshOnce();

        assertThat(applied).containsExactly("2=0.0,1.0");
        assertThat(count(ItemEmbeddingRefresher.SKIPPED, "reason", "corrupt")).isEqualTo(1.0);
    }

    @Test
    void storeFailureThrowsAndTouchesNothing() {
        failure = new IllegalStateException("redis down");
        ItemEmbeddingRefresher r = refresher(Map.of(1, A), recording);

        assertThatThrownBy(r::refreshOnce).isSameAs(failure);
        assertThat(applied).isEmpty();
    }

    @Test
    void sinkFailureLeavesTheItemPendingForTheNextPass() {
        redis.put(1, B);
        boolean[] fail = {true};
        ItemEmbeddingRefresher r = refresher(Map.of(1, A), (id, v) -> {
            if (fail[0]) throw new IllegalStateException("index closed");
            applied.add("" + id);
        });

        assertThatThrownBy(r::refreshOnce).hasMessage("index closed");
        fail[0] = false;
        r.refreshOnce();

        assertThat(applied).containsExactly("1");
    }

    @Test
    void loopRecordsAFailedPassInsteadOfDying() {
        failure = new IllegalStateException("redis down");
        ItemEmbeddingRefresher r = refresher(Map.of(), recording);

        r.loop().run();   // must not throw

        assertThat(r.loop().failureCount()).isEqualTo(1);
    }

    @Test
    void intervalDefaultsTo60sZeroDisablesNegativeIsRejected() {
        assertThat(ItemEmbeddingRefresher.intervalFromEnv(name -> null)).isEqualTo(60_000L);
        assertThat(ItemEmbeddingRefresher.intervalFromEnv(Map.of(ItemEmbeddingRefresher.INTERVAL_ENV, "0")::get))
                .isZero();
        assertThatThrownBy(() -> ItemEmbeddingRefresher.intervalFromEnv(
                Map.of(ItemEmbeddingRefresher.INTERVAL_ENV, "-1")::get))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ItemEmbeddingRefresher.INTERVAL_ENV);
    }

    @Test
    void aRefreshedVectorChangesWhatRecallReturns() {
        System.setProperty("recsys.vector.backend", "exact");   // deterministic ranking for the assertion
        try {
            DataManager dm = mock(DataManager.class);
            when(dm.getMovieById(anyInt())).thenAnswer(i -> new Movie(i.getArgument(0), "m", 2020, List.of("Drama")));
            when(dm.getWatchedMovieIds(anyInt())).thenReturn(Set.of());
            EmbeddingStore users = mock(EmbeddingStore.class);
            CandidateGenerator gen = new CandidateGenerator(dm, users);
            float[] query = new float[gen.embeddingDimension()];
            query[0] = 1f;
            when(users.getEmbedding(5)).thenReturn(query);
            int before = gen.byEmbedding(5, 1).get(0).id();
            int target = gen.seedEmbeddings().keySet().stream().filter(id -> id != before).findFirst().orElseThrow();
            ItemEmbeddingReader moved = (ids, onCorrupt) -> Map.of(target, query.clone());   // cosine 1.0
            ItemEmbeddingRefresher r = ItemEmbeddingRefresher.forGenerator(
                    gen, () -> gen.seedEmbeddings().keySet(), null, List.of(), registry, moved);

            r.refreshOnce();

            assertThat(gen.byEmbedding(5, 1).get(0).id()).isEqualTo(target);
        } finally {
            System.clearProperty("recsys.vector.backend");
        }
    }
}
