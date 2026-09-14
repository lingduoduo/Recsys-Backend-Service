package com.recsys.infrastructure.vectordb;

import com.recsys.domain.item.Movie;
import com.recsys.infrastructure.dataloading.DataManager;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CoRatedTokenBagsTest {

    private static Movie movie(int id) {
        return new Movie(id, "m" + id, 2000, List.of());
    }

    private static final float[] V1 = {1f, 0f};
    private static final float[] V2 = {0f, 1f};
    private static final float[] V3 = {0.5f, 0.5f};

    @Test
    void bag_isOwnVectorThenCoRatedNeighbourVectors() {
        DataManager data = mock(DataManager.class);
        EmbeddingStore store = mock(EmbeddingStore.class);
        when(data.getSimilarMovies(anyInt())).thenReturn(List.of());
        when(data.getSimilarMovies(1)).thenReturn(List.of(movie(2), movie(3)));
        when(store.getEmbeddings(any())).thenReturn(Map.of(1, V1, 2, V2, 3, V3));

        Map<Integer, float[][]> bags = new CoRatedTokenBags(store, data).bagsFor(Set.of(1));

        assertThat(bags).containsOnlyKeys(1);
        assertThat(bags.get(1)).isDeepEqualTo(new float[][]{V1, V2, V3});
    }

    @Test
    void bag_skipsNeighboursWithoutAnEmbedding() {
        DataManager data = mock(DataManager.class);
        EmbeddingStore store = mock(EmbeddingStore.class);
        when(data.getSimilarMovies(1)).thenReturn(List.of(movie(2), movie(3)));
        when(store.getEmbeddings(any())).thenReturn(Map.of(1, V1, 3, V3)); // no vector for 2

        Map<Integer, float[][]> bags = new CoRatedTokenBags(store, data).bagsFor(Set.of(1));

        assertThat(bags.get(1)).isDeepEqualTo(new float[][]{V1, V3});
    }

    @Test
    void bag_omitsItemsWithoutTheirOwnEmbedding() {
        // Neighbours alone are not evidence about the item; without its own vector it is dropped
        // rather than ranked on borrowed tokens.
        DataManager data = mock(DataManager.class);
        EmbeddingStore store = mock(EmbeddingStore.class);
        when(data.getSimilarMovies(1)).thenReturn(List.of(movie(2)));
        when(data.getSimilarMovies(2)).thenReturn(List.of());
        when(store.getEmbeddings(any())).thenReturn(Map.of(2, V2)); // 1 has no vector

        Map<Integer, float[][]> bags = new CoRatedTokenBags(store, data).bagsFor(Set.of(1, 2));

        assertThat(bags).containsOnlyKeys(2);
        assertThat(bags.get(2)).isDeepEqualTo(new float[][]{V2});
    }

    @Test
    void bagsFor_readsTheStoreOnceForTheUnionOfItemsAndNeighbours() {
        DataManager data = mock(DataManager.class);
        EmbeddingStore store = mock(EmbeddingStore.class);
        when(data.getSimilarMovies(1)).thenReturn(List.of(movie(2), movie(3)));
        when(data.getSimilarMovies(2)).thenReturn(List.of(movie(3)));
        when(store.getEmbeddings(any())).thenReturn(Map.of(1, V1, 2, V2, 3, V3));

        new CoRatedTokenBags(store, data).bagsFor(Set.of(1, 2));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<Collection<Integer>> ids = ArgumentCaptor.forClass(Collection.class);
        verify(store, times(1)).getEmbeddings(ids.capture());
        assertThat(new HashSet<>(ids.getValue())).containsExactlyInAnyOrder(1, 2, 3);
    }

    @Test
    void bagsFor_emptyInput_isEmptyAndDoesNotTouchTheStore() {
        DataManager data = mock(DataManager.class);
        EmbeddingStore store = mock(EmbeddingStore.class);

        assertThat(new CoRatedTokenBags(store, data).bagsFor(Set.of())).isEmpty();
        verify(store, times(0)).getEmbeddings(any());
    }
}
