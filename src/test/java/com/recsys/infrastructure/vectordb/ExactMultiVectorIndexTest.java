package com.recsys.infrastructure.vectordb;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class ExactMultiVectorIndexTest {

    private static final float[][] QUERY = {{1f, 0f}, {0f, 1f}};

    // doc 1 covers both query tokens exactly (2.0); doc 2 covers only the first (1.0 + 0.2);
    // doc 3 is a single token halfway between (0.5 + 0.5 = 1.0).
    private static Map<Integer, float[][]> docs() {
        return Map.of(
                1, new float[][]{{1f, 0f}, {0f, 1f}},
                2, new float[][]{{1f, 0f}, {0.2f, 0.2f}},
                3, new float[][]{{0.5f, 0.5f}});
    }

    @Test
    void search_ranksBySumOfMaxSimDescending() {
        ExactMultiVectorIndex idx = new ExactMultiVectorIndex(docs());
        List<SearchResult> results = idx.search(QUERY, 3, Set.of());
        assertThat(results).extracting(SearchResult::id).containsExactly(1, 2, 3);
        assertThat(results.get(0).score()).isCloseTo(2.0, within(1e-6));
        assertThat(results.get(1).score()).isCloseTo(1.2, within(1e-6));
        assertThat(results.get(2).score()).isCloseTo(1.0, within(1e-6));
    }

    @Test
    void search_truncatesToK() {
        ExactMultiVectorIndex idx = new ExactMultiVectorIndex(docs());
        assertThat(idx.search(QUERY, 2, Set.of())).extracting(SearchResult::id).containsExactly(1, 2);
    }

    @Test
    void search_honoursExcludedIds() {
        ExactMultiVectorIndex idx = new ExactMultiVectorIndex(docs());
        assertThat(idx.search(QUERY, 3, Set.of(1))).extracting(SearchResult::id).containsExactly(2, 3);
    }

    @Test
    void search_nullExcludeIsTreatedAsEmpty() {
        ExactMultiVectorIndex idx = new ExactMultiVectorIndex(docs());
        assertThat(idx.search(QUERY, 3, null)).hasSize(3);
    }

    @Test
    void search_nonPositiveKOrNullQuery_returnsEmpty() {
        ExactMultiVectorIndex idx = new ExactMultiVectorIndex(docs());
        assertThat(idx.search(QUERY, 0, Set.of())).isEmpty();
        assertThat(idx.search(null, 3, Set.of())).isEmpty();
    }

    @Test
    void search_skipsUnscorableDocuments() {
        // Doc 9 has the wrong token width; it must be dropped, not ranked at -inf.
        Map<Integer, float[][]> withBad = Map.of(
                1, new float[][]{{1f, 0f}},
                9, new float[][]{{1f, 0f, 0f}});
        ExactMultiVectorIndex idx = new ExactMultiVectorIndex(withBad);
        assertThat(idx.search(QUERY, 5, Set.of())).extracting(SearchResult::id).containsExactly(1);
    }

    @Test
    void addOrUpdate_newDocIsSearchable() {
        ExactMultiVectorIndex idx = new ExactMultiVectorIndex(Map.of(3, new float[][]{{0.5f, 0.5f}}));
        idx.addOrUpdate(1, new float[][]{{1f, 0f}, {0f, 1f}});
        assertThat(idx.search(QUERY, 2, Set.of()).get(0).id()).isEqualTo(1);
    }

    @Test
    void addOrUpdate_replacesExistingDoc() {
        ExactMultiVectorIndex idx = new ExactMultiVectorIndex(Map.of(3, new float[][]{{0.5f, 0.5f}}));
        idx.addOrUpdate(3, new float[][]{{1f, 0f}, {0f, 1f}});
        assertThat(idx.search(QUERY, 1, Set.of()).get(0).score()).isCloseTo(2.0, within(1e-6));
    }

    @Test
    void name_isExactMultiVector() {
        assertThat(new ExactMultiVectorIndex(Map.of()).name()).isEqualTo("exact-multivector");
    }

    @Test
    void staticSearch_matchesInstanceSearch() {
        List<SearchResult> viaStatic = ExactMultiVectorIndex.search(docs(), QUERY, 3, Set.of());
        List<SearchResult> viaInstance = new ExactMultiVectorIndex(docs()).search(QUERY, 3, Set.of());
        assertThat(viaStatic).extracting(SearchResult::id).containsExactlyElementsOf(
                viaInstance.stream().map(SearchResult::id).toList());
    }

    @Test
    void implementsMultiVectorIndexInterface() {
        MultiVectorIndex idx = new ExactMultiVectorIndex(docs());
        assertThat(idx.search(QUERY, 1, Set.of())).extracting(SearchResult::id).containsExactly(1);
    }
}
