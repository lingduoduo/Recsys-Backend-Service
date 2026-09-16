package com.recsys.retrieval.service.audit;

import com.recsys.retrieval.service.content.NormalizedProfile;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CatalogPreferenceIndexTest {

    private static NormalizedProfile item(Set<String> genres, Set<String> tags) {
        return new NormalizedProfile("", genres, tags, tags, "", false, 0L);
    }

    private static CatalogPreferenceIndex index() {
        Map<String, NormalizedProfile> catalog = new LinkedHashMap<>();
        catalog.put("item9", item(Set.of("sci-fi", "adventure"), Set.of("space")));
        catalog.put("item1", item(Set.of("sci-fi"), Set.of("space", "classic")));
        catalog.put("item5", item(Set.of("drama"), Set.of()));
        return CatalogPreferenceIndex.build(catalog);
    }

    @Test
    void genreLookupReturnsMatchingItemsSortedById() {
        assertEquals(List.of("item1", "item9"), index().lookup(CatalogPreferenceIndex.KIND_GENRE, "sci-fi"));
        assertEquals(List.of("item5"), index().lookup(CatalogPreferenceIndex.KIND_GENRE, "drama"));
    }

    @Test
    void tagLookupIsSeparateFromGenres() {
        assertEquals(List.of("item1", "item9"), index().lookup(CatalogPreferenceIndex.KIND_TAG, "space"));
        assertEquals(List.of("item1"), index().lookup(CatalogPreferenceIndex.KIND_TAG, "classic"));
        assertTrue(index().lookup(CatalogPreferenceIndex.KIND_TAG, "sci-fi").isEmpty());
    }

    @Test
    void missingValueOrUnknownKindReturnsEmpty() {
        assertTrue(index().lookup(CatalogPreferenceIndex.KIND_GENRE, "film-noir").isEmpty());
        assertTrue(index().lookup("keyword", "space").isEmpty());
        assertTrue(index().lookup(CatalogPreferenceIndex.KIND_GENRE, null).isEmpty());
    }

    @Test
    void catalogSizeCountsItemsNotPostings() {
        assertEquals(3, index().catalogSize());
        assertEquals(0, CatalogPreferenceIndex.build(Map.of()).catalogSize());
    }

    @Test
    void lookupResultIsUnmodifiable() {
        List<String> hit = index().lookup(CatalogPreferenceIndex.KIND_GENRE, "sci-fi");
        assertThrows(UnsupportedOperationException.class, () -> hit.add("x"));
    }

    @Test
    void probeReturnsTotalAndACappedSample() {
        CatalogPreferenceIndex.ContentNeighbors neighbors =
            index().probe(CatalogPreferenceIndex.KIND_GENRE, "sci-fi", 1);

        assertEquals(2, neighbors.total());
        assertEquals(List.of("item1"), neighbors.sample());
    }

    @Test
    void probeSampleIsTheWholePostingListWhenLimitExceedsIt() {
        CatalogPreferenceIndex.ContentNeighbors neighbors =
            index().probe(CatalogPreferenceIndex.KIND_TAG, "space", 10);

        assertEquals(2, neighbors.total());
        assertEquals(List.of("item1", "item9"), neighbors.sample());
    }

    @Test
    void probeIsEmptyForMissUnknownKindNullValueOrNonPositiveLimit() {
        CatalogPreferenceIndex index = index();

        assertEquals(0, index.probe(CatalogPreferenceIndex.KIND_GENRE, "film-noir", 5).total());
        assertTrue(index.probe(CatalogPreferenceIndex.KIND_GENRE, "film-noir", 5).sample().isEmpty());
        assertTrue(index.probe("keyword", "space", 5).sample().isEmpty());
        assertTrue(index.probe(CatalogPreferenceIndex.KIND_GENRE, null, 5).sample().isEmpty());

        CatalogPreferenceIndex.ContentNeighbors zeroLimit =
            index.probe(CatalogPreferenceIndex.KIND_GENRE, "sci-fi", 0);
        assertEquals(2, zeroLimit.total());
        assertTrue(zeroLimit.sample().isEmpty());

        CatalogPreferenceIndex.ContentNeighbors negativeLimit =
            index.probe(CatalogPreferenceIndex.KIND_GENRE, "sci-fi", -3);
        assertEquals(2, negativeLimit.total());
        assertTrue(negativeLimit.sample().isEmpty());
    }
}
