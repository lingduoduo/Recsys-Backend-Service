package com.demo.retrieval.service.filters;

import java.util.Set;

public record FilterContext(
    Set<String> mutedProductTypes,
    Set<String> mutedGenres,
    Set<String> mutedKeywords
) {
    public FilterContext {
        mutedProductTypes = mutedProductTypes == null ? Set.of() : Set.copyOf(mutedProductTypes);
        mutedGenres = mutedGenres == null ? Set.of() : Set.copyOf(mutedGenres);
        mutedKeywords = mutedKeywords == null ? Set.of() : Set.copyOf(mutedKeywords);
    }

    public static FilterContext empty() {
        return new FilterContext(Set.of(), Set.of(), Set.of());
    }
}
