package com.demo.retrieval.service.filters;

import java.util.LinkedHashSet;
import java.util.Set;

public final class GenreExpansion {
    private GenreExpansion() {
    }

    public static Set<Integer> expand(Set<Integer> genreIds) {
        return genreIds == null ? Set.of() : new LinkedHashSet<>(genreIds);
    }
}
