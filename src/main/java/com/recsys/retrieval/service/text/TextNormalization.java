package com.demo.retrieval.service.text;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public final class TextNormalization {

    private TextNormalization() {
    }

    public static Set<String> normalize(List<String> values) {
        return values == null ? Set.of() : values.stream()
            .map(TextNormalization::normalizeValue)
            .filter(value -> !value.isBlank())
            .collect(Collectors.toSet());
    }

    public static String normalizeValue(String value) {
        return value == null ? "" : value.toLowerCase(Locale.ROOT).trim();
    }
}
