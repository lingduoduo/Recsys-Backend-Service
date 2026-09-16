package com.demo.retrieval.service.text;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TextNormalizationTest {

    @Test
    void normalizeLowercasesTrimsAndDropsBlanks() {
        assertEquals(Set.of("drama", "sci-fi"),
            TextNormalization.normalize(List.of("  Drama ", "SCI-FI", "   ")));
        assertEquals(Set.of(), TextNormalization.normalize(null));
    }

    @Test
    void normalizeValueLowercasesTrimsAndNullSafe() {
        assertEquals("drama", TextNormalization.normalizeValue("  Drama "));
        assertEquals("", TextNormalization.normalizeValue(null));
    }
}
