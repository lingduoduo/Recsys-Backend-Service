package com.demo.retrieval.model;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UserBehaviorProfileTest {
    @Test
    void personaDefensivelyCopiesEvidence() {
        Map<String, Double> evidence = new HashMap<>();
        evidence.put("evidence_count", 3.0);

        UserBehaviorProfile.Persona persona = new UserBehaviorProfile.Persona("type", "label", 0.5, evidence);
        evidence.put("evidence_count", 9.0);

        assertEquals(Map.of("evidence_count", 3.0), persona.evidence());
        assertThrows(UnsupportedOperationException.class,
            () -> persona.evidence().put("minimum_evidence", 5.0));
    }

    @Test
    void personaNormalizesNullEvidenceToEmpty() {
        UserBehaviorProfile.Persona persona = new UserBehaviorProfile.Persona("type", "label", 0.5, null);

        assertEquals(Map.of(), persona.evidence());
    }
}
