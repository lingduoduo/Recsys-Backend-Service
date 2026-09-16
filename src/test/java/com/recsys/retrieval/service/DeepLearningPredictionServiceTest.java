package com.demo.retrieval.service;

import com.demo.retrieval.model.ModelPrediction;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeepLearningPredictionServiceTest {

    @Test
    void normalizeBatchScoresPadsAndTruncatesToN() {
        assertArrayEquals(new double[]{1.0, 2.0, 0.0},
            DeepLearningPredictionService.normalizeBatchScores(new float[]{1.0f, 2.0f}, 3), 1e-9);
        assertArrayEquals(new double[]{1.0, 2.0},
            DeepLearningPredictionService.normalizeBatchScores(new float[]{1.0f, 2.0f, 9.0f}, 2), 1e-9);
        assertArrayEquals(new double[]{5.0, 0.0},
            DeepLearningPredictionService.normalizeBatchScores(new float[][]{{5.0f}, {}}, 2), 1e-9);
    }

    @Test
    void loadsClasspathArtifactsAndPredictsWithStringLookups() throws Exception {
        DeepLearningPredictionService service = new DeepLearningPredictionService(new ObjectMapper());
        try {
            Optional<ModelPrediction> prediction = service.predict("user_employee_01", "action_benefits");

            assertTrue(prediction.isPresent());
            assertEquals("mlp_embedding", prediction.get().model());
            assertEquals(0L, prediction.get().userId());
            assertEquals(0L, prediction.get().itemId());
            assertTrue(prediction.get().score() >= 0.0);
            assertTrue(prediction.get().score() <= 1.0);
            assertEquals(32, service.metadata().get("users"));
            assertEquals(12, service.metadata().get("items"));
        } finally {
            service.close();
        }
    }

    @Test
    void returnsEmptyForUnknownLookupIds() throws Exception {
        DeepLearningPredictionService service = new DeepLearningPredictionService(new ObjectMapper());
        try {
            assertFalse(service.predict("unknown_user", "action_benefits").isPresent());
            assertFalse(service.predict("user_employee_01", "unknown_item").isPresent());
        } finally {
            service.close();
        }
    }

    @Test
    void rejectsNumericIdsOutsideModelVocabulary() throws Exception {
        DeepLearningPredictionService service = new DeepLearningPredictionService(new ObjectMapper());
        try {
            IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> service.predict(0L, 42L)
            );
            assertEquals(
                "Model indices out of range: userId must be 0..31 and itemId must be 0..11",
                error.getMessage()
            );
        } finally {
            service.close();
        }
    }
}
