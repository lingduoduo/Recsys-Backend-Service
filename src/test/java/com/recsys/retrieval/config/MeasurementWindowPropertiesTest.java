package com.demo.retrieval.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MeasurementWindowPropertiesTest {

    @Test
    void defaultsThroughputAndPercentileWindows() {
        RecommendationProperties.Measurements measurements =
            new RecommendationProperties().getMeasurements();

        assertEquals(60, measurements.getThroughputWindowSeconds());
        assertEquals(300, measurements.getPercentileWindowSeconds());
    }

    @Test
    void acceptsOverriddenWindows() {
        RecommendationProperties.Measurements measurements =
            new RecommendationProperties().getMeasurements();

        measurements.setThroughputWindowSeconds(15);
        measurements.setPercentileWindowSeconds(120);

        assertEquals(15, measurements.getThroughputWindowSeconds());
        assertEquals(120, measurements.getPercentileWindowSeconds());
    }
}
