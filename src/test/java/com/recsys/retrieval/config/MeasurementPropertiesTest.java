package com.demo.retrieval.config;

import com.demo.retrieval.RetrievalServiceApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = RetrievalServiceApplication.class)
class MeasurementPropertiesTest {

    @Autowired
    private RecommendationProperties properties;

    @Test
    void bindsConfiguredMeasurementDefaults() {
        RecommendationProperties.Measurements measurements = properties.getMeasurements();

        assertEquals(100, measurements.getFairnessMinSupport());
        assertEquals(30, measurements.getFreshnessWindowDays());
        assertEquals(0.80, measurements.getLongTailPercentile());
        assertEquals("catalog-filter-v1", measurements.getSafetyPolicyVersion());
        assertEquals(List.of(5L, 10L, 25L, 50L, 100L, 250L, 500L, 1000L, 2500L),
            measurements.getLatencyBucketsMs());
    }
}
