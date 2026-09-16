package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpoImpressionEventsTest {

    private ServedMovie movie(String id, double banditScore) {
        return new ServedMovie(id, 0.4, 0.3, 0.05, banditScore, false, 10, 2,
            Map.of("predictionScore", banditScore));
    }

    private ServingSideEffectRequest request(List<ServedMovie> selected) {
        return new ServingSideEffectRequest(
            "req-1", "u1", "hybrid", Map.of(), selected, selected,
            List.of(), List.of(), 0L, 0L, selected.size(), 0.0, 0.0, 0.0);
    }

    @Test
    void emitsOneEventPerSelectedItem() {
        List<Map<String, Object>> events =
            GrpoImpressionEvents.build(request(List.of(movie("m1", 0.7), movie("m2", 0.4))), 1000L);
        assertEquals(2, events.size());
    }

    @Test
    void carriesTheFieldsTheJoinerRequiresNonNull() {
        Map<String, Object> event = GrpoImpressionEvents.build(request(List.of(movie("m1", 0.7))), 1000L).get(0);
        // OnlineJoinerStreamingJob.parseEvents gates on exactly these three.
        assertEquals("req-1", event.get("request_id"));
        assertEquals("u1", event.get("user_id"));
        assertEquals("m1", event.get("item_id"));
        assertEquals("impression", event.get("event_type"));
        assertEquals(1000L, event.get("timestamp_ms"));
        assertNotNull(event.get("event_id"));
    }

    @Test
    void positionMatchesSlateOrder() {
        List<Map<String, Object>> events =
            GrpoImpressionEvents.build(request(List.of(movie("m1", 0.7), movie("m2", 0.4))), 1000L);
        assertEquals(0, events.get(0).get("position"));
        assertEquals(1, events.get(1).get("position"));
    }

    @SuppressWarnings("unchecked")
    @Test
    void itemFeaturesCarryPredictionScoreAndPackedVector() {
        Map<String, Object> event = GrpoImpressionEvents.build(request(List.of(movie("m1", 0.73))), 1000L).get(0);
        Map<String, String> itemFeatures = (Map<String, String>) event.get("item_features");
        assertEquals("0.73", itemFeatures.get("prediction_score"));
        assertTrue(itemFeatures.get("grpo_x").startsWith("v2:"), itemFeatures.get("grpo_x"));
    }

    @Test
    void anEmptySlateEmitsNothing() {
        assertTrue(GrpoImpressionEvents.build(request(List.of()), 1000L).isEmpty());
    }

    @SuppressWarnings("unchecked")
    @Test
    void everyEventInOneSlateSharesOneSessionId() {
        List<Map<String, Object>> events =
            GrpoImpressionEvents.build(request(List.of(movie("m1", 0.7), movie("m2", 0.4))), 1000L);
        assertEquals(events.get(0).get("session_id"), events.get(1).get("session_id"));
    }
}
