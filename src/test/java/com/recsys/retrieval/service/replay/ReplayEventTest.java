package com.demo.retrieval.service.replay;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ReplayEventTest {

    @Test
    void applyFeedbackFillsAllFieldsOnEmptyEvent() {
        Map<String, Object> event = new LinkedHashMap<>();
        Map<String, Object> nextState = Map.of("genres", List.of("drama"));
        ReplayEvent.applyFeedback(event, "u1", "m1", true, 0.8, 1234L, nextState);

        assertEquals(ReplayEvent.EVENT_TYPE, event.get(ReplayEvent.TYPE));
        assertEquals(ReplayEvent.SCHEMA_VERSION_VALUE, event.get(ReplayEvent.SCHEMA_VERSION));
        assertEquals("u1", event.get(ReplayEvent.USER));
        assertEquals("m1", event.get(ReplayEvent.ACTION));
        assertEquals(true, event.get(ReplayEvent.CLICKED));
        assertEquals(0.8, event.get(ReplayEvent.REWARD));
        assertEquals(1234L, event.get(ReplayEvent.FEEDBACK_TIMESTAMP));
        assertEquals(nextState, event.get(ReplayEvent.NEXT_STATE));
    }

    @Test
    void applyFeedbackPreservesExistingServeTimeFields() {
        Map<String, Object> event = new HashMap<>();
        event.put(ReplayEvent.TYPE, ReplayEvent.EVENT_TYPE);
        event.put(ReplayEvent.USER, "serve-user");
        event.put(ReplayEvent.ACTION, "serve-item");
        ReplayEvent.applyFeedback(event, "feedback-user", "feedback-item", false, 0.0, 9L, Map.of());

        assertEquals("serve-user", event.get(ReplayEvent.USER));
        assertEquals("serve-item", event.get(ReplayEvent.ACTION));
        assertEquals(false, event.get(ReplayEvent.CLICKED));
        assertEquals(0.0, event.get(ReplayEvent.REWARD));
        assertEquals(9L, event.get(ReplayEvent.FEEDBACK_TIMESTAMP));
    }
}
