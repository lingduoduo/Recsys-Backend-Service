package com.demo.retrieval.service.grpo;

import com.demo.retrieval.model.FeedbackRequest;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GrpoFeedbackEventsTest {

    @Test
    void emitsOneEventOnClick() {
        FeedbackRequest request = new FeedbackRequest("u1", "m1", true, 1.0);
        List<Map<String, Object>> events = GrpoFeedbackEvents.build(request, "req-1", 1000L);
        assertEquals(1, events.size());
    }

    @Test
    void emitsNothingWithoutAClick() {
        FeedbackRequest request = new FeedbackRequest("u1", "m1", false, 0.0);
        assertTrue(GrpoFeedbackEvents.build(request, "req-1", 1000L).isEmpty());
    }

    @Test
    void carriesTheFieldsTheJoinerRequiresNonNull() {
        FeedbackRequest request = new FeedbackRequest("u1", "m1", true, 1.0);
        Map<String, Object> event = GrpoFeedbackEvents.build(request, "req-1", 1000L).get(0);
        // OnlineJoinerStreamingJob.parseEvents gates on exactly these.
        assertEquals("req-1", event.get("request_id"));
        assertEquals("u1", event.get("user_id"));
        assertEquals("m1", event.get("item_id"));
        assertEquals("click", event.get("event_type"));
        assertEquals(1000L, event.get("timestamp_ms"));
        assertNotNull(event.get("event_id"));
    }

    @Test
    void emitsExactlyTheImpressionFieldSetAndNoEnrichment() {
        // rating/dwell/completionRate are set here to prove they are NOT copied onto the event:
        // only a click moves the joiner's label, so nothing else belongs on this event.
        FeedbackRequest request = new FeedbackRequest(
            "u1", "m1", true, 1.0, "req-1", 4.5, null, 3000L, 0.9, null);
        Map<String, Object> event = GrpoFeedbackEvents.build(request, "req-1", 1000L).get(0);
        assertEquals(
            Set.of("event_id", "request_id", "user_id", "item_id", "event_type",
                "timestamp_ms", "position", "user_features", "item_features", "context_features"),
            event.keySet());
    }

    @Test
    void orderedTrueEmitsBothClickAndOrderEvents() {
        FeedbackRequest request = new FeedbackRequest(
            "u1", "m1", true, 1.0, "req-1", null, null, null, null, true);
        List<Map<String, Object>> events = GrpoFeedbackEvents.build(request, "req-1", 1000L);
        assertEquals(2, events.size());
        assertEquals("click", events.get(0).get("event_type"));
        assertEquals("order", events.get(1).get("event_type"));
        for (Map<String, Object> event : events) {
            assertEquals("req-1", event.get("request_id"));
            assertEquals("u1", event.get("user_id"));
            assertEquals("m1", event.get("item_id"));
        }
    }

    @Test
    void orderedTrueEmitsBothEventsEvenWhenClickedIsFalse() {
        // The semantic decision: an order always implies a click happened (producer.py only ever
        // emits "order" inside `if clicked_item:`), so `clicked` is ignored once ordered is true.
        FeedbackRequest request = new FeedbackRequest(
            "u1", "m1", false, 0.0, "req-1", null, null, null, null, true);
        List<Map<String, Object>> events = GrpoFeedbackEvents.build(request, "req-1", 1000L);
        assertEquals(2, events.size());
        assertEquals("click", events.get(0).get("event_type"));
        assertEquals("order", events.get(1).get("event_type"));
    }

    @Test
    void orderedNullYieldsExistingClickOnlyBehaviour() {
        FeedbackRequest request = new FeedbackRequest("u1", "m1", true, 1.0);
        List<Map<String, Object>> events = GrpoFeedbackEvents.build(request, "req-1", 1000L);
        assertEquals(1, events.size());
        assertEquals("click", events.get(0).get("event_type"));
    }

    @Test
    void orderedFalseYieldsExistingClickOnlyBehaviour() {
        FeedbackRequest request = new FeedbackRequest(
            "u1", "m1", true, 1.0, "req-1", null, null, null, null, false);
        List<Map<String, Object>> events = GrpoFeedbackEvents.build(request, "req-1", 1000L);
        assertEquals(1, events.size());
        assertEquals("click", events.get(0).get("event_type"));
    }

    @Test
    void clickedFalseOrderedFalseYieldsNothing() {
        FeedbackRequest request = new FeedbackRequest(
            "u1", "m1", false, 0.0, "req-1", null, null, null, null, false);
        assertTrue(GrpoFeedbackEvents.build(request, "req-1", 1000L).isEmpty());
    }

    @Test
    void theRequestIdOnTheEventIsTheResolvedOneNotFeedbackRequestsOwn() {
        // The caller resolves which requestId to use (serving's own, or the replay context's) and
        // passes it in explicitly; FeedbackRequest.requestId() itself must not leak through here.
        FeedbackRequest request = new FeedbackRequest(
            "u1", "m1", true, 1.0, "from-feedback-request", null, null, null, null, null);
        Map<String, Object> event = GrpoFeedbackEvents.build(request, "resolved-req", 1000L).get(0);
        assertEquals("resolved-req", event.get("request_id"));
    }
}
