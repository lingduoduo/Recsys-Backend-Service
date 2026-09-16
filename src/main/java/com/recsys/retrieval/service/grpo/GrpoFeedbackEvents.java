package com.demo.retrieval.service.grpo;

import com.demo.retrieval.model.FeedbackRequest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Serving's own feedback events, shaped exactly like the click events the Python producers emit.
 *
 * OnlineJoinerStreamingJob.buildTrainingSamples derives label purely from event_type:
 * clicked = max(etype == "click"), ordered = max(etype in ("order","purchase")), and
 * label = ordered ? 2.0 : clicked ? 1.0 : 0.0. This emits a click event when clicked, an order
 * event when FeedbackRequest.ordered() is true (see build() for why an order always brings a
 * click along), and nothing when neither signal is set. The joiner does read
 * rating, negative_feedback_reason, dwell_millis and completion_rate off feedback events
 * (OnlineJoinerStreamingJob.scala's latestFeedback), but they are deliberately not emitted here:
 * FeedbackRequest carries all four, so this is not a data-availability gap: none of them affects
 * label, and emitting them would be scope nobody asked for, so a serving-sourced sample simply
 * carries nulls in those columns.
 *
 * session_id is also deliberately omitted. The joiner aggregates it with
 * first(col("session_id"), ignoreNulls = true), not gated on isImpression, so a fabricated value
 * here would make the sample's session_id nondeterministic depending on event ordering; a null
 * loses to the impression event's real session_id under ignoreNulls.
 *
 * requestId is a parameter rather than request.requestId() because the caller
 * (HybridRecommendationService.recordFeedback) has already resolved which id joins to the
 * impression: serving's own requestId when the client sent one, otherwise the one recovered from
 * the pending replay context. This class only shapes the event; it does not choose the id.
 *
 * position is not meaningful here: FeedbackRequest carries no slate index, and
 * OnlineJoinerStreamingJob.buildTrainingSamples only reads position off impression events
 * (max(when(isImpression, col("position")))), never off feedback ones. 0 is a placeholder, not a
 * claim about where in the slate this item sat.
 */
public final class GrpoFeedbackEvents {

    private GrpoFeedbackEvents() {}

    public static List<Map<String, Object>> build(FeedbackRequest request, String requestId, long nowMs) {
        boolean ordered = Boolean.TRUE.equals(request.ordered());
        // ordered implies clicked regardless of the clicked flag: in producer.py (~lines 93-110),
        // "order" is only ever appended inside `if clicked_item:`, so an order is always a click
        // that converted. Emitting an order without a click would give serving-sourced data a
        // shape producer data never has, so once ordered is true we emit both events unconditionally.
        //
        // The synthetic click is a real Kafka event, not a label-only artefact: it is also consumed by
        // UserEventStreamingJob (sequence store), RedisSink (click aggregates), CtrRankingModelTrainingJob
        // (CTR labels) and the dashboard funnel. A client that sends ordered=true, clicked=false will
        // therefore register a click everywhere, not just in the joiner's label. That is the intended
        // reading -- an order IS a converted click -- but it is a wider blast radius than the label alone.
        if (!request.clicked() && !ordered) {
            return List.of();
        }
        List<Map<String, Object>> events = new ArrayList<>();
        events.add(buildEvent(request, requestId, nowMs, "click"));
        if (ordered) {
            events.add(buildEvent(request, requestId, nowMs, "order"));
        }
        return List.copyOf(events);
    }

    private static Map<String, Object> buildEvent(
            FeedbackRequest request, String requestId, long nowMs, String eventType) {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("event_id", UUID.randomUUID().toString());
        event.put("request_id", requestId);
        event.put("user_id", request.user());
        event.put("item_id", request.item());
        event.put("event_type", eventType);
        event.put("timestamp_ms", nowMs);
        event.put("position", 0);
        event.put("user_features", Map.of());
        event.put("item_features", Map.of());
        event.put("context_features", Map.of());
        return event;
    }
}
