package com.demo.retrieval.service.grpo;

import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServedMovie;
import com.demo.retrieval.service.side_effects.MovieLensServingSideEffects.ServingSideEffectRequest;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Serving's own impression events, shaped exactly like the ones the Python producers emit.
 *
 * The shape is not a choice. OnlineJoinerStreamingJob.parseEvents gates on non-null request_id,
 * user_id and item_id, and every schema downstream of it is built on these field names. Two keys
 * are added inside item_features: prediction_score, the behavior policy's logit, and grpo_x, the
 * versioned feature vector. Nothing else about the event differs from a producer's.
 *
 * The requestId here is serving's own, which is the point: it is what makes the Kafka stream and
 * the Redis replay buffer share an id namespace for the first time.
 *
 * This does not emit surface, device, locale, timezone (top-level fields) or the richer
 * user_features {tier, country} that Python producers emit. ServingSideEffectRequest does not
 * carry them, and fabricating placeholder values would corrupt training rows. Downstream consumers
 * that segment on these fields will see serving-sourced traffic as an unsegmented null bucket.
 */
public final class GrpoImpressionEvents {

    private GrpoImpressionEvents() {}

    public static List<Map<String, Object>> build(ServingSideEffectRequest request, long nowMs) {
        List<ServedMovie> selected = request.selected();
        if (selected.isEmpty()) {
            return List.of();
        }
        String sessionId = "sess_" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        List<Map<String, Object>> events = new ArrayList<>(selected.size());
        for (int position = 0; position < selected.size(); position++) {
            ServedMovie movie = selected.get(position);
            Map<String, String> itemFeatures = new LinkedHashMap<>();
            itemFeatures.put("prediction_score", Double.toString(GrpoFeatures.predictionScore(movie)));
            itemFeatures.put("grpo_x", GrpoFeatures.pack(GrpoFeatures.of(movie)));

            Map<String, Object> event = new LinkedHashMap<>();
            event.put("event_id", UUID.randomUUID().toString());
            event.put("request_id", request.requestId());
            event.put("session_id", sessionId);
            event.put("user_id", request.userId());
            event.put("item_id", movie.movieId());
            event.put("event_type", "impression");
            event.put("timestamp_ms", nowMs);
            event.put("position", position);
            event.put("user_features", Map.of("algorithm", request.algorithm()));
            event.put("item_features", itemFeatures);
            event.put("context_features", Map.of());
            events.add(event);
        }
        return List.copyOf(events);
    }
}
