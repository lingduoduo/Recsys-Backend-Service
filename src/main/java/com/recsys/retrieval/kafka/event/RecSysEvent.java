package com.demo.retrieval.kafka.event;

import com.demo.retrieval.kafka.core.KafkaEventSerializer;
import java.util.List;
import java.util.Map;

public sealed interface RecSysEvent
        permits RecSysEvent.UserUpdated, RecSysEvent.MovieUpdated,
                RecSysEvent.RatingCreated, RecSysEvent.InteractionCreated {

    byte[] toByteArray();

    static RecSysEvent userUpdated(
            String userId, int age, String gender,
            String occupation, String zipCode, long timestamp) {
        return new UserUpdated(userId, age, gender, occupation, zipCode, timestamp);
    }

    static RecSysEvent movieUpdated(
            String movieId, String title, List<String> genres,
            int releaseYear, long timestamp) {
        return new MovieUpdated(movieId, title, genres, releaseYear, timestamp);
    }

    static RecSysEvent ratingCreated(
            String userId, String movieId, double rating, long timestamp) {
        return new RatingCreated(userId, movieId, rating, timestamp);
    }

    static RecSysEvent interactionCreated(
            String userId, String movieId, String eventType, long timestamp) {
        return new InteractionCreated(userId, movieId, eventType, timestamp);
    }

    // Consumed by MovieLensContextCollectorStreamingJob.
    record UserUpdated(String userId, int age, String gender,
                       String occupation, String zipCode, long timestamp)
            implements RecSysEvent {
        public byte[] toByteArray() {
            return KafkaEventSerializer.toJsonBytes(Map.of(
                "user_id", userId, "age", age, "gender", gender,
                "occupation", occupation, "zip_code", zipCode, "timestamp", timestamp));
        }
    }

    // Consumed by MovieLensContextCollectorStreamingJob.
    record MovieUpdated(String movieId, String title, List<String> genres,
                        int releaseYear, long timestamp)
            implements RecSysEvent {
        public byte[] toByteArray() {
            return KafkaEventSerializer.toJsonBytes(Map.of(
                "item_id", movieId, "title", title,
                "genres", genres, "release_year", releaseYear, "timestamp", timestamp));
        }
    }

    // Routes to behavior_logs. OnlineJoinerStreamingJob schema:
    // {request_id, user_id, item_id, event_type, timestamp, position, ...features}
    // Nullable fields (request_id, position, features) are omitted — Spark from_json treats missing as null.
    record RatingCreated(String userId, String movieId, double rating, long timestamp)
            implements RecSysEvent {
        public byte[] toByteArray() {
            return KafkaEventSerializer.toJsonBytes(Map.of(
                "user_id", userId, "item_id", movieId,
                "event_type", "rating", "rating", rating, "timestamp", timestamp));
        }
    }

    // Routes to user_events. UserEventStreamingJob schema: {user_id, item_id, event_type, timestamp}
    record InteractionCreated(String userId, String movieId, String eventType, long timestamp)
            implements RecSysEvent {
        public byte[] toByteArray() {
            return KafkaEventSerializer.toJsonBytes(Map.of(
                "user_id", userId, "item_id", movieId,
                "event_type", eventType, "timestamp", timestamp));
        }
    }
}
