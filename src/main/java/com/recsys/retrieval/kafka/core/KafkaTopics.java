package com.demo.retrieval.kafka.core;

public final class KafkaTopics {
    private KafkaTopics() {}

    // Consumed by UserEventStreamingJob — schema: {user_id, item_id, event_type, timestamp}
    public static final String USER_EVENTS = "user_events";

    // Consumed by OnlineJoinerStreamingJob — schema: {request_id, user_id, item_id, event_type, timestamp, position, ...features}
    public static final String BEHAVIOR_LOGS = "behavior_logs";

    // Consumed by MovieLensContextCollectorStreamingJob — user, movie, and rating context updates
    public static final String MOVIELENS_CONTEXT = "movielens_context";
}
