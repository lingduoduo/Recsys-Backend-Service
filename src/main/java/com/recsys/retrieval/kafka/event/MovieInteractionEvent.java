package com.demo.retrieval.kafka.event;

public record MovieInteractionEvent(
        String userId,
        String movieId,
        String eventType,
        long timestamp) implements MovieLensEvent {

    public String getUserId()    { return userId; }
    public String getMovieId()   { return movieId; }
    public String getEventType() { return eventType; }
    public long getTimestamp()   { return timestamp; }
}
