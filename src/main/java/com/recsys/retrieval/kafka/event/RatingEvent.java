package com.demo.retrieval.kafka.event;

public record RatingEvent(
        String userId,
        String movieId,
        double rating,
        long timestamp) implements MovieLensEvent {

    public String getUserId()  { return userId; }
    public String getMovieId() { return movieId; }
    public double getRating()  { return rating; }
    public long getTimestamp() { return timestamp; }
}
