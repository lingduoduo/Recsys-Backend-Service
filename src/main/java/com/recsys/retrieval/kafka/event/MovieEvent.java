package com.demo.retrieval.kafka.event;

import java.util.List;

public record MovieEvent(
        String movieId,
        String title,
        List<String> genres,
        int releaseYear,
        long timestamp) implements MovieLensEvent {

    public String getMovieId()      { return movieId; }
    public String getTitle()        { return title; }
    public List<String> getGenres() { return genres; }
    public int getReleaseYear()     { return releaseYear; }
    public long getTimestamp()      { return timestamp; }
}
