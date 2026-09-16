package com.demo.retrieval.service.clients;

import java.util.List;

public interface UserMovieHistoryClient {
    /**
     * Fetches watched and rated movie lists in a single pipelined call.
     * Prefer this over getWatchedMovies/getRatedMovies individually.
     */
    UserMovieHistory getMovieHistory(String userId);

    record UserMovieHistory(List<String> watched, List<String> rated) {}
}
