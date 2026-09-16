package com.demo.retrieval.service.clients;

import java.util.List;

public interface ServedHistoryClient {
    List<String> getRecentlyServedMovieIds(String userId);
}
