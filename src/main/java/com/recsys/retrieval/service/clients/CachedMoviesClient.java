package com.recsys.retrieval.service.clients;

import java.util.List;

public interface CachedMoviesClient {
    List<String> getCachedMovieIds(String userId);
}
