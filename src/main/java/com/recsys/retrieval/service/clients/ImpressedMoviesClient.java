package com.recsys.retrieval.service.clients;

import java.util.List;

public interface ImpressedMoviesClient {
    List<String> getImpressedMovieIds(String userId);
}
