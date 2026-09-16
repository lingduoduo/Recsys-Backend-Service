package com.recsys.retrieval.service.clients;

import com.recsys.retrieval.model.MovieLensUserFeatures;

import java.util.Optional;

public interface MovieLensFeatureClient {
    Optional<MovieLensUserFeatures> getUserFeatures(String userId);
}
