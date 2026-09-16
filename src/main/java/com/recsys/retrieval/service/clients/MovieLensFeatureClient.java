package com.demo.retrieval.service.clients;

import com.demo.retrieval.model.MovieLensUserFeatures;

import java.util.Optional;

public interface MovieLensFeatureClient {
    Optional<MovieLensUserFeatures> getUserFeatures(String userId);
}
