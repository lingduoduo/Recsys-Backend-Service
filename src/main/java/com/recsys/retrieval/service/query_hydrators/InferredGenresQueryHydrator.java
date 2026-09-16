package com.recsys.retrieval.service.query_hydrators;

import com.recsys.retrieval.model.MovieLensUserFeatures;
import com.recsys.retrieval.model.ScoredMoviesQuery;
import com.recsys.retrieval.service.*;
import com.recsys.retrieval.service.clients.MovieLensFeatureClient;

import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class InferredGenresQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public InferredGenresQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<Integer> topics = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::inferredGenres)
            .orElseGet(List::of);
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withInferredGrokTopics(topics),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withInferredGrokTopics(hydrated.userFeatures().inferredGenres()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
