package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.model.UserDemographics;
import com.demo.retrieval.service.*;
import com.demo.retrieval.service.clients.MovieLensFeatureClient;

import org.springframework.stereotype.Component;

@Component
public class UserDemographicsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public UserDemographicsQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        UserDemographics demographics = featureClient.getUserFeatures(userId)
            .map(MovieLensUserFeatures::demographics)
            .orElseGet(UserDemographics::empty);
        return new ScoredMoviesQuery(
            userId,
            query.userFeatures().withDemographics(demographics),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withDemographics(hydrated.userFeatures().demographics()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
