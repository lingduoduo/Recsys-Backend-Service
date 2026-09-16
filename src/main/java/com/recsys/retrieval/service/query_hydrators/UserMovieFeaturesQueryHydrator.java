package com.recsys.retrieval.service.query_hydrators;

import com.recsys.retrieval.model.MovieLensUserFeatures;
import com.recsys.retrieval.model.ScoredMoviesQuery;
import com.recsys.retrieval.service.*;
import com.recsys.retrieval.service.clients.MovieLensFeatureClient;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Loads the user's baseline feature record.
 *
 * Ordered ahead of the other feature hydrators because {@link #update} *replaces* userFeatures
 * rather than merging into it: any hydrator that layers onto userFeatures and runs before this
 * one has its contribution silently discarded. Classpath scanning used to place this bean tenth
 * of twelve, which dropped the behavior preferences, demographics, served and impressed history,
 * cached movies, inferred topics, past request timestamps and the impression bloom filter.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class UserMovieFeaturesQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final MovieLensFeatureClient featureClient;

    public UserMovieFeaturesQueryHydrator(MovieLensFeatureClient featureClient) {
        this.featureClient = featureClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        MovieLensUserFeatures userFeatures = featureClient.getUserFeatures(userId)
            .orElseGet(() -> MovieLensUserFeatures.forUser(userId));
        return new ScoredMoviesQuery(
            userId,
            userFeatures,
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            hydrated.userFeatures(),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
