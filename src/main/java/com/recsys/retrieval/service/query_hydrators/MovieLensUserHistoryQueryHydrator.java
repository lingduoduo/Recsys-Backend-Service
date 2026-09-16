package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.service.clients.UserMovieHistoryClient;
import com.demo.retrieval.service.clients.UserMovieHistoryClient.UserMovieHistory;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Loads the user's baseline watched/rated history.
 *
 * Ordered first because {@link #update} *replaces* those two lists rather than merging into
 * them: any hydrator that contributes to watched or rated history and runs before this one has
 * its contribution silently discarded.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class MovieLensUserHistoryQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final UserMovieHistoryClient client;

    public MovieLensUserHistoryQueryHydrator(UserMovieHistoryClient client) {
        this.client = client;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        UserMovieHistory history = client.getMovieHistory(query.userId());
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures(),
            history.watched(),
            history.rated(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures(),
            hydrated.watchedMovieIds(),
            hydrated.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
