package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.clients.PastRequestTimestampsClient;
import com.demo.retrieval.model.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

/**
 * Hydrates pastRequestTimestamps from the request-history store.
 *
 * Mirrors the Rust PastRequestTimestampsQueryHydrator which calls a dedicated
 * PastRequestTimestampsClient to fetch non_polling request timestamps. Timestamps
 * are written by the recommendation service at serve time, separate from the
 * general feature store.
 */
@Component
public class PastRequestTimestampsQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final PastRequestTimestampsClient timestampsClient;

    public PastRequestTimestampsQueryHydrator(PastRequestTimestampsClient timestampsClient) {
        this.timestampsClient = timestampsClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withPastRequestTimestamps(
                timestampsClient.getTimestamps(query.userId())),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withPastRequestTimestamps(
                hydrated.userFeatures().pastRequestTimestamps()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
