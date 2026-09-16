package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.clients.ServedHistoryClient;
import com.demo.retrieval.model.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

/**
 * Hydrates servedMovieIds from the served-history store.
 *
 * Mirrors the Rust ServedHistoryQueryHydrator: fetch recent served entries from
 * a dedicated client (HOME_TIMELINE), extract item IDs, and apply the recency
 * window (recently_served_ids). MovieLens stores served IDs as a capped
 * newest-first list rather than timestamped entries, so the time-window filter
 * is enforced at write time instead of here.
 */
@Component
public class ServedHistoryQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final ServedHistoryClient servedHistoryClient;

    public ServedHistoryQueryHydrator(ServedHistoryClient servedHistoryClient) {
        this.servedHistoryClient = servedHistoryClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withServedMovieIds(
                servedHistoryClient.getRecentlyServedMovieIds(query.userId())),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withServedMovieIds(hydrated.userFeatures().servedMovieIds()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
