package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.clients.CachedMoviesClient;
import com.demo.retrieval.model.ScoredMoviesQuery;

import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Hydrates cachedMovieIds and sets hasCachedMovies when the cached candidate set
 * is large enough to be worth using.
 *
 * Reads from a dedicated Redis key (separate write path from the general feature
 * store) and computes hasCachedMovies from the list size against a threshold,
 * rather than storing the flag separately.
 */
@Component
public class CachedMoviesQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    public static final int MIN_CACHED_MOVIES_THRESHOLD = 100;

    private final CachedMoviesClient cachedMoviesClient;

    public CachedMoviesQueryHydrator(CachedMoviesClient cachedMoviesClient) {
        this.cachedMoviesClient = cachedMoviesClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        String userId = query.userId();
        List<String> cachedMovieIds = cachedMoviesClient.getCachedMovieIds(userId);
        boolean hasCachedMovies = cachedMovieIds.size() >= MIN_CACHED_MOVIES_THRESHOLD;
        return new ScoredMoviesQuery(userId,
            query.userFeatures().withCachedMovieIds(cachedMovieIds, hasCachedMovies),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(query.userId(),
            query.userFeatures().withCachedMovieIds(
                hydrated.userFeatures().cachedMovieIds(),
                hydrated.userFeatures().hasCachedMovies()),
            query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds());
    }
}
