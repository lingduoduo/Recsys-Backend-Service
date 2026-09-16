package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.service.clients.ImpressionBloomFilterClient;
import com.demo.retrieval.model.ScoredMoviesQuery;
import org.springframework.stereotype.Component;

/**
 * Hydrates impressionBloomFilter from the bloom filter store.
 *
 * Mirrors the Rust ImpressionBloomFilterQueryHydrator which calls a dedicated
 * ImpressionBloomFilterClient (SurfaceArea.HOME_TIMELINE) rather than the general
 * feature store. The thrift entry structure (bloom_filter, size_cap,
 * false_positive_rate) is simplified to a flat list of longs since MovieLens
 * does not need multi-surface bloom filter entries.
 */
@Component
public class ImpressionBloomFilterQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {

    private final ImpressionBloomFilterClient bloomFilterClient;

    public ImpressionBloomFilterQueryHydrator(ImpressionBloomFilterClient bloomFilterClient) {
        this.bloomFilterClient = bloomFilterClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withImpressionBloomFilter(
                bloomFilterClient.getBloomFilterBits(query.userId())),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        return new ScoredMoviesQuery(
            query.userId(),
            query.userFeatures().withImpressionBloomFilter(
                hydrated.userFeatures().impressionBloomFilter()),
            query.watchedMovieIds(),
            query.ratedMovieIds(),
            query.candidateMovieIds()
        );
    }
}
