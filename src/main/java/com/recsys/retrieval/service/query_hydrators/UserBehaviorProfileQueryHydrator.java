package com.demo.retrieval.service.query_hydrators;

import com.demo.retrieval.model.MovieLensUserFeatures;
import com.demo.retrieval.model.ScoredMoviesQuery;
import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.service.clients.UserProfileClient;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Adds score-weighted behavioral preferences emitted by the profile pipeline. */
@Component
public class UserBehaviorProfileQueryHydrator implements QueryHydrator<ScoredMoviesQuery> {
    private final UserProfileClient profileClient;

    public UserBehaviorProfileQueryHydrator(UserProfileClient profileClient) {
        this.profileClient = profileClient;
    }

    @Override
    public ScoredMoviesQuery hydrate(ScoredMoviesQuery query) {
        return profileClient.getProfile(query.userId())
            .map(profile -> withPreferences(query, profile))
            .orElse(query);
    }

    @Override
    public ScoredMoviesQuery update(ScoredMoviesQuery query, ScoredMoviesQuery hydrated) {
        MovieLensUserFeatures features = query.userFeatures().withBehaviorPreferences(
            hydrated.userFeatures().genrePreferences(),
            hydrated.userFeatures().tagPreferences()
        );
        return withFeatures(query, features);
    }

    private static ScoredMoviesQuery withPreferences(ScoredMoviesQuery query, UserBehaviorProfile profile) {
        UserBehaviorProfile.Preferences preferences = profile.preferences();
        if (preferences == null) {
            return query;
        }
        MovieLensUserFeatures features = query.userFeatures().withBehaviorPreferences(
            weights(preferences.genres()),
            weights(preferences.tags())
        );
        return withFeatures(query, features);
    }

    private static Map<String, Double> weights(List<UserBehaviorProfile.Preference> preferences) {
        Map<String, Double> weights = new LinkedHashMap<>();
        if (preferences == null) {
            return weights;
        }
        for (UserBehaviorProfile.Preference preference : preferences) {
            if (preference == null || !Double.isFinite(preference.score()) || preference.score() <= 0.0) {
                continue;
            }
            weights.merge(preference.value(), Math.min(preference.score(), 1.0), Math::max);
        }
        return weights;
    }

    private static ScoredMoviesQuery withFeatures(ScoredMoviesQuery query, MovieLensUserFeatures features) {
        return new ScoredMoviesQuery(
            query.userId(), features, query.watchedMovieIds(), query.ratedMovieIds(), query.candidateMovieIds(),
            query.genreIds(), query.excludedGenreIds(), query.bulkTopicRequest(), query.excludeVideos()
        );
    }
}
