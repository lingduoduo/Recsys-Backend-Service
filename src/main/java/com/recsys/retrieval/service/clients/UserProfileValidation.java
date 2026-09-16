package com.recsys.retrieval.service.clients;

import com.recsys.retrieval.model.UserBehaviorProfile;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Locale;

/**
 * The single definition of "this user has a usable profile". Serving ({@link RedisUserProfileClient})
 * and the profile audit both call this, so they cannot disagree about what counts as a profile.
 * Pure: no Redis, no metrics.
 */
public final class UserProfileValidation {
    public static final int PROFILE_VERSION = 1;

    private UserProfileValidation() {
    }

    public sealed interface Result permits Valid, Invalid {
    }

    /** A profile that passed every check, with preference names normalized for serving. */
    public record Valid(UserBehaviorProfile profile) implements Result {
    }

    /** One of: missing_profile, invalid_json, unsupported_version, user_mismatch, run_mismatch. */
    public record Invalid(String reason) implements Result {
    }

    public static Result validate(String rawProfile, String userId, String activeRun, ObjectMapper mapper) {
        if (rawProfile == null) {
            return new Invalid("missing_profile");
        }
        UserBehaviorProfile profile;
        try {
            profile = mapper.readValue(rawProfile, UserBehaviorProfile.class);
        } catch (JsonProcessingException e) {
            return new Invalid("invalid_json");
        }
        if (profile.profileVersion() != PROFILE_VERSION) {
            return new Invalid("unsupported_version");
        }
        if (!userId.equals(profile.userId())) {
            return new Invalid("user_mismatch");
        }
        if (profile.runId() == null || profile.runId().isBlank() || !activeRun.equals(profile.runId())) {
            return new Invalid("run_mismatch");
        }
        return new Valid(normalizePreferenceNames(profile));
    }

    static UserBehaviorProfile normalizePreferenceNames(UserBehaviorProfile profile) {
        UserBehaviorProfile.Preferences preferences = profile.preferences();
        if (preferences == null) {
            return profile;
        }
        return new UserBehaviorProfile(
            profile.userId(),
            profile.profileVersion(),
            profile.runId(),
            profile.generatedAt(),
            profile.sourceWindow(),
            profile.evidenceCount(),
            new UserBehaviorProfile.Preferences(normalize(preferences.genres()), normalize(preferences.tags())),
            profile.behavioralFeatures(),
            profile.personas()
        );
    }

    private static List<UserBehaviorProfile.Preference> normalize(List<UserBehaviorProfile.Preference> preferences) {
        return preferences.stream().map(preference -> new UserBehaviorProfile.Preference(
            preference.value() == null ? null : preference.value().trim().replaceAll("\\s+", " ").toLowerCase(Locale.ROOT),
            preference.score(),
            preference.evidenceCount()
        )).toList();
    }
}
