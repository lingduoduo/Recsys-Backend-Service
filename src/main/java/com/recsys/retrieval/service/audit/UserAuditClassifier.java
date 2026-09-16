package com.demo.retrieval.service.audit;

import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.service.audit.ProfileAuditReport.Finding;
import com.demo.retrieval.service.audit.ProfileAuditReport.PreferenceMatch;
import com.demo.retrieval.service.audit.ProfileAuditReport.PreferenceRef;
import com.demo.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.demo.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.demo.retrieval.service.clients.UserProfileValidation;
import com.demo.retrieval.service.clients.UserProfileValidation.Invalid;
import com.demo.retrieval.service.clients.UserProfileValidation.Result;
import com.demo.retrieval.service.clients.UserProfileValidation.Valid;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * Levels 1–3 for one user: validate the blob exactly as serving does, take the positive genre/tag
 * preferences, resolve each against the catalog index. Pure; a row with no findings is healthy.
 */
final class UserAuditClassifier {
    private static final String PERSONA_NEW_OR_UNKNOWN = "new_or_unknown";

    private UserAuditClassifier() {
    }

    static UserRow classify(RawProfile raw, String activeRun, CatalogPreferenceIndex index, ObjectMapper mapper, int sampleItems) {
        if (activeRun == null) {
            return new UserRow(raw.userId(), false, null,
                List.of(new Finding(ProfileAuditReport.FINDING_NO_PROFILE, ProfileAuditReport.STATUS_MISSING_ACTIVE_RUN, null)), null);
        }
        Result result = UserProfileValidation.validate(raw.json(), raw.userId(), activeRun, mapper);
        if (result instanceof Invalid invalid) {
            return new UserRow(raw.userId(), false, null,
                List.of(new Finding(ProfileAuditReport.FINDING_NO_PROFILE, invalid.reason(), null)), null);
        }
        UserBehaviorProfile profile = ((Valid) result).profile();
        List<Finding> findings = new ArrayList<>();

        if (profile.personas().stream().anyMatch(p -> PERSONA_NEW_OR_UNKNOWN.equals(p.type()))) {
            findings.add(new Finding(ProfileAuditReport.FINDING_NEW_OR_UNKNOWN, null, null));
        }

        List<PreferenceMatch> matches = new ArrayList<>();
        List<PreferenceRef> unmatched = new ArrayList<>();
        UserBehaviorProfile.Preferences preferences = profile.preferences();
        if (preferences != null) {
            resolve(CatalogPreferenceIndex.KIND_GENRE, preferences.genres(), index, sampleItems, matches, unmatched);
            resolve(CatalogPreferenceIndex.KIND_TAG, preferences.tags(), index, sampleItems, matches, unmatched);
        }
        if (matches.isEmpty()) {
            findings.add(new Finding(ProfileAuditReport.FINDING_EMPTY_PREFERENCES, null, null));
        } else if (!unmatched.isEmpty()) {
            findings.add(new Finding(ProfileAuditReport.FINDING_PREFERENCE_WITHOUT_CONTENT, null, List.copyOf(unmatched)));
        }

        Long ttl = raw.ttlSeconds() >= 0 ? raw.ttlSeconds() : null;
        return new UserRow(raw.userId(), true, ttl, List.copyOf(findings), List.copyOf(matches));
    }

    private static void resolve(
        String kind,
        List<UserBehaviorProfile.Preference> preferences,
        CatalogPreferenceIndex index,
        int sampleItems,
        List<PreferenceMatch> matches,
        List<PreferenceRef> unmatched
    ) {
        for (UserBehaviorProfile.Preference preference : preferences) {
            if (preference.value() == null || preference.score() <= 0.0) {
                continue;
            }
            CatalogPreferenceIndex.ContentNeighbors neighbors = index.probe(kind, preference.value(), sampleItems);
            matches.add(new PreferenceMatch(kind, preference.value(), preference.score(), preference.evidenceCount(),
                neighbors.total(), neighbors.sample()));
            if (neighbors.total() == 0) {
                unmatched.add(new PreferenceRef(kind, preference.value()));
            }
        }
    }
}
