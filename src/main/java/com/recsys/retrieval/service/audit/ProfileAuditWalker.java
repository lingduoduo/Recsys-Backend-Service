package com.recsys.retrieval.service.audit;

import com.recsys.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.recsys.retrieval.service.audit.ProfileAuditStore.RawProfile;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * "Neighbors of these accounts", as one bounded operation.
 *
 * <p>Account → Profile is a small fan-out: at most one profile per account, fetched for the whole
 * batch in a single pipelined round trip. Preference → Content is a large fan-out kept cheap by
 * {@link CatalogPreferenceIndex#probe}, which reports a count and a capped sample instead of
 * materializing a posting list. Nothing here scans, aggregates, or holds state across calls, so
 * both entry points — one account or one chunk of a bulk audit — traverse identically.
 */
final class ProfileAuditWalker {

    private final ProfileAuditStore store;
    private final ObjectMapper mapper;
    private final CatalogPreferenceIndex index;
    private final int sampleItems;

    ProfileAuditWalker(ProfileAuditStore store, ObjectMapper mapper, CatalogPreferenceIndex index, int sampleItems) {
        this.store = store;
        this.mapper = mapper;
        this.index = index;
        this.sampleItems = sampleItems;
    }

    /**
     * Classified rows for {@code accounts}, in input order. A null {@code activeRun} yields a
     * {@code missing_active_run} row per account and reads nothing.
     */
    List<UserRow> walk(String activeRun, List<String> accounts) {
        if (accounts.isEmpty()) {
            return List.of();
        }
        if (activeRun == null) {
            List<UserRow> rows = new ArrayList<>(accounts.size());
            for (String account : accounts) {
                rows.add(UserAuditClassifier.classify(new RawProfile(account, null, -2L), null, index, mapper, sampleItems));
            }
            return rows;
        }
        List<RawProfile> raws = store.readProfiles(activeRun, accounts);
        List<UserRow> rows = new ArrayList<>(raws.size());
        for (RawProfile raw : raws) {
            rows.add(UserAuditClassifier.classify(raw, activeRun, index, mapper, sampleItems));
        }
        return rows;
    }
}
