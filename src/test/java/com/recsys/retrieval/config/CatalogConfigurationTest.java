package com.recsys.retrieval.config;

import com.recsys.api.rest.ModelApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The retrieval service's own application.yml never came across with its Java sources, so every
 * recsys.retrieval key fell back to a Java default. The catalog's default is an empty map that 16
 * call sites read, which means the content path starts cleanly and serves nothing — a failure
 * mode with no error anywhere to announce it. This asserts the shipped configuration is present.
 */
@SpringBootTest(classes = ModelApplication.class)
class CatalogConfigurationTest {

    @Autowired
    private RecommendationProperties properties;

    @Test
    void shipsANonEmptyCatalog() {
        assertThat(properties.getCatalog()).isNotEmpty();
    }

    @Test
    void everyCatalogEntryHasATitleAndAtLeastOneGenre() {
        assertThat(properties.getCatalog().values()).allSatisfy(profile -> {
            assertThat(profile.getTitle()).isNotBlank();
            assertThat(profile.getGenres()).isNotEmpty();
        });
    }

    /**
     * Every scalar under recsys.retrieval.* in the shipped application.yml is, by design, the
     * origin's Java default spelled out explicitly (verified by hand against
     * RecommendationProperties: measurements, cache, sequence, grpo, profile-audit, embeddings,
     * candidate-generation, filtering, bandit, replay-buffer and reward-model all resolve to the
     * exact value their field initializer already has). That means MeasurementPropertiesTest,
     * ProfileAuditPropertiesTest and friends would pass just as well against an empty/misspelled
     * recsys.retrieval prefix as against a correctly bound one — they cannot tell the two apart.
     *
     * catalog is the one exception: its Java default is an empty map (`new LinkedHashMap<>()`),
     * while the shipped file seeds 7 entries. Asserting the exact count (not just "non-empty",
     * which shipsANonEmptyCatalog above already covers) and a specific entry's fields is what
     * actually distinguishes "recsys.retrieval.catalog bound correctly" from "fell back to
     * default" — the two are observably different outcomes here, which they are nowhere else in
     * this configuration block.
     */
    @Test
    void catalogSizeAndAKnownEntryMatchTheShippedFixtureRatherThanTheEmptyDefault() {
        assertThat(properties.getCatalog()).hasSize(7);

        RecommendationProperties.MovieProfile item4 = properties.getCatalog().get("item4");
        assertThat(item4).isNotNull();
        assertThat(item4.getTitle()).isEqualTo("Deep Blue Signal");
        assertThat(item4.getGenres()).containsExactly("sci-fi", "thriller");
        assertThat(item4.isNewRelease()).isTrue();
    }
}
