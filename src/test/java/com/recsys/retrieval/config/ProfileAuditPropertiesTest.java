package com.recsys.retrieval.config;

import com.recsys.api.rest.ModelApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = ModelApplication.class)
class ProfileAuditPropertiesTest {

    @Autowired
    private RecommendationProperties properties;

    @Test
    void bindsConfiguredProfileAuditDefaults() {
        assertEquals(500, properties.getProfileAudit().getChunkSize());
    }

    // assertThrows(Exception.class) alone would pass if the context failed to start for ANY
    // reason -- a typo'd property key, a missing bean, anything -- not just the @Min violation
    // these are meant to pin. Walking the cause chain for the bind-validation message makes each
    // test fail again if the property ever silently stops being validated.

    @Test
    void rejectsNonPositiveChunkSize() {
        Exception ex = assertThrows(Exception.class, () -> new SpringApplicationBuilder(ModelApplication.class)
            .web(WebApplicationType.NONE)
            .run("--recsys.retrieval.profile-audit.chunk-size=0"));

        assertTrue(causeChainContains(ex, "must be greater than or equal to 1"),
            "expected a @Min bind-validation failure on profile-audit.chunk-size; got: " + causeChainDescription(ex));
    }

    @Test
    void rejectsNegativeSampleItems() {
        Exception ex = assertThrows(Exception.class, () -> new SpringApplicationBuilder(ModelApplication.class)
            .web(WebApplicationType.NONE)
            .run("--recsys.retrieval.profile-audit.sample-items=-1"));

        assertTrue(causeChainContains(ex, "must be greater than or equal to 0"),
            "expected a @Min bind-validation failure on profile-audit.sample-items; got: " + causeChainDescription(ex));
    }

    private static boolean causeChainContains(Throwable throwable, String expectedMessageFragment) {
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            if (current.getMessage() != null && current.getMessage().contains(expectedMessageFragment)) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return false;
    }

    private static String causeChainDescription(Throwable throwable) {
        StringBuilder description = new StringBuilder();
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            description.append(current.getClass().getName()).append(": ").append(current.getMessage()).append(" | ");
            if (current.getCause() == current) {
                break;
            }
        }
        return description.toString();
    }
}
