package com.demo.retrieval.config;

import com.demo.retrieval.RetrievalServiceApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(classes = RetrievalServiceApplication.class)
class ProfileAuditPropertiesTest {

    @Autowired
    private RecommendationProperties properties;

    @Test
    void bindsConfiguredProfileAuditDefaults() {
        assertEquals(500, properties.getProfileAudit().getChunkSize());
    }

    @Test
    void rejectsNonPositiveChunkSize() {
        assertThrows(Exception.class, () -> new SpringApplicationBuilder(RetrievalServiceApplication.class)
            .web(WebApplicationType.NONE)
            .run("--recsys.profile-audit.chunk-size=0"));
    }

    @Test
    void rejectsNegativeSampleItems() {
        assertThrows(Exception.class, () -> new SpringApplicationBuilder(RetrievalServiceApplication.class)
            .web(WebApplicationType.NONE)
            .run("--recsys.profile-audit.sample-items=-1"));
    }
}
