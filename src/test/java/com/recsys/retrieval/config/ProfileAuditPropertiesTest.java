package com.recsys.retrieval.config;

import com.recsys.api.rest.ModelApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(classes = ModelApplication.class)
class ProfileAuditPropertiesTest {

    @Autowired
    private RecommendationProperties properties;

    @Test
    void bindsConfiguredProfileAuditDefaults() {
        assertEquals(500, properties.getProfileAudit().getChunkSize());
    }

    @Test
    void rejectsNonPositiveChunkSize() {
        assertThrows(Exception.class, () -> new SpringApplicationBuilder(ModelApplication.class)
            .web(WebApplicationType.NONE)
            .run("--recsys.retrieval.profile-audit.chunk-size=0"));
    }

    @Test
    void rejectsNegativeSampleItems() {
        assertThrows(Exception.class, () -> new SpringApplicationBuilder(ModelApplication.class)
            .web(WebApplicationType.NONE)
            .run("--recsys.retrieval.profile-audit.sample-items=-1"));
    }
}
