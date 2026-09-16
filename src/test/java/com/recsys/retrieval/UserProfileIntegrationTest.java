package com.demo.retrieval;

import com.demo.retrieval.support.ContractFixtures;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.hamcrest.Matchers.nullValue;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/*
 * Skips on Docker 25+ daemons, including Colima's, and that is expected rather than a broken test.
 *
 * docker-java 3.x requests Docker API v1.32. Docker 25 raised the minimum accepted version and
 * Docker 29 requires 1.44, so the daemon rejects the handshake with "client version 1.32 is too
 * old" before any container starts. Testcontainers reports "Could not find a valid Docker
 * environment" and disabledWithoutDocker turns that into a skip, which is the right behaviour --
 * the alternative is a hard failure on every machine with a current Docker.
 *
 * Confirm the daemon side with:
 *   curl -s --unix-socket <docker.sock> http://localhost/v1.32/version   # rejected
 *   curl -s --unix-socket <docker.sock> http://localhost/v1.44/version   # succeeds
 *
 * Five fixes were tried on 2026-08-16 and none worked, so do not spend time on them again:
 * bumping Testcontainers to 1.20.4 and 1.21.3 (docker-java 3.4.0 / 3.4.2), setting
 * DOCKER_API_VERSION=1.44, passing -Dapi.version=1.44 into the forked surefire JVM, and forcing
 * EnvironmentAndSystemPropertyClientProviderStrategy instead of the tc.host strategy. The last one
 * proved the point: the strategy named in the error changed, and it still sent 1.32. docker-java
 * pins that version through every configuration surface reachable from this project.
 *
 * The fix belongs upstream in Testcontainers/docker-java. Until then, run this test against a
 * Docker daemon older than 25 if you need it. The Testcontainers version comes from the Spring
 * Boot parent, not from our pom.
 */
@SpringBootTest(properties = {
    "recsys.catalog.a-sci-fi.title=Shared Fixture Sci-Fi",
    "recsys.catalog.a-sci-fi.genres[0]=sci-fi",
    "recsys.catalog.a-sci-fi.tags[0]=space",
    "recsys.catalog.a-sci-fi.new-release=false",
    "recsys.catalog.z-drama.title=Baseline Drama",
    "recsys.catalog.z-drama.genres[0]=drama",
    "recsys.catalog.z-drama.tags[0]=character",
    "recsys.catalog.z-drama.new-release=false",
    "spring.data.redis.port=1"
})
@AutoConfigureMockMvc
@Testcontainers(disabledWithoutDocker = true)
class UserProfileIntegrationTest {
    private static final String USER_ID = "fixture-user";
    private static final String RUN_ID = "fixture-run";
    private static final String ACTIVE_RUN_KEY = "user-profile:v1:active-run";
    private static final String PROFILE_KEY = "user-profile:v1:" + RUN_ID + ":" + USER_ID;
    @Container
    private static final GenericContainer<?> REDIS =
        new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private StringRedisTemplate redis;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void seedRedis() throws Exception {
        try {
            redis.opsForZSet().add("global:item_popularity", "a-sci-fi", 10.0);
            redis.opsForZSet().add("global:item_popularity", "z-drama", 10.0);
        } catch (RuntimeException error) {
            fail("Redis test container and Spring Redis properties are not configured", error);
        }
    }

    @Test
    void sharedSparkFixtureDrivesProfileApiAndRecommendationAffinity() throws Exception {
        List<String> baseline = recommendationOrder("baseline-user");
        assertEquals(List.of("z-drama", "a-sci-fi"), baseline);

        String fixtureJson = ContractFixtures.text("user_profile_v1.json").trim();
        redis.opsForValue().set(PROFILE_KEY, fixtureJson);
        redis.opsForValue().set(ACTIVE_RUN_KEY, RUN_ID);

        mockMvc.perform(get("/users/{user}/profile", USER_ID))
            .andExpect(status().isOk())
            .andExpect(content().contentTypeCompatibleWith("application/json"))
            .andExpect(jsonPath("$.user_id").value(USER_ID))
            .andExpect(jsonPath("$.profile_version").value(1))
            .andExpect(jsonPath("$.run_id").value(RUN_ID))
            .andExpect(jsonPath("$.generated_at").value("1970-01-01T00:16:40Z"))
            .andExpect(jsonPath("$.source_window.start").value("1969-12-02T00:16:40Z"))
            .andExpect(jsonPath("$.source_window.end").value("1970-01-01T00:16:41Z"))
            .andExpect(jsonPath("$.evidence_count").value(1))
            .andExpect(jsonPath("$.behavioral_features.average_rating").value(nullValue()))
            .andExpect(jsonPath("$.preferences.genres.length()").value(1))
            .andExpect(jsonPath("$.preferences.genres[0].value").value("sci-fi"))
            .andExpect(jsonPath("$.preferences.tags.length()").value(1))
            .andExpect(jsonPath("$.preferences.tags[0].value").value("space"))
            .andExpect(jsonPath("$.personas[0].type").value("new_or_unknown"))
            .andExpect(jsonPath("$.personas[0].evidence.evidence_count").value(1.0))
            .andExpect(jsonPath("$.personas[0].evidence.minimum_evidence").value(5.0));

        // Ranked first, ahead of z-drama and of every catalog item the content retriever pulls
        // in on the sci-fi/space preference. z-drama is not asserted to be present: the profile
        // pushes it out of a top-2 slate entirely.
        List<String> personalized = recommendationOrder(USER_ID);
        assertEquals("a-sci-fi", personalized.get(0));

        redis.delete(ACTIVE_RUN_KEY);
        redis.delete(List.of(
            "user:" + USER_ID + ":served_history",
            "user:" + USER_ID + ":impressions",
            "replay:pending:" + USER_ID + ":a-sci-fi",
            "replay:pending:" + USER_ID + ":z-drama"
        ));
        assertEquals(baseline, recommendationOrder(USER_ID));
    }

    private List<String> recommendationOrder(String userId) throws Exception {
        String response = mockMvc.perform(get("/recommend/{user}", userId).param("limit", "2"))
            .andExpect(status().isOk())
            .andReturn()
            .getResponse()
            .getContentAsString();
        JsonNode recommendations = objectMapper.readTree(response).path("recommendations");
        assertEquals(2, recommendations.size());
        return List.of(recommendations.get(0).asText(), recommendations.get(1).asText());
    }
}
