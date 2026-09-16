package com.demo.retrieval.controller;

import com.demo.retrieval.model.FeedbackRequest;
import com.demo.retrieval.measurement.MeasurementSnapshot;
import com.demo.retrieval.measurement.RecommendationMeasurementService;
import com.demo.retrieval.service.DeepLearningPredictionService;
import com.demo.retrieval.service.HybridRecommendationService;
import com.demo.retrieval.service.ModelIndexOutOfRangeException;
import com.demo.retrieval.model.ModelPrediction;
import com.demo.retrieval.model.RecommendationResult;
import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.service.clients.UserProfileClient;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(RecommendationController.class)
@SuppressWarnings({"unchecked", "null"})
class RecommendationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private StringRedisTemplate redis;

    @MockBean
    private HybridRecommendationService recommendationService;

    @MockBean
    private DeepLearningPredictionService predictionService;

    @MockBean
    private RecommendationMeasurementService measurementService;

    @MockBean
    private UserProfileClient userProfileClient;

    // --- /users/{user}/profile ---

    @Test
    void profileEndpointReturnsCompleteNestedProfileContract() throws Exception {
        when(userProfileClient.getProfile("u1")).thenReturn(Optional.of(profile("u1")));

        mockMvc.perform(get("/users/u1/profile"))
            .andExpect(status().isOk())
            .andExpect(content().contentTypeCompatibleWith("application/json"))
            .andExpect(jsonPath("$.user_id").value("u1"))
            .andExpect(jsonPath("$.profile_version").value(1))
            .andExpect(jsonPath("$.source_window.start").value("1969-12-02T00:16:40Z"))
            .andExpect(jsonPath("$.preferences.genres[0].value").value("sci-fi"))
            .andExpect(jsonPath("$.behavioral_features.average_rating").value(org.hamcrest.Matchers.nullValue()))
            .andExpect(jsonPath("$.personas[0].evidence.minimum_evidence").value(5.0));
    }

    @Test
    void profileEndpointReturnsNotFoundWhenNoProfileExists() throws Exception {
        when(userProfileClient.getProfile("u1")).thenReturn(Optional.empty());

        mockMvc.perform(get("/users/u1/profile"))
            .andExpect(status().isNotFound())
            .andExpect(content().contentTypeCompatibleWith("application/json"))
            .andExpect(jsonPath("$.error").value("profile_not_found"))
            .andExpect(jsonPath("$.user_id").value("u1"));
    }

    @Test
    void profileEndpointRejectsInvalidUserId() throws Exception {
        mockMvc.perform(get("/users/user!/profile"))
            .andExpect(status().isBadRequest());
    }

    // --- /embedding ---

    @Test
    void embeddingReturnsVectorFromRedis() throws Exception {
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        when(ops.get("i2vEmb:item1")).thenReturn("0.1 0.2 0.3");

        mockMvc.perform(get("/embedding/item1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.item").value("item1"))
            .andExpect(jsonPath("$.embedding[0]").value(0.1))
            .andExpect(jsonPath("$.embedding[1]").value(0.2));
    }

    @Test
    void embeddingReturnsEmptyListForUnknownItem() throws Exception {
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        when(ops.get("i2vEmb:unknown")).thenReturn(null);

        mockMvc.perform(get("/embedding/unknown"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.embedding").isEmpty());
    }

    @Test
    void embeddingReturnsCorruptDataErrorOnBadValues() throws Exception {
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        when(ops.get("i2vEmb:item1")).thenReturn("0.1 NOT_A_NUMBER 0.3");

        mockMvc.perform(get("/embedding/item1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.error").value("corrupt_data"));
    }

    @Test
    void embeddingReturnsBadRequestForInvalidItemId() throws Exception {
        mockMvc.perform(get("/embedding/item!"))
            .andExpect(status().isBadRequest());
    }

    // --- /recommend ---

    @Test
    void recommendExcludesRecentlyViewedItems() throws Exception {
        when(recommendationService.recommend("u1", 6)).thenReturn(
            new RecommendationResult(
                "u1",
                List.of("item1", "item2"),
                List.of("item3", "item4"),
                List.of(Map.of("item", "item3", "coldStart", true)),
                Map.of("pseudoRegret", 0.1)
            )
        );

        mockMvc.perform(get("/recommend/u1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.recent[0]").value("item1"))
            .andExpect(jsonPath("$.recommendations[0]").value("item3"))
            .andExpect(jsonPath("$.recommendations[1]").value("item4"))
            .andExpect(jsonPath("$.diagnostics[0].item").value("item3"))
            .andExpect(jsonPath("$.metrics.pseudoRegret").value(0.1));
    }

    @Test
    void recommendReturnsEmptyListWhenRedisIsEmpty() throws Exception {
        when(recommendationService.recommend("u1", 6)).thenReturn(
            new RecommendationResult("u1", List.of(), List.of(), List.of(), Map.of("eligibleCandidateCount", 0))
        );

        mockMvc.perform(get("/recommend/u1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.recommendations").isEmpty());
    }

    @Test
    void recommendReturnsBadRequestForInvalidUserId() throws Exception {
        mockMvc.perform(get("/recommend/user!"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void recommendBoundsLimitToMaxLimit() throws Exception {
        when(recommendationService.recommend("u1", 50)).thenReturn(
            new RecommendationResult("u1", List.of(), List.of(), List.of(), Map.of())
        );

        mockMvc.perform(get("/recommend/u1?limit=999"))
            .andExpect(status().isOk());
    }

    @Test
    void predictReturnsModelScoreForKnownLookupIds() throws Exception {
        when(predictionService.predict("user_employee_01", "action_benefits")).thenReturn(
            java.util.Optional.of(new ModelPrediction(
                "user_employee_01",
                "action_benefits",
                0L,
                0L,
                0.42,
                "mlp_embedding"
            ))
        );

        mockMvc.perform(get("/predict/user_employee_01/action_benefits"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.model").value("mlp_embedding"))
            .andExpect(jsonPath("$.user").value("user_employee_01"))
            .andExpect(jsonPath("$.item").value("action_benefits"))
            .andExpect(jsonPath("$.userId").value(0))
            .andExpect(jsonPath("$.itemId").value(0))
            .andExpect(jsonPath("$.score").value(0.42));
    }

    @Test
    void predictReturnsUnknownErrorForMissingLookupIds() throws Exception {
        when(predictionService.predict("missing", "action_benefits")).thenReturn(java.util.Optional.empty());
        when(predictionService.metadata()).thenReturn(Map.of("model", "mlp_embedding", "users", 32, "items", 12));

        mockMvc.perform(get("/predict/missing/action_benefits"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.error").value("unknown_user_or_item"))
            .andExpect(jsonPath("$.metadata.users").value(32));
    }

    @Test
    void predictByIdReturnsModelScore() throws Exception {
        when(predictionService.predict(0L, 1L)).thenReturn(
            new ModelPrediction(null, null, 0L, 1L, 0.61, "mlp_embedding")
        );

        mockMvc.perform(get("/predict/id?userId=0&itemId=1"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.model").value("mlp_embedding"))
            .andExpect(jsonPath("$.userId").value(0))
            .andExpect(jsonPath("$.itemId").value(1))
            .andExpect(jsonPath("$.score").value(0.61));
    }

    @Test
    void predictByIdReturnsBadRequestForIndexOutsideModelVocabulary() throws Exception {
        when(predictionService.predict(0L, 42L)).thenThrow(new ModelIndexOutOfRangeException(
            "Model indices out of range: userId must be 0..31 and itemId must be 0..11"
        ));

        mockMvc.perform(get("/predict/id?userId=0&itemId=42"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").value(
                "Model indices out of range: userId must be 0..31 and itemId must be 0..11"
            ));
    }

    @Test
    void feedbackEndpointAcceptsLegacyFourFieldBody() throws Exception {
        when(recommendationService.recordFeedback(any())).thenReturn(Map.of("status", "ok", "clicked", true));

        mockMvc.perform(post("/feedback")
                .contentType("application/json")
                .content("{\"user\":\"u1\",\"item\":\"item4\",\"clicked\":true,\"reward\":1.0}"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"))
            .andExpect(jsonPath("$.clicked").value(true));

        verify(measurementService).recordRequest(eq("feedback"), any(Duration.class), eq(false), eq(false));
    }

    @Test
    void feedbackEndpointAcceptsEnrichedMeasurementFields() throws Exception {
        when(recommendationService.recordFeedback(any())).thenReturn(Map.of("status", "ok"));

        mockMvc.perform(post("/feedback")
                .contentType("application/json")
                .content("""
                    {
                      "user": "u1",
                      "item": "item1",
                      "clicked": true,
                      "reward": 1.0,
                      "requestId": "req-1",
                      "rating": 4.5,
                      "negativeFeedbackReason": null,
                      "dwellMillis": 12000,
                      "completionRate": 0.75
                    }
                    """))
            .andExpect(status().isOk());

        org.mockito.ArgumentCaptor<FeedbackRequest> request =
            org.mockito.ArgumentCaptor.forClass(FeedbackRequest.class);
        verify(recommendationService).recordFeedback(request.capture());
        assertEquals("req-1", request.getValue().requestId());
        assertEquals(4.5, request.getValue().rating());
        assertNull(request.getValue().negativeFeedbackReason());
        assertEquals(12000L, request.getValue().dwellMillis());
        assertEquals(0.75, request.getValue().completionRate());
    }

    @Test
    void feedbackEndpointRejectsRatingOutsideZeroToFive() throws Exception {
        invalidFeedbackFieldIsRejected("\"rating\":5.1");
    }

    @Test
    void feedbackEndpointRejectsNegativeDwellMillis() throws Exception {
        invalidFeedbackFieldIsRejected("\"dwellMillis\":-1");
    }

    @Test
    void feedbackEndpointRejectsCompletionRateOutsideZeroToOne() throws Exception {
        invalidFeedbackFieldIsRejected("\"completionRate\":1.1");
    }

    private void invalidFeedbackFieldIsRejected(String invalidField) throws Exception {
        mockMvc.perform(post("/feedback")
                .contentType("application/json")
                .content("{\"user\":\"u1\",\"item\":\"item1\",\"clicked\":true,\"reward\":1.0,"
                    + invalidField + "}"))
            .andExpect(status().isBadRequest());
    }

    private static UserBehaviorProfile profile(String userId) {
        return new UserBehaviorProfile(
            userId, 1, "run-7", "1970-01-01T00:16:40Z",
            new UserBehaviorProfile.SourceWindow("1969-12-02T00:16:40Z", "1970-01-01T00:16:41Z"),
            1,
            new UserBehaviorProfile.Preferences(
                List.of(new UserBehaviorProfile.Preference("sci-fi", 0.5, 1)),
                List.of(new UserBehaviorProfile.Preference("space", 0.5, 1))
            ),
            new UserBehaviorProfile.BehavioralFeatures(1.0, 0.0, 0.0, 1.0, 1.0, null, "low"),
            List.of(new UserBehaviorProfile.Persona(
                "new_or_unknown", "New or unknown", 1.0,
                java.util.Map.of("evidence_count", 1.0, "minimum_evidence", 5.0)
            ))
        );
    }

    @Test
    void metricsEndpointReturnsAggregateMetrics() throws Exception {
        when(recommendationService.getAggregateMetrics()).thenReturn(Map.of("ctr", 0.25, "requests", 4));
        when(measurementService.snapshot()).thenReturn(new MeasurementSnapshot(
            "2.1", Map.of(), Map.of(), Map.of(), Map.of(), Map.of(), Map.of()));

        mockMvc.perform(get("/metrics"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.ctr").value(0.25))
            .andExpect(jsonPath("$.requests").value(4))
            .andExpect(jsonPath("$.measurements.schemaVersion").value("2.1"));
    }

    @Test
    void recommendEndpointRecordsBoundedRequestMeasurement() throws Exception {
        when(recommendationService.recommend("u1", 6)).thenReturn(
            new RecommendationResult("u1", List.of(), List.of(), List.of(), Map.of())
        );

        mockMvc.perform(get("/recommend/u1"))
            .andExpect(status().isOk());

        verify(measurementService).recordRequest(eq("recommend"), any(Duration.class), eq(false), eq(false));
    }

    @Test
    void recommendEndpointRecordsTimeoutAsAnErrorAndTimeout() {
        when(recommendationService.recommend("u1", 6)).thenThrow(new IllegalStateException(new TimeoutException()));

        assertThrows(Exception.class, () -> mockMvc.perform(get("/recommend/u1")));

        verify(measurementService).recordRequest(eq("recommend"), any(Duration.class), eq(true), eq(true));
    }

    @Test
    void recommendEndpointRecordsNonTimeoutServiceErrorWithoutTimeoutFlag() {
        when(recommendationService.recommend("u1", 6)).thenThrow(new IllegalStateException("service failed"));

        assertThrows(Exception.class, () -> mockMvc.perform(get("/recommend/u1")));

        verify(measurementService).recordRequest(eq("recommend"), any(Duration.class), eq(true), eq(false));
    }

    @Test
    void feedbackEndpointRecordsTimeoutAsAnErrorAndTimeout() {
        when(recommendationService.recordFeedback(any())).thenThrow(new IllegalStateException(new TimeoutException()));

        assertThrows(Exception.class, () -> mockMvc.perform(post("/feedback")
            .contentType("application/json")
            .content("{\"user\":\"u1\",\"item\":\"item1\",\"clicked\":true,\"reward\":1.0}")));

        verify(measurementService).recordRequest(eq("feedback"), any(Duration.class), eq(true), eq(true));
    }

    @Test
    void recordsLatencyForPredictEmbeddingAndProfileEndpoints() throws Exception {
        ValueOperations<String, String> ops = mock(ValueOperations.class);
        when(redis.opsForValue()).thenReturn(ops);
        when(ops.get("i2vEmb:i1")).thenReturn("0.1 0.2");
        when(predictionService.predict("u1", "i1")).thenReturn(Optional.empty());
        when(predictionService.metadata()).thenReturn(Map.of("model", "test"));
        when(userProfileClient.getProfile("u1")).thenReturn(Optional.of(profile("u1")));

        mockMvc.perform(get("/predict/u1/i1")).andExpect(status().isOk());
        mockMvc.perform(get("/users/u1/profile")).andExpect(status().isOk());
        mockMvc.perform(get("/embedding/i1")).andExpect(status().isOk());

        verify(measurementService).recordRequest(eq("predict"), any(Duration.class), eq(false), eq(false));
        verify(measurementService).recordRequest(eq("profile"), any(Duration.class), eq(false), eq(false));
        verify(measurementService).recordRequest(eq("embedding"), any(Duration.class), eq(false), eq(false));
    }

    @Test
    void treatsAnUnknownProfileAsAbsentRatherThanAServerError() throws Exception {
        when(userProfileClient.getProfile("nobody")).thenReturn(Optional.empty());

        mockMvc.perform(get("/users/nobody/profile")).andExpect(status().isNotFound());

        verify(measurementService).recordRequest(eq("profile"), any(Duration.class), eq(false), eq(false));
    }
}
