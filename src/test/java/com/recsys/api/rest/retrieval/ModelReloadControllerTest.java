package com.recsys.api.rest.retrieval;

import com.recsys.application.auth.AdminTokenGuard;
import com.recsys.retrieval.service.DeepLearningPredictionService;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Standalone rather than {@code @WebMvcTest} so the operator token is a constructor argument
 * instead of a process environment variable: the guard reads {@code SHARD_ADMIN_TOKEN} from
 * {@code System.getenv}, which a slice test cannot set.
 */
class ModelReloadControllerTest {

    private final DeepLearningPredictionService predictionService = mock(DeepLearningPredictionService.class);

    private MockMvc mvcGuardedBy(String token) {
        return MockMvcBuilders
            .standaloneSetup(new ModelReloadController(predictionService, new AdminTokenGuard(token)))
            .build();
    }

    @Test
    void reloadReturnsOk() throws Exception {
        mvcGuardedBy("s3cret").perform(post("/api/v1/retrieval/model/reload")
                .header(AdminTokenGuard.HEADER, "s3cret"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"));
    }

    /**
     * The second layer. The gateway classifies this route OPERATOR, but that binds only callers
     * who arrive through the gateway — anything with in-cluster reach to 8080 does not.
     */
    @Test
    void reloadRejectsAWrongOrMissingOperatorToken() throws Exception {
        mvcGuardedBy("s3cret").perform(post("/api/v1/retrieval/model/reload")
                .header(AdminTokenGuard.HEADER, "wrong"))
            .andExpect(status().isForbidden());
        mvcGuardedBy("s3cret").perform(post("/api/v1/retrieval/model/reload"))
            .andExpect(status().isForbidden());
        verify(predictionService, never()).reload();
    }

    /** Fail closed, matching AdminTokenGuard on 7010: an unprovisioned deployment authorizes nobody. */
    @Test
    void reloadRejectsEveryCallerWhenNoTokenIsConfigured() throws Exception {
        mvcGuardedBy(null).perform(post("/api/v1/retrieval/model/reload")
                .header(AdminTokenGuard.HEADER, "anything"))
            .andExpect(status().isForbidden());
        mvcGuardedBy("").perform(post("/api/v1/retrieval/model/reload")
                .header(AdminTokenGuard.HEADER, ""))
            .andExpect(status().isForbidden());
        verify(predictionService, never()).reload();
    }

    /**
     * A failure whose exception carries no message. {@code Map.of} rejects a null value, so
     * building the error body with {@code e.getMessage()} threw NPE inside the catch block — the
     * operator got an opaque 500 with no diagnostic, exactly when they needed one. Many ONNX/IO
     * failures arrive this way.
     */
    @Test
    void reloadFailureWithNoMessageStillReturnsABody() throws Exception {
        doThrow(new RuntimeException()).when(predictionService).reload();

        mvcGuardedBy("s3cret").perform(post("/api/v1/retrieval/model/reload")
                .header(AdminTokenGuard.HEADER, "s3cret"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.status").value("error"))
            .andExpect(jsonPath("$.message").value("null"));
    }

    /** And the failure is visible from the status line, so an operator script can branch on it. */
    @Test
    void reloadFailureIsNotReportedAsHttpOk() throws Exception {
        doThrow(new IllegalStateException("artifact missing")).when(predictionService).reload();

        mvcGuardedBy("s3cret").perform(post("/api/v1/retrieval/model/reload")
                .header(AdminTokenGuard.HEADER, "s3cret"))
            .andExpect(status().isInternalServerError())
            .andExpect(jsonPath("$.message").value("artifact missing"));
    }
}
