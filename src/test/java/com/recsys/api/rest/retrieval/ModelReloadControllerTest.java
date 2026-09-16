package com.recsys.api.rest.retrieval;

import com.recsys.application.auth.LoginTokenService;
import com.recsys.config.RequestScopeData;
import com.recsys.retrieval.service.DeepLearningPredictionService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ModelReloadController.class)
class ModelReloadControllerTest {

    @Autowired
    MockMvc mvc;

    @MockBean
    DeepLearningPredictionService predictionService;

    @MockBean
    LoginTokenService loginTokenService;

    @MockBean
    RequestScopeData requestScopeData;

    @Test
    void reloadReturnsOk() throws Exception {
        mvc.perform(post("/api/v1/retrieval/model/reload"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"));
    }
}
