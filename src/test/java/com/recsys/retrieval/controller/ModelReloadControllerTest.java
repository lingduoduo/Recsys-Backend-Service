package com.demo.retrieval.controller;

import com.demo.retrieval.service.DeepLearningPredictionService;
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

    @Test
    void reloadReturnsOk() throws Exception {
        mvc.perform(post("/actuator/model-reload"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"));
    }
}
