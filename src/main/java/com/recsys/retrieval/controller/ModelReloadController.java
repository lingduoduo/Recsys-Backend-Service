package com.demo.retrieval.controller;

import com.demo.retrieval.service.DeepLearningPredictionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
public class ModelReloadController {
    private static final Logger log = LoggerFactory.getLogger(ModelReloadController.class);

    private final DeepLearningPredictionService predictionService;

    public ModelReloadController(DeepLearningPredictionService predictionService) {
        this.predictionService = predictionService;
    }

    @PostMapping("/actuator/model-reload")
    public Map<String, String> reload() {
        try {
            predictionService.reload();
            log.info("ONNX model reloaded successfully");
            return Map.of("status", "ok");
        } catch (Exception e) {
            log.error("ONNX model reload failed", e);
            return Map.of("status", "error", "message", e.getMessage());
        }
    }
}
