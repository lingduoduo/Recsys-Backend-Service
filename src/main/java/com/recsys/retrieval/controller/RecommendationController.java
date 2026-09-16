package com.demo.retrieval.controller;

import com.demo.retrieval.service.DeepLearningPredictionService;
import com.demo.retrieval.model.FeedbackRequest;
import com.demo.retrieval.measurement.RecommendationMeasurementService;
import com.demo.retrieval.service.HybridRecommendationService;
import com.demo.retrieval.service.ModelIndexOutOfRangeException;
import com.demo.retrieval.model.ModelPrediction;
import com.demo.retrieval.model.RecommendationResult;
import com.demo.retrieval.model.UserBehaviorProfile;
import com.demo.retrieval.service.clients.UserProfileClient;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.sql.SQLTimeoutException;
import org.springframework.dao.QueryTimeoutException;

@RestController
@Validated
public class RecommendationController {
    private static final Logger log = LoggerFactory.getLogger(RecommendationController.class);
    private static final String DEFAULT_LIMIT = "6";
    private static final int MAX_LIMIT = 50;

    @Value("${recsys.embeddings.item-prefix:i2vEmb}")
    private String itemEmbeddingPrefix;

    private final StringRedisTemplate redis;
    private final HybridRecommendationService recommendationService;
    private final DeepLearningPredictionService predictionService;
    private final RecommendationMeasurementService measurementService;
    private final UserProfileClient userProfileClient;

    public RecommendationController(
        StringRedisTemplate redis,
        HybridRecommendationService recommendationService,
        DeepLearningPredictionService predictionService,
        RecommendationMeasurementService measurementService,
        UserProfileClient userProfileClient
    ) {
        this.redis = redis;
        this.recommendationService = recommendationService;
        this.predictionService = predictionService;
        this.measurementService = measurementService;
        this.userProfileClient = userProfileClient;
    }

    @GetMapping("/embedding/{item}")
    public Map<String, Object> embedding(
        @PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String item
    ) {
        long started = System.nanoTime();
        boolean error = true;
        boolean timeout = false;
        try {
            String key = itemEmbeddingPrefix + ":" + item;
            String raw;
            try {
                raw = redis.opsForValue().get(key);
            } catch (Exception e) {
                log.error("Redis fetch failed for embedding {}", key, e);
                error = false;
                return Map.of("item", item, "embedding", List.of());
            }
            if (raw == null) {
                error = false;
                return Map.of("item", item, "embedding", List.of());
            }
            try {
                List<Double> vector = Arrays.stream(raw.split(" "))
                    .map(Double::parseDouble)
                    .toList();
                error = false;
                return Map.of("item", item, "embedding", vector);
            } catch (NumberFormatException e) {
                log.warn("Corrupt embedding data for key {}", key);
                error = false;
                return Map.of("item", item, "embedding", List.of(), "error", "corrupt_data");
            }
        } catch (RuntimeException | Error e) {
            timeout = isTimeout(e);
            throw e;
        } finally {
            measurementService.recordRequest("embedding", Duration.ofNanos(System.nanoTime() - started), error, timeout);
        }
    }

    @GetMapping("/recommend/{user}")
    public Map<String, Object> recommend(
        @PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String user,
        @RequestParam(defaultValue = DEFAULT_LIMIT) int limit
    ) {
        long started = System.nanoTime();
        boolean error = true;
        boolean timeout = false;
        try {
            int boundedLimit = Math.max(1, Math.min(limit, MAX_LIMIT));
            RecommendationResult result = recommendationService.recommend(user, boundedLimit);
            error = false;
            return Map.of(
                "user", result.user(),
                "recent", result.recent(),
                "recommendations", result.recommendations(),
                "diagnostics", result.candidateDiagnostics(),
                "metrics", result.metrics()
            );
        } catch (RuntimeException | Error e) {
            timeout = isTimeout(e);
            throw e;
        } finally {
            measurementService.recordRequest("recommend", Duration.ofNanos(System.nanoTime() - started), error, timeout);
        }
    }

    @GetMapping("/predict/{user}/{item}")
    public Map<String, Object> predict(
        @PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String user,
        @PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String item
    ) {
        long started = System.nanoTime();
        boolean error = true;
        boolean timeout = false;
        try {
            Map<String, Object> response = predictionService.predict(user, item)
                .map(this::predictionPayload)
                .orElseGet(() -> Map.of(
                    "user", user,
                    "item", item,
                    "error", "unknown_user_or_item",
                    "metadata", predictionService.metadata()
                ));
            error = false;
            return response;
        } catch (RuntimeException | Error e) {
            timeout = isTimeout(e);
            throw e;
        } finally {
            measurementService.recordRequest("predict", Duration.ofNanos(System.nanoTime() - started), error, timeout);
        }
    }

    @GetMapping("/predict/id")
    public Map<String, Object> predictById(
        @RequestParam @Min(0) long userId,
        @RequestParam @Min(0) long itemId
    ) {
        long started = System.nanoTime();
        boolean error = true;
        boolean timeout = false;
        try {
            Map<String, Object> response = predictionPayload(predictionService.predict(userId, itemId));
            error = false;
            return response;
        } catch (RuntimeException | Error e) {
            timeout = isTimeout(e);
            throw e;
        } finally {
            measurementService.recordRequest("predict", Duration.ofNanos(System.nanoTime() - started), error, timeout);
        }
    }

    @GetMapping("/predict/metadata")
    public Map<String, Object> predictionMetadata() {
        return predictionService.metadata();
    }

    @GetMapping("/users/{user}/profile")
    public ResponseEntity<UserBehaviorProfile> profile(
        @PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String user
    ) {
        long started = System.nanoTime();
        boolean error = true;
        boolean timeout = false;
        try {
            ResponseEntity<UserBehaviorProfile> response = userProfileClient.getProfile(user)
                .map(ResponseEntity::ok)
                .orElseThrow(() -> new ProfileNotFoundException(user));
            error = false;
            return response;
        } catch (ProfileNotFoundException e) {
            error = false;
            throw e;
        } catch (RuntimeException | Error e) {
            timeout = isTimeout(e);
            throw e;
        } finally {
            measurementService.recordRequest("profile", Duration.ofNanos(System.nanoTime() - started), error, timeout);
        }
    }

    @PostMapping("/feedback")
    public Map<String, Object> feedback(@Valid @RequestBody FeedbackRequest request) {
        long started = System.nanoTime();
        boolean error = true;
        boolean timeout = false;
        try {
            Map<String, Object> response = recommendationService.recordFeedback(request);
            error = false;
            return response;
        } catch (RuntimeException | Error e) {
            timeout = isTimeout(e);
            throw e;
        } finally {
            measurementService.recordRequest("feedback", Duration.ofNanos(System.nanoTime() - started), error, timeout);
        }
    }

    @GetMapping("/metrics")
    public Map<String, Object> metrics() {
        Map<String, Object> aggregate = new LinkedHashMap<>(recommendationService.getAggregateMetrics());
        aggregate.put("measurements", measurementService.snapshot().asMap());
        return aggregate;
    }

    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleValidation(ConstraintViolationException e) {
        return Map.of("error", "Invalid input: id must be 1-64 alphanumeric characters");
    }

    @ExceptionHandler(ModelIndexOutOfRangeException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleModelIndexOutOfRange(ModelIndexOutOfRangeException e) {
        return Map.of("error", e.getMessage());
    }

    @ExceptionHandler(ProfileNotFoundException.class)
    public ResponseEntity<Map<String, String>> handleProfileNotFound(ProfileNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
            .body(Map.of("error", "profile_not_found", "user_id", e.userId));
    }

    private Map<String, Object> predictionPayload(ModelPrediction prediction) {
        Map<String, Object> payload = new java.util.LinkedHashMap<>();
        payload.put("model", prediction.model());
        if (prediction.user() != null) {
            payload.put("user", prediction.user());
        }
        if (prediction.item() != null) {
            payload.put("item", prediction.item());
        }
        payload.put("userId", prediction.userId());
        payload.put("itemId", prediction.itemId());
        payload.put("score", prediction.score());
        return payload;
    }

    private static boolean isTimeout(Throwable error) {
        for (Throwable current = error; current != null && current.getCause() != current; current = current.getCause()) {
            if (current instanceof TimeoutException
                || current instanceof SocketTimeoutException
                || current instanceof SQLTimeoutException
                || current instanceof QueryTimeoutException) {
                return true;
            }
        }
        return false;
    }

    private static final class ProfileNotFoundException extends RuntimeException {
        private final String userId;

        private ProfileNotFoundException(String userId) {
            this.userId = userId;
        }
    }
}
