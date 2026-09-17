package com.recsys.api.rest.retrieval;

import com.recsys.application.auth.AdminTokenGuard;
import com.recsys.retrieval.service.DeepLearningPredictionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Swaps the live ONNX session for every caller of this JVM.
 *
 * <p>Guarded twice, deliberately. The gateway classifies this route {@code OPERATOR} in
 * {@code BackendRoutePolicy}, which demands {@code X-Admin-Token} before forwarding — but that
 * only binds callers who come through the gateway. Anything with in-cluster reach to 8080 skips
 * it entirely. 7010's operator surfaces ({@code POST /shards/topology}, {@code GET /online/ops})
 * have always been covered on both sides for exactly this reason; this route now matches that
 * precedent, reusing the same {@link AdminTokenGuard} and the same {@code SHARD_ADMIN_TOKEN}.
 *
 * <p>Fail closed, like the guard itself: with no token configured the check authorizes nobody,
 * so an unprovisioned deployment returns 403 rather than leaving the reload open.
 */
@RestController
public class ModelReloadController {
    private static final Logger log = LoggerFactory.getLogger(ModelReloadController.class);

    private final DeepLearningPredictionService predictionService;
    private final AdminTokenGuard adminGuard;

    @Autowired
    public ModelReloadController(DeepLearningPredictionService predictionService) {
        this(predictionService, new AdminTokenGuard(System.getenv("SHARD_ADMIN_TOKEN")));
    }

    ModelReloadController(DeepLearningPredictionService predictionService, AdminTokenGuard adminGuard) {
        this.predictionService = predictionService;
        this.adminGuard = adminGuard;
    }

    @PostMapping("/api/v1/retrieval/model/reload")
    public ResponseEntity<Map<String, String>> reload(
        @RequestHeader(value = AdminTokenGuard.HEADER, required = false) String adminToken
    ) {
        if (!adminGuard.isAuthorized(adminToken)) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body(Map.of("status", "error", "message", "operator token required"));
        }
        try {
            predictionService.reload();
            log.info("ONNX model reloaded successfully");
            return ResponseEntity.ok(Map.of("status", "ok"));
        } catch (Exception e) {
            log.error("ONNX model reload failed", e);
            // String.valueOf, not e.getMessage(): Map.of rejects a null value, and many ONNX/IO
            // failures carry no message — building the body would then NPE inside the catch and
            // the operator would get an opaque 500 instead of the diagnostic. Matches
            // ProfileAuditController. The status is 5xx, not 200, so a script can tell.
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("status", "error", "message", String.valueOf(e.getMessage())));
        }
    }
}
