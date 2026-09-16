package com.recsys.retrieval.controller;

import com.recsys.retrieval.service.audit.ProfileAuditService;
import com.recsys.retrieval.service.audit.ProfileAuditService.AuditBusyException;
import com.recsys.retrieval.service.audit.ProfileAuditService.ProfileAuditFailedException;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.constraints.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Operator tool, like /actuator/model-reload: walks user → has_profile → preferences → catalog
 * content and returns a findings-only report. Records no serving measurement.
 */
@RestController
@Validated
public class ProfileAuditController {
    private static final Logger log = LoggerFactory.getLogger(ProfileAuditController.class);

    private final ProfileAuditService auditService;

    public ProfileAuditController(ProfileAuditService auditService) {
        this.auditService = auditService;
    }

    @GetMapping("/actuator/profile-audit")
    public ResponseEntity<?> audit(@RequestParam(required = false) Integer limit) {
        try {
            return ResponseEntity.ok(auditService.audit(limit));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        } catch (AuditBusyException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("status", "busy"));
        } catch (ProfileAuditFailedException e) {
            log.error("Profile audit failed", e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("status", "error", "message", String.valueOf(e.getMessage())));
        }
    }

    @GetMapping("/actuator/profile-audit/{user}")
    public ResponseEntity<?> auditAccount(
        @PathVariable @Pattern(regexp = "[a-zA-Z0-9_:-]{1,64}") String user
    ) {
        try {
            return ResponseEntity.ok(auditService.auditAccount(user));
        } catch (ProfileAuditFailedException e) {
            log.error("Profile audit failed for account {}", user, e);
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("status", "error", "message", String.valueOf(e.getMessage())));
        }
    }

    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public Map<String, String> handleValidation(ConstraintViolationException e) {
        return Map.of("error", "Invalid input: id must be 1-64 alphanumeric characters");
    }
}
