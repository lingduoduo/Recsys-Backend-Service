package com.recsys.api.rest.retrieval;

import com.recsys.application.auth.LoginTokenService;
import com.recsys.config.RequestScopeData;
import com.recsys.retrieval.service.audit.AccountAuditReport;
import com.recsys.retrieval.service.audit.ProfileAuditReport;
import com.recsys.retrieval.service.audit.ProfileAuditReport.Finding;
import com.recsys.retrieval.service.audit.ProfileAuditReport.Summary;
import com.recsys.retrieval.service.audit.ProfileAuditReport.UserRow;
import com.recsys.retrieval.service.audit.ProfileAuditService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(ProfileAuditController.class)
class ProfileAuditControllerTest {

    @Autowired
    MockMvc mvc;

    @MockBean
    ProfileAuditService auditService;

    @MockBean
    LoginTokenService loginTokenService;

    @MockBean
    RequestScopeData requestScopeData;

    private static ProfileAuditReport report() {
        return new ProfileAuditReport("ok", "run-7", "2026-09-07T10:00:00Z", 812L, false,
            new Summary(3, 2, 1, Map.of("missing_profile", 1), Map.of("new_or_unknown", 1), Map.of(), 3400L, 12),
            List.of(
                new UserRow("u1", false, null, List.of(new Finding("no_profile", "missing_profile", null)), null),
                new UserRow("u2", true, 3400L, List.of(new Finding("new_or_unknown", null, null)), List.of())));
    }

    @Test
    void returnsTheReportWithSnakeCaseFieldsAndNoNulls() throws Exception {
        when(auditService.audit(isNull())).thenReturn(report());

        mvc.perform(get("/actuator/profile-audit"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"))
            .andExpect(jsonPath("$.active_run").value("run-7"))
            .andExpect(jsonPath("$.summary.users_scanned").value(3))
            .andExpect(jsonPath("$.summary.no_profile_by_reason.missing_profile").value(1))
            .andExpect(jsonPath("$.summary.min_profile_ttl_seconds").value(3400))
            .andExpect(jsonPath("$.users[0].user_id").value("u1"))
            .andExpect(jsonPath("$.users[0].has_profile").value(false))
            .andExpect(jsonPath("$.users[0].ttl_seconds").doesNotExist())
            .andExpect(jsonPath("$.users[0].findings[0].reason").value("missing_profile"))
            .andExpect(jsonPath("$.users[1].findings[0].reason").doesNotExist())
            .andExpect(jsonPath("$.users[1].ttl_seconds").value(3400));
    }

    @Test
    void serializesFieldsInDocumentedOrder() throws Exception {
        when(auditService.audit(isNull())).thenReturn(report());

        String body = mvc.perform(get("/actuator/profile-audit"))
            .andExpect(status().isOk())
            .andReturn().getResponse().getContentAsString();

        assertTrue(body.startsWith("{\"status\":\"ok\",\"active_run\":\"run-7\",\"generated_at\":"));
    }

    @Test
    void passesLimitThrough() throws Exception {
        when(auditService.audit(25)).thenReturn(report());

        mvc.perform(get("/actuator/profile-audit").param("limit", "25"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"));
    }

    @Test
    void badLimitIs400() throws Exception {
        when(auditService.audit(any())).thenThrow(new IllegalArgumentException("limit must be between 1 and 10000"));

        mvc.perform(get("/actuator/profile-audit").param("limit", "0"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").value("limit must be between 1 and 10000"));
    }

    @Test
    void busyIs409() throws Exception {
        when(auditService.audit(any())).thenThrow(new ProfileAuditService.AuditBusyException());

        mvc.perform(get("/actuator/profile-audit"))
            .andExpect(status().isConflict())
            .andExpect(jsonPath("$.status").value("busy"));
    }

    @Test
    void storeFailureIs503() throws Exception {
        when(auditService.audit(any())).thenThrow(
            new ProfileAuditService.ProfileAuditFailedException(new IllegalStateException("redis unavailable")));

        mvc.perform(get("/actuator/profile-audit"))
            .andExpect(status().isServiceUnavailable())
            .andExpect(jsonPath("$.status").value("error"))
            .andExpect(jsonPath("$.message").value("redis unavailable"));
    }

    private static AccountAuditReport accountReport() {
        return new AccountAuditReport("ok", "run-7", "2026-09-07T10:00:00Z", 2L, 12,
            new UserRow("u2", true, 3400L, List.of(), List.of()));
    }

    @Test
    void returnsOneAccountRow() throws Exception {
        when(auditService.auditAccount("u2")).thenReturn(accountReport());

        mvc.perform(get("/actuator/profile-audit/u2"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("ok"))
            .andExpect(jsonPath("$.active_run").value("run-7"))
            .andExpect(jsonPath("$.catalog_size").value(12))
            .andExpect(jsonPath("$.user.user_id").value("u2"))
            .andExpect(jsonPath("$.user.ttl_seconds").value(3400))
            .andExpect(jsonPath("$.user.findings").isEmpty());
    }

    @Test
    void anInvalidAccountIdIs400() throws Exception {
        mvc.perform(get("/actuator/profile-audit/bad id!"))
            .andExpect(status().isBadRequest())
            .andExpect(jsonPath("$.error").value("Invalid input: id must be 1-64 alphanumeric characters"));
    }

    @Test
    void accountStoreFailureIs503() throws Exception {
        when(auditService.auditAccount(anyString())).thenThrow(
            new ProfileAuditService.ProfileAuditFailedException(new IllegalStateException("redis unavailable")));

        mvc.perform(get("/actuator/profile-audit/u2"))
            .andExpect(status().isServiceUnavailable())
            .andExpect(jsonPath("$.status").value("error"))
            .andExpect(jsonPath("$.message").value("redis unavailable"));
    }
}
