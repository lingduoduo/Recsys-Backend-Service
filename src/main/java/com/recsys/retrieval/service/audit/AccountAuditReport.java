package com.recsys.retrieval.service.audit;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/** Response of GET /actuator/profile-audit/{user}: one account's row, always present. */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"status", "active_run", "generated_at", "elapsed_ms", "catalog_size", "user"})
public record AccountAuditReport(
    String status,
    @JsonProperty("active_run") String activeRun,
    @JsonProperty("generated_at") String generatedAt,
    @JsonProperty("elapsed_ms") long elapsedMs,
    @JsonProperty("catalog_size") int catalogSize,
    ProfileAuditReport.UserRow user
) {
}
