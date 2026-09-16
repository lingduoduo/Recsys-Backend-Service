package com.demo.retrieval.model;

import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.PositiveOrZero;

public record FeedbackRequest(
    @NotBlank String user,
    @NotBlank String item,
    boolean clicked,
    @DecimalMin("0.0") @DecimalMax("1.0") double reward,
    @Pattern(regexp = "[a-zA-Z0-9_:-]{1,128}") String requestId,
    @DecimalMin("0.0") @DecimalMax("5.0") Double rating,
    @Pattern(regexp = "[a-zA-Z0-9_-]{1,64}") String negativeFeedbackReason,
    @PositiveOrZero Long dwellMillis,
    @DecimalMin("0.0") @DecimalMax("1.0") Double completionRate,
    Boolean ordered
) {
    public FeedbackRequest(String user, String item, boolean clicked, double reward) {
        this(user, item, clicked, reward, null, null, null, null, null, null);
    }
}
