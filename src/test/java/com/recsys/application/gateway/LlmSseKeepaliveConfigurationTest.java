package com.recsys.application.gateway;

import com.recsys.infrastructure.cache.LlmResponseCache;
import com.recsys.ratelimit.LlmTokenRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LlmSseKeepaliveConfigurationTest {

    @ParameterizedTest
    @ValueSource(longs = {10_001, 15_000, 30_000, Long.MAX_VALUE})
    void rejectsIntervalsAboveTheCloudFrontSafeCeiling(long intervalMs) {
        assertThatThrownBy(() -> proxy(intervalMs))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("LLM_SSE_KEEPALIVE_MS")
                .hasMessageContaining("10000");
    }

    @ParameterizedTest
    @ValueSource(longs = {Long.MIN_VALUE, -1, 0, 1, 100, 10_000})
    void acceptsDisabledAndSafeIntervals(long intervalMs) {
        assertThatCode(() -> proxy(intervalMs)).doesNotThrowAnyException();
    }

    @Test
    void defaultLeavesMarginForTwoSchedulerTicksBeforeTheOriginReadTimeout() {
        // An upstream write just after a tick can suppress the next heartbeat.
        // The CDN creation script configures a 30-second origin read timeout.
        assertThat(LlmProxyService.DEFAULT_SSE_KEEPALIVE_MS).isPositive();
        assertThat(2 * LlmProxyService.DEFAULT_SSE_KEEPALIVE_MS).isLessThan(30_000);
        assertThatCode(() -> proxy(LlmProxyService.DEFAULT_SSE_KEEPALIVE_MS))
                .doesNotThrowAnyException();
    }

    private static LlmProxyService proxy(long intervalMs) {
        return new LlmProxyService(
                LlmProxyTestServers.llmRoute(URI.create("http://localhost:11434")),
                Duration.ofSeconds(60), new RouteCircuitBreaker(),
                LlmTokenRateLimiter.disabled(), LlmResponseCache.disabled(),
                1_000, 1_000L, null, null, intervalMs);
    }
}
