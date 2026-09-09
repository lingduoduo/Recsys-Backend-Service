package com.recsys.application.gateway;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpResponseWriter;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import com.recsys.infrastructure.cache.LlmResponseCache;
import com.recsys.ratelimit.LlmTokenRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The LLM token budget is pre-checked against the client-declared {@code max_tokens}, which the
 * client controls and the gateway cannot verify up front. Without a settle-up once the real token
 * count is known, a caller that declares {@code max_tokens: 1} and then consumes hundreds pays for
 * one — the budget stops bounding anything a caller cares to under-declare.
 *
 * <p>Both upstream dialects the deployed {@code LLM_SERVICE_URL} (Ollama) can speak report the real
 * count when they finish: OpenAI-compatible SSE ends with a {@code usage} object, native Ollama
 * NDJSON with {@code eval_count}.
 */
class LlmTokenBudgetReconciliationTest {

    private static final int BURST = 100;
    private static final int ACTUAL_COMPLETION_TOKENS = 500;

    @RegisterExtension
    static final ServerExtension upstream = LlmProxyTestServers.upstream(sb -> {
            // OpenAI-compatible SSE: content frames, then a terminal usage frame, then [DONE].
            sb.service("/openai", (ctx, req) -> {
                HttpResponseWriter w = HttpResponse.streaming();
                w.write(ResponseHeaders.of(HttpStatus.OK,
                        HttpHeaderNames.CONTENT_TYPE, "text/event-stream"));
                w.write(HttpData.ofUtf8(
                        "data: {\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n"));
                w.write(HttpData.ofUtf8(
                        "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":10,"
                                + "\"completion_tokens\":" + ACTUAL_COMPLETION_TOKENS + ","
                                + "\"total_tokens\":" + (ACTUAL_COMPLETION_TOKENS + 10) + "}}\n\n"));
                w.write(HttpData.ofUtf8("data: [DONE]\n\n"));
                w.close();
                return w;
            });
            // Native Ollama NDJSON: the final line carries eval_count.
            sb.service("/ollama", (ctx, req) -> {
                HttpResponseWriter w = HttpResponse.streaming();
                w.write(ResponseHeaders.of(HttpStatus.OK,
                        HttpHeaderNames.CONTENT_TYPE, "application/x-ndjson"));
                w.write(HttpData.ofUtf8("{\"response\":\"hello\",\"done\":false}\n"));
                w.write(HttpData.ofUtf8("{\"done\":true,\"prompt_eval_count\":10,"
                        + "\"eval_count\":" + ACTUAL_COMPLETION_TOKENS + "}\n"));
                w.close();
                return w;
            });
            // Buffered completion, usage in the aggregated body.
            sb.service("/buffered", (ctx, req) -> HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8,
                    "{\"choices\":[{\"text\":\"hello\"}],\"usage\":{\"prompt_tokens\":10,"
                            + "\"completion_tokens\":" + ACTUAL_COMPLETION_TOKENS + "}}"));
    });

    /** A budget big enough for the declared cost, far too small for the real one. */
    private static LlmTokenRateLimiter smallBudget() {
        return LlmTokenRateLimiter.fromEnvironment(
                Map.of("LLM_TOKEN_RATE_LIMIT_TPS", "1",
                       "LLM_TOKEN_RATE_LIMIT_BURST", String.valueOf(BURST))::get,
                System::nanoTime);
    }

    private static ServerExtension gateway(LlmTokenRateLimiter limiter) {
        return LlmProxyTestServers.gateway(upstream::httpUri,
                route -> new LlmProxyService(
                        route, Duration.ofSeconds(30), new RouteCircuitBreaker(),
                        limiter, LlmResponseCache.disabled(), 1_000, 1_000L));
    }

    private static AggregatedHttpResponse post(ServerExtension gw, String path, String body)
            throws Exception {
        return WebClient.of(gw.httpUri())
                .execute(HttpRequest.of(
                        RequestHeaders.builder(HttpMethod.POST, path)
                                .contentType(MediaType.JSON_UTF_8).build(),
                        HttpData.ofUtf8(body)))
                .aggregate().get(30, TimeUnit.SECONDS);
    }

    /**
     * Exactly one follow-up request — no retry loop. Each request debits the budget, so polling
     * would drain it by itself and "pass" against unreconciled code. This is sound because the
     * settle-up happens-before the client can observe the end of the response: on the streaming
     * path it runs in the upstream subscriber's {@code onComplete} before {@code writer.close()},
     * and on the buffered path before the aggregated response is returned.
     */
    private static void assertBudgetExhaustedFor(ServerExtension gw, String path) throws Exception {
        assertThat(post(gw, path, "{\"stream\":true,\"max_tokens\":50}").status())
                .as("the under-declared request's real token cost (%d) is charged to the budget, "
                        + "so a %d-token burst is already spent", ACTUAL_COMPLETION_TOKENS, BURST)
                .isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    void anUnderDeclaredStreamIsChargedItsRealTokenCount() throws Exception {
        ServerExtension gw = gateway(smallBudget());
        gw.start();
        try {
            // Declares 1 token, actually produces 500 — 5x the whole burst.
            assertThat(post(gw, "/api/llm/openai", "{\"stream\":true,\"max_tokens\":1}").status())
                    .isEqualTo(HttpStatus.OK);

            assertBudgetExhaustedFor(gw, "/api/llm/openai");
        } finally {
            gw.stop();
        }
    }

    @Test
    void anUnderDeclaredOllamaStreamIsChargedItsRealTokenCount() throws Exception {
        ServerExtension gw = gateway(smallBudget());
        gw.start();
        try {
            assertThat(post(gw, "/api/llm/ollama", "{\"stream\":true,\"max_tokens\":1}").status())
                    .isEqualTo(HttpStatus.OK);

            assertBudgetExhaustedFor(gw, "/api/llm/ollama");
        } finally {
            gw.stop();
        }
    }

    @Test
    void anUnderDeclaredBufferedCompletionIsChargedItsRealTokenCount() throws Exception {
        ServerExtension gw = gateway(smallBudget());
        gw.start();
        try {
            assertThat(post(gw, "/api/llm/buffered", "{\"max_tokens\":1}").status())
                    .isEqualTo(HttpStatus.OK);

            assertBudgetExhaustedFor(gw, "/api/llm/buffered");
        } finally {
            gw.stop();
        }
    }
}
