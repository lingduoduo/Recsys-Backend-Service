package com.recsys.application.gateway;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpObject;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import com.recsys.infrastructure.cache.LlmResponseCache;
import com.recsys.ratelimit.LlmTokenRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An LLM token stream must outlive the Armeria <em>server</em> request timeout.
 *
 * <p>That timeout covers stream <em>completion</em>, not time-to-first-byte, and the gateway
 * builds its server with a bare {@code Server.builder().http(port)} — so the default applied to
 * {@code /api/llm/*}, not the tuned 120 s client-side {@code LLM_TIMEOUT_MS}, was the real ceiling
 * on every LLM call. A stream still running when it fired was killed with an RST_STREAM after the
 * client had already received {@code 200} and {@code text/event-stream}: a <em>silent</em>
 * truncation mid-generation, which a browser {@code EventSource} reads as a dropped connection and
 * retries, re-paying for the whole prompt.
 *
 * <p>The timeout here is scaled down (300 ms server timeout, a ~1.2 s stream) so the test runs in
 * about a second; the semantics under test are the production 10 s default's.
 */
class LlmProxyStreamTimeoutTest {

    private static final int FRAMES = 8;
    private static final long FRAME_GAP_MS = 150;      // ~1.2 s of streaming
    private static final long SERVER_TIMEOUT_MS = 300; // fires well before the stream ends
    private static final String BUFFERED_REPLY = "{\"choices\":[{\"text\":\"done\"}]}";

    private static final List<String> SSE_FRAMES = IntStream.range(0, FRAMES)
            .mapToObj(i -> "data: frame-" + i + "\n\n")
            .toList();

    /**
     * Emits {@link #FRAMES} SSE frames {@link #FRAME_GAP_MS} apart, like a slow token stream.
     * The upstream must never be the side that cuts the stream off — the helper sets its
     * request timeout to 0.
     */
    @RegisterExtension
    static final ServerExtension slowSseUpstream = LlmProxyTestServers.upstream(sb -> {
        // A slow *buffered* LLM reply: one whole response, well after the server timeout.
        sb.service("/slow", (ctx, req) -> {
            CompletableFuture<HttpResponse> reply = new CompletableFuture<>();
            LlmProxyTestServers.SLOW_UPSTREAM.schedule(
                    () -> reply.complete(HttpResponse.of(
                            HttpStatus.OK, MediaType.JSON_UTF_8, BUFFERED_REPLY)),
                    FRAMES * FRAME_GAP_MS, TimeUnit.MILLISECONDS);
            return HttpResponse.of(reply);
        });
        sb.service("prefix:/", (ctx, req) -> LlmProxyTestServers.slowStream(
                "text/event-stream", FRAME_GAP_MS, FRAME_GAP_MS, SSE_FRAMES));
    });

    /** Mirrors the production gateway: a server request timeout the LLM route must escape. */
    private static ServerExtension gateway() {
        return LlmProxyTestServers.gateway(slowSseUpstream::httpUri,
                sb -> sb.requestTimeoutMillis(SERVER_TIMEOUT_MS),
                route -> new LlmProxyService(
                        route, Duration.ofSeconds(60), new RouteCircuitBreaker(),
                        LlmTokenRateLimiter.disabled(), LlmResponseCache.disabled(),
                        1_000, 1_000L));
    }

    private record StreamOutcome(List<String> frames, String terminalSignal) {}

    private static StreamOutcome streamThrough(ServerExtension gateway, String body) throws Exception {
        List<String> frames = new CopyOnWriteArrayList<>();
        CompletableFuture<String> terminal = new CompletableFuture<>();
        WebClient.of(gateway.httpUri())
                .execute(HttpRequest.of(
                        RequestHeaders.builder(HttpMethod.POST, "/api/llm/v1/chat")
                                .contentType(MediaType.JSON_UTF_8).build(),
                        HttpData.ofUtf8(body)))
                .subscribe(new org.reactivestreams.Subscriber<HttpObject>() {
                    @Override
                    public void onSubscribe(org.reactivestreams.Subscription s) {
                        s.request(Long.MAX_VALUE);
                    }

                    @Override
                    public void onNext(HttpObject o) {
                        if (o instanceof HttpData data && !data.isEmpty()) {
                            frames.add(data.toStringUtf8().trim());
                        }
                    }

                    @Override
                    public void onError(Throwable t) {
                        terminal.complete(t.getClass().getSimpleName());
                    }

                    @Override
                    public void onComplete() {
                        terminal.complete("onComplete");
                    }
                });
        return new StreamOutcome(frames, terminal.get(30, TimeUnit.SECONDS));
    }

    /**
     * The same untuned server timeout capped the buffered path too, and the doc's original sharp
     * edge only suspected the streaming one. Buffered is the <em>default</em> path — {@code stream}
     * absent means buffered — so a slow non-streaming completion returned Armeria's built-in 503,
     * in plain text rather than the gateway's JSON error envelope, and indistinguishable from the
     * circuit breaker's own 503.
     */
    @Test
    void aBufferedCompletionOutlivesTheServerRequestTimeout() throws Exception {
        ServerExtension gateway = gateway();
        gateway.start();
        try {
            AggregatedHttpResponse response = WebClient.of(gateway.httpUri())
                    .execute(HttpRequest.of(
                            RequestHeaders.builder(HttpMethod.POST, "/api/llm/slow")
                                    .contentType(MediaType.JSON_UTF_8).build(),
                            HttpData.ofUtf8("{\"max_tokens\":10}")))
                    .aggregate().get(30, TimeUnit.SECONDS);

            assertThat(response.status())
                    .as("a completion slower than the %d ms server request timeout still succeeds",
                            SERVER_TIMEOUT_MS)
                    .isEqualTo(HttpStatus.OK);
            assertThat(response.contentUtf8()).isEqualTo(BUFFERED_REPLY);
        } finally {
            gateway.stop();
        }
    }

    @Test
    void aTokenStreamOutlivesTheServerRequestTimeout() throws Exception {
        ServerExtension gateway = gateway();
        gateway.start();
        try {
            StreamOutcome outcome = streamThrough(gateway, "{\"stream\":true,\"max_tokens\":10}");

            assertThat(outcome.frames())
                    .as("every upstream SSE frame reaches the client, "
                            + "though the stream outlives the %d ms server request timeout",
                            SERVER_TIMEOUT_MS)
                    .hasSize(FRAMES)
                    .last().isEqualTo("data: frame-" + (FRAMES - 1));
            assertThat(outcome.terminalSignal())
                    .as("the stream ends cleanly rather than being reset mid-generation")
                    .isEqualTo("onComplete");
        } finally {
            gateway.stop();
        }
    }
}
