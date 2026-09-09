package com.recsys.application.gateway;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpObject;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import com.recsys.infrastructure.cache.LlmResponseCache;
import com.recsys.ratelimit.LlmTokenRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A token stream can go quiet for a long time — a cold model load, a large prompt being processed.
 * Armeria itself holds such a stream open (an in-flight response is not idle, measured on both
 * h1c and h2c), but the ALB in front does not: no ingress sets
 * {@code idle_timeout.timeout_seconds}, so its 60 s default applies and it counts a silent
 * connection as idle — while {@code LLM_TIMEOUT_MS} is willing to wait 120 s. A periodic SSE
 * comment frame keeps that hop from cutting a stream that is merely thinking.
 *
 * <p>Comment frames are the SSE spec's own no-op: a line beginning {@code :} is ignored by
 * {@code EventSource}, so this is invisible to a conforming client.
 */
class LlmSseKeepaliveTest {

    private static final long KEEPALIVE_MS = 100;   // scaled down from the 15 s default
    private static final long QUIET_GAP_MS = 700;   // several keepalive intervals

    @RegisterExtension @Order(1)
    static final ServerExtension upstream = LlmProxyTestServers.upstream(sb -> {
        // Well-formed SSE that goes quiet mid-generation.
        sb.service("/sse", (ctx, req) -> quietStream("text/event-stream",
                "data: frame-0\n\n", "data: frame-1\n\n"));
        // Native Ollama NDJSON — NOT SSE. A comment frame here would corrupt the stream.
        // The first chunk ends on a *blank line* deliberately: that satisfies the
        // frame-boundary guard, so the content-type guard is the only thing left standing.
        // Ending it on a single "\n" would let the boundary check mask the bug instead.
        sb.service("/ndjson", (ctx, req) -> quietStream("application/x-ndjson",
                "{\"response\":\"a\",\"done\":false}\n\n", "{\"done\":true}\n"));
        // An SSE frame split so the quiet gap lands *inside* it, with no frame terminator yet.
        sb.service("/split", (ctx, req) -> quietStream("text/event-stream",
                "data: {\"partial\":", "\"tail\"}\n\n"));
    });

    /** The first chunk at once, the second after a quiet gap several keepalive intervals long. */
    private static HttpResponse quietStream(String contentType, String first, String second) {
        return LlmProxyTestServers.slowStream(contentType, 0, QUIET_GAP_MS, List.of(first, second));
    }

    // @Order(2): the gateway reads upstream's URI when it starts, so upstream must start first.
    @RegisterExtension @Order(2)
    static final ServerExtension gw = LlmProxyTestServers.gateway(upstream::httpUri,
            route -> new LlmProxyService(
                    route, Duration.ofSeconds(60), new RouteCircuitBreaker(),
                    LlmTokenRateLimiter.disabled(), LlmResponseCache.disabled(),
                    1_000, 1_000L, null, null, KEEPALIVE_MS));

    /** The exact bytes the client received, in order. */
    private static String streamBody(String path) throws Exception {
        StringBuilder body = new StringBuilder();
        ConcurrentLinkedQueue<String> chunks = new ConcurrentLinkedQueue<>();
        CompletableFuture<String> terminal = new CompletableFuture<>();
        WebClient.builder(gw.httpUri()).responseTimeoutMillis(0).build()
                .execute(HttpRequest.of(
                        RequestHeaders.builder(HttpMethod.POST, path)
                                .contentType(MediaType.JSON_UTF_8).build(),
                        HttpData.ofUtf8("{\"stream\":true,\"max_tokens\":10}")))
                .subscribe(new org.reactivestreams.Subscriber<HttpObject>() {
                    public void onSubscribe(org.reactivestreams.Subscription s) {
                        s.request(Long.MAX_VALUE);
                    }
                    public void onNext(HttpObject o) {
                        if (o instanceof HttpData d && !d.isEmpty()) chunks.add(d.toStringUtf8());
                    }
                    public void onError(Throwable t) { terminal.complete("err"); }
                    public void onComplete() { terminal.complete("ok"); }
                });
        terminal.get(30, TimeUnit.SECONDS);
        chunks.forEach(body::append);
        return body.toString();
    }

    @Test
    void aQuietSseStreamIsHeldOpenWithCommentFrames() throws Exception {
        String body = streamBody("/api/llm/sse");

        assertThat(body)
                .as("the gap between tokens is filled with SSE comment frames")
                .contains(": keep-alive\n\n");
        assertThat(body)
                .as("and the real frames still arrive, in order and unaltered")
                .contains("data: frame-0\n\n")
                .contains("data: frame-1\n\n");
        assertThat(body.indexOf("data: frame-0")).isLessThan(body.indexOf("data: frame-1"));
    }

    /**
     * The passthrough is content-type agnostic, so the heartbeat must not be. Injecting an SSE
     * comment into native Ollama NDJSON would hand the client a line that is not JSON.
     */
    @Test
    void aNonSseStreamIsNeverInjectedInto() throws Exception {
        String body = streamBody("/api/llm/ndjson");

        assertThat(body).doesNotContain("keep-alive");
        assertThat(body).isEqualTo("{\"response\":\"a\",\"done\":false}\n\n{\"done\":true}\n");
    }

    /**
     * A chunk boundary is a network artifact, not a frame boundary. Emitting a comment while a
     * frame is half-written splices it into the middle of that frame and corrupts it.
     */
    @Test
    void aKeepaliveIsNeverSplicedIntoAHalfWrittenFrame() throws Exception {
        String body = streamBody("/api/llm/split");

        assertThat(body)
                .as("the split frame is reassembled intact, with no comment spliced through it")
                .contains("data: {\"partial\":\"tail\"}\n\n");
    }
}
