package com.recsys.application.gateway;

import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpResponseWriter;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Fixtures shared by the {@code LlmProxyService} tests: the one route they all proxy, a
 * gateway {@link ServerExtension} around a caller-built proxy, an upstream that can never be
 * the side that cuts a stream off, and a slow streaming stub.
 *
 * <p>Each test still constructs its own {@link LlmProxyService} — that is where they differ
 * (timeouts, keepalive, token budget, client factory) — but the route, the
 * {@code serviceUnder}, and the "upstream must already be started when the gateway
 * configures" ordering live here once. Register a gateway with
 * {@code @RegisterExtension @Order(n)} <em>after</em> its upstream, or {@code start()} /
 * {@code stop()} it by hand for a per-test instance.
 */
final class LlmProxyTestServers {

    /** Where the proxy is mounted, and what every test's requests are prefixed with. */
    static final String LLM_PREFIX = "/api/llm";

    /**
     * One daemon scheduler for every slow upstream stub in the package, so the shared Surefire
     * fork accumulates one idle thread rather than one per test class.
     */
    static final ScheduledExecutorService SLOW_UPSTREAM = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "llm-proxy-test-upstream");
        t.setDaemon(true);
        return t;
    });

    private LlmProxyTestServers() {}

    /** The LLM route as production builds it from {@code LLM_SERVICE_URL}: no service name. */
    static MicroserviceRoute llmRoute(URI upstream) {
        return new MicroserviceRoute("llm", LLM_PREFIX, "LLM_SERVICE_URL", upstream, "/health", null);
    }

    /**
     * An upstream stub. Sets {@code requestTimeoutMillis(0)} before the caller's routes: a
     * slow stream must be cut by the gateway under test, never by the stub's own default
     * 10 s server timeout — the exact confusion {@code LlmProxyStreamTimeoutTest} exists to
     * guard against.
     */
    static ServerExtension upstream(Consumer<ServerBuilder> routes) {
        return new ServerExtension() {
            @Override
            protected void configure(ServerBuilder sb) {
                sb.requestTimeoutMillis(0);
                routes.accept(sb);
            }
        };
    }

    /**
     * A gateway serving {@code proxy} under {@link #LLM_PREFIX}. The upstream URI is a
     * supplier because it is read when the gateway <em>starts</em>, not when the field is
     * initialised — the upstream extension is not running yet at that point.
     */
    static ServerExtension gateway(Supplier<URI> upstream, Function<MicroserviceRoute, LlmProxyService> proxy) {
        return gateway(upstream, sb -> {}, proxy);
    }

    /** As {@link #gateway(Supplier, Function)}, with server-level configuration first. */
    static ServerExtension gateway(Supplier<URI> upstream,
                                   Consumer<ServerBuilder> server,
                                   Function<MicroserviceRoute, LlmProxyService> proxy) {
        return new ServerExtension() {
            @Override
            protected void configure(ServerBuilder sb) {
                server.accept(sb);
                sb.serviceUnder(LLM_PREFIX, proxy.apply(llmRoute(upstream.get())));
            }
        };
    }

    /**
     * A streaming response: the first chunk after {@code firstDelayMs}, each later chunk
     * {@code gapMs} after the previous, then close. Self-terminating — nothing is left
     * scheduled once the stream ends — and it stops quietly if the client has gone away
     * ({@code tryWrite} on an aborted writer returns false rather than throwing into the
     * scheduler).
     */
    static HttpResponse slowStream(String contentType, long firstDelayMs, long gapMs, List<String> chunks) {
        HttpResponseWriter w = HttpResponse.streaming();
        w.write(ResponseHeaders.of(HttpStatus.OK, HttpHeaderNames.CONTENT_TYPE, contentType));
        SLOW_UPSTREAM.schedule(() -> emit(w, chunks, 0, gapMs), firstDelayMs, TimeUnit.MILLISECONDS);
        return w;
    }

    private static void emit(HttpResponseWriter w, List<String> chunks, int i, long gapMs) {
        if (i == chunks.size()) {
            w.close();
            return;
        }
        if (!w.tryWrite(HttpData.ofUtf8(chunks.get(i)))) return;
        SLOW_UPSTREAM.schedule(() -> emit(w, chunks, i + 1, gapMs), gapMs, TimeUnit.MILLISECONDS);
    }
}
