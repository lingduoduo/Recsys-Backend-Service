package com.recsys.application.gateway;
import com.recsys.config.EnvVars;
import com.recsys.ratelimit.TokenBucket;
import com.recsys.ratelimit.LlmTokenRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;
import com.recsys.infrastructure.cache.LlmResponseCache;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpObject;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpResponseWriter;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.common.util.TimeoutMode;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.ServiceRequestContext;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Armeria-based LLM reverse proxy service using Armeria
 * {@link WebClient} for upstream calls and {@link HttpResponseWriter} for SSE streaming.
 *
 * <p>Proxy responsibilities:
 * <ul>
 *   <li><b>Streaming passthrough</b> — detects {@code "stream":true} in the request body and
 *       pipes the SSE/chunked upstream response directly to the client via a reactive subscription.</li>
 *   <li><b>Retry-on-429</b> — when the LLM service returns 429, respects the {@code Retry-After}
 *       header and retries once (buffered mode only; streaming is surfaced immediately).</li>
 *   <li><b>Token-based rate limiting</b> — pre-checks the token budget using {@code max_tokens}
 *       from the request body before forwarding, rejecting with 429 when the budget is exhausted.</li>
 *   <li><b>Response caching</b> — caches non-streaming 200 responses by SHA-256 of the request
 *       body; cache hits set {@code X-Cache: HIT} and skip the upstream entirely.</li>
 *   <li><b>Circuit breaker</b> — shared with the gateway health endpoint; opens on repeated
 *       upstream 5xx / timeouts and fast-fails with 503 during the cooldown window.</li>
 * </ul>
 */
public final class LlmProxyService implements HttpService {

    public static final int DEFAULT_TIMEOUT_MS = 120_000;
    public static final int DEFAULT_MAX_RETRY_WAIT_MS = 30_000;
    public static final int DEFAULT_TOKEN_ESTIMATE = 1_000;
    /**
     * Idle threshold and scheduler period for SSE comments. A write just after a tick can defer
     * the next comment for nearly two periods. Keep margin below the scripted CloudFront origin
     * read timeout (30 s), which is tighter than the ALB's 60 s idle timeout.
     * Non-positive values disable the heartbeat.
     */
    public static final long DEFAULT_SSE_KEEPALIVE_MS = 10_000;
    public static final long MAX_SSE_KEEPALIVE_MS = 10_000;

    private static final int SC_TOO_MANY_REQUESTS = 429;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Set<String> HOP_BY_HOP = Set.of(
            "connection", "content-length", "expect", "host", "keep-alive",
            "proxy-authenticate", "proxy-authorization", "te", "trailer",
            "transfer-encoding", "upgrade");

    // Credentials the gateway consumes at its auth boundary — never forwarded upstream.
    private static final Set<String> GATEWAY_CONSUMED_CREDENTIALS =
            Set.of("authorization", "x-api-key", GatewayOriginSecret.HEADER);

    private final MicroserviceRoute route;
    private final WebClient webClient;
    private final RouteCircuitBreaker circuitBreaker;
    private final LlmTokenRateLimiter tokenRateLimiter;
    private final LlmResponseCache responseCache;
    private final int defaultTokenEstimate;
    private final long maxRetryWaitMs;
    private final GatewayAuthenticator authenticator;
    private final Duration timeout;
    private final long sseKeepaliveMs;

    public LlmProxyService(MicroserviceRoute route,
                    Duration timeout,
                    RouteCircuitBreaker circuitBreaker,
                    LlmTokenRateLimiter tokenRateLimiter,
                    LlmResponseCache responseCache,
                    int defaultTokenEstimate,
                    long maxRetryWaitMs) {
        this(route, timeout, circuitBreaker, tokenRateLimiter, responseCache,
                defaultTokenEstimate, maxRetryWaitMs, GatewayAuthenticator.disabled(),
                ClientFactory.ofDefault());
    }

    public LlmProxyService(MicroserviceRoute route,
                    Duration timeout,
                    RouteCircuitBreaker circuitBreaker,
                    LlmTokenRateLimiter tokenRateLimiter,
                    LlmResponseCache responseCache,
                    int defaultTokenEstimate,
                    long maxRetryWaitMs,
                    GatewayAuthenticator authenticator) {
        this(route, timeout, circuitBreaker, tokenRateLimiter, responseCache,
                defaultTokenEstimate, maxRetryWaitMs, authenticator, ClientFactory.ofDefault());
    }

    public LlmProxyService(MicroserviceRoute route,
                    Duration timeout,
                    RouteCircuitBreaker circuitBreaker,
                    LlmTokenRateLimiter tokenRateLimiter,
                    LlmResponseCache responseCache,
                    int defaultTokenEstimate,
                    long maxRetryWaitMs,
                    GatewayAuthenticator authenticator,
                    ClientFactory clientFactory) {
        this(route, timeout, circuitBreaker, tokenRateLimiter, responseCache, defaultTokenEstimate,
                maxRetryWaitMs, authenticator, clientFactory,
                EnvVars.readLong("LLM_SSE_KEEPALIVE_MS", DEFAULT_SSE_KEEPALIVE_MS));
    }

    public LlmProxyService(MicroserviceRoute route,
                    Duration timeout,
                    RouteCircuitBreaker circuitBreaker,
                    LlmTokenRateLimiter tokenRateLimiter,
                    LlmResponseCache responseCache,
                    int defaultTokenEstimate,
                    long maxRetryWaitMs,
                    GatewayAuthenticator authenticator,
                    ClientFactory clientFactory,
                    long sseKeepaliveMs) {
        if (sseKeepaliveMs > MAX_SSE_KEEPALIVE_MS) {
            throw new IllegalArgumentException("LLM_SSE_KEEPALIVE_MS must be <= "
                    + MAX_SSE_KEEPALIVE_MS + " to leave margin below CloudFront's 30 s origin read "
                    + "timeout; non-positive values disable keepalives (received " + sseKeepaliveMs + ").");
        }
        // This class is a second forwarding path: it duplicates the credential stripping and
        // identity injection of GatewayRequestForwarder, but it never consults BackendRoutePolicy
        // for the request path at all — no user-scope check, no operator-token check, nothing.
        // That is sound only while no LLM route can reach a backend BackendRoutePolicy knows
        // about, regardless of which access class (USER_SCOPED, OPERATOR, or even NO_PROXY) that
        // backend happens to declare — so the premise is enforced rather than commented: an LLM
        // route pointed at any known backend fails at construction instead of forwarding
        // unchecked. Whoever hits this should add the check here, not delete the guard.
        //
        // Resolved by target, not by label. MicroserviceRoute.fromEnvOptional — the only thing
        // that builds an LLM route in production — always passes serviceName = null, so a guard on
        // the name alone could never fire, and LLM_SERVICE_URL pointed at 8080 would forward
        // /api/llm/api/v1/recommend with no check at all.
        String targetService = BackendRoutePolicy.effectiveServiceName(route, MicroserviceRoute.defaults());
        if (targetService != null) {
            throw new IllegalArgumentException(
                    "LlmProxyService does not consult BackendRoutePolicy for the request path, so "
                            + "it may not target a service that has one. Route \"" + route.name()
                            + "\" targets \"" + targetService + "\", which is a known backend in "
                            + "BackendRoutePolicy. Route it through "
                            + "GatewayProxyService/GatewayRequestForwarder, or implement the check "
                            + "here first (see 20_AuthN_AuthZ §10).");
        }
        this.route = route;
        this.circuitBreaker = circuitBreaker;
        this.tokenRateLimiter = tokenRateLimiter;
        this.responseCache = responseCache;
        this.defaultTokenEstimate = defaultTokenEstimate;
        this.maxRetryWaitMs = maxRetryWaitMs;
        this.authenticator = authenticator == null ? GatewayAuthenticator.disabled() : authenticator;
        this.timeout = timeout;
        this.sseKeepaliveMs = sseKeepaliveMs;

        this.webClient = WebClient.builder(route.baseUri().toString())
                .factory(clientFactory == null ? ClientFactory.ofDefault() : clientFactory)
                .responseTimeoutMillis(timeout.toMillis())
                .build();
    }

    @Override
    public HttpResponse serve(ServiceRequestContext ctx, HttpRequest req) {
        // Normalize before authorization, and before route.rewrite below — the LLM route's prefix
        // is the version-free "/api/llm", so a versioned path would fail its matchesPrefix check.
        ApiVersion apiVersion = ApiVersion.parse(ctx.path());
        if (!apiVersion.supported()) {
            return GatewayProxyService.gatewayError(
                    HttpStatus.BAD_REQUEST, apiVersion.unsupportedMessage());
        }
        String path = apiVersion.path();

        // Armeria's *server* request timeout covers response completion, not time-to-first-byte,
        // and the gateway builds its server with a bare Server.builder().http(port) — so without
        // this line the untouched 10 s default, not the configured LLM_TIMEOUT_MS, is the real
        // ceiling on every LLM call: a token stream is reset mid-generation after the client
        // already has 200 + text/event-stream (a *silent* truncation an EventSource retries,
        // re-paying for the whole prompt), and a buffered call returns Armeria's bare 503.
        // Bind it to the same budget the upstream client uses rather than clearing it outright,
        // so a stuck request still has a backstop instead of pinning a connection forever.
        ctx.setRequestTimeout(TimeoutMode.SET_FROM_NOW, timeout);

        GatewayAuthResult auth = authenticator.check(req.headers(), path);
        if (auth.rejected()) return auth.rejection();
        GatewayPrincipal principal = auth.principal();

        // Aggregate the request body once so we can inspect (stream flag, token count)
        // and forward it. Must happen before any further dispatching.
        return HttpResponse.of(
                req.aggregate().thenCompose(aggReq -> {
                    byte[] requestBody;
                    try {
                        requestBody = shouldForwardBody(req.method().name(), aggReq.content().length())
                                ? aggReq.content().toInputStream().readAllBytes()
                                : null;
                    } catch (java.io.IOException e) {
                        throw new RuntimeException(e);
                    }

                    BodyMeta meta = requestBody != null
                            ? parseBodyMeta(requestBody, defaultTokenEstimate)
                            : new BodyMeta(false, defaultTokenEstimate);
                    boolean streaming = meta.streaming();

                    // Cache check (non-streaming only)
                    if (!streaming && requestBody != null) {
                        LlmResponseCache.Entry cached = responseCache.get(requestBody);
                        if (cached != null) {
                            return CompletableFuture.completedFuture(buildCachedResponse(cached));
                        }
                    }

                    // Token rate limit pre-check
                    TokenBucket.Decision tokenDecision = tokenRateLimiter.tryAcquire(meta.maxTokens());
                    if (!tokenDecision.allowed()) {
                        int retryAfterSec = Math.max(1,
                                (int) Math.ceil(tokenDecision.retryAfter().toMillis() / 1000.0));
                        return CompletableFuture.completedFuture(
                                HttpResponse.of(
                                        ResponseHeaders.builder(HttpStatus.valueOf(SC_TOO_MANY_REQUESTS))
                                                .contentType(MediaType.JSON_UTF_8)
                                                .set(HttpHeaderNames.RETRY_AFTER, String.valueOf(retryAfterSec))
                                                .set(HttpHeaderNames.of("x-ratelimit-limit"),
                                                        String.valueOf(tokenDecision.limit()))
                                                .set(HttpHeaderNames.of("x-ratelimit-remaining"),
                                                        String.valueOf(tokenDecision.remaining()))
                                                .build(),
                                        HttpData.ofUtf8("{\"error\":\"" + route.name()
                                                + " token budget exhausted — retry after " + retryAfterSec + "s\"}")));
                    }

                    // Circuit breaker
                    RouteCircuitBreaker.Permit circuitPermit = circuitBreaker.tryAcquirePermit();
                    if (circuitPermit == null) {
                        return CompletableFuture.completedFuture(
                                GatewayProxyService.gatewayError(HttpStatus.SERVICE_UNAVAILABLE,
                                        route.name() + " circuit open — LLM service unavailable, retry later"));
                    }

                    URI target = route.rewrite(path, ctx.query());
                    String targetPath = target.getRawPath()
                            + (target.getRawQuery() != null ? "?" + target.getRawQuery() : "");

                    RequestHeaders upstreamHeaders = buildUpstreamHeaders(
                            aggReq.headers(), targetPath, ctx, principal);
                    HttpRequest upstreamReq = HttpRequest.of(upstreamHeaders, aggReq.content());

                    if (streaming) {
                        // Return the streaming response directly without wrapping in a future.
                        // We complete the CompletableFuture immediately with the streaming writer.
                        return CompletableFuture.completedFuture(
                                forwardStreaming(ctx, webClient.execute(upstreamReq), circuitPermit,
                                        meta.maxTokens()));
                    } else {
                        return CompletableFuture.completedFuture(
                                forwardBuffered(upstreamReq, requestBody, circuitPermit,
                                        meta.maxTokens()));
                    }
                }));
    }

    // ── Streaming path ──────────────────────────────────────────────────────────────────────────

    private HttpResponse forwardStreaming(
            ServiceRequestContext ctx,
            HttpResponse upstream,
            RouteCircuitBreaker.Permit circuitPermit,
            int declaredTokens) {
        HttpResponseWriter writer = HttpResponse.streaming();
        upstream.subscribe(new org.reactivestreams.Subscriber<HttpObject>() {
            private final AtomicBoolean breakerCompleted = new AtomicBoolean();
            private final AtomicBoolean budgetSettled = new AtomicBoolean();
            private final LlmTokenUsageScanner usageScanner = new LlmTokenUsageScanner(MAPPER);
            private volatile HttpStatus responseStatus;
            /** Non-null only while a keepalive heartbeat is running for this response. */
            private volatile java.util.concurrent.ScheduledFuture<?> keepalive;
            private volatile long lastWriteNanos = System.nanoTime();
            /** The last two bytes forwarded — an SSE frame ends on "\n\n". */
            private volatile char lastByte;
            private volatile char secondLastByte;

            @Override
            public void onSubscribe(org.reactivestreams.Subscription s) {
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(HttpObject obj) {
                if (obj instanceof ResponseHeaders h) {
                    responseStatus = h.status();
                    if (h.status().isServerError()) {
                        recordFailureOnce();
                    }
                    // Strip hop-by-hop headers and forward downstream
                    ResponseHeaders filtered = filterResponseHeaders(h);
                    writer.write(filtered);
                    startKeepaliveIfSse(h);
                } else if (obj instanceof HttpData d) {
                    // Scanned, then forwarded unchanged — the client's copy is never delayed by
                    // this and the stream is never buffered.
                    usageScanner.accept(d.array());
                    trackFrameBoundary(d.array());
                    lastWriteNanos = System.nanoTime();
                    writer.write(d);
                }
            }

            @Override
            public void onError(Throwable t) {
                stopKeepalive();
                recordFailureOnce();
                // A stream that died mid-generation still consumed whatever it had produced.
                settleBudgetOnce();
                writer.close(t);
            }

            @Override
            public void onComplete() {
                stopKeepalive();
                if (responseStatus != null && !responseStatus.isServerError()) {
                    recordSuccessOnce();
                } else {
                    recordFailureOnce();
                }
                // Settle before closing the writer, so the charge is applied before the client can
                // observe the end of the stream and issue its next request.
                usageScanner.finish();
                settleBudgetOnce();
                writer.close();
            }

            /**
             * SSE comment frames keep a hop in front of the gateway from treating a stream that is
             * merely thinking as a dead connection. Armeria holds such a stream open itself, but
             * CloudFront's configured 30 s origin read timeout and the ALB's 60 s idle timeout
             * can close a quiet stream before LLM_TIMEOUT_MS expires.
             *
             * <p>Started only for {@code text/event-stream}: the passthrough forwards whatever the
             * upstream sends, and a comment line injected into native Ollama NDJSON would hand the
             * client a line that is not JSON.
             */
            private void startKeepaliveIfSse(ResponseHeaders headers) {
                if (sseKeepaliveMs <= 0) return;
                MediaType contentType = headers.contentType();
                if (contentType == null || !contentType.is(MediaType.EVENT_STREAM)) return;
                keepalive = ctx.eventLoop().scheduleAtFixedRate(
                        this::writeKeepaliveIfIdle, sseKeepaliveMs, sseKeepaliveMs,
                        TimeUnit.MILLISECONDS);
            }

            private void writeKeepaliveIfIdle() {
                long idleMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastWriteNanos);
                if (idleMs < sseKeepaliveMs) return;
                // Only ever between frames. A chunk boundary is a network artifact, not a frame
                // boundary, so writing while a frame is half-delivered would splice a comment
                // through the middle of it.
                if (!(secondLastByte == '\n' && lastByte == '\n')) return;
                if (!writer.tryWrite(HttpData.ofUtf8(": keep-alive\n\n"))) {
                    stopKeepalive();
                }
            }

            private void trackFrameBoundary(byte[] chunk) {
                if (chunk.length >= 2) {
                    secondLastByte = (char) chunk[chunk.length - 2];
                    lastByte = (char) chunk[chunk.length - 1];
                } else if (chunk.length == 1) {
                    secondLastByte = lastByte;
                    lastByte = (char) chunk[0];
                }
            }

            private void stopKeepalive() {
                java.util.concurrent.ScheduledFuture<?> k = keepalive;
                if (k != null) {
                    k.cancel(false);
                    keepalive = null;
                }
            }

            private void settleBudgetOnce() {
                if (!budgetSettled.compareAndSet(false, true)) return;
                usageScanner.completionTokens().ifPresent(
                        actual -> tokenRateLimiter.reconcile(declaredTokens, actual));
            }

            private void recordSuccessOnce() {
                if (breakerCompleted.compareAndSet(false, true)) {
                    circuitBreaker.recordSuccess(circuitPermit);
                }
            }

            private void recordFailureOnce() {
                if (breakerCompleted.compareAndSet(false, true)) {
                    circuitBreaker.recordFailure(circuitPermit);
                }
            }
        });
        return writer;
    }

    // ── Buffered path ───────────────────────────────────────────────────────────────────────────

    private HttpResponse forwardBuffered(
            HttpRequest upstreamReq,
            byte[] requestBody,
            RouteCircuitBreaker.Permit circuitPermit,
            int declaredTokens) {
        RequestHeaders upstreamHeaders = upstreamReq.headers();
        return HttpResponse.of(
                webClient.execute(upstreamReq).aggregate()
                        .thenCompose(agg -> {
                            if (agg.status().code() == SC_TOO_MANY_REQUESTS) {
                                long waitMs = parseRetryAfterMs(agg.headers());
                                if (waitMs > 0 && waitMs <= maxRetryWaitMs) {
                                    // Non-blocking delay: schedule retry on the common pool
                                    // after waitMs without blocking the Netty event loop.
                                    // Rebuild a fresh HttpRequest — the original publisher is
                                    // single-subscription and already consumed.
                                    HttpRequest retryReq = HttpRequest.of(
                                            upstreamHeaders,
                                            HttpData.wrap(requestBody != null ? requestBody : new byte[0]));
                                    return CompletableFuture.supplyAsync(
                                            () -> null,
                                            CompletableFuture.delayedExecutor(waitMs, TimeUnit.MILLISECONDS))
                                            .thenCompose(ignored ->
                                                    webClient.execute(retryReq).aggregate());
                                }
                            }
                            return CompletableFuture.completedFuture(agg);
                        })
                        .thenApply(agg -> {
                            if (agg.status().isServerError()) {
                                circuitBreaker.recordFailure(circuitPermit);
                            } else {
                                circuitBreaker.recordSuccess(circuitPermit);
                            }

                            byte[] responseBytes;
                            try {
                                responseBytes = agg.content().toInputStream().readAllBytes();
                            } catch (java.io.IOException e) {
                                throw new RuntimeException(e);
                            }

                            // Settle the pre-checked estimate against what the upstream says the
                            // completion actually cost, before the response is handed back.
                            settleTokenBudget(declaredTokens, responseBytes);

                            // Cache successful 200 responses
                            if (agg.status().code() == 200 && requestBody != null) {
                                responseCache.put(requestBody, agg.status().code(),
                                        headersToMap(agg.headers()), responseBytes);
                            }

                            ResponseHeaders withCacheMiss = filterResponseHeaders(agg.headers())
                                    .toBuilder()
                                    .set(HttpHeaderNames.of("x-cache"), "MISS")
                                    .build();
                            return HttpResponse.of(withCacheMiss, HttpData.wrap(responseBytes));
                        })
                        .exceptionally(t -> {
                            circuitBreaker.recordFailure(circuitPermit);
                            return GatewayProxyService.gatewayError(HttpStatus.BAD_GATEWAY,
                                    "LLM upstream unreachable");
                        }));
    }

    // ── Helpers ─────────────────────────────────────────────────────────────────────────────────

    /**
     * Charges the real completion-token count against the budget, refunding or debiting the
     * difference from the caller's declared {@code max_tokens}. A no-op when the upstream reported
     * no usage — see {@link LlmTokenUsageScanner}.
     */
    private void settleTokenBudget(int declaredTokens, byte[] responseBody) {
        LlmTokenUsageScanner scanner = new LlmTokenUsageScanner(MAPPER);
        scanner.accept(responseBody);
        scanner.finish();
        scanner.completionTokens().ifPresent(
                actual -> tokenRateLimiter.reconcile(declaredTokens, actual));
    }

    static RequestHeaders buildUpstreamHeaders(RequestHeaders incoming, String targetPath,
                                                       ServiceRequestContext ctx, GatewayPrincipal principal) {
        RequestHeadersBuilder b = RequestHeaders.builder(incoming.method(), targetPath);
        incoming.forEach((name, value) -> {
            String n = name.toString();
            // Strip any client-supplied identity header — the gateway is the sole authority.
            if (!isHopByHop(n) && !isGatewayCredential(n)
                    && !n.regionMatches(true, 0, "x-authenticated-", 0, 16)) {
                b.add(name, value);
            }
        });
        principal.identityHeaders().forEach((hn, hv) -> b.set(HttpHeaderNames.of(hn), hv));
        b.set(HttpHeaderNames.of("x-gateway-service"), "recsys-llm-gateway");
        String host = incoming.get(HttpHeaderNames.HOST);
        if (host != null && !host.isBlank()) {
            b.set(HttpHeaderNames.of("x-forwarded-host"), host);
        }
        b.set(HttpHeaderNames.of("x-forwarded-proto"), "http");
        if (ctx.remoteAddress() != null) {
            String clientIp = ctx.remoteAddress().getAddress().getHostAddress();
            String existing = incoming.get(HttpHeaderNames.of("x-forwarded-for"));
            String newValue = (existing != null && !existing.isBlank())
                    ? existing + ", " + clientIp : clientIp;
            b.set(HttpHeaderNames.of("x-forwarded-for"), newValue);
        }
        return b.build();
    }

    private static HttpResponse buildCachedResponse(LlmResponseCache.Entry entry) {
        var headersBuilder = ResponseHeaders.builder(HttpStatus.valueOf(entry.status()));
        entry.headers().forEach((name, values) -> {
            if (!isHopByHop(name)) {
                values.forEach(v -> headersBuilder.add(HttpHeaderNames.of(name), v));
            }
        });
        headersBuilder.set(HttpHeaderNames.of("x-cache"), "HIT");
        return HttpResponse.of(headersBuilder.build(), HttpData.wrap(entry.body()));
    }

    private static ResponseHeaders filterResponseHeaders(ResponseHeaders headers) {
        var b = ResponseHeaders.builder(headers.status());
        headers.forEach((name, value) -> {
            if (!isHopByHop(name.toString())) b.add(name, value);
        });
        return b.build();
    }

    /**
     * Converts Armeria {@link ResponseHeaders} to the {@code Map<String, List<String>>} format
     * expected by {@link LlmResponseCache#put}.
     */
    private static Map<String, List<String>> headersToMap(ResponseHeaders headers) {
        return headers.names().stream()
                .filter(n -> !isHopByHop(n.toString()))
                .collect(Collectors.toUnmodifiableMap(
                        Object::toString,
                        n -> headers.getAll(n)));
    }

    private static boolean shouldForwardBody(String method, int contentLength) {
        if ("GET".equalsIgnoreCase(method) || "HEAD".equalsIgnoreCase(method)
                || "DELETE".equalsIgnoreCase(method)) {
            return false;
        }
        return contentLength > 0;
    }

    private static boolean isHopByHop(String name) {
        return name != null && HOP_BY_HOP.contains(name.toLowerCase(Locale.ROOT));
    }

    private static boolean isGatewayCredential(String name) {
        return name != null && GATEWAY_CONSUMED_CREDENTIALS.contains(name.toLowerCase(Locale.ROOT));
    }

    /**
     * Parses {@code Retry-After} (integer seconds) from upstream 429 response headers.
     * Returns 0 if absent or unparseable.
     */
    private static long parseRetryAfterMs(ResponseHeaders headers) {
        String header = headers.get(HttpHeaderNames.RETRY_AFTER);
        if (header == null || header.isBlank()) return 0L;
        try {
            long seconds = Long.parseLong(header.trim());
            return seconds > 0 ? seconds * 1000L : 0L;
        } catch (NumberFormatException ignored) {
            return 0L;
        }
    }

    public record BodyMeta(boolean streaming, int maxTokens) {}

    public static BodyMeta parseBodyMeta(byte[] body, int defaultTokenEstimate) {
        if (body == null || body.length == 0) return new BodyMeta(false, defaultTokenEstimate);
        try {
            JsonNode root = MAPPER.readTree(body);
            JsonNode streamNode = root.get("stream");
            boolean streaming = streamNode != null && streamNode.isBoolean() && streamNode.booleanValue();
            JsonNode maxTokensNode = root.get("max_tokens");
            int maxTokens = (maxTokensNode != null && maxTokensNode.isInt())
                    ? Math.max(1, maxTokensNode.intValue()) : defaultTokenEstimate;
            return new BodyMeta(streaming, maxTokens);
        } catch (Exception ignored) {
            return new BodyMeta(false, defaultTokenEstimate);
        }
    }
}
