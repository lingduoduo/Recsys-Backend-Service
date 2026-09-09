package com.recsys.application.gateway;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpObject;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpResponseWriter;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import com.recsys.infrastructure.cache.LlmResponseCache;
import com.recsys.ratelimit.LlmTokenRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The LLM proxy forwards a token stream on Armeria's event loops and holds no thread for
 * the life of the stream. That is what lets one gateway pod carry many simultaneous
 * generations: its concurrency ceiling is outstanding subscriptions, not a thread pool.
 * Measured on 2026-09-09 — 2000 concurrent streams completed on 18 JVM threads — and
 * pinned here so a future change that wraps {@code forwardStreaming} in a blocking task
 * fails loudly instead of quietly capping the proxy at the blocking executor's size.
 *
 * <p>Two signals, neither timing-based. The primary one is a mid-burst stack sample: no
 * thread other than an Armeria event loop may be inside {@code LlmProxyService} while streams
 * are in flight. It is order-independent, which matters — the secondary signal, JVM thread
 * growth during the burst, is not: a blocking-executor regression grows the JVM by ~N threads
 * on the first test method to run, but those pool threads outlive that method and become the
 * baseline of the next one, whose growth check then passes. Both are kept; the growth number
 * is the one a human reads. The wall-time ceiling is deliberately loose — it exists only to
 * catch full serialization (N × stream duration), not to measure latency.
 *
 * <p>Both an h2c and an h1c client leg are exercised because they hit different
 * server-side connection handling: one multiplexed connection versus one connection per
 * stream. The gateway→upstream leg is whatever Armeria negotiates (h2c here, as in
 * production against an Armeria peer).
 */
class LlmProxyStreamConcurrencyTest {

    private static final int N = 200;
    private static final int FRAMES = 5;
    private static final long FRAME_GAP_MS = 100;
    /** Measured growth was 7 (h2c) and 12 (h1c); a thread-per-stream regression is ~N. */
    private static final int MAX_THREAD_GROWTH = 32;
    /** Full serialization would take N × FRAMES × FRAME_GAP_MS = 100 s. */
    private static final long MAX_WALL_MS = 10_000;

    private static final ScheduledExecutorService SCHED =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "stream-concurrency-upstream");
                t.setDaemon(true);
                return t;
            });

    @RegisterExtension
    static final ServerExtension upstream = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            sb.requestTimeoutMillis(0);
            sb.service("/sse", (ctx, req) -> {
                HttpResponseWriter w = HttpResponse.streaming();
                w.write(ResponseHeaders.of(HttpStatus.OK,
                        HttpHeaderNames.CONTENT_TYPE, "text/event-stream"));
                AtomicInteger i = new AtomicInteger();
                SCHED.scheduleAtFixedRate(() -> {
                    int n = i.getAndIncrement();
                    if (n < FRAMES) {
                        w.write(HttpData.ofUtf8("data: frame-" + n + "\n\n"));
                    } else if (n == FRAMES) {
                        w.close();
                    }
                }, 0, FRAME_GAP_MS, TimeUnit.MILLISECONDS);
                return w;
            });
        }
    };

    // Built the way MicroserviceGatewayServer.buildLlmClientFactory builds the production one.
    private static final ClientFactory LLM_CLIENT_FACTORY = ClientFactory.builder()
            .connectTimeout(Duration.ofMillis(2_000))
            .idleTimeout(Duration.ofMillis(60_000))
            .pingIntervalMillis(20_000)
            .build();

    // Started lazily: JUnit does not guarantee static @RegisterExtension field order, and a
    // gateway extension that reads upstream.httpUri() during configure() fails with
    // "server did not start" whenever it happens to run first.
    private static ServerExtension gw;

    private static synchronized ServerExtension gw() {
        if (gw == null) {
            gw = new ServerExtension() {
                @Override
                protected void configure(ServerBuilder sb) {
                    MicroserviceRoute route = new MicroserviceRoute(
                            "llm", "/api/llm", "LLM_SERVICE_URL",
                            URI.create(upstream.httpUri().toString()), "/health", null);
                    // Keepalive disabled (last arg 0) so the frame count is exact.
                    sb.serviceUnder("/api/llm", new LlmProxyService(
                            route, Duration.ofSeconds(60), new RouteCircuitBreaker(),
                            LlmTokenRateLimiter.disabled(), LlmResponseCache.disabled(),
                            1_000, 1_000L, null, LLM_CLIENT_FACTORY, 0L));
                }
            };
            gw.start();
        }
        return gw;
    }

    @Test
    void concurrentStreamsOverOneMultiplexedConnectionHoldNoThreadPerStream() throws Exception {
        assertNoThreadPerStream(SessionProtocol.H2C);
    }

    @Test
    void concurrentStreamsOverOneConnectionEachHoldNoThreadPerStream() throws Exception {
        assertNoThreadPerStream(SessionProtocol.H1C);
    }

    private static void assertNoThreadPerStream(SessionProtocol clientLeg) throws Exception {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        WebClient client = WebClient.builder(gw().uri(clientLeg))
                .responseTimeoutMillis(0)
                .build();

        threads.resetPeakThreadCount();
        int before = threads.getThreadCount();
        AtomicInteger frames = new AtomicInteger();
        List<CompletableFuture<String>> outcomes = new ArrayList<>(N);
        long start = System.nanoTime();

        for (int k = 0; k < N; k++) {
            CompletableFuture<String> outcome = new CompletableFuture<>();
            outcomes.add(outcome);
            client.execute(HttpRequest.of(
                            RequestHeaders.builder(HttpMethod.POST, "/api/llm/sse")
                                    .contentType(MediaType.JSON_UTF_8).build(),
                            HttpData.ofUtf8("{\"stream\":true,\"max_tokens\":10}")))
                    .subscribe(new org.reactivestreams.Subscriber<HttpObject>() {
                        @Override
                        public void onSubscribe(org.reactivestreams.Subscription s) {
                            s.request(Long.MAX_VALUE);
                        }

                        @Override
                        public void onNext(HttpObject o) {
                            if (o instanceof HttpData d && !d.isEmpty()) {
                                frames.addAndGet(countFrames(d.toStringUtf8()));
                            }
                        }

                        @Override
                        public void onError(Throwable t) {
                            outcome.complete("error: " + t);
                        }

                        @Override
                        public void onComplete() {
                            outcome.complete("ok");
                        }
                    });
        }

        // Sample while the burst is in flight: which threads are inside LlmProxyService right now?
        CompletableFuture<Void> all = CompletableFuture.allOf(outcomes.toArray(new CompletableFuture[0]));
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(MAX_WALL_MS * 2);
        int maxForeignThreadsInsideProxy = 0;
        while (!all.isDone() && System.nanoTime() < deadline) {
            maxForeignThreadsInsideProxy = Math.max(
                    maxForeignThreadsInsideProxy, nonEventLoopThreadsInsideProxy());
            Thread.sleep(25);
        }
        all.get(1, TimeUnit.SECONDS);
        long wallMs = (System.nanoTime() - start) / 1_000_000;
        int growth = threads.getPeakThreadCount() - before;

        List<String> failures = outcomes.stream()
                .map(CompletableFuture::join)
                .filter(s -> !"ok".equals(s))
                .distinct()
                .limit(3)
                .toList();

        assertThat(failures)
                .as("[%s] every one of %d concurrent streams completes", clientLeg, N)
                .isEmpty();
        assertThat(frames.get())
                .as("[%s] every frame of every stream is delivered", clientLeg)
                .isEqualTo(N * FRAMES);
        assertThat(maxForeignThreadsInsideProxy)
                .as("[%s] threads other than Armeria event loops found inside LlmProxyService "
                        + "while %d streams were in flight — the proxy must run only on event "
                        + "loops", clientLeg, N)
                .isZero();
        assertThat(growth)
                .as("[%s] JVM thread growth while %d streams were in flight — a thread held "
                        + "per stream would grow this by ~%d", clientLeg, N, N)
                .isLessThanOrEqualTo(MAX_THREAD_GROWTH);
        assertThat(wallMs)
                .as("[%s] streams ran concurrently, not serially (serial would be ~%d ms)",
                        clientLeg, (long) N * FRAMES * FRAME_GAP_MS)
                .isLessThan(MAX_WALL_MS);
    }

    /**
     * Threads other than Armeria's event loops that are executing {@code LlmProxyService} code
     * at this instant. On conforming code this is always 0: every proxy frame runs on an
     * {@code armeria-common-worker-*} thread. A blocking-executor regression parks ~N
     * {@code armeria-common-blocking-tasks-*} threads inside the proxy for the life of each
     * stream, so a 25 ms sampling loop cannot miss it.
     */
    private static int nonEventLoopThreadsInsideProxy() {
        int n = 0;
        for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
            if (e.getKey().getName().startsWith("armeria-common-worker")) continue;
            for (StackTraceElement frame : e.getValue()) {
                if (frame.getClassName().startsWith(LlmProxyService.class.getName())) {
                    n++;
                    break;
                }
            }
        }
        return n;
    }

    /** SSE frames end with a blank line; a chunk may carry several or a fraction of one. */
    private static int countFrames(String chunk) {
        int count = 0;
        int i = 0;
        while ((i = chunk.indexOf("\n\n", i)) >= 0) {
            count++;
            i += 2;
        }
        return count;
    }
}
