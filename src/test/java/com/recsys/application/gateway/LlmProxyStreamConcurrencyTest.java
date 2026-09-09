package com.recsys.application.gateway;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.CommonPools;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import com.recsys.api.gateway.MicroserviceGatewayServer;
import com.recsys.infrastructure.cache.LlmResponseCache;
import com.recsys.ratelimit.LlmTokenRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;
import io.netty.util.concurrent.EventExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The LLM proxy forwards a token stream on Armeria's event loops and holds no thread for
 * the life of the stream. That is what lets one gateway pod carry many simultaneous
 * generations: its concurrency ceiling is outstanding subscriptions, not a thread pool.
 * Measured on 2026-09-09 — 2000 concurrent streams completed on 18 JVM threads — and
 * pinned here so a future change that wraps {@code forwardStreaming} in a blocking task
 * fails loudly instead of quietly capping the proxy at the blocking executor's size.
 *
 * <p>Two signals, neither timing-based, both taken from periodic stack samples while the
 * burst is in flight, and both ignoring Armeria's common event loops (classified by identity
 * against {@link CommonPools#workerGroup()}, which both the gateway server and the production
 * LLM client factory use — not by thread name). The primary signal: no other thread may be
 * <em>held</em> inside gateway production code, where held means seen there in two
 * consecutive samples. A thread-per-stream regression parks a thread for the whole ≥500 ms
 * life of each stream; a conforming short hop to a bounded pool lasts microseconds and is
 * never seen twice in a row, so this cannot flake in either direction. The secondary signal:
 * the number of non-event-loop threads may not grow by more than N/2 during the burst. The
 * primary is order-independent; the secondary is not — a regression grows the pool by ~N on
 * the first test method to run, but those threads outlive it and become the next method's
 * baseline. Both are kept; the growth number is the one a human reads. The wall-time ceiling
 * is deliberately loose — it exists only to catch full serialization (N × stream duration).
 *
 * <p>Both an h2c and an h1c client leg are exercised because they hit different
 * server-side connection handling: one multiplexed connection versus one connection per
 * stream. The gateway→upstream leg is HTTP/1.1 in both, because that is the production
 * shape: Ollama (Go {@code net/http}) serves HTTP/1.1 only — measured 2026-09-09 on 0.21.2,
 * where an h2c prior-knowledge connect is refused and an upgrade attempt stays on 1.1 — so
 * the gateway opens one upstream connection per concurrent stream. N equals Armeria's
 * common blocking executor size (200) on purpose: a thread-per-stream regression then also
 * shows up as the production cap, every pool thread parked at once.
 *
 * <p>Not pinned here, deliberately: blocking <em>on</em> an event loop (a synchronous call
 * inside the subscriber stalls the worker that carries streams, but holds no extra thread),
 * and virtual threads (invisible to {@code Thread.getAllStackTraces()}; unreachable on the
 * JDK 17 build, but a JDK bump would make this test vacuous for them, not red).
 */
class LlmProxyStreamConcurrencyTest {

    private static final int N = 200;
    private static final int FRAMES = 5;
    private static final long FRAME_GAP_MS = 100;
    private static final long SAMPLE_INTERVAL_MS = 50;
    /** Non-event-loop thread growth: ~0 on conforming code, ~N under a regression. */
    private static final int MAX_THREAD_GROWTH = N / 2;
    /** Full serialization would take N × FRAMES × FRAME_GAP_MS = 100 s. */
    private static final long MAX_WALL_MS = 10_000;
    private static final List<String> SSE_FRAMES = IntStream.range(0, FRAMES)
            .mapToObj(n -> "data: frame-" + n + "\n\n")
            .toList();
    private static final String EXPECTED_BODY = String.join("", SSE_FRAMES);
    private static final String GATEWAY_PACKAGE = LlmProxyService.class.getPackageName() + ".";
    private static final String TEST_SERVERS = LlmProxyTestServers.class.getName();

    @RegisterExtension @Order(1)
    static final ServerExtension upstream = LlmProxyTestServers.upstream(sb ->
            sb.service("/sse", (ctx, req) -> LlmProxyTestServers.slowStream(
                    "text/event-stream", 0, FRAME_GAP_MS, SSE_FRAMES)));

    /** The factory production uses, at its defaults — not a hand-copied approximation. */
    private static final ClientFactory LLM_CLIENT_FACTORY =
            MicroserviceGatewayServer.buildLlmClientFactory(k -> null);

    // @Order(2): the gateway reads upstream's URI when it starts, so upstream must start first.
    // HTTP/1.1 upstream leg: the production shape (see class Javadoc). Keepalive disabled
    // (last arg 0) so the body is exactly the upstream frames.
    @RegisterExtension @Order(2)
    static final ServerExtension gw = LlmProxyTestServers.gateway(
            () -> upstream.uri(SessionProtocol.H1C),
            route -> new LlmProxyService(
                    route, Duration.ofSeconds(60), new RouteCircuitBreaker(),
                    LlmTokenRateLimiter.disabled(), LlmResponseCache.disabled(),
                    1_000, 1_000L, null, LLM_CLIENT_FACTORY, 0L));

    @AfterAll
    static void closeClientFactory() {
        LLM_CLIENT_FACTORY.close();
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
        WebClient client = WebClient.builder(gw.uri(clientLeg))
                .responseTimeoutMillis(0)
                .build();

        int threadsBefore = sample().nonEventLoopThreads();
        CountDownLatch done = new CountDownLatch(N);
        Queue<String> bodies = new ConcurrentLinkedQueue<>();
        Queue<String> errors = new ConcurrentLinkedQueue<>();
        long start = System.nanoTime();

        for (int k = 0; k < N; k++) {
            client.execute(HttpRequest.of(
                            RequestHeaders.builder(HttpMethod.POST, "/api/llm/sse")
                                    .contentType(MediaType.JSON_UTF_8).build(),
                            HttpData.ofUtf8("{\"stream\":true,\"max_tokens\":10}")))
                    .aggregate()
                    .whenComplete((res, t) -> {
                        if (t != null) {
                            errors.add(String.valueOf(t));
                        } else if (res.status() != HttpStatus.OK) {
                            errors.add("status " + res.status());
                        } else {
                            bodies.add(res.contentUtf8());
                        }
                        done.countDown();
                    });
        }

        // Sample while the burst is in flight. One deadline (MAX_WALL_MS), and every assertion
        // below still runs if it is hit, so a hang reports the sampled counts, not a bare timeout.
        int maxHeldInsideGateway = 0;
        int maxThreads = threadsBefore;
        Set<Thread> insidePreviously = Set.of();
        while (done.getCount() > 0 && elapsedMs(start) < MAX_WALL_MS) {
            Sample s = sample();
            Set<Thread> held = new HashSet<>(s.insideGateway());
            held.retainAll(insidePreviously);
            maxHeldInsideGateway = Math.max(maxHeldInsideGateway, held.size());
            maxThreads = Math.max(maxThreads, s.nonEventLoopThreads());
            insidePreviously = s.insideGateway();
            Thread.sleep(SAMPLE_INTERVAL_MS);
        }
        long wallMs = elapsedMs(start);
        int growth = maxThreads - threadsBefore;

        assertThat(errors.stream().distinct().limit(3).toList())
                .as("[%s] no stream failed", clientLeg)
                .isEmpty();
        assertThat(done.getCount())
                .as("[%s] streams still open after %d ms", clientLeg, wallMs)
                .isZero();
        assertThat(bodies)
                .as("[%s] every stream delivered every frame, in order", clientLeg)
                .hasSize(N)
                .containsOnly(EXPECTED_BODY);
        assertThat(maxHeldInsideGateway)
                .as("[%s] threads other than Armeria event loops held inside gateway code "
                        + "across consecutive %d ms samples while %d streams were in flight — "
                        + "the proxy must hold no thread per stream", clientLeg,
                        SAMPLE_INTERVAL_MS, N)
                .isZero();
        assertThat(growth)
                .as("[%s] growth in non-event-loop threads while %d streams were in flight — a "
                        + "thread held per stream would grow this by ~%d", clientLeg, N, N)
                .isLessThanOrEqualTo(MAX_THREAD_GROWTH);
        assertThat(wallMs)
                .as("[%s] streams ran concurrently, not serially (serial would be ~%d ms)",
                        clientLeg, (long) N * FRAMES * FRAME_GAP_MS)
                .isLessThan(MAX_WALL_MS);
    }

    private static long elapsedMs(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }

    /**
     * One stack sample over every thread that is not a common event loop:
     * {@code nonEventLoopThreads} is how many there are, {@code insideGateway} which of them
     * are executing {@code com.recsys.application.gateway} production code right now. On
     * conforming code the latter is always empty: every proxy frame runs on an event loop.
     */
    private record Sample(int nonEventLoopThreads, Set<Thread> insideGateway) {}

    private static Sample sample() {
        int threads = 0;
        Set<Thread> inside = new HashSet<>();
        for (Map.Entry<Thread, StackTraceElement[]> e : Thread.getAllStackTraces().entrySet()) {
            Thread thread = e.getKey();
            if (isCommonEventLoop(thread)) continue;
            threads++;
            for (StackTraceElement frame : e.getValue()) {
                if (isProductionGatewayClass(frame.getClassName())) {
                    inside.add(thread);
                    break;
                }
            }
        }
        return new Sample(threads, inside);
    }

    /** By identity, not by name: survives an Armeria rename and never mistakes a look-alike. */
    private static boolean isCommonEventLoop(Thread thread) {
        for (EventExecutor loop : CommonPools.workerGroup()) {
            if (loop.inEventLoop(thread)) return true;
        }
        return false;
    }

    /**
     * Gateway-package production code only. Sibling test classes and the shared
     * {@link LlmProxyTestServers} fixtures live in the same package and the same Surefire fork
     * (this class's sampling thread and the upstream emitter among them), and a task of theirs
     * must not read as "the proxy is on a foreign thread".
     */
    private static boolean isProductionGatewayClass(String className) {
        if (!className.startsWith(GATEWAY_PACKAGE)) return false;
        int nested = className.indexOf('$');
        String topLevel = nested < 0 ? className : className.substring(0, nested);
        return !topLevel.endsWith("Test") && !topLevel.equals(TEST_SERVERS);
    }
}
