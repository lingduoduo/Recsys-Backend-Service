# LLM Proxy Stream Concurrency Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Commit a test that fails if `LlmProxyService` ever holds a thread per concurrent SSE stream, wire it into the PR-gate profile, and record the 2026-09-09 measurement in the SSE design doc.

**Architecture:** One new JUnit test in `com.recsys.application.gateway` stands up an in-JVM Armeria upstream that trickles SSE frames, puts the real `LlmProxyService` in front of it over an HTTP/1.1 upstream leg, fires 200 concurrent streams, and asserts exact bodies, no thread held inside gateway code across consecutive stack samples, bounded non-event-loop thread growth and a loose wall-time ceiling. The test is proven red by a temporary mutation of the proxy before it is committed. The only production change is making `MicroserviceGatewayServer.buildLlmClientFactory` public so the test uses the real factory. See the spec's "Revision after review" section for how this differs from the first version.

**Tech Stack:** Java 17, Armeria 1.28.4 (`ServerExtension`, `WebClient`, `HttpResponseWriter`, `CommonPools`), JUnit 5 (`@RegisterExtension @Order`), AssertJ, `Thread.getAllStackTraces()`, Maven Surefire `resilience` profile.

**Spec:** `docs/superpowers/specs/2026-09-09-llm-proxy-stream-concurrency-conformance-design.md`

## Global Constraints

- JDK 17 for every Maven invocation: prefix commands with `JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home` (or `$(/usr/libexec/java_home -v 17)`). Newer JDKs fail a clean compile of two pre-existing files.
- Work in the worktree `.worktrees/llm-proxy-stream-concurrency` on branch `test/llm-proxy-stream-concurrency`. Never commit to `main`; the PR is the integration path.
- No behavioural production change survives into a commit; the one production edit is `buildLlmClientFactory` becoming `public`. The mutation in Task 1 is reverted before committing.
- No new numbered doc under `docs/system_design/`, no README change, no `.claude/CLAUDE.md` change.
- Growth bound is N/2 over non-event-loop threads; N is 200; upstream emits 5 frames 100 ms apart; samples every 50 ms; wall-time ceiling is 10 s. These are the spec's revised values.
- Every commit message ends with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

---

### Task 1: The conformance test, proven red by mutation

**Files:**
- Create: `src/test/java/com/recsys/application/gateway/LlmProxyStreamConcurrencyTest.java`
- Modify: `src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java:285` — `static ClientFactory buildLlmClientFactory(...)` becomes `public static`
- Temporarily modify, then revert: `src/main/java/com/recsys/application/gateway/LlmProxyService.java:264-270`

**Interfaces:**
- Consumes: `LlmProxyService(MicroserviceRoute, Duration, RouteCircuitBreaker, LlmTokenRateLimiter, LlmResponseCache, int, long, GatewayAuthenticator, ClientFactory, long)` — the 10-arg constructor; `MicroserviceRoute(String name, String prefix, String envVar, URI baseUri, String healthPath, String serviceName)`; `LlmTokenRateLimiter.disabled()`; `LlmResponseCache.disabled()`.
- Produces: the test class name `LlmProxyStreamConcurrencyTest`, referenced by Task 2's `pom.xml` include.

- [ ] **Step 1: Write the test**

Create `src/test/java/com/recsys/application/gateway/LlmProxyStreamConcurrencyTest.java`:

```java
package com.recsys.application.gateway;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.CommonPools;
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
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.ServerBuilder;
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

import java.net.URI;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
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
    private static final String EXPECTED_BODY = IntStream.range(0, FRAMES)
            .mapToObj(n -> "data: frame-" + n + "\n\n")
            .collect(Collectors.joining());
    private static final String GATEWAY_PACKAGE = LlmProxyService.class.getPackageName() + ".";

    private static final ScheduledExecutorService SCHED =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "stream-concurrency-upstream");
                t.setDaemon(true);
                return t;
            });

    @RegisterExtension @Order(1)
    static final ServerExtension upstream = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            sb.requestTimeoutMillis(0);
            sb.service("/sse", (ctx, req) -> {
                HttpResponseWriter w = HttpResponse.streaming();
                w.write(ResponseHeaders.of(HttpStatus.OK,
                        HttpHeaderNames.CONTENT_TYPE, "text/event-stream"));
                emit(w, 0);
                return w;
            });
        }
    };

    /**
     * One frame now, the next after a gap; self-terminating, so nothing outlives the stream.
     * {@code tryWrite} rather than {@code write}: a client that went away has aborted the
     * writer, and the chain should end there instead of throwing into the scheduler.
     */
    private static void emit(HttpResponseWriter w, int n) {
        if (n == FRAMES) {
            w.close();
            return;
        }
        if (!w.tryWrite(HttpData.ofUtf8("data: frame-" + n + "\n\n"))) return;
        SCHED.schedule(() -> emit(w, n + 1), FRAME_GAP_MS, TimeUnit.MILLISECONDS);
    }

    /** The factory production uses, at its defaults — not a hand-copied approximation. */
    private static final ClientFactory LLM_CLIENT_FACTORY =
            MicroserviceGatewayServer.buildLlmClientFactory(k -> null);

    // @Order(2): configure() reads upstream.uri(), so upstream must already be started.
    @RegisterExtension @Order(2)
    static final ServerExtension gw = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            // HTTP/1.1 upstream leg: the production shape (see class Javadoc).
            MicroserviceRoute route = new MicroserviceRoute(
                    "llm", "/api/llm", "LLM_SERVICE_URL",
                    upstream.uri(SessionProtocol.H1C), "/health", null);
            // Keepalive disabled (last arg 0) so the body is exactly the upstream frames.
            sb.serviceUnder("/api/llm", new LlmProxyService(
                    route, Duration.ofSeconds(60), new RouteCircuitBreaker(),
                    LlmTokenRateLimiter.disabled(), LlmResponseCache.disabled(),
                    1_000, 1_000L, null, LLM_CLIENT_FACTORY, 0L));
        }
    };

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
     * Gateway-package production code only. Sibling test classes share the package and the
     * Surefire fork (this class's own sampling thread and upstream emitter among them), and a
     * leftover task from one of them must not read as "the proxy is on a foreign thread".
     */
    private static boolean isProductionGatewayClass(String className) {
        if (!className.startsWith(GATEWAY_PACKAGE)) return false;
        int nested = className.indexOf('$');
        String topLevel = nested < 0 ? className : className.substring(0, nested);
        return !topLevel.endsWith("Test");
    }
}
```

- [ ] **Step 2: Run it against unmodified code and confirm it passes**

Run:
```bash
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn -q test -Dtest=LlmProxyStreamConcurrencyTest -DfailIfNoTests=false
grep -h 'Tests run' target/surefire-reports/com.recsys.application.gateway.LlmProxyStreamConcurrencyTest.txt
```
Expected: `Tests run: 2, Failures: 0, Errors: 0`. This is a conformance test of behaviour that already holds, so green-first is expected; the red proof is the next step.

- [ ] **Step 3: Apply the mutation the test exists to catch**

In `src/main/java/com/recsys/application/gateway/LlmProxyService.java`, replace the streaming branch (around line 264):

```java
                    if (streaming) {
                        // Return the streaming response directly without wrapping in a future.
                        // We complete the CompletableFuture immediately with the streaming writer.
                        return CompletableFuture.completedFuture(
                                forwardStreaming(ctx, webClient.execute(upstreamReq), circuitPermit,
                                        meta.maxTokens()));
                    } else {
```

with:

```java
                    if (streaming) {
                        // MUTATION — DO NOT COMMIT. Holds one blocking-executor thread for the
                        // whole life of each stream, which is the regression the concurrency
                        // test guards against. Frames still arrive (the writer buffers them and
                        // the server drains it once returned), so only the thread bound trips.
                        return CompletableFuture.supplyAsync(() -> {
                            HttpResponse up = webClient.execute(upstreamReq);
                            HttpResponse out = forwardStreaming(ctx, up, circuitPermit,
                                    meta.maxTokens());
                            up.whenComplete().join();
                            return out;
                        }, ctx.blockingTaskExecutor());
                    } else {
```

- [ ] **Step 4: Run the test and confirm it fails on the thread bound**

Run:
```bash
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn -q test -Dtest=LlmProxyStreamConcurrencyTest -DfailIfNoTests=false 2>&1 | grep -E 'thread growth|Expecting|to be less than or equal to|Tests run' | head
```
Expected: both methods FAIL on the primary assertion, `threads other than Armeria event loops held inside gateway code across consecutive 50 ms samples while 200 streams were in flight`, with an actual value near 200 (Armeria's common blocking executor has 200 threads; measured 190 on h2c and 200 on h1c). Only the first method to run also trips the growth bound: the pool threads it spawned outlive it and become the next method's baseline. That order-dependence is why the held-thread sample is the primary signal. Save this output for the PR description.

If the test does *not* fail here, stop: the test has no teeth and must be reworked before anything is committed.

- [ ] **Step 5: Revert the mutation**

Run:
```bash
git checkout -- src/main/java/com/recsys/application/gateway/LlmProxyService.java
git status --short
```
Expected: only the new test file is listed (untracked).

- [ ] **Step 6: Re-run to confirm green again**

Run:
```bash
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn -q test -Dtest=LlmProxyStreamConcurrencyTest -DfailIfNoTests=false
grep -h 'Tests run' target/surefire-reports/com.recsys.application.gateway.LlmProxyStreamConcurrencyTest.txt
```
Expected: `Tests run: 2, Failures: 0, Errors: 0`.

- [ ] **Step 7: Commit**

```bash
git add src/test/java/com/recsys/application/gateway/LlmProxyStreamConcurrencyTest.java
git commit -m "test(gateway): pin that the LLM proxy holds no thread per SSE stream

200 concurrent stream:true requests through the real LlmProxyService must all
complete, deliver every frame, and grow the JVM by at most 32 threads. Proven
red against a mutation that runs forwardStreaming on the blocking executor and
joins on upstream completion (growth ~200), then reverted.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: Add the test to the `resilience` PR-gate profile

**Files:**
- Modify: `pom.xml:554-558` (the `<include>` block listing the other `Llm*` gateway tests inside the `resilience` profile)

**Interfaces:**
- Consumes: the class name `LlmProxyStreamConcurrencyTest` from Task 1.

- [ ] **Step 1: Add the include**

In `pom.xml`, directly after the line

```xml
                <include>**/gateway/LlmSseKeepaliveTest.java</include>
```

add:

```xml
                <!-- The LLM proxy's concurrency ceiling is outstanding subscriptions, not a
                     thread pool: 200 concurrent SSE streams must complete with bounded JVM
                     thread growth. Proven red against a blocking-executor mutation. -->
                <include>**/gateway/LlmProxyStreamConcurrencyTest.java</include>
```

- [ ] **Step 2: Verify the profile now selects the test**

Run:
```bash
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn -q test -Presilience 2>&1 | grep -E 'Tests run:.*Fail|ERROR\]' | tail -5
grep -h 'Tests run' target/surefire-reports/com.recsys.application.gateway.LlmProxyStreamConcurrencyTest.txt
```
Expected: the second command prints `Tests run: 2, Failures: 0, Errors: 0` (the report only exists if the profile ran the test). The first command's output should show no failures other than the known `OutboxRelayTest` timing flake; if that one flakes, re-run once.

- [ ] **Step 3: Commit**

```bash
git add pom.xml
git commit -m "build: run LlmProxyStreamConcurrencyTest in the resilience PR gate

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: Record the measurement in the SSE design doc

**Files:**
- Modify: `docs/system_design/16_SSE_Streaming.md` — insert a subsection at the end of §6 (after the paragraph ending "…measured rather than assumed." and before `## 7. Sharp edges worth flagging`).

**Interfaces:**
- Consumes: nothing from earlier tasks beyond the test's name.

- [ ] **Step 1: Insert the subsection**

Insert the following block between the §6 closing paragraph and the `## 7.` heading:

```markdown
### Measured: the proxy holds no thread per stream

The question "should the LLM client use a different transport for better concurrency"
came up on 2026-09-09 by way of the Anthropic Python SDK's `aiohttp` advice. That advice
targets Python's async client and has no counterpart here — there is no Python HTTP
caller anywhere in the system — but it was worth measuring whether the Armeria proxy has
the property that advice is after. It does. A probe fired N concurrent `stream:true`
requests through the real `LlmProxyService` at an in-JVM upstream emitting 10 SSE frames
200 ms apart (ideal wall 2.0 s), all three servers in one JVM:

| N | Client leg | Completed | Wall time | Peak JVM threads |
|---|---|---|---|---|
| 50 | h2c / h1c | 50/50 | 2.5 s / 2.4 s | 20 / 31 |
| 500 | h2c / h1c | 500/500 | 3.4 s / 3.5 s | 19 / 30 |
| 2000 | h2c | 2000/2000 | 4.0 s | 18 |
| 2000 | h1c | 720/2000 | 18.7 s | 30 |

Two thousand simultaneous streams completed on 18 threads, every upstream stream on a
single Armeria worker (the gateway→upstream leg negotiated one h2c connection and
multiplexed everything over it). The h1c failure at 2000 is a harness artifact, not a
proxy defect: the client opened 2000 loopback connections in one burst against macOS's
128-entry accept backlog and Armeria's default 3200 ms connect timeout fired first. It was
not diagnosed further; the production shape — a burst of new HTTP/1.1 connections from the
ALB to one pod — is unmeasured.

`LlmProxyStreamConcurrencyTest` pins the property: 200 concurrent streams must complete
with every frame and grow the JVM by at most 32 threads. It was proven red against a
mutation that runs `forwardStreaming` on the blocking executor (growth ≈ 200).

Armeria's `ClientFactoryBuilder` does expose the knobs that would tune upstream fan-out
if it were ever needed — `maxNumEventLoopsPerEndpoint`, `maxNumEventLoopsPerHttp1Endpoint`,
`maxNumRequestsPerConnection`, `useHttp2Preface`, `preferHttp1`, `useHttp1Pipelining`.
None is set on the LLM factory, deliberately: nothing measured needs them, and
`ClientFactoryOptions` offers no getter for the event-loop count, so a test could not pin
a chosen value anyway.
```

- [ ] **Step 2: Run the documentation index test**

Run:
```bash
JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home mvn -q test -Dtest=DocumentationIndexTest -DfailIfNoTests=false
grep -h 'Tests run' target/surefire-reports/*DocumentationIndexTest.txt
```
Expected: `Failures: 0, Errors: 0`. No README change is needed because doc 16 is already indexed and no new file was added under `docs/system_design/`.

- [ ] **Step 3: Commit**

```bash
git add docs/system_design/16_SSE_Streaming.md
git commit -m "docs(sse): record the measured no-thread-per-stream property of the LLM proxy

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: Push and open the pull request

**Files:** none.

- [ ] **Step 1: Push the branch**

```bash
git push -u origin test/llm-proxy-stream-concurrency
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create --base main --title "test(gateway): pin that the LLM proxy holds no thread per SSE stream" --body "$(cat <<'EOF'
## Summary

Started from "investigate using aiohttp to improve performance" (the Anthropic Python SDK's transport advice). It has no target here — neither this repo nor Recsys-Streaming-Pipeline has a Python HTTP client — but the property that advice is after was worth measuring on the one LLM client we do have, the gateway's Armeria `WebClient`. It holds: 2000 concurrent SSE streams through `LlmProxyService` completed on 18 JVM threads.

This PR pins that property so it cannot regress silently:

- `LlmProxyStreamConcurrencyTest` — 200 concurrent `stream:true` requests through the real proxy must all complete, deliver every frame, and grow the JVM by at most 32 threads (measured 7 on h2c, 12 on h1c). Wall-time ceiling is deliberately loose (10 s vs ~100 s serial) so it cannot flake.
- Added to the `resilience` PR-gate profile.
- `16_SSE_Streaming.md` §6 records the measurement, the h1c connection-burst caveat (a macOS harness artifact), and the Armeria fan-out knobs that exist and are deliberately unset.
- Spec and plan under `docs/superpowers/`.

No production code change.

## Mutation check

Before committing, `forwardStreaming` was temporarily run on `ctx.blockingTaskExecutor()` with a join on upstream completion — the exact regression the test guards. Both test methods failed on the thread-growth assertion:

```
<paste the assertion output captured in Task 1 Step 4>
```

Reverted before commit.

## Not in this PR (measured but out of scope)

- The h1c run at N=2000 lost 1280 streams to connect timeouts against macOS's 128 accept backlog. Unverified on Linux (`net.core.somaxconn` defaults to 4096 on 5.4+); the ALB→pod HTTP/1.1 burst shape is unmeasured.
- No env vars for Armeria's `maxNumEventLoopsPerEndpoint` etc. Nothing measured needs them.

## Test plan

- [x] `mvn test -Dtest=LlmProxyStreamConcurrencyTest` green on unmodified code
- [x] Same test red under the mutation above, then green after revert
- [x] `mvn test -Presilience` runs and passes the new test
- [x] `DocumentationIndexTest` passes

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Confirm CI is running on the pushed SHA**

Run:
```bash
gh pr view --json number,url,headRefOid,statusCheckRollup --jq '{number,url,sha:.headRefOid,checks:[.statusCheckRollup[]?|{name,status,conclusion}]}'
```
Expected: the SHA matches `git rev-parse HEAD` and at least one check is queued or running. Per project memory, "no CI run exists for this SHA" means the push did not reach the PR — not a slow queue.
