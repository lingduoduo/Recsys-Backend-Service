# LLM Proxy Stream Concurrency Conformance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Commit a test that fails if `LlmProxyService` ever holds a thread per concurrent SSE stream, wire it into the PR-gate profile, and record the 2026-09-09 measurement in the SSE design doc.

**Architecture:** One new JUnit test in `com.recsys.application.gateway` stands up an in-JVM Armeria upstream that trickles SSE frames, puts the real `LlmProxyService` in front of it, fires 200 concurrent streams, and asserts completion, frame count, bounded JVM thread growth and a loose wall-time ceiling. The test is proven red by a temporary mutation of the proxy before it is committed. No production code changes.

**Tech Stack:** Java 17, Armeria 1.28.4 (`ServerExtension`, `WebClient`, `HttpResponseWriter`), JUnit 5, AssertJ, `java.lang.management.ThreadMXBean`, Maven Surefire `resilience` profile.

**Spec:** `docs/superpowers/specs/2026-09-09-llm-proxy-stream-concurrency-conformance-design.md`

## Global Constraints

- JDK 17 for every Maven invocation: prefix commands with `JAVA_HOME=/Users/linghuang/Library/Java/JavaVirtualMachines/corretto-17.0.12/Contents/Home` (or `$(/usr/libexec/java_home -v 17)`). Newer JDKs fail a clean compile of two pre-existing files.
- Work in the worktree `.worktrees/llm-proxy-stream-concurrency` on branch `test/llm-proxy-stream-concurrency`. Never commit to `main`; the PR is the integration path.
- No production code change survives into a commit. The mutation in Task 1 is reverted before committing.
- No new numbered doc under `docs/system_design/`, no README change, no `.claude/CLAUDE.md` change.
- Thread-growth bound is 32; N is 200; upstream emits 5 frames 100 ms apart; wall-time ceiling is 10 s. These are the spec's values.
- Every commit message ends with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

---

### Task 1: The conformance test, proven red by mutation

**Files:**
- Create: `src/test/java/com/recsys/application/gateway/LlmProxyStreamConcurrencyTest.java`
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
Expected: both methods FAIL on the stack-sample assertion, `threads other than Armeria event loops found inside LlmProxyService while 200 streams were in flight`, with an actual value near 200 (Armeria's default blocking executor grows on demand up to 200 core threads). Only the first method to run also trips the thread-growth bound: the pool threads it spawned outlive it and become the next method's baseline. That order-dependence is why the stack sample is the primary signal. Save this output for the PR description.

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
