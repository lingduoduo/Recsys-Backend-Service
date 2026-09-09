# LLM Proxy Stream Concurrency Conformance Design

## Objective

Pin down, with a committed test, the property that makes the gateway's LLM reverse proxy
scale under many simultaneous token streams: `LlmProxyService` forwards each SSE stream on
Armeria's event loops and holds no thread per in-flight stream. The property was measured
on 2026-09-09 but nothing guards it, so a future change that wraps the forwarding path in a
blocking task would regress it silently. Record the measurement in the SSE design doc.

No production code changes. No new configuration.

## Background

The question that prompted this work was whether to adopt `aiohttp` as the HTTP transport
"for improved concurrency performance", quoting the Anthropic Python SDK README. That advice
targets Python's async client (`pip install anthropic[aiohttp]` plus
`AsyncAnthropic(http_client=DefaultAioHttpClient())`) and swaps its httpx2 transport for an
event-loop-driven one.

It has no target in this system. Neither this repository nor Recsys-Streaming-Pipeline
contains a Python HTTP client: the pipeline's Python dependencies are `kafka-python`, `lz4`,
`fastavro`, `pyarrow` and `torch`, with no `requests`, `httpx`, `aiohttp`, `anthropic` or
`openai` import anywhere. The only LLM client in the system is `LlmProxyService`'s Armeria
`WebClient`, which already runs on Netty event loops, the Java equivalent of what `aiohttp`
gives Python. `aiohttp` is also HTTP/1.1-only, so even for a Python caller it would give up
the HTTP/2 multiplexing the Armeria client already does.

The concurrency-model write-up in `17_Scalability` §2 already classifies the gateway proxy
path as the system's one non-blocking idiom. That classification was made by reading the
code; this work measures it and guards it.

## Measurement

A throwaway JUnit probe (never committed) issued N concurrent `stream:true` POSTs through the
real `LlmProxyService` to an in-JVM Armeria upstream emitting 10 SSE frames 200 ms apart
(ideal wall time 2.0 s). Client, gateway and upstream shared one JVM on an arm64 macOS
laptop, Armeria 1.28.4, JDK 17.

| N | Client leg | Completed | Wall time | Peak JVM threads | Threads named `*blocking*` |
|---|---|---|---|---|---|
| 50 | h2c / h1c | 50/50 | 2.5 s / 2.4 s | 20 / 31 | 0 |
| 500 | h2c / h1c | 500/500 | 3.4 s / 3.5 s | 19 / 30 | 0 |
| 2000 | h2c | 2000/2000 | 4.0 s | 18 | 0 |
| 2000 | h1c | 720/2000 | 18.7 s | 30 | 0 |

Two thousand simultaneous streams completed on 18 threads. Every upstream stream was served
on a single Armeria worker thread, because the gateway's upstream leg negotiated one h2c
connection and multiplexed all streams over it.

The h1c failure at N=2000 is a harness artifact, not a proxy defect: the test client opened
2000 TCP connections to the gateway in one burst on loopback against a macOS accept backlog
of 128 (`kern.ipc.somaxconn`), and Armeria's default 3200 ms connect timeout fired before the
listener drained. The h2c run at the same N multiplexed one connection and was clean. This
was not diagnosed further; the equivalent on a Linux pod would be `net.core.somaxconn`, which
defaults to 4096 on kernels 5.4 and later. A burst of new HTTP/1.1 connections from the ALB
to one pod is the production shape of this scenario and remains unmeasured.

Armeria 1.28.4's `ClientFactoryBuilder` exposes the knobs that would tune upstream fan-out
if it were ever needed: `maxNumEventLoopsPerEndpoint`, `maxNumEventLoopsPerHttp1Endpoint`,
`maxNumRequestsPerConnection`, `useHttp2Preface`, `preferHttp1` and `useHttp1Pipelining`.
None is set on the LLM `ClientFactory`, and the measurement gives no reason to set one.
`ClientFactoryOptions` has no getter for the event-loop count, so a test cannot read it back.

## Scope

1. A committed conformance test, `LlmProxyStreamConcurrencyTest`, that fails if
   `LlmProxyService` ever holds a thread per concurrent stream.
2. The test added to the `resilience` Surefire profile, which is the PR gate.
3. A measured subsection in `docs/system_design/16_SSE_Streaming.md` §6 recording the
   table above, the unset fan-out knobs, and the h1c caveat.

## Design

### The test

`src/test/java/com/recsys/application/gateway/LlmProxyStreamConcurrencyTest.java`, built on
the same `ServerExtension` pattern as `LlmSseKeepaliveTest`, including the lazy
`gw()` accessor: JUnit does not guarantee static `@RegisterExtension` field order, and a
gateway extension that reads the upstream's URI during `configure` fails with "server did
not start" when it runs first.

- **Upstream:** an Armeria server with `requestTimeoutMillis(0)` whose `/sse` route writes
  `text/event-stream` headers, then 5 frames 100 ms apart from a single daemon scheduled
  executor, then closes. Ideal wall time is 500 ms.
- **Gateway:** `LlmProxyService` in front of that upstream with a `ClientFactory` built the
  same way `MicroserviceGatewayServer.buildLlmClientFactory` builds the production one,
  the token limiter and response cache disabled, and the SSE keepalive interval set to 0 so
  the frame count is exact.
- **Load:** N = 200 concurrent `POST /api/llm/sse` with body `{"stream":true,"max_tokens":10}`,
  issued from one Armeria `WebClient` with no response timeout, each subscribed with an
  unbounded request. The run is repeated for an h2c and an h1c client leg, because the
  two exercise different server-side connection handling (one multiplexed connection versus
  one connection per stream).
- **Assertions, in order:**
  1. All 200 streams complete with `onComplete`, not `onError`.
  2. Exactly 1000 SSE frames were received (frames are counted by `\n\n` terminators).
  3. The JVM thread count grew by at most 32 between just before the burst and its
     completion, measured with `ThreadMXBean.resetPeakThreadCount` / `getPeakThreadCount`.
     The measured growth was 7 (h2c) and 12 (h1c); a thread-per-stream regression grows it
     by roughly N.
  4. Wall time for the burst is under 10 s. This is a coarse guard against full
     serialization (200 × 0.5 s = 100 s), not a latency assertion, and is deliberately loose
     so it cannot flake on a loaded CI runner.

The thread bound is the primary signal. A timing bound tight enough to detect thread-pool
saturation would flake, since the probe already showed 1.4 s of first-byte spread at N=500
on an idle laptop.

### Mutation check

Per `feedback-conformance-test-blind-spots`, the test is proven red before it is committed:
`LlmProxyService.serve` is temporarily changed to run `forwardStreaming` on
`ctx.blockingTaskExecutor()` and join on stream completion, which is the exact regression the
test exists to catch. The test must fail on the thread-growth assertion (and may also fail on
completion, since Armeria's blocking executor is bounded). The mutation is reverted before
commit and the failure output is quoted in the PR description.

### CI wiring

`pom.xml`'s `resilience` profile is an include allow-list. Add
`**/gateway/LlmProxyStreamConcurrencyTest.java`. The test is not tagged `load` or `docker`;
it runs in the default suite too.

### Documentation

`16_SSE_Streaming.md` §6 gains a short subsection, "Measured: the proxy holds no thread per
stream", carrying the table, one sentence on the single upstream worker thread, the list of
unset `ClientFactoryBuilder` fan-out knobs, and the h1c caveat labelled as a macOS harness
artifact. No new numbered document, no README change (`DocumentationIndexTest` is scoped to
`docs/system_design` and `docs/runbooks`, and doc 16 is already indexed).

## Out of scope

- Wiring Armeria's fan-out knobs to environment variables. Nothing measured needs them.
- Accept-backlog (`SO_BACKLOG`) tuning on the gateway server. The burst failure was observed
  only on macOS with a 128 backlog and is unverified on Linux.
- Any Python-side change. There is no Python HTTP caller to change.
- Measuring against a real Ollama, which was installed but not running. The upstream
  protocol Ollama negotiates (Go `net/http` has no h2c by default) is a separate question
  and does not affect liveness: Armeria never idles a connection with an in-flight
  response (sharp edge 4).

## Revision after review (2026-09-09)

The first committed version of the test differed from what shipped. A mutation check and a
multi-angle code review changed the design as follows; the sections above describe the
original intent and are kept for the record.

- **Primary signal is "held", not "seen".** The growth-only test caught the mutation in one
  method but not the other: the ~200 blocking-executor threads the first method spawned
  outlived it and became the second method's baseline. The primary signal is now a stack
  sample every 50 ms: a thread other than a common event loop counts only if it is inside
  gateway production code in two *consecutive* samples. A thread-per-stream regression parks
  a thread for the whole stream; a conforming short hop to a bounded pool is never seen twice
  in a row, so the signal cannot flake in either direction. Measured under the mutation: 190
  (h2c) and 200 (h1c) held threads.
- **Event loops are classified by identity** against `CommonPools.workerGroup()`, which both
  the gateway server and the production LLM client factory use, not by thread-name prefix.
- **Growth counts non-event-loop threads only and is bounded by N/2**, derived from the
  invariant rather than tuned to one laptop. Netty starts common workers lazily, 2×CPUs of
  them, so a JVM-wide bound of 32 would fail conforming h1c runs on a many-core host.
- **Gateway→upstream leg is HTTP/1.1**, the production shape. Ollama 0.21.2 was started
  locally and probed: an h2c prior-knowledge connect is refused and an upgrade attempt stays
  on HTTP/1.1. The test's route targets the upstream's h1c URI, so the gateway opens one
  upstream connection per stream, as it does against Ollama.
- **The test uses the production client factory.** `MicroserviceGatewayServer.buildLlmClientFactory`
  is made `public` (it was package-private) and called with no environment overrides, so a
  knob added to the production factory is exercised here rather than silently missed. This
  is the one production-code change in the PR; it changes no behaviour.
- **Bodies are aggregated and compared exactly** to the expected frame sequence, replacing a
  per-chunk `\n\n` counter that would have miscounted a terminator split across two chunks,
  and pinning frame order, which the counter never did.
- **Fixtures are torn down.** The upstream emitter is a self-terminating one-shot chain
  (the fixed-rate task was never cancelled and would have ticked for the rest of the shared
  Surefire fork); the gateway is a `@RegisterExtension @Order(2)` extension JUnit stops,
  following `RecommendationGatewayServiceTest`'s precedent, instead of a lazily started
  singleton; the client factory is closed in `@AfterAll`.
- **N = 200 is deliberate**: it equals Armeria's common blocking executor size, so a
  thread-per-stream regression also manifests as the production cap.

Declined from the review, as follow-ups outside this PR: a shared test helper for the four
gateway tests that each build a `ServerExtension` around `LlmProxyService`; giving
`LlmSseKeepaliveTest` the same `@Order` treatment; a parameterized test in place of two named
methods. Not pinnable by this test and stated as non-goals in its Javadoc: blocking *on* an
event loop, and virtual threads.

## Success criteria

- `mvn test -Dtest=LlmProxyStreamConcurrencyTest` passes on main plus this change.
- The same test fails under the mutation described above.
- `mvn test -Presilience` includes and passes the test.
- `DocumentationIndexTest` still passes.
