# SSE Streaming in Recsys-Backend-Service

An investigation of how Server-Sent Events (SSE) streaming works in the system:
where it lives, how the streaming request lifecycle differs from buffered
requests, and how it interacts with the gateway's resilience, security, and
caching layers.

## The big picture

The system uses **no raw WebSockets**. The only client-facing streaming path is
**SSE / chunked passthrough in the LLM proxy** — the API Gateway
(`MicroserviceGatewayServer`) reverse-proxies an upstream LLM endpoint (Ollama or
an OpenAI-compatible service) and, when the client requests a token stream,
pipes the upstream's `text/event-stream` response straight through to the client
frame-by-frame. SSE is one-way (server → client), which is exactly what an LLM
token stream needs; nothing here requires a bidirectional socket.

The same reasoning rules out **gRPC**, which is absent from the system entirely —
and note that gRPC would not deliver bidirectional streaming to a browser even if
it were adopted, since gRPC-Web supports server-streaming only. See
[10_MicroServices §5 — Why not gRPC](10_MicroServices.md#why-not-grpc-and-why-not-bidirectional-streaming).

Everything else in the system is either request/response HTTP or *internal*
server-to-server streaming (Redis Streams `sr:stream:<shard>`, the
Kafka → Flink → Redis feature pipeline) that never reaches a browser as a
socket.

## 1. Where SSE lives

- **Service:** [LlmProxyService.java](../../src/main/java/com/recsys/application/gateway/LlmProxyService.java)
  — an Armeria `HttpService` that reverse-proxies the LLM route(s).
- **Routes** (opt-in; only registered when the env var is set —
  [MicroserviceRoute.java:40-41](../../src/main/java/com/recsys/application/gateway/MicroserviceRoute.java#L40-L41)):
  - `llm-explanation` → prefix `/api/explanations`, `LLM_EXPLANATION_SERVICE_URL`
  - `llm` → prefix `/api/llm`, `LLM_SERVICE_URL`
- **Wiring:** the gateway splits LLM routes out from regular routes
  (`LLM_ROUTE_NAMES = {"llm", "llm-explanation"}`) and gives them a **dedicated,
  tuned `ClientFactory` and longer timeouts** so slow inference does not block
  the shared proxy pool ([MicroserviceGatewayServer.java:67-72, 148-162](../../src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java#L67-L72)).
  LLM routes are registered **before** the catch-all proxy so Armeria's
  longest-prefix match picks them first.

## 2. The streaming vs. buffered decision

Every LLM request is aggregated once so the proxy can inspect the body, then
dispatched down one of two paths based on a single flag
([LlmProxyService.java:148-203](../../src/main/java/com/recsys/application/gateway/LlmProxyService.java#L148-L203)):

```
stream:true  in JSON body  → forwardStreaming()   (SSE passthrough)
stream:false / absent      → forwardBuffered()    (aggregate + cache + retry)
```

`parseBodyMeta` ([LlmProxyService.java:400-413](../../src/main/java/com/recsys/application/gateway/LlmProxyService.java#L400-L413))
reads `"stream"` (boolean) and `"max_tokens"` (int) from the request JSON; a
malformed body falls back to non-streaming with a default token estimate.

## 3. The SSE streaming lifecycle

`forwardStreaming` ([LlmProxyService.java:209-245](../../src/main/java/com/recsys/application/gateway/LlmProxyService.java#L209-L245))
is a **reactive-streams passthrough** — it never buffers the body:

1. Create an Armeria `HttpResponseWriter` via `HttpResponse.streaming()` and
   return it to the client immediately (before the upstream has responded).
2. Subscribe to the upstream `HttpResponse` with `request(Long.MAX_VALUE)`
   (unbounded demand — the client's TCP backpressure flows through Armeria).
3. On each upstream `HttpObject`:
   - `ResponseHeaders` → record circuit-breaker success/failure by status,
     strip hop-by-hop headers, and `writer.write(filtered)`.
   - `HttpData` (an SSE chunk / `data:` frame) → `writer.write(d)` straight to
     the client.
4. `onError` → `circuitBreaker.recordFailure()` + `writer.close(t)`.
5. `onComplete` → `writer.close()`.

Key property: the client sees the first token as soon as the upstream emits it;
there is no aggregation, no size cap on the stream, and the `text/event-stream`
content type is preserved from the upstream headers.

## 4. Streaming vs. buffered: feature matrix

The two paths deliberately differ — streaming trades away caching and retry for
immediacy:

| Concern | Streaming (`forwardStreaming`) | Buffered (`forwardBuffered`) |
|---|---|---|
| Response delivery | Frame-by-frame passthrough | Aggregated, then sent whole |
| Response cache | **Skipped** — never cached | 200s cached by SHA-256 of body (`X-Cache: HIT/MISS`) |
| Retry-on-429 | **No** — surfaced to client immediately | Retries once, respects `Retry-After` (≤ `LLM_MAX_RETRY_WAIT_MS`) |
| Token rate-limit pre-check | Yes (`max_tokens` before forwarding) | Yes |
| Circuit breaker | Recorded per-frame on headers/error | Recorded on aggregate status/exception |
| Upstream-unreachable handling | `writer.close(t)` mid-stream | `502 Bad Gateway` "LLM upstream unreachable" |

The class doc calls this out explicitly: *"retries once (buffered mode only;
streaming is surfaced immediately)"* and *"caches non-streaming 200 responses."*

## 5. Cross-cutting concerns applied to SSE

**Security / identity.** `buildUpstreamHeaders`
([LlmProxyService.java:308-334](../../src/main/java/com/recsys/application/gateway/LlmProxyService.java#L308-L334))
runs on both paths: it strips client-spoofed `x-authenticated-*` identity
headers, strips gateway-consumed credentials (`authorization`, `x-api-key`, the
CloudFront `x-origin-secret`), injects the authenticated `GatewayPrincipal`'s
identity headers, and adds `x-forwarded-for/-host/-proto`. The gateway is the
sole identity authority; none of its credentials reach the LLM upstream. This is
the only behavior with dedicated tests
([LlmProxyServiceTest.java](../../src/test/java/com/recsys/application/gateway/LlmProxyServiceTest.java)).

**Token budget.** Before forwarding (streaming or not), `LlmTokenRateLimiter`
pre-checks the `max_tokens` budget and rejects with `429` +
`Retry-After`/`x-ratelimit-*` headers when exhausted.

**Circuit breaker.** Shared with the gateway health endpoint; opens on repeated
upstream 5xx/timeouts and fast-fails new requests with `503` during cooldown.
On the streaming path it observes the upstream *response headers* and any
mid-stream `onError`.

**Edge caching.** The gateway proxy path forces `Cache-Control: no-store`
([GatewayProxyService.java:68-73](../../src/main/java/com/recsys/application/gateway/GatewayProxyService.java#L68-L73))
so CloudFront never pins LLM responses (its 10 s default error-cache TTL would
otherwise cache a transient failure). LLM routes are POST-only and are not in
the CDN cache behaviors.

## 6. Connection tuning (why SSE stays alive)

The dedicated LLM `ClientFactory`
([MicroserviceGatewayServer.java:213-222](../../src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java#L213-L222))
is what keeps a long, slow token stream healthy:

| Env var | Default | Purpose |
|---|---|---|
| `LLM_TIMEOUT_MS` | 120 000 (120 s) | The whole LLM budget — WebClient `responseTimeoutMillis` **and** the per-request server timeout `LlmProxyService.serve` sets on itself. Before that second binding the untuned 10 s server default cut first (sharp edge 1) |
| `LLM_CONNECT_TIMEOUT_MS` | 2 000 | Upstream connect timeout |
| `LLM_IDLE_TIMEOUT_MS` | 60 000 | Idle-connection reaper |
| `LLM_PING_INTERVAL_MS` | 20 000 | Gateway-to-upstream HTTP/2 PING; keep below `LLM_IDLE_TIMEOUT_MS`. Does not protect the client-facing hop |
| `LLM_MAX_RETRY_WAIT_MS` | 30 000 | Cap on honored `Retry-After` (buffered only) |
| `LLM_SSE_KEEPALIVE_MS` | 10 000 | Idle threshold and scheduler period for SSE comments; values above 10 000 fail startup, non-positive values disable. Allows margin below CloudFront's 30 s origin read timeout (sharp edge 4) |
| `LLM_DEFAULT_TOKEN_ESTIMATE` | 1 000 | Token budget when `max_tokens` is absent |

HTTP/2 PING (`pingIntervalMillis`) is configured on the gateway-to-upstream
client connection. Armeria does not count an in-flight response as idle, so
PING is not what preserves a quiet response. Client-facing intermediaries
instead need response bytes, supplied by SSE comments at complete frame
boundaries. See sharp edge 4 for the measured behavior and remaining limits.

### Measured: the proxy holds no thread per stream

The question "should the LLM client use a different transport for better
concurrency" came up on 2026-09-09 by way of the Anthropic Python SDK's `aiohttp`
advice. That advice targets Python's async client and has no counterpart here —
there is no Python HTTP caller anywhere in the system — but it was worth
measuring whether the Armeria proxy has the property that advice is after. It
does. A probe fired N concurrent `stream:true` requests through the real
`LlmProxyService` at an in-JVM upstream emitting 10 SSE frames 200 ms apart
(ideal wall 2.0 s), all three servers in one JVM:

| N | Client leg | Completed | Wall time | Peak JVM threads |
|---|---|---|---|---|
| 50 | h2c / h1c | 50/50 | 2.5 s / 2.4 s | 20 / 31 |
| 500 | h2c / h1c | 500/500 | 3.4 s / 3.5 s | 19 / 30 |
| 2000 | h2c | 2000/2000 | 4.0 s | 18 |
| 2000 | h1c | 720/2000 | 18.7 s | 30 |

Two thousand simultaneous streams completed on 18 threads, every upstream stream
on a single Armeria worker (the gateway→upstream leg negotiated one h2c
connection and multiplexed everything over it). The h1c failure at 2000 is a
harness artifact, not a proxy defect: the client opened 2000 loopback
connections in one burst against macOS's 128-entry accept backlog and Armeria's
default 3200 ms connect timeout fired first. It was not diagnosed further; the
production shape — a burst of new HTTP/1.1 connections from the ALB to one
pod — is unmeasured.

`LlmProxyStreamConcurrencyTest` pins the property: 200 concurrent streams must
complete with every frame in order, no thread other than a common event loop may
be *held* inside gateway code (seen there in two consecutive 50 ms stack
samples), and the non-event-loop thread count may grow by at most N/2. It was
proven red against a mutation that runs `forwardStreaming` on the blocking
executor (190 and 200 held threads). "Held" rather than "seen" is what makes the
signal safe: a regression parks a thread for the whole stream, while a conforming
short hop to a bounded pool is never sampled twice in a row. Growth is kept as the
human-readable number but is order-dependent on its own: the pool threads a
regression spawns in the first test method outlive it and become the second
method's baseline, which is exactly how the first, growth-only version of the
test missed the mutation in one of its two methods.

The test's gateway→upstream leg is HTTP/1.1, because that is the production
shape and not the h2c the probe table above shows: Ollama serves HTTP/1.1 only
(measured on 0.21.2 — an h2c prior-knowledge connect is refused, an upgrade
attempt stays on 1.1), so `LLM_PING_INTERVAL_MS`'s HTTP/2 PING never applies to
the deployed upstream and the gateway opens one upstream connection per
concurrent stream.

Armeria's `ClientFactoryBuilder` does expose the knobs that would tune upstream
fan-out if it were ever needed — `maxNumEventLoopsPerEndpoint`,
`maxNumEventLoopsPerHttp1Endpoint`, `maxNumRequestsPerConnection`,
`useHttp2Preface`, `preferHttp1`, `useHttp1Pipelining`. None is set on the LLM
factory, deliberately: nothing measured needs them, and `ClientFactoryOptions`
offers no getter for the event-loop count, so a test could not pin a chosen
value anyway.

### Maintaining the LLM proxy tests

`LlmProxyTestServers` supplies the shared route, upstream and gateway server
extensions, and slow-stream emitter. Each test still constructs its own
`LlmProxyService` to choose the behavior under test. Register upstream before
gateway with `@RegisterExtension @Order`; JUnit then manages server shutdown.
The stub disables its own request timeout so timeout tests measure the gateway,
and the shared scheduler's emitter stops after completion or client cancellation.

The concurrency test exercises h2c and h1c client legs with an HTTP/1.1 upstream
and the production client factory. Its stack filter excludes test fixtures;
keep that separation when adding helpers in the gateway package. This checks
for platform threads held per stream, not event-loop blocking, virtual-thread
allocation, or production capacity. Run `mvn -Dtest='Llm*Test' test` from the
repository root and add new regression classes to the resilience profile's
explicit includes in `pom.xml`.

## 7. Sharp edges worth flagging

1. **The 10 s server request timeout used to cap every LLM call — measured, and
   now fixed.** The gateway builds its server with `Server.builder().http(port)`
   ([MicroserviceGatewayServer.java:159](../../src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java#L159))
   and never overrides Armeria's server request timeout. That timeout covers
   response **completion**, not time-to-first-byte, so before the fix below the
   configured `LLM_TIMEOUT_MS` of 120 s was unreachable on both paths — the
   effective ceiling was 10 s, whatever the env var said.

   Measured against the exact production wiring (`LlmProxyService` mounted on a
   `Server.builder().http(port)` server, Armeria 1.28.4, upstream held open):

   | Probe | Result |
   |---|---|
   | `ServiceConfig.requestTimeoutMillis()` for `/api/llm/*` | **10000** (the default, never overridden) |
   | Streaming, 24 frames over 12 s | **19/24 frames**, then `ClosedStreamException` (RST_STREAM `INTERNAL_ERROR`) at **10 021 ms** |
   | Streaming, same upstream, timeout disabled | 24/24 frames, clean `onComplete` |
   | Buffered, single response after 12 s | **503** at ~11.4 s, body `Status: 503 / Description: Service Unavailable` |

   Two distinct failure modes fell out of that:
   - **Streaming truncated silently.** The client already received `200` and
     `text/event-stream` headers, so there is no error status to observe — the
     token stream just stops mid-generation. A browser `EventSource` treats that
     as a dropped connection and *auto-reconnects*, re-issuing the whole prompt
     and paying for the tokens a second time.
   - **Buffered returned Armeria's built-in 503**, in plain text, not the
     gateway's JSON error envelope — so a client parsing `{"error": ...}` off the
     LLM route gets a parse failure instead of a readable message. Note this is
     Armeria's own timeout response, not the circuit breaker's 503, though the
     two are indistinguishable to the caller.

   This was **not dormant**: `k8s/base/configmap.yaml:20-21` sets both
   `LLM_SERVICE_URL` and `LLM_EXPLANATION_SERVICE_URL` to `http://ollama:11434`
   and the gateway `envFrom`s that ConfigMap
   ([api-gateway.yaml:40-42](../../k8s/base/api-gateway.yaml#L40-L42)), so the LLM
   routes are registered in every deployed gateway. No manifest sets
   `LLM_TIMEOUT_MS`, so the intended budget is the 120 s default — 12× the
   ceiling actually enforced. Local generation on Ollama routinely exceeds 10 s.

   The non-LLM routes are unaffected by construction, not by luck:
   `GATEWAY_TIMEOUT_MS` is 3 000 and the catch-all proxy's own outbound timeout
   fires first. Only the LLM path is configured to outlive the server timeout.

   **The fix.** `LlmProxyService.serve` now opens with
   `ctx.setRequestTimeout(TimeoutMode.SET_FROM_NOW, timeout)`, binding the server
   timeout to the same `LLM_TIMEOUT_MS` budget the upstream `WebClient` already
   uses — so one env var means one ceiling on both sides. It *sets* rather than
   clears deliberately: `clearRequestTimeout()` would fix the streaming
   truncation but leave a stuck request pinning a connection with no backstop but
   the client-side timeout, and it would not have fixed the buffered path at all.
   Both failure modes are covered by
   [LlmProxyStreamTimeoutTest.java](../../src/test/java/com/recsys/application/gateway/LlmProxyStreamTimeoutTest.java),
   which is in the `resilience` PR-gate profile. Raising an LLM call's ceiling is
   now what it looks like: raise `LLM_TIMEOUT_MS`.
2. **Streaming has no retry and no cache.** A `429` or `5xx` that arrives after
   the upstream headers are written is surfaced mid-stream; the client must
   handle a partial/failed stream itself. This is intentional but asymmetric
   with the buffered path.
3. **Token pre-check uses the client-declared `max_tokens` — now settled up
   afterwards.** The pre-check can only spend the caller's own `max_tokens`, which
   the caller controls and the gateway cannot verify before forwarding. With no
   reconciliation, declaring `max_tokens: 1` and then consuming hundreds cost one
   token, and the budget stopped bounding anything a caller cared to under-declare.

   `LlmTokenUsageScanner` now reads the completion-token count the upstream
   reports and `LlmTokenRateLimiter.reconcile` settles the difference — refunding
   an over-estimate, debiting an under-estimate. Three properties are worth
   knowing:
   - **The stream is still never buffered.** Frames are scanned as they pass and
     forwarded unchanged; only a bounded partial-line carry-over (64 KB) is held
     between chunks, since a chunk boundary can split a usage frame.
   - **An overage drives the bucket negative,** deliberately. That is what makes
     an under-declaration self-correcting rather than free: the deficit must
     refill before the next request is admitted. A refund is capped at the burst,
     so over-declaring cannot mint capacity.
   - **Settling happens-before the client sees the end of the response** — in the
     upstream subscriber's `onComplete`, ahead of `writer.close()` — so a caller
     cannot race its own next request in ahead of the charge.

   Both upstream dialects the deployed `LLM_SERVICE_URL` (Ollama) can speak are
   covered: OpenAI-compatible SSE (`usage.completion_tokens`) and native NDJSON
   (`eval_count`). **Residual gap:** an upstream that reports no usage at all
   leaves the declared estimate standing. That is a deployment property, not a
   caller-controlled one — the upstream is fixed by `LLM_SERVICE_URL`, so a caller
   cannot select a silent one to evade the budget. A mid-stream failure is charged
   for whatever it had produced before dying.
4. **No SSE keepalive frame — added, and the original reasoning was wrong.**
   This edge claimed liveness rested on the HTTP/2 PING interval. Measured, it
   did not, in two ways:
   - `LLM_PING_INTERVAL_MS` is set on the LLM **`ClientFactory`**
     ([MicroserviceGatewayServer.java:291-292](../../src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java#L291-L292)),
     so it governs the gateway→upstream leg, not the client-facing one.
   - On the client-facing leg the gateway's own defaults are
     `idleTimeoutMillis=15000` and `pingIntervalMillis=0` — **PING is disabled** —
     and it does not matter: a 25 s silent gap mid-stream survives cleanly on both
     h1c and h2c, because Armeria does not count a connection with an in-flight
     response as idle. The gateway will not cut a slow stream.

   The real exposure is the hops in front. No ingress sets
   `alb.ingress.kubernetes.io/load-balancer-attributes:
   idle_timeout.timeout_seconds`, so the ALB's **60 s default** applies, and
   unlike Armeria it counts a silent streaming connection as idle — while
   `LLM_TIMEOUT_MS` is willing to wait 120 s. A model that thinks for longer than
   60 s between tokens therefore loses the connection at the load balancer, and
   the client sees the same silent truncation as sharp edge 1.

   CloudFront is the tighter limit: `scripts/create-cdn-distribution.sh` sets
   `OriginReadTimeout: 30`. Its [response timeout](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/RequestAndResponseBehaviorCustomOrigin.html#request-custom-response-timeout)
   applies between response packets, including on a streaming response.

   `LlmProxyService` emits `: keep-alive\n\n` while an SSE stream is quiet.
   `LLM_SSE_KEEPALIVE_MS` is both the idle threshold and the scheduler period
   (default and maximum 10 000 ms; non-positive values disable). Larger values
   fail construction with a configuration error. A data write just after a
   scheduler tick can suppress the next tick, delaying a comment until nearly
   **twice** the interval after that write. The 10 s ceiling leaves about 10 s
   of margin below CloudFront's 30 s timeout; it is not a timing guarantee
   during event-loop stalls. Explicit overrides above 10 000 must be reduced.

   A leading `:` is the SSE spec's comment syntax, ignored by `EventSource`.
   Two guards keep the heartbeat from corrupting what it is protecting:
   - **Only for `text/event-stream`.** The passthrough is content-type agnostic;
     a comment line injected into native Ollama NDJSON would hand the client a
     line that is not JSON.
   - **Only at a frame boundary** (the bytes so far end `\n\n`). A chunk boundary
     is a network artifact, not a frame boundary, so writing while a frame is
     half-delivered would splice a comment through the middle of it.

   Setting the ALB's `idle_timeout.timeout_seconds` above `LLM_TIMEOUT_MS` is a
   complementary infra-side knob. It does not raise CloudFront's timeout.
   Heartbeats start only after upstream SSE headers arrive and are suppressed
   inside a partial frame. Waiting for headers, incomplete frames, non-SSE
   streams, and explicitly disabled heartbeats remain exposed to hop timeouts.

## Summary

SSE streaming is a thin, well-factored reverse-proxy passthrough scoped entirely
to the LLM gateway. It reuses the gateway's auth, token-budget, circuit-breaker,
and header-sanitization machinery, and gets its own long-timeout HTTP/2 client so
slow inference streams stay alive — while deliberately opting out of caching and
retry that only make sense for buffered responses.

The one thing that did *not* hold was the timeout budget: a tuned 120 s
client-side timeout defeated by an untuned 10 s server-side request timeout that
covers response completion, capping every LLM call — streamed or buffered — at
10 s. Streams truncated silently mid-token; buffered calls returned a bare 503.
Both were measured, both were live in `k8s/base`, and both are now fixed by
binding the server timeout to `LLM_TIMEOUT_MS`. See sharp edge 1.
