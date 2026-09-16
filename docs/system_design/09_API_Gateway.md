# API Gateway in Recsys-Backend-Service

An investigation of the single public edge — `MicroserviceGatewayServer` (Armeria,
port 8010) — that fronts the three backend services: how it routes and prefix-strips, how it
is the trust boundary that authenticates callers and strips their credentials before
proxying, how it aggregates downstream health, and how the cross-cutting concerns
(circuit breaking, rate limiting, upstream discovery, the LLM proxy) compose into
one request pipeline. Several of those concerns have their own deep-dives; this doc
is the map that ties them together.

## The big picture — one edge, one pipeline

The gateway implements the **API Gateway pattern**: a single edge that concentrates
cross-cutting concerns so backends stay simple and clients learn just one hostname
and port. The tradeoff is one shared choke point — mitigated by health-checked
upstreams and per-route circuit breakers — in exchange for that simplicity. The three
backend services sit behind it.

A request traverses a deliberate pipeline. Some stages are Armeria server decorators
(applied before dispatch); the rest run inside the proxy per request:

| Stage | Where | Effect |
|---|---|---|
| `/metrics`, `/health`, `/health/live` | server routes | Prometheus, health aggregation, liveness; before auth |
| **Origin secret** | server decorator | Reject non-CDN traffic `403` (only when enabled) |
| Route match | `MicroserviceRouteTable` | Longest-prefix; LLM + `/api/recommend` win over catch-all |
| **Auth** | inside `GatewayProxyService.serve` | Edge auth → principal, or `401` |
| **Rate limit** | inside `GatewayRequestForwarder.forward` | Per `(route, principal)` → `429` |
| **Circuit breaker** | inside `forward` | Open → `503` fast-fail |
| Header rewrite | `buildUpstreamHeaders` | Strip credentials, inject identity |
| Proxy | `forward` | Prefix-strip, pick upstream, pipe, map errors |

The bootstrap wiring, decorator order, and shutdown hook live in
[`MicroserviceGatewayServer`](../../src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java)
(LLM routes and the canonical `/api/recommend` are registered *before* the catch-all
`prefix:/` so Armeria's more-specific match wins). Key knobs: `GATEWAY_PORT` (8010),
`GATEWAY_TIMEOUT_MS` (3000), and `networkaddress.cache.ttl=30` for Cloud Map
blue/green.

## 1. Routing and prefix-strip

The route table
([`MicroserviceRoute`](../../src/main/java/com/recsys/application/gateway/MicroserviceRoute.java))
is a list of `(name, prefix, envVar, baseUri, healthPath, serviceName)` records;
[`MicroserviceRouteTable`](../../src/main/java/com/recsys/application/gateway/MicroserviceRouteTable.java)
is a **longest-prefix router** (exact-prefix map first, then routes sorted
longest-first). Registered routes:

| Gateway prefix | Backend | Notes |
|---|---:|---|
| `/api/recommend/{embedding,model,online,sequential}` | 6010/8080/7010/8080 | Strategy-specific recommendation routes |
| `/api/users`, `/api/movies` | 6010 | User / movie metadata |
| `/api/features` | 7010 | Online feature snapshot |
| `/api/knowledge` | 8080 | Knowledge service |
| `/api/catalog`, `/api/model`, `/api/online` | 6010/8080/7010 | Deprecated back-compat aliases |
| `/api/llm`, `/api/explanations` | (LLM) | Opt-in, registered only when the env var is set |

The **canonical** entry point is `POST /api/recommend`: it takes an optional JSON
`strategy` (`embedding` / `model` / `online` / `sequential`, default `model`),
selects the matching backend, and **removes the `strategy` selector** before
forwarding so the upstream receives its normal schema.

Prefix-strip is `MicroserviceRoute.rewrite`: it validates the prefix match, takes
`suffix = path.substring(prefix.length())` (blank → `/`), joins it onto the route's
`baseUri`, and preserves the raw query string. `matchesPrefix` enforces segment
boundaries (`path == prefix || startsWith(prefix + "/")`), so `/api/usersettings`
does **not** match `/api/users`.

[`GatewayRequestForwarder.forward`](../../src/main/java/com/recsys/application/gateway/GatewayRequestForwarder.java)
does the actual proxying and maps failures to a clean contract:

| Condition | Status |
|---|---:|
| Rate limit exceeded | `429` + `Retry-After` / `x-ratelimit-*` |
| Circuit open | `503` "circuit open — upstream unavailable" |
| No healthy endpoint (`EmptyEndpointGroupException`) | `503` |
| Upstream 5xx | passthrough (records CB failure) |
| Upstream failure before final headers | `502` "upstream unreachable" |
| Upstream failure after final headers | Response stream terminates; no replacement JSON error |

A single retry (`maxTotalAttempts=2`, fixed 50 ms backoff) covers transient
`IOException` — but **not** `SocketTimeoutException` — to ride out Cloud Map
deregistration windows. `GatewayProxyService.gatewayError` emits `{"error":...}` JSON
with `Cache-Control: no-store` so CloudFront never caches an error.

### Response streaming

Ordinary proxy responses are forwarded as streams with backpressure. The gateway
can deliver the first chunk before the upstream finishes, and does not retain the
entire response body. Request bodies are still aggregated for authorization and
recommendation request translation. This does not change the separate LLM proxy.

`GatewayUpstreamResponse` preserves response headers, data, and trailers. Circuit
permits settle exactly once on termination, three ways:

| Termination | Permit |
|---|---|
| Completed, final status not 5xx | success |
| Completed 5xx, or stream error | failure |
| Downstream cancel, or abort before subscription | **neutral** (`releasePermit`) |

A successful header alone never settles a probe — success is recorded only after
the stream completes.

Each settle is counted as `gateway_circuit_settle_total{route,outcome}`
(`GatewayCircuitMetrics`), with `outcome` one of `success` / `failure` / `neutral`.
This exists because circuit state alone cannot distinguish a healthy route from a
route whose callers are constantly hanging up: neutral settles are invisible to the
breaker by design, so without this meter a route can churn permits while `/health`
reports `circuitState: CLOSED` and nothing explains why latency or upstream load
looks odd. A rising `failure` rate is the upstream; a rising `neutral` rate is the
clients. The counter increments inside the same compare-and-set that settles the
permit, so the three outcomes always sum to the permits actually taken — no
separate dedup, and no way for the two cancellation paths (`onCancellation` and the
`whenComplete` hook) to double-count. There is deliberately **no alert** on it: with
cancellations neutral, an open circuit already means genuine failures, so the
existing circuit-state signal is the alertable one and this meter is for diagnosis.

The neutral settle matters. A caller that hangs up mid-response carries no
evidence about upstream health, and counting it as a failure is directly
exploitable: `/api/catalog/item` and `/api/catalog/similar` are in the default
`GATEWAY_PUBLIC_PATHS`, so five unauthenticated hang-ups would open the catalog
circuit and 503 the next genuine caller (measured; pinned red by
`GatewayUpstreamResponseTest#cancellationsNeverOpenTheRouteCircuit`). The
buffered implementation was immune only by accident — `aggregate()` subscribed to
the upstream independently of the client, so a disconnect never reached the
breaker. `CircuitBreaker.releasePermit` frees a half-open probe slot without
touching the failure count, so a cancellation neither opens the circuit nor heals
one, and the next real request still gets to probe.

One residual asymmetry: because backpressure now propagates end to end, a
*slow* consumer can stall the upstream read until the client `responseTimeout`
(`GATEWAY_TIMEOUT_MS`) fires. That arrives as a stream error and still counts as a
failure. It is far harder to exploit than a hang-up — it requires holding a
connection open for the whole timeout — and a genuinely stalled upstream is
indistinguishable from it at this layer, so it is deliberately left as a failure.

Failures before final headers retain the existing 502/503 JSON mapping (including
`Cache-Control: no-store`). After final headers have reached the caller, an upstream
failure closes/resets the stream; the gateway cannot replace its status or body.
Consumers must detect incomplete responses. Informational headers do not prevent
pre-final-header recovery. Slow consumers may still occupy upstream connections
and encounter the configured response timeout; streaming does not remove timeout
or network-buffer limits.

### API versioning and deprecation

The public surface is versioned in the path, immediately after `/api`:
`POST /api/v1/recommend`, `GET /api/v1/catalog/item`. The **gateway owns the version** —
backends keep their existing internal paths, so four services do not each reimplement
versioning.

**Edge paths carry API versions; internal paths carry pipeline names.** `/api/v1/recommend`
is API version 1. `/v2/recommend` on 6010, 7010, and 8080 is the *v2 pipeline* — the shared
recall → rank → hydrate → paginate contract that `CrossPathConsistencyTest` pins — and is
**not** API version 2. Internal paths are not part of the public contract and are
unreachable from outside the cluster.

[`ApiVersion`](../../src/main/java/com/recsys/application/gateway/ApiVersion.java) strips a
leading `/api/v{n}` segment, where `{n}` is one to four digits ending at a segment boundary.
Anything else is an ordinary path segment: `/api/version/x` and `/api/v1x/foo` are untouched.
An unversioned `/api` path is **implicit v1**, which is what keeps every existing client
working. An explicit unsupported version returns `400`
(`{"error":"unsupported API version: v2; supported: v1"}`) rather than `404`, matching the
precedent already set by the canonical `/api/recommend` strategy validation.

**Normalization strictly precedes authorization**, at all three entry points
(`GatewayProxyService`, `RecommendationGatewayService`, `LlmProxyService`). `/api/v1/users`
becomes `/api/users` before `authenticator.check` runs, so `PROTECTED_PREFIXES` and
`GATEWAY_PUBLIC_PATHS` keep working with **no versioned entries** and a version segment
cannot be used to slip past the never-public guard. The route table, rate-limit keys, and
circuit-breaker names are likewise unchanged, because route matching still sees
`/api/users`. Registering versioned prefixes in the route table instead would have required
a versioned twin in each of those lists, where a missed entry is a silent auth bypass.

Two entry points need explicit versioned twin registrations, because they are exact/prefix
Armeria routes rather than route-table entries: the canonical `/api/recommend`, and each LLM
route (LLM routes are filtered out of `proxyRoutes`, so the catch-all cannot serve them).

[`ApiDeprecationDecorator`](../../src/main/java/com/recsys/application/gateway/ApiDeprecationDecorator.java)
is a single server-wide decorator adding `Deprecation: true` and `Sunset` to two classes:

| Class | Example | `Link: rel="successor-version"` |
|---|---|---|
| Unversioned spelling | `/api/catalog/item` | yes — `</api/v1/catalog/item>` |
| Back-compat alias route | `/api/v1/catalog/item` | no |

Back-compat alias routes stay deprecated even when versioned, because their deprecation is a
different one: they duplicate the resource-oriented routes. No `Link` is emitted for them —
they strip to different backend paths, so there is no mechanically equivalent successor to
advertise. `/health` and `/metrics` are exempt. The decorator is a no-op when
`GATEWAY_DEPRECATION_SUNSET` is unset, so a `Deprecation` header is never emitted without a
published expiry.

### The compatibility contract

> Consolidated here on 2026-07-28 from a separate `docs/api-compatibility-policy.md`. It was
> split out to address a client audience, but this project has no client inventory — as the
> removal rule below says outright — so the split bought a second place for the same facts to
> drift rather than a genuinely different document. Everything below is unchanged in substance.

Everything above is *how* the gateway implements versioning. This is *what it promises*.

**Additive — may ship at any time, without a version bump:** a new optional request field, a
new response field, a new route, a new value in a field documented as open-ended. Clients must
tolerate unknown response fields; a client that rejects unrecognised JSON keys is not
compatible with this contract.

**Breaking — requires a new version:** removing or renaming a response field, tightening
validation on an existing request field, changing the status code returned for an unchanged
condition, changing default behaviour when a field is omitted, or removing a route.

**Support window.** Two versions concurrently — the current version N and its predecessor
N−1. A third is never promised.

**Notice.** `Sunset` is published when a deprecation is announced, never later, and there is a
minimum of **twelve months** between that announcement and removal.

**Deprecated today:**

| Deprecated | Replacement | Notes |
|---|---|---|
| Any unversioned `/api/...` path | The same path under `/api/v1` | Bodies identical; path change only. The only row where the `Link` header's successor matches this replacement. |
| `/api/catalog/...` (either spelling) | `/api/v1/movies/...` and `/api/v1/recommend` | Not one-to-one — check the route you need. The unversioned form's `Link` points at `/api/v1/catalog/...`, a *mechanical* stop that is itself still deprecated. |
| `/api/model/...` (either spelling) | `/api/v1/recommend` with `{"strategy":"model"}` | Same caveat: the `Link` points at `/api/v1/model/...`, not directly at this replacement. |
| `/api/online/...` (either spelling) | `/api/v1/recommend` with `{"strategy":"online"}`, `/api/v1/features` | Same caveat: the `Link` points at `/api/v1/online/...`, not directly at this replacement. |

**Breaking changes shipped as reviewed exceptions:**

| Date | Change | Why it did not wait for notice |
|---|---|---|
| 2026-07-29 | `GET /api/catalog/item` and `/api/catalog/similar` (either spelling) reject non-canonical numeric parameters — leading zeros, `+`, whitespace, `-0` — and reject `k` outside `[1, 200]` instead of clamping it. Repeated occurrences of `id`, `movieId` or `k` are also rejected. All return a `no-store` 400. | These parameters form the CloudFront cache key. Clamping and alias-accepting made them unbounded cache-busters on the only public, unauthenticated, compute-heavy route, with gateway rate limiting off by default. Waiting twelve months would have left the amplification open for twelve months. Affected callers were already receiving something other than what they asked for — a clamped `k` silently returned fewer results. See `docs/superpowers/specs/2026-07-29-cdn-cache-key-and-edge-config-hardening-design.md`. |

An exception is a reviewed decision recorded here, not a precedent: the default remains a
new version plus twelve months' notice.

**Removal is never automatic.** It is always an explicit, reviewed pull request; nothing in
the gateway expires a route, and a `Sunset` date passing does not by itself change behaviour.
This is deliberate — the project has no client inventory, so it cannot know who is still
calling a deprecated path, and an enforcing sunset would be a scheduled outage for whoever did
not read the header. The date commits to the *earliest* removal, not an automated one.

**Detecting deprecation.** Check for the `Deprecation` header on any response. Failing a build
when a dependency starts returning `Deprecation: true` is the cheapest way to catch it early.

```bash
curl -sI https://<gateway>/api/catalog/item?id=1 | grep -i '^deprecation\|^sunset\|^link'
```

## 2. Identity propagation and credential stripping

The gateway is the **trust boundary**, and
[`buildUpstreamHeaders`](../../src/main/java/com/recsys/application/gateway/GatewayRequestForwarder.java)
is where that boundary is enforced on every proxied request:

- **Credentials are consumed, never forwarded** — `Authorization`, `x-api-key`, and
  the `x-origin-secret` header are stripped, so upstreams never see raw credentials.
  Hop-by-hop headers are dropped too.
- **Client-supplied `x-authenticated-*` headers are stripped** (case-insensitive
  16-char prefix match) — an anti-spoofing defense, since backends may trust those
  headers *only* because the gateway is their sole source.
- **The gateway re-injects the authenticated identity** as
  [`GatewayPrincipal.identityHeaders()`](../../src/main/java/com/recsys/application/gateway/GatewayPrincipal.java):
  `x-authenticated-subject` / `x-authenticated-client-id` / `x-authenticated-token-use`
  (only when non-blank), plus `x-gateway-service: recsys-api-gateway` and the
  `x-forwarded-*` set. The principal comes from a Cognito JWT (claims) or an API-key
  caller (SHA-256 short-hash id), or `anonymous()` for public/disabled paths.

This holds identically for the service proxy and the LLM proxy. Designs:
[credential stripping](../superpowers/specs/2026-07-02-gateway-credential-stripping-design.md),
[principal propagation](../superpowers/specs/2026-07-02-gateway-principal-propagation-design.md).

## 3. Authentication

Edge authentication is the gateway's slice of the broader AuthN/AuthZ concern; the
whole picture — every credential, the operator-token tier, and the service-local
tokens on 8080 — is in the [AuthN/AuthZ investigation](20_AuthN_AuthZ.md).
[`GatewayAuthenticator`](../../src/main/java/com/recsys/application/gateway/GatewayAuthenticator.java)
accepts **either** a static API key (`GATEWAY_API_KEYS`, constant-time compare, via
`X-API-Key` or `Authorization: Bearer`) **or** a Cognito RS256 JWT
(`GATEWAY_COGNITO_ISSUER` / `_AUDIENCE`; a dependency-free verifier that fetches JWKS,
caches keys 5 min, validates `iss`/`aud`/`exp`/`token_use`, and serves last-good keys
stale on a transient JWKS failure). A bearer token is tried as an API key first, then
a JWT; matching neither yields `401 WWW-Authenticate: Bearer`.

Two guards make the open/closed posture safe: `GatewayAuthenticator.fromEnvironment`
**fails closed** — it refuses to start wide-open unless `GATEWAY_ALLOW_ANONYMOUS=true`
explicitly opts in — and `PROTECTED_PREFIXES` (`/api/catalog/user`, `/api/users`) can
never be made public even if listed in `GATEWAY_PUBLIC_PATHS`
([never-public design](../superpowers/specs/2026-07-19-gateway-never-public-user-paths-design.md)).
Public-path matching and the CloudFront origin secret (`GatewayOriginSecret`) are
covered from the edge-caching angle in the
[CDN Edge investigation](12_CDNS.md#3-origin-lockdown--proving-a-request-came-from-our-distribution);
operational auth setup is in [gateway-auth.md](../runbooks/gateway-auth.md).

## 4. Rate limiting

[`GatewayRateLimiter`](../../src/main/java/com/recsys/ratelimit/GatewayRateLimiter.java) is
a token bucket keyed **per `(route, principal)`**, so one noisy caller can't exhaust
another's budget. Buckets refill at `GATEWAY_RATE_LIMIT_RPS` with a
`GATEWAY_RATE_LIMIT_BURST` burst; excess requests get `429`. The principal is the
authenticated identity (Cognito `sub` or a hashed API-key id; `anonymous` when auth
is off). Buckets live in a bounded Caffeine cache (`GATEWAY_RL_MAX_PRINCIPALS`,
default 100000) so a flood of distinct identities can't grow memory without bound.
Per-route overrides use the route name upper-snake-cased:
`model-inference` → `GATEWAY_RATE_LIMIT_MODEL_INFERENCE_RPS`. This is the gateway's
per-instance limiter; how it fits alongside the model per-user, LLM token-budget, and
global Redis limiters is covered in the
[Rate Limiting investigation](08_Rate_Limits.md).

## 5. Resilience the gateway applies (cross-links)

Three edge concerns are documented in depth elsewhere; the gateway is where they
attach:

- **Per-route circuit breakers** — `RouteCircuitBreaker`, one per route, sharing the
  state machine that also backs the LLM proxy and Redis rate limiter. See
  [Fault Tolerance §1](18_Fault_Tolerance.md#1-request-tier-resilience--circuit-breakers-bulkheads-fault-injection).
- **Health-checked upstreams + registry resolution** — `UpstreamEndpointGroups` drop
  a down backend using **GET** probes so a request fast-fails `503` instead of hanging, and the opt-in
  registry resolves upstream addresses dynamically with static fallback. See the
  [Service Discovery investigation](11_Service_Discovery.md).
- **The LLM proxy** — `LlmProxyService` proxies LLM routes on a dedicated client with
  token budgets, SHA-256 response caching, and SSE streaming passthrough. See the
  [SSE Streaming investigation](16_SSE_Streaming.md).

## 6. Health aggregation

`GET /health`
([`GatewayHealthService`](../../src/main/java/com/recsys/application/gateway/GatewayHealthService.java))
probes every registered downstream with **GET**, **in parallel** (latency is max, not sum) and
returns two views:

- a deduped **`ports` rollup** — one entry per distinct backend port, plus the
  gateway's own `8010` as a self-check; a port is `UP` only when *every* route
  targeting it is healthy;
- the full per-route **`services`** detail (status, prefix, health URL, status code,
  latency, `circuitState`, error).

The overall `status` is `DEGRADED` (HTTP `503`) whenever any backend is down — the
gateway's self-check never masks a failing backend. When the service registry is
enabled the response also carries a `registry` section (resolution source + snapshot
age). `/health` is a public path (no auth), so probes always reach it.

The health response and the data-path endpoint groups are separate checks.
Both use GET: catalog and online handlers implement GET and return 405 for
HEAD. Before the endpoint groups explicitly selected GET, `/health` could
report UP while data requests failed with `no healthy endpoint`. Diagnose that
mismatch using probe logs and the deployed version; do not infer endpoint
selectability from the aggregation alone. `GatewayUpstreamHealthCheckIntegrationTest`
uses a GET-only upstream to guard this behavior and runs in the resilience gate.

### Liveness is a different question from readiness

`GET /health/live`
([`GatewayLivenessService`](../../src/main/java/com/recsys/application/gateway/GatewayLivenessService.java))
is a constant `200` that consults nothing. It exists because `/health` above cannot serve as a
liveness probe: it returns `503` whenever any upstream is down, and to kubelet a failing
liveness probe means *kill this container*.

The gateway had all three probes on `/health` until 2026-09. The consequences were real in both
directions:

- **Liveness** (`20s × 3`): an upstream outage of about a minute restarted every gateway pod —
  discarding warm connection pools and circuit state on the one component still able to serve
  the routes whose upstreams were healthy.
- **Startup** (`5s × 24`): a cold start whose upstreams took longer than 120 s to boot — a DR
  region coming up, a full-namespace restart — killed the gateway before it ever started, and
  it restarted into the same condition.

Liveness and startup now target `/health/live`. **Readiness deliberately keeps `/health`**: "is
this pod fit to receive traffic" is a question upstream state legitimately answers. The accepted
cost is that one upstream being down pulls the gateway out of its target group entirely, even
for routes whose upstreams are fine — redundant with `GATEWAY_UPSTREAM_HEALTHCHECK_ENABLED`
and the per-route circuit breakers, which degrade the affected route more precisely. Recorded
as a decision so it is not later mistaken for the same bug.

`/health/live` needs no `GATEWAY_PUBLIC_PATHS` entry and no origin-secret exemption of its own.
Both gates match with the same prefix-with-boundary rule —
`path.equals(p) || path.startsWith(p + "/")`, in `GatewayAuthenticator.matchesPrefix` and
`GatewayOriginSecret.isExempt` — so the existing `/health` entry already covers it. That is
load-bearing and invisible: tightening either match to exact equality would read as a hardening
change and would break every gateway pod's liveness probe in the EKS overlays, where
`GATEWAY_ALLOW_ANONYMOUS=false`. `GatewayLivenessRouteTest` pins it, along with the fact that
the exact route still wins over the `prefix:/` catch-all that would otherwise proxy the probe to
an upstream. `GatewayLivenessManifestTest` pins the probe wiring itself.

## 7. Metrics

The gateway exposes Prometheus at `GET /metrics` (registered before auth, via
`PrometheusExpositionService` on Armeria's default registry). Origin-secret
rejections (`gateway_origin_secret_rejected_total`) and, when the registry is
enabled, the `gateway_registry_*` meters
([`GatewayRegistryMetrics`](../../src/main/java/com/recsys/metrics/GatewayRegistryMetrics.java))
publish through the same registry.

## 8. Testing

- **Routing** — `GatewayRouteTableTest` (canonical strategy mapping, no duplicate
  prefixes, back-compat aliases kept, rewrite targets) and `MicroserviceRouteTest`
  (longest-prefix match, no partial-segment match, prefix-strip + query preservation,
  `healthUri`, service-name assignment).
- **Trust boundary** — `GatewayRequestForwarderTest` (strips spoofed identity and
  injects the principal, strips gateway-consumed credentials, strips the origin
  secret).
- **End-to-end** — `GatewayServerIntegrationTest` on a live server (health 200 + port
  rollup + gateway self, proxies to upstream, canonical recommend default/explicit/
  non-POST, legacy alias proxies, unmatched → 404, circuit-open → 503, auth rejects
  no key) and `GatewayMetricsEndpointTest`.

## Sharp edges — notes

1. **The edge is a single choke point — on purpose.** Everything flows through 8010;
   the mitigations are per-route breakers, health-checked upstreams, and the parallel
   health check. A gateway outage is a full outage, so it runs `minReplicas ≥ 2`
   behind the ALB.
2. **Backends trust `x-authenticated-*` only because the gateway is their sole
   source.** The anti-spoof strip is load-bearing — a backend exposed directly
   (bypassing the gateway) would trust attacker-supplied identity headers.
3. **Auth and rate limit run inside the proxy, not as server decorators.** Only the
   origin-secret check and `/health` `/metrics` are true decorators; auth/rate-limit
   are per-request in `serve`/`forward`, which is why `/health` and `/metrics` are
   reachable without auth.
4. **Route precedence is registration order + longest prefix.** LLM routes and
   `/api/recommend` are registered before the catch-all so they win; adding a broad
   new prefix can accidentally shadow a narrower one if boundaries aren't respected.
5. **`502` vs `503` is a real distinction.** `503` means the gateway declined
   (circuit open / no healthy endpoint / rate limit); `502` means it tried and the
   upstream was unreachable. They point at different problems during an incident.
6. **`/v2` is a pipeline name, and it is now protected like V1.** The canonical
   `POST /api/recommend` with the default `model` strategy forwards to `/v2/recommend` on
   8080, which used to be a bare `pipeline.recommend(query)` passthrough while
   `/api/v1/recommend` carried every request-tier control. Both paths now share them:
   [`ProtectedRecommendationPipeline`](../../src/main/java/com/recsys/application/recommendation/ProtectedRecommendationPipeline.java)
   wraps the `onnxRecommendationPipeline` bean with the per-user `ModelRateLimiter`, the
   `LoadShedder`, `InferenceMetricsService`, and A/B exposure logging, throwing the same
   exceptions so the 429/503 contract is identical to V1's. On 7010, `/v2/recommend` is
   wrapped in `OnlineAdmissionControl` like `/online/recommendation` beside it.
   **The two services shed differently** — 7010 returns `429` + `Retry-After` from
   `OnlineAdmissionControl`, 8080 returns `503` from `ServiceOverloadedException`. Both are
   load shedding; a client seeing one must not assume the other.
   `/v2/sequential/recommend` is deliberately left unwrapped: it is a stub that always returns
   `501`, and recording a failure per call would corrupt the readiness signal.
   Still V1-only, by choice: the submit-token CSRF check and the degraded-cache fallback.
