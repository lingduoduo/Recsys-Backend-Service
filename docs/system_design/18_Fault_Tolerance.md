# Fault Tolerance in Recsys-Backend-Service

An investigation of how the system stays up when things break: how a single
request is isolated from a sick dependency, how each instance sheds load rather
than collapsing, how a degraded answer is served in place of no answer, and how
the deployment survives the loss of a zone or an entire region.

## The big picture

Fault tolerance here is **layered by failure domain** and governed by four rules:

- **Fail fast, never queue unbounded** — a request that cannot be served
  promptly is rejected (`429`/`503` with `Retry-After`) or fast-failed, never
  parked on an unbounded queue that turns a latency blip into an OOM.
- **Degrade, don't collapse** — when a recall channel, a cache, or Redis is
  unavailable, the request path falls back to a non-personalized floor (trending,
  popularity, cold-start, or a cached result) and returns `200` with a partial
  answer instead of erroring.
- **Isolate the blast radius** — circuit breakers, bulkheads, per-channel
  timeouts, and per-`(route, principal)` rate limits keep one bad upstream, one
  slow channel, or one noisy caller from taking down the whole path.
- **Survive zone and region loss** — pod anti-affinity spread, AZ-aware Redis
  reads, PodDisruptionBudgets, and a warm-standby second region keep a zonal or
  regional failure from becoming an outage.

The layers, from the request inward:

| Layer | Mechanism | Failure it absorbs |
|---|---|---|
| Edge / route | `RouteCircuitBreaker`, gateway upstream health-check fast-fail | A dead or failing backend |
| Admission | `OnlineAdmissionControl`, `LoadShedder` (semaphore), rate limiters | Overload / thundering herd |
| Fan-out | `WorkerBulkhead` + bounded queue, per-channel timeout, `ChannelHealthMonitor` | A slow/failing recall channel |
| Data | Redis replica routing + Sentinel, single-flight, bloom/hot-key, MySQL retry | A sick or lagging store |
| Fallback | Cold-start / trending / popularity floors, degrade-to-cache | Empty or unavailable candidates |
| Lifecycle | Graceful drain on SIGTERM, readiness → `503` | Rolling deploys / scale-in |
| Topology | Pod spread, PDB, AZ-aware reads, multi-region warm standby | Zone / region loss |

The recurring philosophy mirrors the [scalability investigation](17_Scalability.md):
keep the request path lock-free, push state into replicable stores, and reject
or degrade fast rather than let a single fault propagate.

## 1. Request-tier resilience — circuit breakers, bulkheads, fault injection

### Circuit breakers

A single CAS-based state machine backs every breaker in the system:
[`CircuitBreaker`](../../src/main/java/com/recsys/resilience/CircuitBreaker.java) is
lock-free with an injectable clock. It is **CLOSED** while consecutive failures
stay below the threshold; once tripped it is **OPEN** until `cooldownMs` elapses,
then **HALF_OPEN**, where `tryAcquirePermit()` admits exactly one probe. Every
admission returns a permit bound to the breaker generation and completion must
present that same permit. Opening or successfully recovering advances the
generation, so a late completion from an older request cannot mutate the
current recovery attempt. State and probe ownership move together in one
immutable CAS snapshot.

[`RouteCircuitBreaker`](../../src/main/java/com/recsys/resilience/RouteCircuitBreaker.java)
wraps one breaker per gateway route (default threshold **5**, cooldown **10 s**;
`GATEWAY_CB_FAILURE_THRESHOLD` / `GATEWAY_CB_COOLDOWN_MS`). In
[`GatewayRequestForwarder`](../../src/main/java/com/recsys/application/gateway/GatewayRequestForwarder.java)
an open circuit fast-fails with `503 "circuit open"`; a `5xx` upstream response
records a failure, anything else records success. The same primitive is embedded
in the LLM proxy and the Redis rate limiter (§4).

Circuit state is visible per route in the gateway health surface:

```bash
curl http://localhost:8010/health | jq '.services["model"].circuitState'
# "CLOSED"    ← healthy
# "OPEN"      ← tripped; fast-failing during the cooldown window
# "HALF_OPEN" ← testing recovery with a single probe
```

### Bulkheads and bounded queues

Recall fan-out is blocking I/O (Redis, DB), so it runs on an isolated pool:
[`WorkerBulkhead`](../../src/main/java/com/recsys/resilience/WorkerBulkhead.java) is a
fixed `ThreadPoolExecutor` over an `ArrayBlockingQueue` with daemon threads and
**no rejection handler** — a full queue throws `RejectedExecutionException`
rather than growing memory. Both serving paths wire one
(`recall-catalog` on 6010, `recall-online` on 7010; pool `= cores × 2`, queue
`RECALL_BULKHEAD_QUEUE_CAPACITY`, default `pool × 4`).

Rejection is a **degrade signal**, not an error: in
[`MultiChannelRecallService`](../../src/main/java/com/recsys/application/retrieval/multichannel/MultiChannelRecallService.java)
a rejected non-primary channel returns an empty result (the merge gap-fills from
the others); only a rejected *primary* channel raises
`PrimaryRecallUnavailableException`. Because the bulkhead saturates at roughly
`cores × 10` tasks — before the 64-slot concurrency gate — 6010 tends to degrade
to partial/empty results (`200`) rather than shed with `429` under recall
pressure. See [overload-protection.md](../runbooks/overload-protection.md).

### Fault injection

[`FaultInjector`](../../src/main/java/com/recsys/resilience/FaultInjector.java) defines
named injection points that can add latency or throw. The production default is
the `NOOP` singleton (wired into `RecallConfig` at both servers), and
`MultiChannelRecallService` calls `maybeInject("channel:" + name)` before each
channel runs — so the degradation and bulkhead tests (§7) can deterministically
make a channel slow or fail without touching production behavior.

## 2. Overload protection — shed fast, never queue unbounded

Layered admission gates keep the serving paths responsive under load instead of
collapsing: a request that can't be served promptly is rejected fast
(`429`/`503` with `Retry-After`), trading a few rejected requests for bounded
latency and protected capacity for the rest.

> **Changed 2026-07-27 — the canonical path is now measured.** `/v2/recommend`, the route
> `POST /api/recommend` actually reaches, was previously unguarded on both 8080 and 7010 while
> its siblings (`/api/v1/recommend`, `/online/recommendation`) were fully protected. It now
> carries admission control on both services, plus the per-user rate limiter and inference
> metrics on 8080. Two consequences worth knowing before you read a graph:
>
> - **`InferenceMetricsService` previously recorded only `/api/v1/recommend`**, so the
>   `high failure rate` and `high inference latency` reasons behind model-serving readiness were
>   computed with none of the canonical traffic in the sample. Readiness may now report degraded
>   where it previously looked healthy — that is the signal being measured correctly for the first
>   time, not a new fault.
> - **The two services shed with different status codes.** 7010's `OnlineAdmissionControl`
>   returns `429` + `Retry-After`; 8080's `ProtectedRecommendationPipeline` throws
>   `ServiceOverloadedException` → `503`. Both mean "shed"; do not alert on only one.
> - **`400`s are not inference failures.** `ProtectedRecommendationPipeline` rethrows
>   `IllegalArgumentException` without recording, mirroring the carve-out in
>   `RecommendationController`. This matters more on the v2 path, which reaches the pagination
>   cursor codec: cursor rejection is entirely client-driven, so counting it would let a client
>   looping malformed cursors drive `recentFailureRate` toward 1.0 and pull a healthy instance
>   out of the load balancer. A readiness degradation therefore reflects real inference trouble,
>   never a burst of bad requests.

### Admission control (concurrency gates)

[`OnlineLoadShedder`](../../src/main/java/com/recsys/loadshed/OnlineLoadShedder.java) is
a lock-free concurrency gate (CAS on an in-flight counter). `tryAcquire()`
rejects when full or shutting down; `shouldDrain()` trips at the drain
utilization or on SIGTERM; `retryAfterSeconds()` is `1` while draining and
`suggestedWeight` = `(1 − utilization) × 100` feeds the load balancer.
[`OnlineAdmissionControl`](../../src/main/java/com/recsys/loadshed/OnlineAdmissionControl.java)
is the Armeria decorator that calls the gate before entering the blocking
executor and returns `429` + `Retry-After` + `{"reason":"concurrency_limit"}`
on rejection. The catalog path (6010) reuses both; there is no separate
`CatalogAdmissionControl` class.

The model server (8080) uses a Spring
[`LoadShedder`](../../src/main/java/com/recsys/loadshed/LoadShedder.java) built on a
`Semaphore` with Micrometer counters. On rejection it first tries
`recommendationService.tryServeFromCache` (degrade-to-cache) and only then throws
`ServiceOverloadedException`, and it emits an `X-Capacity-Weight` header.

| Env var | Default | Purpose |
|---|---:|---|
| `CATALOG_MAX_CONCURRENT_REQUESTS` | `64` | In-flight cap on 6010 `/getrecommendation` before shedding |
| `CATALOG_DRAIN_UTILIZATION` | `0.90` | Utilization where 6010 reports drain to the load balancer |
| `ONLINE_MAX_CONCURRENT_REQUESTS` | `64` | In-flight cap on 7010 |
| `ONLINE_DRAIN_UTILIZATION` | `0.95` | Utilization where 7010 reports drain |
| `RECSYS_HEALTH_MAX_CONCURRENT_REQUESTS` | `64` | Model-serving (8080) semaphore cap (`recsys.health.max-concurrent-requests`) |
| `RECALL_BULKHEAD_QUEUE_CAPACITY` | `4 × recall pool` | Bounded recall queue on 6010 + 7010 before overflow is rejected |

### Gate ordering

The gates compose in a deliberate order — cheapest and most global first — so a
request is stopped as early as possible: **rate limit → admission (concurrency)
→ bulkhead**. This ordering is pinned by
[`OverloadGateOrderingCharacterizationTest`](../../src/test/java/com/recsys/loadshed/OverloadGateOrderingCharacterizationTest.java);
the layered table (gateway token bucket/CB, online admission + global Redis QPS,
model per-user + semaphore, recall bulkhead, timeouts) lives in
[overload-protection.md](../runbooks/overload-protection.md). The gate *knees*
are characterized by the `@Tag("load")` harnesses (§7); tuning them against
real latency curves in a prod-like environment remains deferred.

## 3. Graceful degradation — a degraded answer beats no answer

### Channel health and per-channel timeouts

Every recall channel runs with `RECALL_CHANNEL_TIMEOUT_MS` (default **200 ms**,
applied as `.orTimeout(...)`) and is guarded by
[`ChannelHealthMonitor`](../../src/main/java/com/recsys/application/retrieval/multichannel/ChannelHealthMonitor.java):
after **3** consecutive failures a channel enters exponential backoff
(`base 5 s × 2^(failures−3)`, capped at **60 s**) and is skipped until it
recovers, so one slow/failing channel doesn't stall every request. A timeout,
error, or bulkhead rejection returns an empty channel result — the two-phase
quota merge gap-fills the shortfall from the remaining channels.

**Every layer in this section is inside the fan-out.** The per-channel `exceptionally`, the
health-monitor backoff and the quota gap-fill all protect work dispatched *as a channel*.
`OnlineRecommendationService.recommend` makes three Redis reads outside it — recent history
before recall, the cold-start probe inside `recall()` (whose `catch` names only
`NumberFormatException`), and the trending response snapshot after the fan-out — and on a cold
cache a Redis failure in any of them bypasses all of this and returns 500 with readiness still
green. Measured in
[02_Caching §9](02_Caching.md#9-what-happens-when-redis-goes-down).

**`orTimeout` protects the response, not the thread.** It completes the dependent future
exceptionally without cancelling or interrupting the task already running on the bulkhead;
a task blocked in a socket read would not observe an interrupt anyway. Measured with a
1-thread pool and a 2000 ms task: the caller degraded at 220 ms, and the next task waited
1790 ms for a worker. So "one slow channel doesn't stall every request" holds for the
*request* and not for the *pool* — the Redis command timeout is what bounds worker
occupancy, and it is capped in a different place for each of the three recall services. See
[17_Scalability §2](17_Scalability.md#ortimeout-bounds-the-callers-wait-not-the-work--measured).

[`RecallDegradationMetrics`](../../src/main/java/com/recsys/application/retrieval/multichannel/RecallDegradationMetrics.java)
classifies failures (REJECTED / TIMEOUT / ERROR), tracks
`degradedRatio = degradedRecalls / totalRecalls`, and records four bounded
outcomes. `GET /health/load` retains per-channel operational detail;
Prometheus exposes `recsys_recall_degradation_outcomes_total` with only the
bounded `outcome` tag:

| Outcome | `X-Recall-Degradation-Reason` | Meaning |
|---|---|---|
| healthy | absent | All attempted channels succeeded, including a naturally empty result. |
| partial | `partial` | At least one channel degraded and at least one attempted channel succeeded. |
| all channels | `all_channels` | Every attempted channel degraded; candidate count is not used to infer this. |
| fallback | `fallback` | The Spring overload path recovered the response from cache. |

For partial/all-channel outcomes, `X-Recall-Degraded` remains the sorted,
comma-separated degraded-channel list. Healthy status codes and response
bodies are unchanged; non-healthy recommendation responses retain their
successful status and add the bounded reason header.

### Non-personalized floors and degrade-to-cache

When personalization is unavailable the path still returns something useful:

- **Cold-start** —
  [`ColdStartChannel`](../../src/main/java/com/recsys/application/retrieval/coldstart/ColdStartChannel.java)
  blends trending windows (last_day 0.7, last_month 0.5) with global popularity
  (0.4) for users with no embedding.
  [`QuotaPolicy`](../../src/main/java/com/recsys/application/retrieval/coldstart/QuotaPolicy.java)
  picks a cold quota (weighted toward `cold_start` / `trending` / `popularity`)
  when the embedding is absent or the `userId` is unparseable. Note the quotas bound
  candidate **sources**, not content: they guarantee an embedding-led page still contains
  trending and popularity items, and guarantee nothing about genre spread. Content diversity
  is unimplemented — see "Which serving rules are applied" in [README](../../README.md#which-serving-rules-are-applied).
- **Trending fallback** — if every channel returns empty, the trending snapshot
  is served so the response is never blank.
- **Fail-open stores** —
  [`GlobalPopularityStore`](../../src/main/java/com/recsys/infrastructure/redis/GlobalPopularityStore.java)
  catches a Redis outage and returns an empty list, letting the popularity
  channel fall back to its in-memory `DataManager`.
- **Degrade-to-cache** — the model server tries the result cache before it sheds
  (§2), so an overloaded 8080 can still answer a repeat query from cache.

## 4. Dependency resilience — surviving a sick downstream

### Gateway upstream health-check fast-fail

By default the gateway data path wraps every upstream in a health-checked
Armeria endpoint group
([`UpstreamEndpointGroups`](../../src/main/java/com/recsys/application/gateway/UpstreamEndpointGroups.java)):
each backend is probed on an interval and a down backend is dropped from
selection, so a request to a dead upstream **fast-fails with `503`** instead of
hanging until the timeout. `allowEmptyEndpoints(false)` means an all-unhealthy
group fails selection immediately (`EmptyEndpointGroupException` →
`GatewayRequestForwarder.isNoHealthyEndpoint` → `503`). A single IOException is
retried once after 50 ms (max 2 attempts, never on socket timeout). Host
resolution and the 30 s Cloud Map DNS cache are unchanged.

The probe is a **`GET`** (`useGet(true)`), not Armeria's default `HEAD`. The
catalog and online health handlers are `BaseApiService` subclasses that override
`doGet` alone, so they answer `405` to `HEAD`, which the checker counts as
unhealthy — under the default, both Armeria upstreams were permanently
unselectable and every `/api/catalog`, `/api/users`, `/api/movies`,
`/api/online`, and `/api/features` request fast-failed `503` while the gateway's
own `/health` aggregation (which always used `GET`) reported them `UP`. Model
serving alone survived because Spring MVC maps `HEAD` onto `GET` handlers. The
integration test's stub upstream is now `GET`-only for the same reason: a lambda
`HttpService` accepts every method and cannot see a probe-method mismatch.

| Env var | Default | Purpose |
|---|---:|---|
| `GATEWAY_UPSTREAM_HEALTHCHECK_ENABLED` | `true` | Wrap each upstream in a health-checked endpoint group (set `false` for local dev without all backends) |
| `GATEWAY_UPSTREAM_HEALTHCHECK_INTERVAL_MS` | `10000` | Probe interval |

### Redis resilience

Redis is the hot dependency, so its failure paths are the most developed:

- **Replica routing + Sentinel** —
  [`RedisReadReplicaRouter`](../../src/main/java/com/recsys/infrastructure/redis/RedisReadReplicaRouter.java)
  and [`RoutingRedisExecutor`](../../src/main/java/com/recsys/infrastructure/redis/RoutingRedisExecutor.java)
  keep writes on the single primary while reads prefer the same-AZ replica
  (`AWS_AZ`), fall back to another replica, then fall back to the primary when
  none are configured — so a briefly-unreachable replica AZ degrades read
  latency, not availability. Sentinel
  ([`LettuceClientFactory`](../../src/main/java/com/recsys/infrastructure/redis/LettuceClientFactory.java))
  handles primary leader election; the router handles read fan-out. See
  [Replication](04_Replication.md#1-redis-read-replicas--az-aware-read-routing).
- **Cache-stampede protection** —
  [`SingleFlight`](../../src/main/java/com/recsys/infrastructure/resilience/SingleFlight.java)
  dedupes concurrent same-key recomputations and **fails open** (independent
  compute) on wait timeout, so a hot expired key can't stampede Redis.
- **Cache-penetration / hot-key guards** —
  [`BloomFilterGuard`](../../src/main/java/com/recsys/infrastructure/resilience/BloomFilterGuard.java)
  skips the Redis round-trip for known-absent IDs;
  [`HotKeyDetector`](../../src/main/java/com/recsys/infrastructure/resilience/HotKeyDetector.java)
  flags keys exceeding a per-second threshold for local caching.

### Rate limiters — bounded fail-open with an embedded breaker

[`RedisRateLimiter`](../../src/main/java/com/recsys/ratelimit/RedisRateLimiter.java) is
the only *global* ceiling while Redis is healthy (a weighted sliding-window
counter in Lua). It embeds a `CircuitBreaker` (threshold 5, reset 30 s). A valid
Redis decision is authoritative. On an exception, malformed Redis reply, OPEN
circuit, or lost HALF_OPEN probe race, the request consults one conservative
local emergency token bucket. An emergency allowance is flagged
`failOpen=true`; emergency exhaustion uses the existing `429` with a positive
`Retry-After`.

The emergency budget is per online-serving replica and is inactive on healthy
Redis decisions. Kubernetes defaults it to 50 requests/s with burst 50; absent
values derive one quarter of global QPS (minimum one). Invalid boolean,
negative/non-finite rate, or invalid/negative burst values fail startup.
`ONLINE_REDIS_EMERGENCY_LIMIT_ENABLED=false` or a zero rate/burst restores the
former unlimited fail-open behavior for rollback. Metrics expose only bounded
`source=redis|emergency` and `result=allowed|rejected` tags; `/online/ops`
reports circuit state, emergency configuration, and cumulative counts without
bucket/principal dimensions. The per-`(route, principal)`
[`GatewayRateLimiter`](../../src/main/java/com/recsys/ratelimit/GatewayRateLimiter.java),
per-user [`ModelRateLimiter`](../../src/main/java/com/recsys/ratelimit/ModelRateLimiter.java),
and token-budget [`LlmTokenRateLimiter`](../../src/main/java/com/recsys/ratelimit/LlmTokenRateLimiter.java)
all disable (fail open) when their limit is `0`.

### MySQL read retry and status contract

The read-only catalog pool retries narrowly:
[`MySqlClient.withReadRetry`](../../src/main/java/com/recsys/infrastructure/persistence/MySqlClient.java)
retries **only** transient connection failures
([`MySqlExceptionClassifier`](../../src/main/java/com/recsys/infrastructure/persistence/MySqlExceptionClassifier.java):
`SQLTransientConnectionException` or SQLState class `08`) up to
`MYSQL_READ_MAX_ATTEMPTS` (default **2**), sleeping `MYSQL_READ_RETRY_BACKOFF_MS`
(default **50 ms**) between attempts. Timeouts, auth, and syntax errors — and
row-mapping errors — are propagated without retry. Failures map to a fixed status
contract rather than leaking SQL internals:

| Condition | Status |
|---|---:|
| Invalid `limit`/`genre`, malformed / tampered cursor, cursor–filter mismatch | `400` |
| MySQL disabled, or HikariCP pool unavailable/exhausted (`MySqlPoolUnavailableException`) | `503` |
| Query exceeds `MYSQL_QUERY_TIMEOUT_SECONDS` (`SQLTimeoutException`) | `504` |
| Unexpected SQL or mapping failure | `500` |

### Retry policy, in one place

| Path | Policy |
|---|---|
| gateway → backend | 1 retry, `IOException` **only**, explicitly *not* `SocketTimeoutException`, `Backoff.fixed(50)`, `maxTotalAttempts(2)` |
| gateway → LLM | retry-once on upstream `429`, scheduled non-blocking |
| MySQL reads | retry with 50 ms backoff; not on timeout, auth, or syntax errors |
| **any Redis call** | **none** |

The gateway rule is the load-bearing one and it is right: retrying an `IOException` recovers the
Cloud Map deregistration window, while refusing to retry a timeout is what stops the gateway
amplifying a slow backend into 2× load at the moment it can least absorb it. The cost is
bounded and visible — worst-case upstream time becomes `3 s + 50 ms + 3 s ≈ 6.05 s`, which is
the number that exceeds 7010's own 500 ms ceiling twelvefold
([17_Scalability §2](17_Scalability.md#the-request-time-budget-end-to-end)).

Redis has no retry **by design**: `disconnectedBehavior(REJECT_COMMANDS)` plus
`TimeoutOptions.enabled()` make a disconnected Redis fail immediately rather than buffer, which
is precisely what hands control to the serve-stale caches. Retrying there would convert a fast,
absorbable failure into a slow one.

### The primary-read path has no fallbacks, deliberately

Everything in §3 describes the default read path. When a caller presents a consistency token,
`recommendPrimary` / `recallPrimary` disable **every** one of those layers: an unavailable
channel throws `PrimaryRecallUnavailableException` → 503, replica reads are bypassed, and stale
values are never served. That is the correct trade for read-your-writes, but it means the
consistency path has a strictly worse availability profile than the default path — and it is
also the path carrying the longest wait in the system.

`ConsistencyWaiter.await(eventId, userId, Duration.ofSeconds(2))` polls the lineage key every
50 ms up to a 2 s budget, on the blocking-task thread, inside a request whose Armeria deadline
is **500 ms**. When materialization is fast it returns well inside the deadline. When it is
not, Armeria terminates the response at 500 ms while the handler keeps polling for the
remaining ~1.5 s and then constructs the `202 Accepted` + `Retry-After: 1` that the design uses
to tell the client to come back. Nobody receives it: the client sees a generic timeout instead
of the backpressure signal, and the thread stays held for 4× the request budget. This is
**latent, not live** — `ONLINE_DURABLE_EVENTS_ENABLED` is `"false"` in `k8s/base` and
overridden in no overlay — so it becomes real on the day that flag is turned on, which is
exactly when the `202` contract is being relied upon. Either the wait must be bounded by the
request's remaining budget or `ONLINE_REQUEST_TIMEOUT_MS` must exceed it; the two numbers
cannot both stay as they are.

### LLM proxy timeouts and retry

The LLM proxy
([`LlmProxyService`](../../src/main/java/com/recsys/application/gateway/LlmProxyService.java))
uses a longer 120 s timeout and a non-blocking retry-once on an upstream `429`
that honors `Retry-After` — but only in buffered mode; streaming errors are
surfaced immediately (the SSE path skips retry and caching).

## 5. Graceful shutdown and drain

All four services share a graceful-drain baseline so a rolling deploy or scale-in
never drops in-flight requests. On `SIGTERM`:

- The Armeria servers (6010, 7010) and the Spring model server (8080) flip
  readiness to `503` **before** draining — `markShuttingDown()` on the load
  shedder / [`GracefulShutdownSupport`](../../src/main/java/com/recsys/loadshed/GracefulShutdownSupport.java)
  (a `HIGHEST_PRECEDENCE` `ContextClosedEvent` listener) — so Kubernetes stops
  routing new work while in-flight requests finish.
- [`GracefulServers`](../../src/main/java/com/recsys/loadshed/GracefulServers.java)
  drains with a 1 s quiet period and a 30 s timeout, comfortably under the k8s
  `terminationGracePeriodSeconds: 60`; recall executors drain via
  [`GracefulExecutors`](../../src/main/java/com/recsys/loadshed/GracefulExecutors.java)
  (`RECSYS_EXECUTOR_SHUTDOWN_TIMEOUT_MS`, default 5 s).
- The catalog (6010) and gateway (8010) additionally rely on Kubernetes Endpoint
  removal to stop new routing and simply serve out the drain window.

The model-serving `/health/ready` surface makes each drain / shed reason explicit
(`"shutting down"`, `"overloaded"`, `"high failure rate"`, `"high inference
latency"`), each returning `503`; the controller and its health contract tests are
the authoritative behavior source.

## 6. Multi-AZ and multi-region survival

### Zonal resilience

`topologySpreadConstraints` (`maxSkew:1`, `DoNotSchedule`) force even AZ
distribution; PodDisruptionBudgets ([k8s/base/pdb.yaml](../../k8s/base/pdb.yaml)) keep
`minAvailable:1` for gateway/catalog/online and `maxUnavailable:1` for
model-serving (2 of 3 up during voluntary disruption); AZ-aware Redis reads (§4)
keep a briefly-unreachable zone from stalling the hot read path;
`trafficDistribution: PreferClose` keeps service-to-service traffic same-AZ
best-effort with a cluster-wide fallback. See
[zonal-resilience.md](../runbooks/zonal-resilience.md).

### Multi-region DR

`k8s/eks-us-west-2/` is a warm-standby overlay for a second region (reduced HPA
`minReplicas` — gateway/catalog 1, model 2, online 1 — while inheriting
`maxReplicas` so HPA + cluster-autoscaler can surge on failover). Each region
overlay composes `../base` + the `k8s/eks-shared/` component and overrides only
region-specific values (us-west-2 ECR/ElastiCache/WAF). On failover an operator
**pre-scales** the standby to the primary baseline with
`scripts/dr-standby-capacity.sh promote`, which validates every rendered
workload but applies one canonical payload containing **only the four HPAs**.
It refuses placeholder/inconsistent images, a wrong context/region/endpoint,
ambiguous HPA state, unhealthy rollout/PDB/topology/services, or missing fresh
dependency evidence. `demote` restores only the warm HPA floor; `verify` is an
offline drift guard; `cutover-check` and `failback-check` are read-only
evidence gates; `manifest-digest --target <command>` is the offline helper that
prints the canonical HPA digest those evidence files must carry, so operators
derive it instead of transcribing it. Reports are schema-v1, locked, atomic,
and never overwrite an existing path. Data roles, DNS, and traffic remain explicit operator actions.
The design is
[2026-07-08-multi-region-dr-failover-design.md](../superpowers/specs/2026-07-08-multi-region-dr-failover-design.md);
operations are in [dr-regional-failover.md](../runbooks/dr-regional-failover.md),
[dr-failback.md](../runbooks/dr-failback.md),
[dr-data-tier-promotion.md](../runbooks/dr-data-tier-promotion.md), and
[dr-game-day.md](../runbooks/dr-game-day.md).

## 7. Proving the failure paths

The failure paths are exercised, not just asserted in prose.

The pull-request gate runs:

```bash
mvn --batch-mode validate
mvn --batch-mode -Presilience test
```

Validation enforces Java 17 and dependency convergence. The deterministic
profile excludes `load,docker` and covers breakers, admission, bulkheads,
limiting, degradation, drain, outbox, Saga, and TCC behavior.

**Chaos / fault-exercising unit + integration tests** (run in the default suite):
`CircuitBreakerTest`, `RouteCircuitBreakerTest`, `FaultInjectorTest`,
`WorkerBulkheadTest`, `LoadShedderTest`, `OnlineLoadShedderTest`,
`OnlineAdmissionControlTest`, `GracefulExecutorsTest`, `GracefulServersTest`,
`MultiChannelRecallDegradationTest`, `MultiChannelRecallServiceBulkheadTest`,
`WorkerIsolationFailureTest`, `RedisRateLimiterTest` (fail-open + embedded
breaker), and `GatewayUpstreamHealthCheckIntegrationTest`.

**Scheduled/manual environmental suites** use isolated report directories/jobs
and the exact selectors:

```bash
mvn --batch-mode test -DexcludedGroups=docker -Dgroups=load \
  -Dresilience.evidence.suite=load \
  -Dresilience.evidence.output=target/resilience-measurements-load.json
mvn --batch-mode test -DexcludedGroups=load -Dgroups=docker \
  -Dresilience.evidence.suite=docker \
  -Dresilience.evidence.output=target/resilience-measurements-docker.json
```

The load sidecar proves serving admission, bulkhead rejection, bounded recall
degradation/recovery, timeout recovery, and graceful drain. The Docker sidecar
proves the real Testcontainers Redis/Lua boundary. The schema-v1 summarizer
requires matching suite applicability and fresh Surefire XML and fails on
malformed/empty evidence or false invariants. Shared-runner latency and
throughput remain report-only.

The load / characterization harnesses pin the gate knees:
`OnlineLoadShedderCharacterizationTest`,
`OverloadGateOrderingCharacterizationTest`,
`WorkerBulkheadCharacterizationTest`, plus the per-path load tests
(`EmbeddingRecallLoadTest`, `OnlinePredictionLoadTest`, `V2CrossPathLoadTest`,
`InferenceLoadTest`, `KafkaFlinkPartitionLoadTest`). How to run them is in
[overload-characterization.md](../runbooks/overload-characterization.md).

## 8. Observability — knowing that any of this happened

Sections 1–7 describe mechanisms that degrade, shed and recover. This one is how you find out
they did. Two questions, two tools, and **deliberately no derivation between them**.

**Splunk answers "what happened in this request?"** — a specific `traceId`, a stack trace, one
user's malformed cursor. Per-event, high-cardinality (a `userId` is a fine log field and a
forbidden metric label), delivered **at-most-once**: `SplunkHecAppender` drops on a full queue
or failed POST rather than blocking a serving thread, so a Splunk search is a lower bound on
what happened. Gated entirely on `SPLUNK_HEC_TOKEN`; unset, it is a no-op. Console output is
the authoritative copy. Full design in
[splunk-hec-logging](../runbooks/splunk-hec-logging.md).

**Prometheus answers "is the system healthy right now?"** — rates, saturation, percentiles,
scraped every 15 s and retained so `rate()` means something. It cannot say which user hit the
bug; it can say 12% of requests have been failing for six minutes.

**Neither is derived from the other.** No metric is computed by parsing a log line — there is
no log processor or Prometheus exporter reading `logs/*.log`; and no log line represents a
metric. Gauges and counters are read straight off the same `AtomicLong`/`LongAdder` fields the
request path already updates. The apparent exception proves the rule: `SplunkHecMetrics`
publishes the *appender's own* delivery counters, which is metrics about the logging
pipeline's mechanics, not metrics derived from log content. Operationally: **alerting reads
Prometheus, investigation reads Splunk.** No alert here fires on "an ERROR log appeared".

**Latency and memory appear in both tools, and that is not an exception to the rule.** Splunk
carries edge-triggered *events* — a request that exceeded a threshold, a GC pause, a heap
crossing — each a discrete occurrence with high-cardinality context attached. Prometheus carries
continuous *series* over the same underlying runtime state, sampled independently by a Micrometer
binder or an Armeria decorator. Neither reads the other: no alert parses a log line, and nothing
emits a log line so that a metric can be computed from it. The property that keeps this honest is
that **nothing ships to Splunk on a timer** — a periodic heap sample would be a metric wearing a
log's clothes, and was considered and rejected when this was built.

### 8.1 What each service exposes, and what collects it

| Service | Port | Metrics path | `ServiceMonitor` |
|---|---:|---|---|
| RecSys Serving API (`RecSysServer`) | 6010 | `/metrics` | `recsys-catalog-serving` |
| Online Serving (`OnlinePredictionServer`) | 7010 | `/metrics` | `recsys-online-serving` |
| API Gateway (`MicroserviceGatewayServer`) | 8010 | `/metrics` | `recsys-api-gateway` |
| Model Serving (Spring Boot, `ModelApplication`) | 8080 | `/actuator/prometheus` | `recsys-model-serving` |
| Outbox relay (`OutboxRelayCommand`) | 7020 | `/metrics` | `recsys-outbox-relay` |

The three Armeria mains wire a `PrometheusMeterRegistry` and mount
`PrometheusExpositionService`; the Spring model service uses Actuator's
`/actuator/prometheus` — different path, same format. All five `ServiceMonitor`s live in
[servicemonitor.yaml](../../k8s/base/servicemonitor.yaml), scraping every 15 s.

**Two different selectors are involved and are easy to blur.** Each `ServiceMonitor`'s
`spec.selector.matchLabels` picks the *Service* it scrapes — by the Service's
`metadata.labels`, **not** its pod selector. Separately, `release: kube-prometheus-stack` sits
on the `ServiceMonitor`'s own `metadata`, and is what the Prometheus CR's
`serviceMonitorSelector` matches to decide which monitors it honours at all. Get the first
wrong and the monitor scrapes nothing; get the second wrong and Prometheus never looks at the
monitor.

### 8.2 The scrape gap was three layers deep, and each fails silently

Online-serving and the gateway once published exposition that nothing could collect:

| Layer | What was missing | Symptom without it |
|---|---|---|
| `ServiceMonitor` | No CR existed for `recsys-online-serving` / `recsys-api-gateway` | Prometheus never learns the target exists — no series, no error, nothing to alert on because there is no `up{job=...}` to be `0` |
| Service `metadata.labels` | The two Services had a pod `selector` but no `metadata.labels` | Even with a `ServiceMonitor` present, `spec.selector.matchLabels` matches the **Service's own labels**, not the Service's pod selector — the easy confusion. A monitor pointed at a label no Service carries matches nothing and scrapes nothing, with no error anywhere |
| `NetworkPolicy` ingress | No rule admitting traffic from the `monitoring` namespace | Prometheus's scrape connection is dropped at the network layer. This looks identical to a slow or hung target from Prometheus's side — it just times out |

Fixing any one alone would have changed nothing observable. This is the clearest example in
the repo of instrumentation that looked present — metrics computed, endpoint answering `curl`
— and was observable by nothing.

**The outbox relay was a fifth target and failed differently**, which is the instructive part.
Its `/metrics` always answered *and it looked configured*: the deployment carried
`prometheus.io/scrape: "true"` and friends. Those annotations are a convention for an
annotation-scraping Prometheus that this cluster does not run — a ServiceMonitor-driven
Operator discovers `Service`+`Endpoints` and never reads them, and no `Service` existed for the
relay to select. **Configuration that documents an intent nobody implemented is worse than
none, because it reads as evidence the wiring exists.** The cost was concrete: `up{}` had no
series for the relay, so `RecsysTargetDown` could not cover the very component
`OutboxBacklogGrowing` is about. All three layers now exist and
`ScrapeTargetManifestTest`'s `EXPECTED_SCRAPE_TARGETS` lists five services.

### 8.3 Metric inventory, by subsystem

Every metric is named at its registration call site; **the file is the source of truth, not
this table.**

**Serving** — request-shape and outcome metrics for the online-serving and catalog paths.

| Metric | Registered in |
|---|---|
| `online_serving_qps`, `_failure_rate`, `_rejected_rate`, `_p50_ms`, `_p95_ms`, `_p99_ms` (hand-rolled gauges; the real request-duration histogram, `online_serving_request_duration_seconds`, is in the Runtime table below) | [`metrics/OnlineServingMetricsService.java`](../../src/main/java/com/recsys/metrics/OnlineServingMetricsService.java) |
| `recsys.recall.degradation.outcomes` (dotted → `recsys_recall_degradation_outcomes_total`, tagged `outcome`) | [`application/retrieval/multichannel/RecallDegradationMetrics.java`](../../src/main/java/com/recsys/application/retrieval/multichannel/RecallDegradationMetrics.java) |
| `recsys.pagination.cursor.rejected` (tagged `reason`), `.cursor.legacy.accepted`, `.cursor.previous_key.verified`, `.page.returned` (tagged `terminal`), `.budget.exhausted` | [`application/pagination/RecommendationPaginationMetrics.java`](../../src/main/java/com/recsys/application/pagination/RecommendationPaginationMetrics.java) |

**Gateway** — service-registry resolution health and origin-secret enforcement.

| Metric | Registered in |
|---|---|
| `gateway_registry_services_total`, `_services_resolved`, `_snapshot_age_seconds`, `_refresh_total`, `_refresh_failures_total` | [`metrics/GatewayRegistryMetrics.java`](../../src/main/java/com/recsys/metrics/GatewayRegistryMetrics.java) |
| `gateway_origin_secret_rejected_total` | [`application/gateway/GatewayOriginSecret.java`](../../src/main/java/com/recsys/application/gateway/GatewayOriginSecret.java) |

**Redis** — cache-tier and replication health, independent of application-side hit rates.

| Metric | Registered in |
|---|---|
| `redis_cache_available`, `_used_memory_bytes`, `_max_memory_bytes`, `_evicted_keys`, `_keyspace_hits`, `_keyspace_misses`, `_evicts_only_volatile_keys`, `redis_keyspace_sampled_keys`, `redis_keyspace_sample_available`, `redis_unexpected_persistent_keys` | [`metrics/RedisCacheMetrics.java`](../../src/main/java/com/recsys/metrics/RedisCacheMetrics.java) |
| `redis_replica_lag_available`, `redis_replica_lag_seconds`, `redis_feature_version_min`, `_max`, `_age_seconds`, `redis_feature_version_sample_available` | [`metrics/ConsistencyMetrics.java`](../../src/main/java/com/recsys/metrics/ConsistencyMetrics.java) |
| `recsys_online_rate_limit_decisions_total` (tagged `source`, `result`) | [`ratelimit/RedisRateLimiter.java`](../../src/main/java/com/recsys/ratelimit/RedisRateLimiter.java) |

`redis_feature_version_sample_available` is the same availability-companion pattern as
`redis_keyspace_sample_available` in the row above, and exists for the same reason:
`redis_feature_version_age_seconds` initializes to `0` and is written only by a *successful*
sample from [`RedisFeatureVersionSampler`](../../src/main/java/com/recsys/infrastructure/redis/RedisFeatureVersionSampler.java),
and is deliberately **not** cleared on failure so the last known-good age survives for
diagnosis. Both properties mean a low age is not by itself evidence of fresh data — a
process that never sampled, and one whose sampler died, both read as a small healthy number
forever. Only the companion gauge distinguishes them. Note it is registered by *every*
`ConsistencyMetrics`, including the relay's, which runs no sampler and so reports `0`
permanently; §4's alerts scope to `job="recsys-online-serving"` for that reason.

**Outbox / consistency** — transactional outbox backlog and durable-delivery bookkeeping,
all in the one class because they share the online server's `ConsistencyMetrics` instance.

| Metric | Registered in |
|---|---|
| `outbox_pending_events`, `outbox_in_flight_events`, `outbox_delivery_lag_seconds` (tagged `destination`), `outbox_delivery_failures_total` (tagged `destination`), `async_events_dropped_total` (tagged `event_type`), `consistency_token_validation_total` (tagged `outcome`), `consistency_wait_total` (tagged `outcome`), `consistency_wait_duration_seconds`, `reconciliation_events_total` (tagged `outcome`) | [`metrics/ConsistencyMetrics.java`](../../src/main/java/com/recsys/metrics/ConsistencyMetrics.java) |

**Inference** — the model-serving (8080) request path.

| Metric | Registered in |
|---|---|
| `recsys.inference.requests` (dotted → `recsys_inference_requests_total`, tagged `result`), `.recent_failure_rate`, `.throughput_per_second` | [`metrics/InferenceMetricsService.java`](../../src/main/java/com/recsys/metrics/InferenceMetricsService.java) |
| `recsys.model.onnx.runs` (→ `recsys_model_onnx_runs_total`, tagged `variant`) — one increment per `OrtSession.run`, including the startup smoke inference. The proof that inference happened: a request rate that rises while this stays flat is caches or out-of-vocab fallback ranking, not the model, which is exactly what `InferenceLoadTest` and `scripts/load-test/model-serving.js` assert on | [`application/retrieval/UserTowerInferenceService.java`](../../src/main/java/com/recsys/application/retrieval/UserTowerInferenceService.java) |
| `recsys.model.runtime_load_failures` (→ `recsys_model_runtime_load_failures_total`, tagged `variant`, `phase` ∈ {`warmup`, `request`}) — a variant failed to build; the default variant never reaches this because its failure fails startup | [`application/model/ModelRuntimeProvider.java`](../../src/main/java/com/recsys/application/model/ModelRuntimeProvider.java) (`warmup`), [`application/experiment/VariantRuntimeResolver.java`](../../src/main/java/com/recsys/application/experiment/VariantRuntimeResolver.java) (`request`) |
| `recsys.model.recall.tasks` (→ `recsys_model_recall_tasks_total`, tagged `result` ∈ {`rejected`, `timeout`}, `channel`) — recall channel work that produced nothing: rejected by the bounded executor at submit time, or cancelled with interruption at the channel deadline. `channel` is closed to the configured channel names plus `unknown`; every series is registered at zero | [`application/retrieval/multichannel/RecallTaskMetrics.java`](../../src/main/java/com/recsys/application/retrieval/multichannel/RecallTaskMetrics.java) |

**Background loops** — 7010's fixed-delay loops, guarded so no `Throwable` can cancel them (§9.4).

| Metric | Registered in |
|---|---|
| `recsys.loop.seconds_since_success` (→ `recsys_loop_seconds_since_success`, tagged `loop` ∈ {`shard-topology-refresh`, `redis-feature-version-sampler`, `learner-flush`}) — computed at scrape time from the loop's last-success timestamp, `-1` before the first success; `recsys.loop.failures` (→ `recsys_loop_failures_total`, same tag) — iterations that threw any `Throwable` and were absorbed | [`resilience/GuardedLoop.java`](../../src/main/java/com/recsys/resilience/GuardedLoop.java), bound in `OnlinePredictionServer` |

**Load shedding** — the model-serving semaphore gate.

| Metric | Registered in |
|---|---|
| `recsys.load_shedder.requests` (dotted → `recsys_load_shedder_requests_total`, tagged `result`), `.in_flight_requests`, `.utilization` | [`loadshed/LoadShedder.java`](../../src/main/java/com/recsys/loadshed/LoadShedder.java) |

**Splunk shipping** — the Phase 1 → Phase 2 bridge (§2, §4).

| Metric | Registered in |
|---|---|
| `splunk_hec_events_sent_total`, `_dropped_total`, `_failed_total`, `_indeterminate_total`, `splunk_hec_queue_depth` | [`metrics/SplunkHecMetrics.java`](../../src/main/java/com/recsys/metrics/SplunkHecMetrics.java) |

**These five Splunk metrics nearly went dark on every restart, silently.** Micrometer's
`Gauge.builder`/`FunctionCounter.builder` hold the *state* object as a `WeakReference` by
design; the value *function* is held strongly. `SplunkHecMetrics.register()` originally built
its `SnapshotHolder` as a plain local and no value-lambda closed over it, so nothing held a
strong reference once `register()` returned, and the first GC cleared it. The two meter types
then fail **differently**, which is what makes this dangerous rather than merely broken: a
`Gauge` reading a cleared reference reports `NaN` — visibly wrong — while a `FunctionCounter`
**freezes at its last pre-GC value and reports no error** (`DefaultGauge.value()` returns
`NaN`; `CumulativeFunctionCounter.count()` falls back to a cached `last`). A frozen counter is
indistinguishable from a quiet system, so `SplunkHecDroppingEvents` would have stopped being
able to fire the moment the first GC ran. The fix is `SplunkHecMetrics.RETAINED`, a static
list holding every `SnapshotHolder` for the JVM's life — bounded, deliberate retention, **do
not "clean up" that field** — proven by
`SplunkHecMetricsTest#metersSurviveGarbageCollectionOfTheirBackingState`, which forces a real
GC verified via a canary `WeakReference`.

Every other class in the inventory was checked individually rather than assumed, and each
state object has a lifetime independent of its registration call: `RedisCacheMetrics`' fields
ride along on the long-lived probe callbacks; `ConsistencyMetrics` keeps counters on a `State`
in a static `WeakHashMap<MeterRegistry, State>`, which holds only its *keys* weakly;
`InferenceMetricsService` and `LoadShedder` pass `this` as Spring singletons and additionally
call `.strongReference(true)`. One narrow case is worth flagging rather than papering over:
`gateway_registry_services_total`'s state is a `names` list built only inside `register()` —
the same shape as the bug — and it survives today only because a sibling meter's lambda
happens to close over the identical list. That is real protection but **incidental**;
rewriting that sibling would silently reintroduce the bug in that one gauge.

**Two naming conventions coexist deliberately.** Most metrics are registered with
Prometheus-native `snake_case`; a handful (`recsys.inference.*`, `recsys.load_shedder.*`,
`recsys.recall.degradation.*`, `recsys.pagination.*`) use dotted Micrometer convention, which
the Prometheus registry converts to underscores on exposition. Both end up snake_case on the
wire.

**Runtime** — JVM and request-duration metrics. Nothing bound these before 2026-08-18: Armeria's
`PrometheusMeterRegistries.configureRegistry` is a no-op, so heap usage, GC pause time and thread
counts were unscrapeable on 6010, 7010 and 8010 while looking entirely present. Only the model
service had them, from Actuator's auto-configuration.

| Metric | Registered in |
|---|---|
| `jvm_memory_used_bytes`, `_committed_bytes`, `_max_bytes`, `jvm_gc_pause_seconds`, `jvm_threads_live_threads`, `system_cpu_count` (and the rest of Micrometer's JVM binder set) | [`metrics/JvmMetricsBinder.java`](../../src/main/java/com/recsys/metrics/JvmMetricsBinder.java) |
| `online_serving_request_duration_seconds`, `catalog_serving_request_duration_seconds`, `api_gateway_request_duration_seconds` | Armeria `MetricCollectingService`, mounted in each service's main |

`JvmMetricsBinder` is idempotent per registry because `PrometheusMeterRegistries.defaultRegistry()`
is a JVM-wide singleton that more than one caller may reasonably ask for the JVM metrics on, and
`JvmGcMetrics` installs a JMX notification listener per bind — a second bind on the same registry
would double-count every pause. A second bind on a genuinely different registry still gets its own
listener.

`RequestDurationHistogram` closes a gap the decorator alone leaves open:
`MetricCollectingService`'s default `DistributionStatisticConfig` publishes only client-side
`quantile=` series (`_count`/`_sum`/`_max` plus nine `quantile=` lines) for `*.request.duration`
timers, not the `_bucket{le=}` series `histogram_quantile(...)` needs — an alert built against
buckets that don't exist would sit in Prometheus looking like coverage and never fire.
`RequestDurationHistogram.configure(registry)` installs a `MeterFilter` giving every
`*.request.duration` meter explicit `serviceLevelObjectives` buckets (50ms, 100ms, 250ms, 400ms,
500ms, 1s, 2s, 5s), and it must run before the first such meter is registered on that registry — a
`MeterFilter` only affects meters registered after it is added — which is why it precedes both
`SplunkHecMetrics.register` and `JvmMetricsBinder.bindTo` in all three Armeria mains.

**Queues** — the bounded queues on the message path. Before 2026-08-25 neither published
anything: `AsyncEventPublisher` computed `queue.size()` into a `Snapshot` that reached no
registry, and `WorkerBulkhead` had no metrics at all, its depth served as JSON by
`CatalogLoadService` and read by no collector. Drops were visible only after the fact, so a queue
filling up was invisible until it overflowed.

| Metric | Registered in |
|---|---|
| `recsys_queue_depth`, `_capacity`, `_utilization` (all tagged `queue`), `recsys_queue_rejected_total` (tagged `queue`, `reason`) | [`metrics/QueueMetrics.java`](../../src/main/java/com/recsys/metrics/QueueMetrics.java) |

Registered queues: `recall-catalog` (6010), `recall-online` (7010), and `ab-exposures` (8080,
[`QueueMetricsConfig`](../../src/main/java/com/recsys/config/QueueMetricsConfig.java)). The two
recall bulkheads don't size from a fixed constant: `RECALL_BULKHEAD_QUEUE_CAPACITY` defaults to
`poolSize * 4`, and `poolSize` is itself `availableProcessors() * 2` — so the 64 you'll see on an
8-core instance is a derived number, not a repo-wide default; read the formula on different
hardware, not the figure. `ab-exposures` defaults to `ASYNC_EVENT_QUEUE_CAPACITY`'s 10 000, same
as every `AsyncEventPublisher`.

**This registered set moved on 2026-08-29.** The 7010 `AsyncEventPublisher` was previously
registered as `async-events`, but **registered is not the same as fed**: nothing on 7010 ever
called `publish()` on that instance (`OnlineOpsService` only reads its `snapshot()`; the online
serving path that was meant to publish to it is wired to a different object or to `null` instead
— see [07_Message_Queue §4](07_Message_Queue.md#4-producers-and-event-envelopes)). So every
`recsys_queue_*{queue="async-events"}` series was structurally zero, which is indistinguishable
on its own from a healthy, idle queue — the exact failure mode this metrics work exists to close
(see the class javadoc on `QueueMetrics`). The registration moved to the model service's
`abExposurePublisher` bean instead: `AbExposureLogger` genuinely calls `publish()` on it, so
`ab-exposures`'s series move with real A/B-exposure traffic. The 7010 producer gap itself is
unchanged and still real — `/online/features` still doesn't publish feature-view events — that is
a separate, deliberately deferred product decision, not something this move fixes; see
07_Message_Queue §4.

- `capacity` is the **effective** bound, not the configured one — both constructors clamp
  `Math.max(1, n)`, so a requested `0` yields a one-entry queue and the metric says `1`; both now
  log a WARN when the clamp engages, naming the requested and effective values.
- `reason` separates `full` from `shutdown` and `invalid_key` — only `full` is a saturation
  signal, and `async_events_dropped_total` remains the all-reasons total, so it is **not** a pure
  saturation signal.
- Each reason is counted by its own monotonic `AtomicLong`, not derived as
  `dropped - shutdown - invalid_key`. An earlier draft of this branch did the subtraction; reading
  three independent counters as if they were one untorn triple let a concurrent reader observe a
  transiently negative `full`, and a Prometheus `FunctionCounter` reads any decrease as a
  **reset**, so the next `increase()` comes out spuriously large. The subtraction looked harmless
  and was the shape that shipped first — worth remembering next time a "just subtract the others"
  counter looks tempting.
- Registration's duplicate-name handling is three-way, not a plain throw. Re-registering the
  **same `Source` instance** under a name already claimed is a no-op — same object, same
  readings, so a defensive double-call costs nothing. A **different** `Source` under that name
  still throws `IllegalStateException`, because Micrometer would otherwise silently return the
  first meter and discard the second source — measured, not assumed. Making registration
  idempotent across the board (any duplicate name accepted) was rejected: it would reproduce, in
  `QueueMetrics`'s own layer, exactly the silent aliasing Micrometer already does one level down.
  `QueueMetrics.unregister(registry, queueName)` is the explicit way to replace a registration —
  it removes the four meters from `registry` **before** removing the name from the internal map,
  so a failure partway through leaves the name still claimed and the next `register()` call fails
  loudly instead of aliasing onto stale meters.
- `QueueMetrics.register` also throws `IllegalArgumentException` if `source.capacity()` is not
  strictly positive, but that guard is **unreachable from both production `Source`s** —
  `WorkerBulkhead` and `AsyncEventPublisher` each clamp their constructor argument with
  `Math.max(1, n)` before `capacity()` can ever report anything but a positive number (see the
  clamping bullet above). Don't read the guard's presence as meaning a bad
  `RECALL_BULKHEAD_QUEUE_CAPACITY`/`ASYNC_EVENT_QUEUE_CAPACITY` env var fails the boot — it
  doesn't; it silently becomes a one-entry queue instead, logged at WARN.

### 8.4 Alerts

Twenty-three, in six groups, all in [prometheus-rules.yaml](../../k8s/base/prometheus-rules.yaml). Every
expression was checked against a real metric name in `src/main/java` first — an alert on a
metric that is never emitted looks like coverage and can never fire.

| Alert | Means | Likely cause | First response |
|---|---|---|---|
| `RecsysTargetDown` | `up{namespace="recsys"} == 0` for 5 m — Prometheus cannot scrape a target at all | Pod crashed, is stuck starting, or its metrics endpoint hung | `kubectl get pods -n recsys`, then the pod's logs for startup errors. **Every other alert in this file is blind for that service until this resolves** — its blind spot is the mirror image of §3.2: `up` only exists for targets Prometheus already knows about, so a `ServiceMonitor` that was never created produces no series to be zero. `ScrapeTargetManifestTest` covers that side; this alert covers a target that existed and stopped answering. |
| `SplunkHecDroppingEvents` | `increase(splunk_hec_events_dropped_total[10m]) > 0` for 2 m — the bounded queue filled and events were discarded | A burst of log volume outran the drain thread, or Splunk itself is slow/down | These log events are gone for good (at-most-once, §2) — check `SPLUNK_HEC_QUEUE_CAPACITY` headroom and whether Splunk is reachable; raise the capacity if this is a recurring burst pattern rather than a one-off. |
| `SplunkHecIndeterminateDelivery` | `increase(splunk_hec_events_indeterminate_total[10m]) > 0` for 2 m — a batch was sent but never acknowledged | Usually a read timeout, or `stop()` running past its 2 s drain budget on shutdown | Delivery is unknown, not lost — check whether the events actually landed in Splunk before assuming loss; if this correlates with deploys, it is probably shutdown timing, not a live outage. |
| `GatewayRegistryStale` | `gateway_registry_snapshot_age_seconds > 120 or gateway_registry_snapshot_age_seconds == -1` for 5 m — the gateway is resolving upstreams from an old registry snapshot, or has never completed one | Redis is unreachable from the gateway, or `SERVICE_REGISTRY_REFRESH_MS` polling has stalled | Check Redis availability from the gateway pod and `gateway_registry_refresh_failures_total`; the gateway falls back to static routes when a service is unregistered, so this is a staleness warning, not an outage by itself. |
| `OnlineServingShedding` | `online_serving_rejected_rate > 0.1` for 10 m — admission control is rejecting more than 10% of traffic on a sustained basis | Genuine overload, or `ONLINE_MAX_CONCURRENT_REQUESTS` set too low for real traffic | Check `recsys_load_shedder_utilization` and the pod's CPU; this is a partial-degradation warning (90%+ of traffic still succeeds), not a full outage. |
| `RedisCacheUnavailable` | `redis_cache_available == 0` for 5 m — the cache stats probe cannot reach Redis at all | Redis pod down, or a NetworkPolicy egress rule blocking the connection | Check Redis pod status and the relevant service's egress rule in `k8s/base/network-policy.yaml`; serving degrades to stale-or-empty results depending on the path (§1 of [02_Caching](02_Caching.md)) rather than erroring outright. |
| `RedisReplicaLagHigh` | `redis_replica_lag_seconds > 10 or redis_replica_lag_available == 0` for 10 m — replica reads are stale, or the lag probe itself can't tell | Replica pod slow/overloaded, or a network partition to the replica | Check replica pod status and network connectivity between replicas; a read routed to a lagging replica can contradict a write that already succeeded elsewhere. |
| `OutboxBacklogGrowing` | `outbox_pending_events > 1000 and delta(outbox_pending_events[15m]) > 0` for 3 m — more than 1000 events pending **and** still rising | The outbox relay is falling behind its publish target (Kafka or SNS) | Check `outbox_delivery_failures_total` and `outbox_delivery_lag_seconds` to identify which destination is backed up; eventual-consistency windows widen for as long as this holds. |

**Serving data-freshness SLOs** — five more in the `recsys.data` group, watching the *data* at
two boundaries rather than the request path. Incident procedure in
[serving-data-freshness](../runbooks/serving-data-freshness.md). Internal objectives, not
customer SLAs.

| Alert | Means | Likely cause | First response |
|---|---|---|---|
| `OnlineFeatureDataStale` | `redis_feature_version_age_seconds{job="recsys-online-serving"} > 60` for 5 m | The Flink online-feature job is stalled, restarting, or falling behind its Kafka sources | 60 s is the entire stale-on-error budget `OnlineFeatureStore` may serve within, so this means five minutes outside serving's own contract. Check the Flink job's state and checkpoints first, then Kafka consumer lag. Serving continues on bounded stale data — restore the writer, do **not** delete feature keys. |
| `OnlineFeatureDataCriticallyStale` | Same series `> 300` for 5 m — **critical** | The feature view has effectively stopped advancing | Treat recommendations as built from abandoned data. Same diagnosis order; also check whether Redis lost the feature keyspace entirely. |
| `OnlineFeatureVersionSampleUnavailable` | `redis_feature_version_sample_available{job="recsys-online-serving"} == 0` for 5 m | Redis unreachable from online serving, no `*:updated_at` keys exist, or the sampler never started | **The two alerts above are blind while this fires.** The age gauge is frozen at its last good value, so a low age is stale evidence, not fresh data. Cross-check `RedisCacheUnavailable`: if both fire, this is a Redis connectivity incident. |
| `OutboxDeliveryLatencyHigh` | `outbox_delivery_lag_seconds_max{destination="kafka_online"} > 30` for 10 m | Relay throughput, Kafka partition leadership churn, or a contended MySQL outbox table | `_max`, not the bare meter name — Micrometer exposes a Timer as `_count`/`_sum`/`_max`. It is a *decaying window* max, so a relay that stops delivering **entirely** falls back toward 0 and does not raise this; that case is `OutboxBacklogGrowing`'s. The two are complementary. |
| `OutboxDeliveryFailuresSustained` | `increase(outbox_delivery_failures_total{destination="kafka_online"}[5m]) > 0` for 10 m | A destination that keeps rejecting — auth, unknown topic, timeout, serialization | Events stay durable in the MySQL outbox; the consistency window widens until delivery recovers. Check relay logs for the failure class. Do not clear the outbox table to silence it. |

Two properties of that group are deliberate and easy to "fix" into being wrong. **The `job`
selector on the three feature alerts is load-bearing**: every process building a
`ConsistencyMetrics` registers the feature gauges, including the relay, which runs no sampler
and reports a permanent `0` — unscoped, `OnlineFeatureVersionSampleUnavailable` would page
forever about a process never meant to observe the feature view. And
**`OutboxDeliveryFailuresSustained` inverts the range/`for:` rule below on purpose**: the
counter increments per failed *attempt* and the relay retries, so a short burst is routine and
must not page; measured with `promtool`, a four-attempt burst under `[10m]`/`for: 10m` fires
spuriously at t=11 m, while a 5 m window cannot span a 10 m hold.

**Runtime and request-latency alerts** — five more in the `recsys.runtime` group, watching the
request path and the JVM underneath it rather than data at rest or the freshness boundary above.

| Alert | Means | Likely cause | First response |
|---|---|---|---|
| `JvmHeapPressureHigh` | Heap `used / max` over the heap area only (non-heap pools excluded, and pools with no maximum, which report `-1`, are filtered before the division) `> 0.90` for 10 m | A leak, an undersized heap for real traffic, or a burst of unusually large responses | Cross-check the Splunk heap-pressure events for the same service and window — `GcEventTracker` logs the same crossing independently, so agreement between the two rules out a metric artifact. Expect GC pause time to follow. |
| `JvmGcTimeFractionHigh` | `rate(jvm_gc_pause_seconds_sum[5m])` — stop-the-world seconds per second of wall time — `> 0.10` for 10 m | Usually heap pressure; check `JvmHeapPressureHigh` on the same instance first | If heap pressure isn't also firing, look at allocation rate and object lifetime rather than heap sizing. Measured detection latency on a sustained overshoot is roughly 13–15 m depending on how far past the threshold the true rate sits — see the comment in `prometheus-rules.yaml`, measured with `promtool`, not derived from a formula. |
| `RequestLatencyP99High` | `histogram_quantile(0.99, ...)` over each service's own request-duration buckets, against a per-service threshold (0.4 s online serving, 1 s catalog serving, 2 s gateway, 1 s model service) for 10 m. **All four services now have a branch.** The first three name `online_serving_request_duration_seconds_bucket`, `catalog_serving_request_duration_seconds_bucket`, and `api_gateway_request_duration_seconds_bucket` — all three come from `MetricCollectingService`/`RequestDurationHistogram` on the Armeria mains. The fourth queries the Spring model service's (8080) own `http_server_requests_seconds_bucket` — `application.yml` sets `management.metrics.distribution.percentiles-histogram.http.server.requests: true`, so `/actuator/prometheus` exposes it (confirmed by running the service and inspecting the scrape: 207 series, on Micrometer's own exponential bucket boundaries, not the other three's explicit 0.05/0.1/.../5 SLO set). That metric name is generic — every Spring Boot app emits it — so this branch is scoped with `namespace="recsys"` the same way `JvmHeapPressureHigh`/`JvmGcTimeFractionHigh` are scoped below, and additionally excludes Actuator's own `/actuator/prometheus` scrape traffic with `uri!~"/actuator.*"` so that fast self-scrapes (every 15 s) don't dilute the p99 of real serving traffic. | A slow downstream, a GC pause, or genuine load past capacity | Search Splunk with `outcome=slow` for the same window to see which routes and requests — except on the gateway, where `route` is the catch-all pattern for every proxied request (see the Splunk runbook), so group by `service` there instead, or correlate with the backend's own event. Check `JvmGcTimeFractionHigh` on the same instance before assuming a downstream fault. Measured detection latency on a clean sustained step is roughly 11 m, essentially the `for:` window with no extra `rate()`-side delay — a different shape from the counter-rate alert above and not a number to generalize from. |
| `RecsysQueueFillingUp` | `recsys_queue_utilization > 0.7` for 10 m — a bounded queue (`recall-catalog`, `recall-online`, or `ab-exposures`) is sustained above 70% full | The consumer is draining slower than the queue fills: the recall workers for a bulkhead, or the `async-event-publisher` drain thread and its broker for `ab-exposures`. Unlike the earlier `async-events` registration on 7010 (see §8.3), `ab-exposures` is genuinely fed, so sustained pressure here is real signal, not a structurally-zero series | Check whether the consumer has actually slowed before raising `RECALL_BULKHEAD_QUEUE_CAPACITY` or `ASYNC_EVENT_QUEUE_CAPACITY` — a larger queue buys latency, not throughput, and just delays the same rejection if the consumer is the real problem. Nothing has been lost yet; this is early warning, not evidence of loss. |
| `RecsysLoopStale` | `recsys_loop_seconds_since_success > 300 or == -1` for 5 m — a guarded background loop has not completed successfully for ten of its 30 s intervals, or never has | The loop's body is failing every time (Redis down for that call path), or the loop stopped — which, with `GuardedLoop`, can only mean its executor is gone | `recsys_loop_failures_total{loop}` says whether it is failing or absent; the loop's own WARN line carries the stack. For `redis-feature-version-sampler` cross-check `OnlineFeatureVersionSampleUnavailable`; for `shard-topology-refresh`, a reshard published meanwhile is invisible to this instance until it recovers. The `-1` branch fires on an instance that never completed a first run — a boot that stalled before the first interval, not a loop that died. |
| `RecsysQueueRejecting` | `increase(recsys_queue_rejected_total{reason="full"}[10m]) > 0` for 3 m — a queue is discarding work now | Genuine saturation, or a burst that outran the queue's bound | The `reason="full"` label match is deliberate and load-bearing: `reason="shutdown"` is excluded on purpose, since both queue implementations count late-arriving submissions during a clean drain under that reason, and seeing it during a rolling deploy is expected, not a page. Cross-check `recsys_queue_utilization` for how long pressure had been building. |

**ONNX model serving** — four in the `recsys.model` group, from the
[2026-09-03 hardening](../superpowers/specs/2026-09-03-onnx-serving-hardening-design.md).
Incident procedure in [model-artifact-rollout](../runbooks/model-artifact-rollout.md). All four
are scoped to `job="recsys-model-serving"` explicitly, because two of the metric names are not
the model service's alone: `recsys_load_shedder_requests_total` is also emitted by catalog
serving's admission control, and `http_server_requests_seconds_bucket` by every Spring Boot
application a shared Prometheus might scrape.

| Alert | Means | Likely cause | First response |
|---|---|---|---|
| `ModelServingUnavailable` | `kube_deployment_status_replicas_ready{deployment="recsys-model-serving"} == 0 or absent(up{job="recsys-model-serving"})` for 5 m — no ready model-serving pod. **Two branches because `up == 0` cannot express this**: Prometheus Operator discovers targets through the Service's Endpoints, and Endpoints exclude pods that are not Ready, so a fleet with no ready pod has no `up` series to be zero. `absent()` says it with nothing but our own `ServiceMonitor`; the kube-state-metrics branch says it directly (and distinguishes "scaled to zero") but depends on a collector this repo does not ship, so it is never the only branch. | Every pod failing readiness — most likely a control-variant artifact that fails manifest, checksum, ONNX-contract, or smoke-inference validation, which fails startup by design | Pod status, then the first startup log lines: the loader names the exact file and check that rejected the bundle. Roll `current` back to the previous generation (runbook §6). The gateway's model routes fast-fail with 503 meanwhile. |
| `ModelServingShedding` | Rejected ÷ all admission decisions `> 0.05` over 10 m, `and` at least 100 decisions in the window, for 10 m. The ratio of two `rate()`s is `NaN` on an idle instance and `NaN > 0.05` is false, so idleness never fires; the volume guard is what keeps two requests a minute with one rejected (50%, and noise) from paging. | Genuine overload — the HPA reacts to CPU, not to this — or `RECSYS_HEALTH_MAX_CONCURRENT_REQUESTS` (8 per two-CPU pod) set below what the pod can actually run | Rejected requests are served from the degraded cache when one exists and 503 otherwise, so this is partial degradation. Scale out first; raise the cap only with `InferenceLoadTest` numbers from the same CPU shape. |
| `ModelInferenceLatencyHigh` | p95 of `/api/v1/recommend` and `/v2/recommend` over `http_server_requests_seconds_bucket` `> 0.5` for 10 m — the 500 ms readiness budget (`RECSYS_HEALTH_MAX_AVG_LATENCY_MS`), so it pages before instances start draining themselves. The `uri` matcher is load-bearing both ways: Actuator's own scrape would dilute the quantile downward, and a slow `/api/v1/token` is not inference. | Recall channels timing out and feeding ranking late, CPU throttling from oversubscribed ONNX threads, or genuine load | Check `recsys_model_onnx_runs_total` against request rate (is the model actually running, or are caches answering?), then `recsys_model_recall_tasks_total{result="timeout"}`. ONNX runs one intra-op thread per request by design (`RECSYS_MODEL_ONNX_*`). |
| `ModelRuntimeLoadFailure` | `increase(recsys_model_runtime_load_failures_total[15m]) > 0` for 5 m, by `variant` and `phase` — one failed variant load. **Range longer than the hold, the inverse of `OutboxDeliveryFailuresSustained`**, and deliberately so: there, one retry blip is routine and only continuing failure should page; here, one failed load *is* the event — it happens at warm-up or once per cooldown, never continuously — so a hold longer than the range would let the single increment age out before it was ever satisfied (§8.4's knife edge). It self-clears when the window slides past the increment. | A treatment bundle that fails manifest, checksum, contract, or smoke validation | The pod stays ready and that bucket is served by control (`recsys_abtest_variant_fallback_total`) until the retry after the 60 s cooldown succeeds. The log names the artifact and the check. A *control* failure never reaches this alert — it fails startup and is `ModelServingUnavailable`'s. |


The four latency thresholds differ per service rather than sharing one value because a threshold at or
above an enforced request timeout can never fire — the reason this group pushes the trap list
below to four entries. The model service's 1 s is the odd one out even among the four: unlike
the other three, it isn't derived from any enforced request timeout — `application.yml` sets none
for inbound requests (`timeout-per-shutdown-phase` governs graceful shutdown, not per-request
processing) — so 1 s is a chosen value, picked to match catalog serving's since both share the
same 500 ms `SLOW_REQUEST_LOG_THRESHOLD_MS` default.

**A 15 s scrape cannot see a queue that fills and drains between samples.** `recsys_queue_depth`
and `_utilization` catch sustained pressure; `recsys_queue_rejected_total` catches the bursty case,
because a counter records an event a sampled gauge can miss entirely. A flat depth graph is
therefore half the picture, and the two alerts are complementary rather than redundant. The
remaining gap is a queue that repeatedly reaches ~95% and drains without ever rejecting: invisible
to both. Closing it needs a peak-depth metric, which was deliberately deferred — see
[the design doc](../superpowers/specs/2026-08-25-queue-backpressure-observability-design.md) for
why, including the hot-path cost and the decaying-max sharp edge already documented for
`OutboxDeliveryLatencyHigh`.

**Four traps this file fell into once, worth not repeating elsewhere:**

- **`for:` cannot equal the `increase()`/`delta()` range window.** An isolated spike ages out
  of the range and flips the expression false *before* `for:` is satisfied, so the alert can
  only fire on a continuously regenerating condition — which defeats alerting on a burst. Fix
  by shrinking `for:` well below the range (2–3 m against 10–15 m), not widening the range.
  `promtool check rules` does not catch this.
- **A gauge that goes `NaN` on failure silently defeats a `>` comparison.**
  `RedisReplicaLagHigh` once read only `redis_replica_lag_seconds > 10`; an unreachable
  replica reports `NaN`, and `NaN > 10` is false — so the worst case the alert existed to
  catch produced no alert. The fix checks the paired availability gauge explicitly.
- **A sentinel value defeats `>` the same way, without needing missing data.**
  `GatewayRegistryStale` once read only `> 120`; `GatewayRegistryMetrics` reports a literal
  `-1` when the gateway has never completed a refresh, and `-1 > 120` is false — so a gateway
  that never resolved anything stayed silent while one three minutes stale correctly paged.
  Ask what a gauge reports for "never happened yet", not only "happened too long ago".
  Note `GatewayRegistryStale` **cannot fire in any configuration this repo ships**:
  `SERVICE_REGISTRY_ENABLED` defaults false and appears nowhere under `k8s/`, so the gauge is
  never registered.
- **A threshold above an enforced timeout can never fire, and `promtool` cannot see it.**
  `RequestLatencyP99High` uses 0.4 s for online serving where catalog serving, the gateway, and
  the model service get 1 s, 2 s, and 1 s, because `OnlinePredictionServer` sets
  `requestTimeoutMillis` from `ONLINE_REQUEST_TIMEOUT_MS` (default 500 ms) — the histogram is
  bounded by the timeout, so a 1 s threshold there is unreachable. An unreachable threshold
  passes a near-miss case perfectly and looks like coverage, which makes this invisible to every
  mechanism in §8.6. If a service gains an explicit request timeout, its latency threshold has
  to move with it. The model service is the mirror case, and worth naming explicitly: it has no
  enforced request timeout at all, so its 1 s threshold cannot become unreachable this way — but
  it also cannot be *derived* the way the other three can, which is why its value is justified
  differently (matching catalog serving's `SLOW_REQUEST_LOG_THRESHOLD_MS`) rather than tied to a
  timeout.

`prometheus-rules.test.yaml` (run by `promtool test rules` in
[prometheus-rules.yml](../../.github/workflows/prometheus-rules.yml)) gives every alert both a
firing case and a **near-miss** that must not fire — the near-miss matters more than it looks,
because an expression that fires on everything passes a fire-only suite while being useless.

### 8.5 What is deliberately absent

Read this before assuming any of it works.

- **No Grafana.** Nothing renders or validates a dashboard. A panel querying a renamed metric
  fails silently, showing a flat line that looks like normal quiet.
- **No log-derived metrics, no metric-derived logs** — restated because it is the
  load-bearing design decision, not a gap.
- **No tracing backend.** `traceId` exists in MDC but only the Spring model service populates
  it (`TraceIdAspect` is Spring AOP; the three Armeria mains have no container to weave into),
  and there is no collector — no Jaeger, Zipkin or OTel — anywhere. **Do not assume
  distributed tracing works**: a `traceId` search surfaces model-service events only and
  cannot follow a request across the gateway → backend hop it actually took.
  The slow-request events added in 2026-08 inherit this limit exactly: they carry `traceId`
  where MDC has one, which is the model service only. A slow gateway request and the slow
  backend request it caused **cannot be correlated by `traceId`** — only by timestamp, service
  and route. The field being present on some events and absent on others invites precisely the
  wrong inference, so do not read its absence as "this request had no trace".
- **No Prometheus.** Every `ServiceMonitor` and the `PrometheusRule` are CRs a **Prometheus
  Operator** interprets, and nothing here installs one. `kubectl apply` succeeds, the objects
  exist, and nothing evaluates them. **A committed alert file is evidence that alerts are
  written, not that anything evaluates them** — the same shape of failure as §8.2's scrape gap.
- **The gateway's `/metrics` is public by design, and the NetworkPolicy rule added for
  Prometheus is redundant.** `GatewayOriginSecret.EXEMPT_PATHS` includes `/metrics`, the EKS
  ALB routes `/` to the gateway, and that policy already had an ingress rule on 8010 with **no
  `from:`** — all sources were already admitted. So the full exposition is reachable from the
  internet behind only WAF. This bullet exists so nobody reads "ingress rules added for
  Prometheus" as "gateway metrics are access-controlled".

### 8.6 How this stays honest

Six mechanisms, each closing a gap the others cannot see. `RecsysTargetDown` catches a target
that existed and went silent — blind to one that never existed, since `up{}` has no series for
a `ServiceMonitor` never created. **`ScrapeTargetManifestTest`** closes exactly that blind
spot statically for all three layers of §8.2, in the `-Presilience` gate. **`promtool`**
actually executes the alert expressions against synthetic series — the only mechanism proving
an expression behaves as its prose claims — but it takes both the metric names inside those
expressions and the fixtures' `job` labels on faith. **`QueueMetricWireNamesTest`** closes the
first of those gaps: it scrapes a real `PrometheusMeterRegistry` after `QueueMetrics.register`
and asserts the exact Prometheus wire names, including the `_total` suffix Micrometer's client
appends to the `recsys.queue.rejected` `FunctionCounter`, then asserts every `recsys_queue_*`
identifier named in an alert `expr:` is one the code actually emits — the §8.4 first-trap failure
shape (a metric that looks emitted and is emitted by nothing) closed for this file's own alerts
rather than assumed away. **`PromtoolJobLabelManifestTest`** closes the second: it pins every
`recsys-`-prefixed `job` label in the promtool fixtures to a Service some `ServiceMonitor` in
`k8s/base` actually selects, and separately asserts no `ServiceMonitor` declares `jobLabel` — the
fact that lets the Operator default `job` to the Service name in the first place, and the one a
future manifest change could silently invalidate without failing the first assertion.
**`DocumentationIndexTest`** keeps the docs indexed, though it cannot check that content still
matches code.

None proves the whole chain: a Prometheus Operator that silently stopped evaluating rules, or
a cluster whose `serviceMonitorSelector` uses a different `release` label, slips past all
six. Neither of the two newer tests narrows that gap — `PromtoolJobLabelManifestTest` checks a
`ServiceMonitor`'s `job`-label consequences against other manifests in this repo, not the
`release` label a live Prometheus CR matches against, and `QueueMetricWireNamesTest` scrapes an
in-process `PrometheusMeterRegistry`, not a cluster. That gap is inherent to testing manifests
that assume infrastructure this repo does not provision.

## 9. Exceptions versus JVM Errors — what each boundary actually does

Everything above is written in terms of *exceptions*: a channel throws, a Redis call times
out, a bundle fails validation. Java has a second failure family the code almost never names —
`java.lang.Error`: `OutOfMemoryError`, `StackOverflowError`, `NoClassDefFoundError`,
`ExceptionInInitializerError`, `AssertionError`. They are not `Exception`s, so
`catch (Exception e)` — this repo's dominant idiom, 73 sites — does not see them, and
`catch (RuntimeException e)` — 46 sites — does not either. Whether that matters depends
entirely on *which boundary* the Error reaches, and the answer differs per boundary. This
section records what was measured (2026-09-03, JDK 17, Armeria 1.28.4, Spring Boot 3.3.4),
not what the type hierarchy suggests.

### 9.1 The census

| Idiom | Sites in `src/main` | Sees a JVM `Error`? |
|---|---:|---|
| `catch (Exception …)` | 73 | No |
| `catch (RuntimeException …)` | 46 | No |
| `catch (Throwable …)` | 14 | Yes — all in background loops that must not die (`WorkerBulkhead.submit`, the outbox relay and its cycle loop, the three Redis probes, the Splunk drain thread, `TransactionalMySql`, `DeliveryAttempt.cancel`) |
| `catch (Exception \| Error …)` | 1 | Yes — `OnlineAdmissionControl`, to release the admission permit on any failure before rethrowing |
| `instanceof Error` rethrow | 1 | `TransactionalMySql.propagate`: rolls back, then rethrows the Error unwrapped rather than wrapping it in a `RuntimeException` |
| Thread `UncaughtExceptionHandler` | 0 | JVM default: stack trace to stderr, nothing else |
| `-XX:+ExitOnOutOfMemoryError` / `CrashOnOutOfMemoryError` | 0 | Not set anywhere; `-XX:+HeapDumpOnOutOfMemoryError` is set in `config/jvm/*.jvmopts` (local runs only), not in any `JAVA_OPTS` under `k8s/` |

The 21 custom exceptions all extend `RuntimeException` (or `IllegalArgumentException`); the
only custom checked type is `RowMappingException extends SQLException`. Checked exceptions
cross method boundaries as `SQLException` (30 `throws`), `IOException` (17), `OrtException`
(12). All 26 `catch (InterruptedException …)` sites restore the interrupt flag.

### 9.2 Request boundaries: both frameworks convert Errors to 500 and keep serving

Measured with a real server in each case, throwing `IllegalStateException`, `AssertionError`,
`NoClassDefFoundError`, `StackOverflowError` and `OutOfMemoryError` from a handler.

**Armeria (6010, 7010, 8010).** Every service body is `try { … } catch (Exception e) { 500 }`
(`BaseApiService`, `CatalogService`, `RecommendationService`, `ShardedRecordService`, …), so an
Error escapes the body. Armeria then answers **`500 Internal Server Error`** and the server keeps
listening — for a throw directly out of `serve()` and for the
`HttpResponse.of(CompletableFuture.supplyAsync(…))` shape alike, and for `OutOfMemoryError`
and `StackOverflowError` too. Armeria does *not* rethrow "fatal" errors or leave the request
hanging, which was the working assumption before measuring. Pinned by
`ArmeriaErrorBoundaryTest`.

**Spring MVC (8080).** `GlobalExceptionHandler`'s catch-all is `@ExceptionHandler(Exception.class)`,
which by type cannot match an Error — yet the client receives the handler's own
`{"error":"internal server error"}` body with **500** for all five cases. The reason is one
layer down: `DispatcherServlet.doDispatch` catches `Throwable` from the handler and wraps it in
`ServletException("Handler dispatch failed")` before consulting the exception resolvers, and
*that* is an `Exception`. The cause chain is logged. Pinned by `SpringErrorBoundaryTest`,
against the real embedded Tomcat — MockMvc would have thrown the wrapper at the test instead.

Consequence: on the request path, the `catch (Exception)` idiom is *adequate*. An Error
becomes one failed request, counted by `InferenceMetricsService`/the Armeria decorators exactly
as an exception would be, and the `finally`/`whenComplete` permit releases in
`RecommendationController`, `ProtectedRecommendationPipeline` and `OnlineAdmissionControl` run
for Errors as they do for exceptions (`finally` is Throwable-agnostic; `OnlineAdmissionControl`
spells it out as `catch (Exception | Error)`).

### 9.3 Executor boundaries: the answer depends on the submission idiom

Measured with the JDK executors the code uses:

| Idiom | An `Error` thrown by the task… | Where it is used |
|---|---|---|
| `ScheduledExecutorService.scheduleWithFixedDelay` | **Cancels the schedule permanently and silently.** The Error is stored in the `ScheduledFuture` nobody reads; no log line, no thread death, no further runs. A `catch (Exception)` inside the task does not intercept it. | 11 sites — see 9.4 |
| `ThreadPoolExecutor.execute(Runnable)` | Kills that worker thread (stack trace to stderr via the default handler), pool replaces it, next task runs normally | `WorkerBulkhead.submit`, `OutboxRelay.submitTerminal` — both already wrap the body in `catch (Throwable)` |
| `ExecutorService.submit(Callable)` | Captured; `get()` throws `ExecutionException(cause = the Error)` | `MultiChannelRecallService` (recall channels): treated as a degraded channel, correct |
| `CompletableFuture.supplyAsync`/`runAsync` + `join()` | Captured; `join()` throws **`CompletionException`**, which *is* a `RuntimeException` wrapping the Error | 12 sites: the Armeria service bodies (framework answers 500, §9.2) and `ModelRuntimeProvider.warmUp` |

The `CompletionException` rewrap is the one place the type hierarchy inverts: an
`OutOfMemoryError` inside async work reaches a `catch (RuntimeException)` at the `join()` as a
plain runtime failure. In `warmUp` this means a treatment variant whose load throws an *Error*
is **not** isolated the way a `RuntimeException` is (the `catch (RuntimeException)` sits inside
the lambda, before the rewrap), so it propagates out of `warmUp` and fails startup — for an OOM
that is the right outcome, and it is noted here so nobody reads the treatment-isolation
guarantee as covering Errors.

### 9.4 Where a JVM Error silently kills something that is supposed to keep running

This is the actual exposure. Eleven fixed-delay loops keep this system's shared state fresh;
by the first row of 9.3 an Error in any of them stops that loop for the life of the process
with no signal, and the code's guard decides whether that can happen:

| Loop | Guard in the task | An Error here means… | Does anything notice? |
|---|---|---|---|
| `RedisReplicaLagProbe`, `RedisCacheStatsProbe`, `RedisPersistentKeyProbe`, outbox relay cycle | `catch (Throwable)` | nothing — the loop survives by design | n/a |
| `WatchdogLock.renewLease` | **was** `catch (Exception)`, now `catch (Throwable)` | *Before:* renewal stopped; Redis expired the lease on schedule; the holder's `held` flag stayed `true` because `markOwnershipLost` ran only from the dead task — two holders, neither told. *Now:* the schedule survives, and `isHeld()` also checks the local lease deadline at call time, so a renewal that never runs at all still reads as lost. | `hasLostOwnership()`; no metric (per-lock labels would be unbounded, and the class has no production caller today) |
| `RedisFeatureVersionSampler` | **was** `catch (RuntimeException)`, now `GuardedLoop` + `catch (RuntimeException \| Error)` in `sample()` | *Before:* sampling stopped with both freshness gauges frozen at "healthy". *Now:* an Error flips `redis_feature_version_sample_available` to 0 and the schedule continues | `recsys_loop_seconds_since_success{loop="redis-feature-version-sampler"}`, `RecsysLoopStale` |
| `ShardTopologyProvider.refresh` | **was** `catch (Exception)`, now `GuardedLoop` | *Before:* topology never refreshed again; a later reshard was invisible until restart. *Now:* last-good is kept and the next refresh runs | `recsys_loop_seconds_since_success{loop="shard-topology-refresh"}`, `RecsysLoopStale` |
| `LearnerFlushScheduler.tryFlush` | **was** `catch (Exception)`, now `GuardedLoop` | *Before:* flushes stopped, and the class's `Snapshot` has no consumer anywhere. *Now:* the schedule continues and failures are counted | `recsys_loop_seconds_since_success{loop="learner-flush"}`, `RecsysLoopStale` |
| `ServiceRegistrar.heartbeat` | `catch (Exception)` | this instance's registry key expires after `SERVICE_REGISTRY_TTL_MS` | Yes — the gateway falls back to the static address and reports `source: static` |
| `ServiceRegistryProvider.refresh` | `catch (Exception)` | gateway keeps its last snapshot | Yes — `gateway_registry_snapshot_age_seconds` grows and `GatewayRegistryStale` fires |
| `CapacityController.tickSafely` | `catch (RuntimeException)` | reference implementation, not started in production | n/a |

How likely is an Error in one of these? `OutOfMemoryError` is the realistic one: these threads
allocate (Lua results, JSON, scan cursors) and an OOM is raised on whichever thread happens
to fail the allocation, not on the thread that caused the pressure. The three probes and the
relay were written with exactly that in mind — the same author's `SplunkHecAppender` says so in
its comment — and the other five were not. The sampler, topology refresh, and learner flush now use
[`resilience/GuardedLoop`](../../src/main/java/com/recsys/resilience/GuardedLoop.java): a
`Runnable` wrapper that absorbs every `Throwable` but `ThreadDeath`, logs and counts it, and
exposes the loop's health as a value **read at scrape time** from a success timestamp
(`recsys_loop_seconds_since_success{loop}`, `-1` before the first success) rather than written by
the loop — so a loop that stops running shows a growing age instead of a frozen healthy number.
`WatchdogLock` separately catches renewal failures and checks its local lease deadline
in `isHeld()`; it does not use `GuardedLoop`.
`GuardedLoopTest` pins that a scheduled loop keeps running after a `StackOverflowError`; each
loop's own test pins its Error case, and `WatchdogLockTest`'s two new cases were run against the
old code first (mutation check: both fail there).

### 9.5 The process boundary: an OOM now exits, and liveness answers one question

Until 2026-09 no container set `-XX:+ExitOnOutOfMemoryError`, so a JVM that had thrown
`OutOfMemoryError` kept running. §9.2 shows request threads keep answering (500s while heap is
short, 200s once GC recovers something), and, per §9.4, a background loop could die at the same
moment and stay dead after the heap recovered. Nothing restarted the container; the operator saw
`JvmHeapPressureHigh` and `JvmGcTimeFractionHigh` (§8.4) plus a climbing 5xx rate, and restarted
the pod by hand.

Every `k8s/base` workload now sets `-XX:+ExitOnOutOfMemoryError` — the four services, the outbox
relay and the reconciliation CronJob — so an OOM becomes a process exit and a restart the
platform already knows how to perform. `OomPolicyManifestTest` derives the workload set from the
manifests rather than listing it, so a service added later without the flag fails the build, and
pins that the `Dockerfile` entrypoint still expands `$JAVA_OPTS` unquoted: the flag and its
delivery live in different files and neither fails on its own.

Three limits are worth stating plainly.

**The exit skips the drain.** `-XX:+ExitOnOutOfMemoryError` halts the JVM without running
shutdown hooks, so the §5 graceful-drain sequence does not run and in-flight requests are
dropped — `terminationGracePeriodSeconds: 60` and `preStop: sleep 5` buy nothing on this path.
Accepted deliberately, but the reason differs by workload and only one of the two reasons is
about replicas:

- **The four serving deployments** each run multiple replicas behind a readiness-gated Service
  (gateway 2, catalog 2, model 3, online 2), so losing one replica's in-flight requests is a
  partial failure of a service that is already failing.
- **The outbox relay is `replicas: 1`**, so that argument does not apply to it at all. What
  makes a hard exit safe there is the lease: `OUTBOX_RELAY_LEASE_SECONDS: "30"` bounds how long
  rows claimed by the dead process stay unavailable, and `runOnce` claims-then-delivers, so an
  exit mid-flight is at-least-once redelivery rather than loss. The reconciliation CronJob is
  the same shape — `restartPolicy: Never` with `backoffLimit: 3` turns the exit into an ordinary
  Job retry, and a reconciliation pass is idempotent.

Two second-order effects on the CronJob are worth knowing rather than fixing:
`activeDeadlineSeconds: 600` is wall-clock *across* retries, so an OOM late in a run eats the
retry budget and the Job ends `DeadlineExceeded`; and `RECONCILIATION_LEASE_SECONDS: "300"`
exceeds the Job's backoff intervals, so a retry can hit a still-held lease and skip exactly the
events the dead run had claimed, exiting 0. Both are dormant today — `RECONCILIATION_REPAIR` is
`"false"`, so no lease is taken.

**It does not fire on every OOM-adjacent failure.** It triggers when the JVM throws
`OutOfMemoryError`. A native allocation failure inside `onnxruntime` is a SIGSEGV that kills the
process anyway (below), and a JVM merely thrashing GC without throwing is untouched — the heap
and GC alerts remain the signal for that case.

**In containers, this flag preempts the Error boundaries of §9.1–§9.4 for OOM specifically.**
`GuardedLoop` advertises surviving an `OutOfMemoryError` raised on its thread so the next
iteration runs, and `RedisFeatureVersionSampler` and `SplunkHecAppender` say the same. With the
flag set the JVM exits at throw time, so that branch is unreachable under Kubernetes — it stays
reachable for local runs, where `config/jvm/*.jvmopts` set `HeapDumpOnOutOfMemoryError` and
never the exit flag. Those guards are not redundant: they still absorb every *other* `Error`,
which is what they were written for. Read §9.3–§9.4 as "what happens to a non-fatal Error", and
this section as "what happens to an OOM", rather than as two answers to one question.

**The claim this section used to make about liveness was wrong for one service.** It said every
liveness probe in the system was a constant `200 OK` that inspects nothing. That held for
`HealthController.liveness` (model serving), `OnlineServices.Live` (7010), the relay's
`/health/live`, and catalog serving's `/health` (`RecommendationService.Health`, a constant
`{"ok": true}` under an inconsistent name). It did not hold for the gateway:
`k8s/base/api-gateway.yaml` pointed `livenessProbe`, `readinessProbe` **and** `startupProbe` at
`/health`, which is `GatewayHealthService` — a 503 whenever any upstream is down. So the gateway
had the opposite defect to the one this section describes: an upstream outage lasting past
`20s × 3` restarted every gateway pod, discarding warm pools and circuit state on the one
component still able to serve the routes whose upstreams were healthy, and a cold start whose
upstreams took longer than the startup probe's `24 × 5s` budget killed the gateway before it
ever started. Liveness and startup now target a constant-200 `/health/live`
(`GatewayLivenessService`); readiness keeps `/health` by decision, since "should traffic come
here" is a question upstream state legitimately answers. `GatewayLivenessManifestTest` enforces
that split and, generally, that no liveness path in `k8s/base` is outside the allow-list of
known constant-200 handlers.

Heap dumps were considered and not added. `-XX:+HeapDumpOnOutOfMemoryError` is set only in
`config/jvm/*.jvmopts`, which containers do not read (they take `JAVA_OPTS`), and the container
root filesystem is read-only with only an `emptyDir` `/tmp` writable — so a dump from model
serving's 2 GiB heap would land on ephemeral storage, vanish with the pod, and risk pressuring
the node into a disk-eviction while the workload is already degraded. Dumps need a persistent
volume and a retention story before they are worth enabling.

The ONNX native layer is the other process-level case and behaves differently from all of the
above: a fault inside `onnxruntime` is a SIGSEGV, not a Java `Error`. No `catch` sees it; the
process dies with an `hs_err_pid` file and Kubernetes restarts it — which is, ironically, the
cleanest failure mode in this section. `OrtException` (checked) is the only ONNX failure Java
code can observe, and it is handled at every call site.

### 9.6 What to conclude

- **Request path:** correct as written. `catch (Exception)` is enough because both frameworks
  convert an escaping Error into a 500 and survive; the two boundary tests pin that.
- **Background loops:** four of the eleven (`WatchdogLock`, `RedisFeatureVersionSampler`,
  `ShardTopologyProvider`, `LearnerFlushScheduler`) could be killed silently by one Error, and two
  of those (`WatchdogLock`, `RedisFeatureVersionSampler`) failed in the direction of *reporting
  health they no longer had*. Fixed: the three 7010 loops run through `GuardedLoop` and publish a
  scrape-time age that `RecsysLoopStale` watches; the watchdog catches `Throwable` and checks its
  lease deadline on `isHeld()`. `ServiceRegistrar`/`ServiceRegistryProvider` keep their
  `catch (Exception)` because their failure is already externally visible (§9.4);
  `CapacityController` is not started in production.
- **Process:** the OOM policy is now decided. Every `k8s/base` workload sets
  `-XX:+ExitOnOutOfMemoryError`, turning "a JVM in an unknown state, with some threads dead,
  reporting live" into a restart the platform already handles. The cost is the §5 drain: an
  exit skips it, and that was accepted rather than avoided (§9.5). The same change corrected
  this section's liveness claim, which was wrong for the gateway — its liveness and startup
  probes were wired to the upstream-aware `/health` and now target `/health/live`.

## Sharp edges — status

1. **Bulkhead saturates before the concurrency gate (by design).** On 6010 the
   recall bulkhead (~`cores × 10` tasks) fills before the 64-slot admission gate,
   so heavy recall pressure degrades to partial/empty results (`200`) rather than
   a clean `429`. Documented in
   [overload-protection.md](../runbooks/overload-protection.md); intentional,
   not a bug.
2. **Gate knees are characterized, not yet prod-tuned.** The `@Tag("load")`
   harnesses pin the mechanism (64-concurrency / 0.95-drain / 200 ms-timeout)
   against the box that runs them; tuning those constants against real prod-like
   latency curves remains deferred.
3. **DR authority remains manual.** The script can mutate only the four HPAs
   and can prove cutover/failback prerequisites. Operators own data-tier roles,
   DNS, and traffic. A checked-in placeholder image intentionally blocks real
   cluster commands until all overlays use one approved immutable digest.
4. **`RedisRateLimiter` global bound assumes NTP-synced clocks.** The weighted
   sliding window is stamped from each instance's wall clock, so the ≈1×-the-limit
   bound holds only under synced clocks (proven by the Docker Redis/Lua boundary
   probe). It is the only global ceiling while Redis is healthy; the emergency
   fail-open ceiling is per-instance and moves with replica count.
5. **`CapacityController` is a reference, not a production controller.**
   [`application/autoscaling/CapacityController`](../../src/main/java/com/recsys/application/autoscaling/CapacityController.java)
   and the `infrastructure/autoscaling` ASG model are tested simulations — real
   scaling and failover capacity come from EKS HPA + cluster-autoscaler and the
   manual DR promote above.
- **The consistency path's `202` is unreachable when it matters** (§4). A 2 s
  `ConsistencyWaiter` poll inside a 500 ms Armeria deadline means a slow materialization yields
  a generic timeout, not the designed `202 + Retry-After`. Latent behind
  `ONLINE_DURABLE_EVENTS_ENABLED=false`; live the day the token path is enabled. Fixing it
  means changing one of two numbers, and which one depends on whether the token path is meant
  to be slower than the default path — a product question, not a tuning one.
- **Observability assumes infrastructure this repo does not provision** (§8.5). Every
  `ServiceMonitor` and the `PrometheusRule` are CRs a Prometheus Operator interprets, and
  nothing installs one; a committed alert file is evidence alerts are written, not that
  anything evaluates them.
- **A JVM `Error` in a fixed-delay loop cancels the loop silently** (§9.4) — closed for the four
  loops that let it (`GuardedLoop` + `RecsysLoopStale`; `WatchdogLock` checks its lease deadline
  on `isHeld()`), but the rule stands for any *new* scheduled body: guard with
  `catch (Throwable)` or `GuardedLoop`, never `catch (Exception)`, and have the loop's freshness
  read at scrape time rather than written by the loop. `ServiceRegistrar` and
  `ServiceRegistryProvider` still use `catch (Exception)` deliberately: their failure is
  visible without a guard (registry key expiry, `GatewayRegistryStale`).
- **OOM policy: resolved, with two accepted residuals** (§9.5). Every `k8s/base` workload sets
  `-XX:+ExitOnOutOfMemoryError`, so an OOM'd JVM exits and is restarted instead of serving on
  with dead background threads. Residual one: the exit skips the §5 drain, dropping in-flight
  requests, which is the better of the two bad outcomes on a replica that is already failing.
  Residual two: the gateway's *readiness* probe stays coupled to upstream health, so one
  upstream down still pulls the gateway from its target group even though
  `GATEWAY_UPSTREAM_HEALTHCHECK_ENABLED` and the per-route circuit breakers already degrade
  that route more precisely. Left by decision, not oversight.
