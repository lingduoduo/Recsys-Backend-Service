# Scalability in Recsys-Backend-Service

An investigation of how the system scales: the compute tier (stateless services
behind HPA), the data tier (consistent-hash shards, read replicas, Kafka
partitions), and the overload-protection layers that keep each instance from
collapsing before autoscaling catches up.

## The big picture

Scalability here is **multi-dimensional and layered**:

- the **compute tier** scales *out* — stateless services behind Kubernetes HPA;
- the **data tier** scales *horizontally* — consistent-hash record shards,
  Redis read replicas, and Kafka partitions;
- a stack of **overload-protection layers** keeps each instance responsive while
  HPA reacts.

The recurring philosophy: keep the request path lock-free and stateless, push
state into shardable/replicable stores, and **reject fast rather than queue
unbounded** when saturated.

## 1. Compute-tier scaling — HPA is the real autoscaler

Four `autoscaling/v2` HPAs ([k8s/base/hpa.yaml](../../k8s/base/hpa.yaml)), all on
CPU/memory Resource metrics — no custom/external metrics, no KEDA, no
Prometheus-adapter.

| Service | min→max | CPU target | Mem target | Behavior |
|---|---|---|---|---|
| api-gateway | 2→6 | 70% | 80% | scaleDown stabilize 120s |
| catalog-serving | 2→8 | 70% | 80% | scaleDown stabilize 120s |
| **model-serving** | 3→6 | **60%** | 80% | scale-up 2 pods/60s (stabilize 120s); scale-in 1 pod/120s (stabilize 300s) |
| online-serving | 2→8 | 70% | 80% | scaleDown stabilize 120s |

**model-serving is the sensitive one** — heaviest pod (requests 500m/2Gi, limits
2/3Gi; longest 180s startup budget for ONNX model load), lowest CPU target, most
aggressive scale-out with deliberately conservative scale-in.

Supporting mechanisms:

- **Zonal spread** — identical `topologySpreadConstraints` on all four
  deployments (`maxSkew:1`, `topology.kubernetes.io/zone`, `DoNotSchedule`,
  `nodeTaintsPolicy: Honor`) force even AZ distribution.
- **Topology-aware routing** — `PreferClose` (`trafficDistribution`) on the three
  internal ClusterIP backends prefers same-AZ endpoints to cut inter-AZ traffic;
  the gateway is deliberately excluded (external WAF ALB edge).
- **PodDisruptionBudgets** — `minAvailable:1` for gateway/catalog/online;
  model-serving uniquely uses `maxUnavailable:1` (keeps 2 of 3 up during
  voluntary disruption).
- **DR warm standby (us-west-2)** — a strategic-merge patch drops `minReplicas`
  to ~50% of primary (gateway/catalog/online 2→1, model 3→2) while inheriting
  `maxReplicas` so HPA + cluster-autoscaler can surge to full capacity on
  failover. On cutover an operator **pre-scales** the standby back to the primary
  baseline via `scripts/dr-standby-capacity.sh promote`, which applies the
  `k8s/eks-us-west-2-active` overlay **HPA-documents-only** (`kubectl kustomize |
  filter HorizontalPodAutoscaler docs | apply -f -`, deliberately *not* `apply -k`,
  so it never rolls Deployments to the out-of-band placeholder image digest);
  `demote` restores the warm floor and `verify` is an offline drift guard against
  `k8s/base`.

**Two things labeled "autoscaling" that are NOT controllers:**

- `infrastructure/autoscaling/AutoScalingGroup` + `InstanceProvisioner` — an
  AWS-ASG-style node-fleet abstraction with **clamp-only bounds and AZ balancing,
  but no metric-driven desired-capacity computation and no cooldowns**.
  `setDesiredCapacity(int)` takes the target as a caller-supplied argument; the
  only guardrails are `ScalingConfig.clamp` (min/max) and least-loaded-AZ
  placement. Externally triggered. The missing signal→capacity "brain" now exists
  as a **tested reference** — `application/autoscaling/CapacityController` does
  target-tracking (`desired = ⌈running × util ÷ 0.7⌉`) with a surge step and
  **asymmetric cooldowns** (60s scale-out / 300s scale-in); `AsgCapacityActuator`
  drives this `AutoScalingGroup` and `OnlineCapacitySignalSource` maps an
  `OnlineCapacityService` snapshot to a `(qpsUtilization, overloaded)` signal — but
  **no server schedules it**, so HPA is still the only autoscaler in production.
- `health/OnlineCapacityService` — **observability of static sizing
  assumptions**, not a controller. It surfaces constants (`ONLINE_TARGET_DAU`
  2,000,000 / `ONLINE_PEAK_QPS` 8,000 / `ONLINE_PEAK_TPS` 20,000) plus live
  `qpsUtilization` / `headroomQps` / `overloaded` on the operator-gated
  `/online/ops` surface. Peak TPS ≫ QPS because Kafka absorbs bursty writes and
  Flink compacts them into Redis aggregates, while stateless (HPA-scalable)
  instances serve peak read QPS.

## 2. Overload protection — the layers that let it scale without collapsing

Documented in [docs/runbooks/overload-protection.md](../runbooks/overload-protection.md).
All gates are **per-instance** (aggregate cluster capacity = per-instance ×
replicas) and fail fast with `Retry-After`.

### Admission control (concurrency gate)

`loadshed/OnlineLoadShedder` — lock-free CAS loop on an `AtomicInteger` in-flight
counter. Defaults: **64 concurrent** (`ONLINE_MAX_CONCURRENT_REQUESTS`), drain at
the **0.95 application default** (`ONLINE_DRAIN_UTILIZATION`); the base
Kubernetes ConfigMap overrides online serving to **0.90**, and catalog 6010
uses **0.90** via `CATALOG_*`. `OnlineAdmissionControl` (Armeria decorator)
returns **429** +
`Retry-After` past the ceiling. `suggestedWeight = round((1−util)×100)` feeds ALB
target weights so a saturated node bleeds off traffic before it hard-rejects.
SIGTERM sets a one-way `shuttingDown` flag → `/health/ready` flips to 503 for
graceful drain (Armeria 1s quiet / 30s timeout, under k8s
`terminationGracePeriodSeconds: 60`).

### Bulkheads + bounded queues

`resilience/WorkerBulkhead` — a fixed `ThreadPoolExecutor` (`cores×2`) backed by a
**bounded `ArrayBlockingQueue`** (`RECALL_BULKHEAD_QUEUE_CAPACITY`, default
`pool×4`) with **no rejection handler**: overflow throws
`RejectedExecutionException` immediately, bounding memory and tail latency.
Per-channel `RECALL_CHANNEL_TIMEOUT_MS` (default **200ms**) via
`CompletableFuture.orTimeout` bounds each recall channel's tail contribution
independently; timed-out non-primary channels degrade to empty results. That
degradation is **observable** on catalog 6010: `GET /health/load` (`CatalogLoadService`)
reports `recall.degradedRatio` (fraction of non-primary recall *requests* that
degraded), a per-channel `recall.channelDegraded` breakdown
(`REJECTED`/`TIMEOUT`/`ERROR`), and live bulkhead `active`/`queued`; degraded
responses also carry an `X-Recall-Degraded` header on `/getrecommendation`,
`/recommendation`, and `/v2/recommend` (observe-only, request outcomes unchanged),
alongside the bounded `X-Recall-Degradation-Reason`
(`partial` / `all_channels` / `fallback`) that separates a healthy empty result
from an empty one caused by unavailable channels — see
[Fault Tolerance](18_Fault_Tolerance.md#channel-health-and-per-channel-timeouts).

Two sizing facts qualify the above. `orTimeout` bounds the **caller's** wait, not the
task: the bulkhead worker stays occupied until the underlying Redis command times out, so
the bulkhead's drain rate is set by `REDIS_TIMEOUT_MS` rather than by
`RECALL_CHANNEL_TIMEOUT_MS`. And `cores×2` threads with a `pool×4` queue is **10 task
slots** under `limits.cpu: "1"`, against an admission gate that allows 64 concurrent
requests fanning out to 6 channels each — so channel rejection is the steady state well
before the admission gate engages, which is what `recall.degradedRatio` is actually
reporting. Both are measured and quantified in
the subsections below.

### Rate limiting (`ratelimit/TokenBucket` primitive)

| Limiter | Scope | Notes |
|---|---|---|
| `GatewayRateLimiter` | per-(route, principal), per instance | bounded Caffeine cache (`GATEWAY_RL_MAX_PRINCIPALS` 100k); 429 on deny |
| `ModelRateLimiter` | per-user, per instance | access-ordered LRU capped at max-users (10k); fairness between users |
| `LlmTokenRateLimiter` | token-count-aware | consumes `max_tokens` per call so large-context requests can't drain the quota |
| `RedisRateLimiter` | **global / cluster-wide** | distributed **weighted sliding-window** (rolling rate ≈1× the limit, not the ~2× a fixed window admits across a boundary); **no local fast-path** — every request consults Redis; Redis-outage admission is bounded by a per-replica emergency token bucket only when all construction gates below are active |

Rule of thumb: **rate limiters fail open** (disabled at 0; on Redis error the online
limiter uses its per-replica emergency ceiling only when a Redis executor exists,
Redis QPS is positive, the emergency flag is enabled, and emergency rate and burst
are both positive). If the Redis limiter is active but the emergency flag, rate, or
burst disables that rollback bucket, Redis errors, malformed decisions, and circuit
rejections use **unlimited fail-open**. Without an executor or positive Redis QPS,
the Redis limiter itself is disabled and admits all requests. **Load shedders are
always on**. The online Redis QPS limit is the only *global* ceiling; everything
else is per-instance, so effective limits move with replica count.

### Circuit breakers

`resilience/CircuitBreaker` (CLOSED/OPEN/HALF_OPEN, CAS-based). `RouteCircuitBreaker`
defaults: **5 consecutive failures → open 10s → single half-open probe**; the
gateway returns **503** while open, shedding load off a failing upstream for the
cooldown. The `RedisRateLimiter` embeds its own breaker (5 failures / 30s reset)
so a Redis outage can't cascade into latency on every rate-limit check.

### Graceful degradation

The model 8080 path is uniquely graceful: on a full shedder it first tries
**degrade-to-cache** (`tryServeFromCache` → HTTP 200 + `X-Served-From:
degraded-cache`) and only throws `ServiceOverloadedException` (latency-derived
`Retry-After`) on a cache miss.

### The concurrency model — parallelism, not asynchrony

Everything below assumes a thread is held for the duration of a serving request, so it is worth
saying plainly what the async machinery is and is not.

There is **no `async`/`await`** — this is Java 17 — and no Spring `@Async`, no reactive library,
and no virtual threads. Every asynchronous path is hand-rolled `CompletableFuture` over Armeria's
async HTTP model, in **two idioms that behave differently under load**:

| Idiom | Where | Thread held while waiting? |
|---|---|---|
| `HttpResponse.of(future.thenApply(...))` | gateway proxy, `GatewayHealthService` | **No** — Armeria completes the response when the future does |
| `HttpResponse.of(supplyAsync(..., ctx.blockingTaskExecutor()))` | every serving path (6010, 7010, and the sharded-record and catalog services) | **Yes**, for the whole request |

The second idiom wraps *synchronous* code. Inside it,
[`MultiChannelRecallService`](../../src/main/java/com/recsys/application/retrieval/multichannel/MultiChannelRecallService.java)
fans channels out onto the recall bulkhead and then `allOf(...).join()`s. So on a serving path
`CompletableFuture` buys **parallelism** — channels run concurrently — and not **asynchrony**: a
blocking-executor thread is occupied from admission to response.

That is the reason the arithmetic in the next three subsections is load-bearing rather than
academic. **Concurrency here is bounded by threads, not by outstanding futures**, so a gate
expressed in requests (admission: 64) and a pool expressed in threads (bulkhead: 10 task slots)
are the same currency, and the smaller one wins.

`GatewayHealthService`'s `futures.get(i).join()` inside `thenApply` is not a counter-example: it
runs only after `allOf` has completed, so it never waits. **Nothing blocks an Armeria event
loop** — every request-path `supplyAsync` passes `ctx.blockingTaskExecutor()`, which is what that
executor exists for.

One site takes no explicit executor: `ModelRuntimeProvider.warmUp()`. It is startup-only, and the
obvious reading — that it degrades to serial on a one-CPU container because the common pool's
parallelism is `cores - 1` — is **wrong, and was measured**: `CompletableFuture`'s `ASYNC_POOL`
falls back to a thread-per-task executor whenever `ForkJoinPool.getCommonPoolParallelism()` is not
greater than 1, so the constrained case is the one where it parallelises best. Recorded because it
invites a fix it does not need.

### The request time budget, end to end

The gates above each bound one layer. Read outside-in they form a **budget chain**, and the
chain is what determines whether a gate protects anything — as deployed:

| Layer | catalog 6010 | online 7010 | model 8080 | gateway 8010 |
|---|---|---|---|---|
| Armeria server request timeout | **10 s** (unset → Armeria default) | **500 ms** (`ONLINE_REQUEST_TIMEOUT_MS`) | — (Tomcat) | **10 s** (unset → Armeria default) |
| Upstream client timeout | — | — | — | **3 s** (`GATEWAY_TIMEOUT_MS`) + 1 retry ≈ **6.05 s** |
| Recall channel timeout | **200 ms** | **200 ms** | **200 ms** | — |
| Redis command timeout | **2000 ms** (unset → code default) | **200 ms** (`k8s/base` env) | **150 ms** (code constant) | — |
| Redis pool max-wait | 250 ms (default) | 100 ms (`k8s/base`) | 250 ms | — |
| Admission limit | 64 concurrent | 64 concurrent | semaphore | rate limiter |
| Recall bulkhead | `cores×2` threads, `pool×4` queue | same | same | — |
| Container CPU limit | `1` | `1` | `2` | `750m` |

`RECALL_CHANNEL_TIMEOUT_MS` is set in no manifest, so all three recall paths run at the

`RECALL_CHANNEL_TIMEOUT_MS` is set in no manifest, so all three recall paths run at the 200 ms
default. `REDIS_TIMEOUT_MS` is set in one, `k8s/base/online-serving.yaml`. 7010 is the only
Armeria service that sets `requestTimeoutMillis`; 6010 and 8010 inherit Armeria's 10 s default
(`DefaultFlagsProvider.DEFAULT_REQUEST_TIMEOUT_MILLIS`).

**The chain is not monotonically decreasing.** The gateway waits up to ~6 s for a backend that
stops itself at 500 ms, and inside 6010 a 200 ms channel budget sits above a 2000 ms command
budget with no server deadline between them. Where a chain inverts, the inner timeout is
decorative for the *client* and load-bearing only for *thread occupancy* — which is the next
subsection.

### `orTimeout` bounds the caller's wait, not the work — measured

Each channel is dispatched as
`CompletableFuture.supplyAsync(channel, recallBulkhead).orTimeout(200ms)`. `orTimeout`
completes the *dependent* future; it does not cancel or interrupt the task already running on
the bulkhead, and a task blocked in a socket read would not observe an interrupt anyway.
Running that exact shape — a 1-thread pool, a task blocking 2000 ms, `.orTimeout(200ms)`:

```
caller saw 'degraded:TimeoutException' at 220ms
pool active=1 queued=0 at 238ms
  [task] completed normally at 2014ms
next task waited 1790ms for a worker (submitted at 2035ms)
```

The caller degraded on schedule. The worker stayed occupied, and **the next request's channel
task waited 1790 ms for a thread**. So the recall bulkhead's drain rate under a stalled backing
store is set by the **Redis command timeout**, not by `RECALL_CHANNEL_TIMEOUT_MS`. The channel
timeout protects the *response*; only the command timeout protects the *thread*. Two of the
three recall paths cap that command timeout, and they do it in different places:

| Service | How the command timeout is bounded | Effective cap |
|---|---|---|
| model 8080 | `ModelRuntimeProvider.RECALL_REDIS_TIMEOUT_MS` code constant, passed to `LettuceClientFactory.routingFromEnv(int)` | 150 ms |
| online 7010 | `REDIS_TIMEOUT_MS: "200"` in `k8s/base/online-serving.yaml` | 200 ms |
| catalog 6010 | nothing | 2000 ms |

The same hazard was mitigated twice, in two different layers, and missed on the third — 6010
calls the *uncapped* `LettuceClientFactory.routingFromEnv()` where 8080 calls the capped
overload. Setting `REDIS_TIMEOUT_MS: "200"` for catalog-serving in `k8s/base` closed it the way
7010's is closed, and `CatalogRedisTimeoutManifestTest` pins the pairing in both directions:
every recall service must set it, and the value must not exceed the channel budget as
`RecallConfig` resolves it. That test reads manifest text — it proves a coupling between two
files, not what a cluster receives, and a deployment not applying `k8s/base` still gets the
2000 ms code default.

### Bulkhead width versus admitted concurrency

The recall bulkhead is `availableProcessors() * 2` threads with a `pool × 4` queue. Under
`limits.cpu: "1"` the JVM reports 1 processor, so a pod gets **2 threads and 8 queue slots** —
10 task slots. Admission control allows **64** concurrent requests, each fanning out to **6**
channels: up to 384 channel tasks against 10 slots. The two gates are dimensioned in different
units (requests vs. tasks) from different inputs (an env default vs. the CPU limit), and
nothing ties them together.

That is not a bug — the bounded queue exists so overflow throws and degrades to an empty
channel result. But it means **channel rejection is the normal steady state above a few
concurrent requests**, not an overload signal, and `recall.degradedRatio` should be read with
that in mind. What the gap changes is *which* mechanism sheds first: the bulkhead, long before
the admission gate it nominally sits behind.

### Batching and connection pools

Batching is implemented wherever a batch exists: `RedisEmbeddingStore.getEmbeddings` MGETs in
`REDIS_EMBEDDING_MGET_BATCH_SIZE` (500) chunks, `setEmbeddings` uses one pipelined batch,
`loadAll` MGETs each SCAN page immediately (bounded heap), `OnlineFeatureStore` batches the
same way, `LogicalExpiryEmbeddingCache.getEmbeddings` collapses all cold misses into one
backing-store call, and `UserTowerInferenceService.scoreCandidates` runs **one** batched ONNX
`session.run` for every candidate.

The gap is *between* stages. The online path issues its independent reads serially:
`getRecentMovieIds` (Redis) → the cold-start probe inside `recall()` (Redis, on the calling
thread, outside both the channel timeout and the fan-out) → the 6-channel fan-out → the
trending snapshot (Redis). Three of those are mutually independent and could share one round
trip; they don't. Two are also **read twice per request** — recent history by both
`OnlineRecommendationService` and the `OnlineRecentHistory` channel, trending by both the
`Trending` channel and the response snapshot — absorbed by the 5 s / 2 s JVM caches on a warm
pod, genuine extra round trips on a cold one. There is no **cross-request** batching anywhere,
which for per-user features is the right call but is worth stating, because "batching" in a
serving system usually means the cross-request kind.

**The pool knobs do not bound the serving path.** `LettuceRedisExecutor` serves all normal
reads and writes from **one shared multiplexed connection**; the commons-pool2 pool is used
only for pipelines and timed primary reads. So `REDIS_POOL_MAX_TOTAL`, `_MAX_IDLE`, `_MIN_IDLE`
and `_MAX_WAIT_MS` govern neither serving read — 7010's `REDIS_POOL_MAX_TOTAL: "64"` applies to
`executePipelined` and `executePrimaryRead` only. Request-path concurrency against Redis is
bounded by the admission gate and the bulkhead instead, which is why multiplexing was chosen,
but it means tuning the pool in response to a serving-latency incident would change nothing.
`testOnBorrow` also defaults `true`, so each pipeline or primary read pays a validation round
trip before its own.

## 3. Data-tier scaling — horizontal everywhere

> Store-level companion:
> [03_DB_Scaling_Sharding §6](03_DB_Scaling_Sharding.md#6-scaling--the-levers-and-what-each-actually-buys)
> lists the levers per store, what each one actually buys, and what breaks first. Note the
> qualifier on "horizontal everywhere": the Redis shards are logical prefixes on one
> primary, so levers 1–2 there spread contention rather than adding nodes.

### Record sharding

`infrastructure/redis/sharding/ConsistentHashRing` — 150 virtual nodes per shard,
FNV1a-64 hashing. Adding a shard remaps only ~1/N of keys, and a runtime-swappable
**versioned topology** lets an operator **reshard live (e.g. 4→8)** with a bounded
dual-read window. Per-shard `INCR` counters avoid global write contention.
Resharding is operator-triggered, not automatic/elastic.

Mechanics, key schema, and the reshard procedure:
[03_DB_Scaling_Sharding §1 and §3](03_DB_Scaling_Sharding.md#1-shardedrecordstore--the-sharded-record-database).

### Hot trending keys

`infrastructure/redis/ShardedTopKStore` — the few trending windows
(`topk:last_hour`, `topk:last_day`) are read on every request across all JVMs. Each
window is a single canonical snapshot key; a short JVM cache and inline single-flight
absorb the bulk, so an instance issues at most one Redis read per window per cache TTL
regardless of request volume. (This store used to replicate each window across 4
identical shard keys; those were never written and were removed on 2026-07-28.)

Cache TTLs, the guards that stop a miss storm undoing this, and why the cache — not
the sharding — is the primary mechanism:
[03_DB_Scaling_Sharding §6](03_DB_Scaling_Sharding.md#6-scaling--the-levers-and-what-each-actually-buys).

### Kafka / Flink

`online/flink/OnlineFeatureStreamingJob` — **24 topic partitions**, 24-way
source/operator parallelism, `max-parallelism 128` (fixed key-group ceiling for
later growth via savepoint restore), targeting **50,000 events/s**. Two-stage
bucketed Top-K removes the single-task `windowAll` bottleneck (partials keyed by
movie-bucket, final merge keyed by window-end). Partition resizes create a **new
topic generation** (`movie_events_v3`) rather than an in-place change, to preserve
per-user ordering; a startup validator fails fast if the partition count drifts.

### Read scaling

`infrastructure/redis/RedisReadReplicaRouter` + `RoutingRedisExecutor` — writes to
primary, reads to the **same-AZ replica** (lowest latency, survives primary-AZ
loss), each replica backed by its own connection pool. Anti-stampede guards
(`SingleFlight`, `HotKeyDetector`, `BloomFilterGuard`) keep cache misses from
capping the gain.

Routing config, pool sizing, and guard thresholds:
[03_DB_Scaling_Sharding §6](03_DB_Scaling_Sharding.md#6-scaling--the-levers-and-what-each-actually-buys).
Consistency and lag: [04_Replication](04_Replication.md).

### Pagination at scale

`application/pagination/MillionScalePaginationSql` — keyset seek / covering-index
/ delayed-join with `FORCE INDEX` and a **1000-row page cap** avoids the OFFSET
deep-scan cliff, keeping page latency flat to 10M+ rows. (That class is a reference
implementation; the live catalog path hand-writes the equivalent `FORCE INDEX` keyset
query in `MovieCatalogRepository` — see
[13_DB_Indexing §3](13_DB_Indexing.md#3-index-access-patterns-via-millionscalepaginationsql).) `/v2/recommend` uses an
HMAC-signed, query-bound `(score, itemId)` live-keyset cursor over a recomputed
bounded ranking window. Catalog, Spring model, and online serving share
validation-before-source-work, deterministic tuple seek, and exact
`hasMore`/`nextCursor` semantics; MySQL catalog cursors remain separately
HMAC-signed and filter-bound. Current behavior, live-feed limitations, and
rotation operations are documented in
[Pagination](19_Pagination.md).

### Caching / CDN offload

Embedding caches in front of Redis (`LocalEmbeddingCache` heap 100k on 6010;
`LogicalExpiryEmbeddingCache` soft-expiry on 7010 to prevent TTL stampede) plus `RecommendationCache` (RW-lock TTL-LRU with in-flight
dedup) absorb load and offload the origin. CloudFront offloads the two shared
catalog reads at the edge (`/api/catalog/item` s-maxage 3600, `/api/catalog/similar`
s-maxage 300); personalized `/api/recommend` stays `no-store` (0% hit ratio, by
design).

## 4. How adding capacity raises throughput (summary)

| Lever | Effect |
|---|---|
| More service replicas (HPA) | Linear read/serve throughput; stateless instances, per-instance gates scale with them |
| More record shards (reshard) | Spreads keys across more **logical shards on the same primary** — not hash slots or nodes; ~1/N remap; per-shard counters avoid write contention. Raises the contention ceiling, not node capacity ([03 §6](03_DB_Scaling_Sharding.md#6-scaling--the-levers-and-what-each-actually-buys)) |
| More top-K shards | N× per-key read QPS for hot trending windows |
| More Kafka partitions + Flink parallelism | More parallel ingest up to key cardinality; 50k events/s target |
| More Redis read replicas | Spreads read load off primary, each +50 connections, cuts cross-AZ cost |
| Caching / CDN | Flattens origin load under spikes; edge-offloads shared catalog reads |
| Keyset pagination | Page latency stays flat as tables grow (no OFFSET cliff) |

## 5. Sharp edges — status after the 2026-07-20 hardening pass

The original investigation flagged five sharp edges; four have since been
instrumented or fixed (each as its own spec→plan→build cycle), leaving two
genuinely deferred assumptions. Current status:

1. **Silent recall-quality degradation now has a signal — fixed (PR #202).** On
   catalog 6010 the recall bulkhead (~`cores×10` task ceiling) can still saturate
   *before* the 64-concurrency gate rejects — on **≤6-core** boxes non-primary
   channels drop to empty results (HTTP 200, fewer candidates) before any 429. That
   ordering is now **observable** via `GET /health/load` (`recall.degradedRatio`,
   per-channel `recall.channelDegraded`, live bulkhead `active`/`queued`) plus the
   `X-Recall-Degraded` response header — watch those, not just 429 rates. The
   ordering itself is pinned by a characterization harness (edge #3 below).
2. **`AutoScalingGroup` still isn't a live autoscaler, but the loop is now closed
   by a tested reference — partially addressed.** `application/autoscaling/CapacityController`
   supplies the missing signal→capacity brain (target-tracking `⌈running × util ÷
   0.7⌉`, surge step, asymmetric 60s/300s cooldowns) over the existing ASG via
   `AsgCapacityActuator`. **It is a reference — no server schedules it**, so HPA is
   still the only production autoscaler and anything relying on the ASG to *react*
   needs an external trigger.
3. **Overload defaults are now characterized, not yet prod-tuned — mechanism
   addressed (PR #206).** Three opt-in `@Tag("load")` harnesses lock in each gate's
   invariants and give a repeatable way to find its knee (shedder never exceeds
   `max` and drains at `⌈0.95×64⌉ = 61`; bulkhead accepts exactly `pool+queue` then
   rejects immediately; the smaller of {64 gate, `cores×10` ceiling} trips first).
   See [docs/runbooks/overload-characterization.md](../runbooks/overload-characterization.md).
   They characterize the *mechanism* on the box that runs them — tuning
   64-concurrency / 0.95-drain / 200ms-timeout against real latency curves in a
   prod-like environment remains deferred.
4. **`RedisRateLimiter` no longer admits ~2× at the window boundary — fixed
   (PR #203).** It is now a weighted **sliding-window** counter (rolling rate ≈1×
   the limit) and the per-instance local fast-path was **removed** (it leaked
   `N × fraction × limit` before the global limit engaged), so every request
   consults Redis. Fail-open + embedded circuit breaker kept. Caveat: the window
   bucket is stamped from each instance's wall clock, so the ≈1× bound assumes
   NTP-synced clocks (proven by a `@Tag("docker")` boundary test with an injected
   clock). It's still the only *global* ceiling — every other limit is per-instance
   and moves with replica count. Its Redis-outage path is bounded only while the
   executor, positive-QPS, emergency-enable, positive-rate, and positive-burst gates
   all hold; disabling the emergency bucket for rollback restores unlimited
   fail-open.
5. **DR warm standby can now be pre-scaled on failover — fixed (PR #204).** The
   standby still *sits* at ~50% minReplicas to save cost, but an operator promotes
   it to the primary baseline in one step (`dr-standby-capacity.sh promote`, HPA-docs
   only). It remains a **manual** step, so expect a brief scale-out window if a
   cutover lands before promote runs.
6. **The time budget inverts between layers (§2).** The gateway waits ~6 s for a backend that
   stops at 500 ms, and 6010 ran a 200 ms channel budget over a 2000 ms command timeout with no
   server deadline above it. The command-timeout half is **fixed** for 6010 via
   `REDIS_TIMEOUT_MS: "200"` in `k8s/base` — but that fix is configuration: any deployment not
   applying base (local `run-microservices-local.sh`, an overlay not composing it) still gets
   the 2000 ms code default. The durable fix is to cap it in code the way 8080 does. **Open:**
   6010 and 8010 still set no server request timeout at all, so they run on Armeria's 10 s
   default and no layer bounds a request end-to-end.
7. **Bulkhead width vs. admitted concurrency (§2) — open.** 10 task slots against 64 admitted
   requests × 6 channels under `limits.cpu: "1"`. Deliberate bounded-queue behaviour, but it
   makes channel rejection the steady state rather than an overload signal. Sizing both gates
   from one input needs a decision about which is the real limit; that decision has not been
   made.
8. **Serial stages and duplicated reads (§2) — open by omission.** Three independent Redis
   round trips issued serially, with recent history and trending each read twice. Invisible on
   a warm pod, real on a cold one. Pipelining them has a measurable benefit only under
   cold-cache conditions that nothing currently measures.
9. **The recall bulkhead's background refreshes run on the common pool.** Blocking Redis I/O on
   a one-worker `ForkJoinPool.commonPool()` under `limits.cpu: "1"`, contradicting the warning
   `MultiChannelRecallService` gives about that same pool. Degrades refresh freshness, not
   availability — see [02_Caching](02_Caching.md#sharp-edges--notes).
