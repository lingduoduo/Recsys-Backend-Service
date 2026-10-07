# Caching in Recsys-Backend-Service

An investigation of the caching layers that keep the recommendation hot path fast: a
heap embedding cache in front of Redis, a soft-TTL cache that serves stale while it refreshes, a
generic single-flight snapshot cache, and result caches for recommendations and LLM
responses. The recurring discipline is the same everywhere — **bounded, single-flight,
serve-stale-on-error, and keyed so a deploy sidesteps stale data**.

## The big picture

Every cache here follows four rules, because they run on the request path where a slow
or unbounded cache is worse than no cache:

- **Bounded** — a fixed capacity with LRU/sentinel eviction, so no cache can grow the
  heap without limit.
- **Single-flight** — when an entry expires, exactly one caller recomputes it while the
  rest wait or serve stale; a hot key never triggers a thundering herd on the backing
  store.
- **Serve-stale-on-error** — a backing-store failure serves the last-good value within a
  bounded stale window rather than erroring (fail-open — the availability choice from
  [05_CAP](05_CAP.md#2-reads-are-ap-by-default)). **Only when a last-good value exists.**
  With nothing cached for the key, three of the four families rethrow instead: the rule is
  fail-open on a *warm* cache and fail-closed on a *cold* one. §9 measures what that cost —
  a 500 on 7010 and a failed boot on 6010 — and how the callers now absorb it.
- **Keyed by version** — result caches key on the A/B variant + model version, so a
  deploy naturally invalidates without an explicit purge.

These are properties of the cache *classes*. What each cached **object** actually gets —
which of these caches is wired in front of it, in which service — is §10; it does not follow
from the table below, and in two cases it contradicts it.

The caches:

| Cache | Caches | Expiry strategy | Bound |
|---|---|---|---|
| `LocalEmbeddingCache` | embeddings (L2 local) | access-order LRU | `LOCAL_EMBEDDING_CACHE_MAX_ENTRIES` (100,000) |
| `LogicalExpiryEmbeddingCache` | user embeddings (`u2vEmb`) | **soft** TTL (~30 s) + serve-stale + 1 refresh | `LOGICAL_EXPIRY_CACHE_MAX_ENTRIES` (10,000) |
| `TtlSingleFlightCache<V>` | any snapshot | fresh 1 s / stale 60 s + single-flight | per-key |
| `RecommendationCache` | rec results + cold-start pools | 300 s / 3600 s, keyed by variant+version | bounded map |
| `LlmResponseCache` | LLM responses | 300 s TTL | 500 entries |
| Redis (the L2 everything shares) | embeddings, top-K, features | per-key TTL **+ jitter**; `volatile-lru` at `maxmemory`, so only TTL'd keys evict (§8) | `maxmemory 200mb` |
| CloudFront edge | catalog reads | 1 h / 5 min (see [12_CDNS](12_CDNS.md)) | edge |

## 1. Embedding caches — the hot path

Embeddings are read on every recall, so they get the most cache machinery.

There used to be a third, explicit three-tier class here — `MultiLevelEmbeddingCache` (heap L1 with
hot-key promotion, L2 store, L3 file-system snapshot). It was wired into no service, so its L3
"Redis-outage defense" never existed in any running process, and it was **deleted on 2026-10-07**
rather than left documented as if it did. What actually happens when Redis is unavailable is §9.

**`LocalEmbeddingCache`** ([infrastructure/cache/LocalEmbeddingCache.java](../../src/main/java/com/recsys/infrastructure/cache/LocalEmbeddingCache.java))
is the cache actually wired in production — a Caffeine JVM-heap cache in front of Redis
(`maximumSize = LOCAL_EMBEDDING_CACHE_MAX_ENTRIES`, default 100,000). It layers three
cache-penetration defenses: a **Bloom filter** guard (after warm-up, `mightContain==false`
short-circuits the Redis round-trip for known-absent IDs), a bounded **null-sentinel**
cache (100k, 30 s), and **single-flight** miss loading (2 s wait). `warmUp()` bulk-loads
from Redis and `preload()` seeds from the classpath. The production "multi-level" shape is
`LocalEmbeddingCache` (heap) → Redis.

**`LogicalExpiryEmbeddingCache`** ([infrastructure/cache/LogicalExpiryEmbeddingCache.java](../../src/main/java/com/recsys/infrastructure/cache/LogicalExpiryEmbeddingCache.java))
solves the hot-key TTL stampede a different way: it embeds a **soft (logical) expiry**
inside each entry and gives the backing-store key a much longer **hard** TTL (2× soft).
On a read *before* soft expiry it's a plain hit; *past* soft expiry it **returns the
stale-but-valid value immediately and schedules exactly one background refresh** — so
the herd never forms. Refreshes (and cold misses) are deduped per ID via a `refreshing`
map. Used for user embeddings (`u2vEmb`, ~30 s soft TTL — see the staleness table in
[15_Eventual_Consistency](15_Eventual_Consistency.md)).

**Every embedding-cache tier is size-bounded** (since 2026-07-28). Values:
`LocalEmbeddingCache` (`LOCAL_EMBEDDING_CACHE_MAX_ENTRIES`, default 100,000) and
`LogicalExpiryEmbeddingCache` (`LOGICAL_EXPIRY_CACHE_MAX_ENTRIES`, default 10,000). Negative caches
of confirmed-absent IDs are bounded per class, with no shared knob: `LocalEmbeddingCache`'s at a
hard-coded `NULL_SENTINEL_MAX = 100_000`, and `LogicalExpiryEmbeddingCache`'s sentinel and
refresh-guard maps by the same `LOGICAL_EXPIRY_CACHE_MAX_ENTRIES` as its values. All use Caffeine
`maximumSize`, so a sweep over many distinct or absent IDs cannot grow the heap without limit.
(`EMBEDDING_NULL_SENTINEL_MAX_ENTRIES` was read only by the deleted `MultiLevelEmbeddingCache`; it
configures nothing.)

One deliberate asymmetry: the sentinel and refresh-guard maps also carry
`expireAfterWrite`, but `LogicalExpiryEmbeddingCache`'s *value* map is bounded by **size
only**. A time-based eviction there would defeat the pattern — an entry past its soft
expiry must stay servable until a refresh replaces it, or a backing-store outage would
evaporate every entry and turn each read into a cold miss, which is precisely the herd
this cache exists to prevent.

## 2. `TtlSingleFlightCache` — the generic serve-stale primitive

[`TtlSingleFlightCache<V>`](../../src/main/java/com/recsys/infrastructure/cache/TtlSingleFlightCache.java)
generalizes the fresh/stale lifecycle the infra stores use inline. Each key has a
**fresh** window (`DEFAULT_FRESH_TTL_MS = 1_000`) and a **stale** window
(`DEFAULT_STALE_TTL_MS = 60_000`), and reads take one of three paths:

1. **Fresh hit** (`now < freshUntil`) — return cached, loader never called.
2. **Stale window** (`freshUntil ≤ now < staleUntil`) — **one** caller refreshes
   asynchronously; everyone else serves the stale value, and if the refresh loader
   throws, the stale value is kept and served.
3. **Cold miss** (beyond stale) — block-and-load, coalescing concurrent callers via a
   `refreshing` key set.

Its concrete user is
[`GlobalPopularityStore`](../../src/main/java/com/recsys/infrastructure/redis/GlobalPopularityStore.java)
(a 100-item popularity snapshot that fails open to an empty list when Redis is down and
no snapshot exists). It is the reusable form of the same pattern `OnlineFeatureStore`
(5 s / 60 s) and `ShardedTopKStore` (2 s / 60 s) implement inline.

## 3. `RecommendationCache` — result and cold-start caching

[`RecommendationCache`](../../src/main/java/com/recsys/application/recommendation/RecommendationCache.java)
caches finished recommendation results and the pre-scored cold-start pools on the model
service. It is **keyed by A/B variant + model version**, so a new model or variant deploy
sidesteps stale results with no explicit invalidation. TTLs are ~**300 s** for
recommendations and ~**3600 s** for cold-start pools (`RecommendationCacheProperties`),
and its hit/miss rates are exposed at `GET /health/cache`. Its concurrency was tuned from
a `synchronized` + access-order map (which serialized every read) to a
`ReentrantReadWriteLock` + insertion-order map so reads run in parallel.

## 4. `LlmResponseCache` — buffered LLM responses

[`LlmResponseCache`](../../src/main/java/com/recsys/infrastructure/cache/LlmResponseCache.java)
caches non-streaming LLM proxy responses keyed by a **SHA-256 of the request body**
(bounded at `LLM_CACHE_MAX_SIZE`, default **500**; TTL `LLM_CACHE_TTL_SECONDS`, default
**300 s**), returning `X-Cache: HIT/MISS`. It applies **only to the buffered path** — the
SSE streaming path skips caching entirely (a stream can't be replayed from a cache). The
justification for caching a nondeterministic model is that the demo runs at
temperature 0. It sits inside the [API Gateway](09_API_Gateway.md) LLM proxy; its
streaming-vs-buffered behavior is owned by [SSE Streaming](16_SSE_Streaming.md).

## 5. Supporting machinery

- **`HotKeyDetector`** — a lock-free two-bucket, alpha-weighted sliding window over key
  access frequency. Its one consumer is `ShardedTopKStore`, which uses it to expose which
  trending windows are hot (it was also the L1 promotion policy of the deleted
  `MultiLevelEmbeddingCache`).
- **`SingleFlight`** ([infrastructure/resilience/SingleFlight.java](../../src/main/java/com/recsys/infrastructure/resilience/SingleFlight.java))
  — the general dedup primitive behind the caches; on a wait timeout it fails open to an
  independent compute rather than blocking ([18_Fault_Tolerance](18_Fault_Tolerance.md#redis-resilience)).
- **Infra serve-stale caches** — `OnlineFeatureStore` (5 s fresh / 60 s stale) and
  `ShardedTopKStore` (2 s / 60 s) implement the same fresh+stale+single-flight lifecycle
  inline; see [03_DB_Scaling_Sharding §2](03_DB_Scaling_Sharding.md#2-shardedtopkstore--sharded-trending).
- **CloudFront edge cache** — the outermost cache tier, for the two catalog reads, is the
  [CDN Edge investigation](12_CDNS.md#1-what-is-cached-and-what-isnt).

## 6. Where each cache invalidates

The one meaningful axis of difference is **write-through vs TTL-only**:

- **Write-through (invalidate on write)** — `LocalEmbeddingCache` (via `/setembedding`) and
  `LogicalExpiryEmbeddingCache` update on the write path, and the CDN has manual operator
  invalidation. These are the caches where a same-version data change is reflected
  promptly.
- **TTL-only** — everything else (recommendation results, LLM responses, feature/top-K
  snapshots) is *not* invalidated on writes; a same-version change is served stale up to
  the TTL. Result caches sidestep this with **version keying** (a deploy changes the key),
  which is why they can afford a longer TTL. The full staleness-window catalog is in
  [15_Eventual_Consistency §2](15_Eventual_Consistency.md).

## 7. Testing

- **Embedding caches** — `LocalEmbeddingCacheTest` (LRU, batch dedup),
  `LogicalExpiryEmbeddingCacheTest` (soft-expiry serve-stale + single refresh).
- **Generic** — `TtlSingleFlightCacheTest` (fresh/stale/cold paths, serve-stale-on-error,
  coalescing).
- **Result caches** — `RecommendationCacheTest` (version keying, hit rates),
  `LlmResponseCacheTest` (SHA-256 key, TTL, bound).

## 8. Redis itself as a cache tier

Everything above treats Redis as "the backing store", but Redis is really **half cache,
half state**: both the primary and the replica StatefulSets
([k8s/base/redis-cluster.yaml](../../k8s/base/redis-cluster.yaml), mirrored in
`docker-compose.streaming.yml`) run with

```
--maxmemory 200mb --maxmemory-policy volatile-lru
```

RDB snapshots (`--save`) are on, so data survives a restart, and **`volatile-lru` confines
eviction to keys that carry a TTL**. That split is the whole design: everything cache-like
sets an explicit TTL, so the keys *without* one are exactly the authoritative ones and are
structurally protected from eviction.

The policy is not a tuning knob — it is a correctness invariant, pinned by
`RedisEvictionPolicyManifestTest` and reported at runtime as
`redis_cache_evicts_only_volatile_keys` (§8, "observability"). Under the previous
`allkeys-lru`, an evicted `shard:topology` would be silently recreated at **version 1** by
`ShardTopologyStore.bootstrap`, resetting a resharded cluster's generation and addressing
data under the wrong key prefix. The trade is deliberate: when memory fills with
non-evictable keys, writes fail loudly with an OOM error instead of quietly dropping state.

### The same invariant, two different mechanisms

**In EKS none of the above applies.** `k8s/eks-shared` scales `redis-primary`,
`redis-replica`, and `redis-sentinel` to **0** in every region — ElastiCache serves
instead — so `k8s/base/redis-cluster.yaml` is inert in production. ElastiCache is
**ElastiCache for Redis**, not a different engine (the overlay requires "Engine mode: Redis,
not Cluster Mode"), so the semantics are the same; what differs is that its config lives in
an AWS **parameter group**, out of reach of the manifests.

That leaves the invariant enforced by two mechanisms and verified by a third:

| Where | Mechanism | Checked by |
|---|---|---|
| Local / `k8s/base` | `--maxmemory-policy volatile-lru` in the manifests | `RedisEvictionPolicyManifestTest` |
| EKS (both regions) | `maxmemory-policy` on a **custom** ElastiCache parameter group | [`scripts/set-elasticache-parameters.sh`](../../scripts/set-elasticache-parameters.sh) (`apply` / `verify`) |
| Any | the running policy as Redis reports it | `redis_cache_evicts_only_volatile_keys` |

ElastiCache is provisioned out-of-band (this repo has no IaC), following the same
convention as CloudFront and the WAF, so the repo can only *state* the requirement — the
two `redis-elasticache-patch.yaml` headers list it alongside Multi-AZ and the reader
endpoint, and the manifest test asserts they keep saying so. Two operational details the
script encodes:

- **A `default.*` parameter group cannot carry it.** AWS rejects edits to its managed
  default groups, so the cluster needs a custom group; `apply` fails fast with that
  instruction rather than surfacing an opaque API error.
- **Run it once per region.** Global Datastore replicates *data*, not parameter groups, and
  the us-west-2 secondary is promoted to primary on failover — a secondary that evicted
  untl'd keys is promoted already missing them.

Only the metric closes the loop. The manifest test cannot see a live cluster, and the
script only sees the group it is pointed at; `redis_cache_evicts_only_volatile_keys`
reports what the serving path is actually talking to, which is what catches a manual
`CONFIG SET`, an unmanaged instance, or a region nobody ran the script against.

### Running the claim instead of asserting it

ElastiCache is out-of-band, so the argument above would otherwise never execute.
[`scripts/simulate-elasticache-eviction.sh`](../../scripts/simulate-elasticache-eviction.sh)
starts a throwaway local `redis-server` (no Docker, no AWS) and applies real memory pressure
under each policy — see [the runbook](../runbooks/elasticache-local.md) for the full output.
At `maxmemory=8mb` with 51 authoritative keys and ~3000 filler writes:

| Scenario | Authoritative kept | Note |
|---|---|---|
| `volatile-lru`, TTL'd pressure | **51/51** | 1579 keys evicted, 0 writes refused |
| `allkeys-lru`, TTL'd pressure | **19/51** | `shard:topology` evicted in **4 of 5** trials |
| `volatile-lru`, un-TTL'd pressure | 51/51 | 1565 writes refused with OOM — sharp edge 6, made concrete |

Two things this measured that the prose had wrong or missing:

- **The old policy's damage is probabilistic, not certain.** Redis samples for approximate
  LRU, so survival moves run to run (0–24 of 50 embeddings across trials). The fix removes a
  coin flip rather than improving odds — worth stating precisely, because "it might be fine"
  is exactly the reasoning that leaves it unfixed.
- **`maxmemory` is enforced per dispatched command, not per `redis.call` inside a script.**
  A single Lua script runs to completion and overshoots the limit — measured at 15.8 MB
  against 8 MB, near 2×. No eviction policy changes this, and the mechanism is real: the
  Flink sinks write through Lua (`SET_IF_NEWER_WITH_LINEAGE_SCRIPT`, `ATOMIC_TOPK_SCRIPT`).
  But the 2× magnitude is not — those sinks write far fewer keys per invocation than the
  simulation's synthetic script does. In this system the per-invocation writes are small,
  so the practical overshoot is
  kilobytes, not the 2× the simulation shows: `SET_IF_NEWER_WITH_LINEAGE_SCRIPT` touches 5
  keys and `ATOMIC_TOPK_SCRIPT` writes `top-k` members (default **10**) into 2 ZSets. The
  simulation reaches 15.8 MB only because it writes 3000 keys in one `EVAL`, which no sink
  does. The mechanism is worth knowing before someone adds a batching writer; it does not
  justify resizing `maxmemory` today.

**TTL jitter — the cache-avalanche defense.** Redis-side TTLs are never used raw:
[`RedisEmbeddingStore.jitteredTtlMillis`](../../src/main/java/com/recsys/infrastructure/redis/RedisEmbeddingStore.java)
adds uniform **positive** jitter in `[0, jitterFraction]` of the base TTL (default
`jitterFraction = 0.1`, clamped to `[0, 0.5]`), and `setEmbeddings` draws a **fresh
jitter per key inside the pipeline** — so a bulk write of N embeddings does not create a
synchronized expiry cliff N keys wide. The jitter is one-sided on purpose: adding time
never shortens the caller's intended freshness window, it only spreads the tail.

**Reads leave the primary.** `getEmbedding`/`getEmbeddings` use `executeRead`, which
[`RoutingRedisExecutor`](../../src/main/java/com/recsys/infrastructure/redis/RoutingRedisExecutor.java)
routes to the AZ-local replica; only writes and pipelines go to the primary. So a cached
read's staleness is *replication lag on top of* the JVM-tier TTL. `executePrimaryRead`
is the read-your-writes escape hatch (used by `getTopKIdsPrimary` and the correlated lag
probe) — see [04_Replication §1](04_Replication.md#1-redis-read-replicas--az-aware-read-routing).

**Batching keeps the round-trip count flat.** Multi-key reads go out as `MGET` in
batches of `REDIS_EMBEDDING_MGET_BATCH_SIZE` (default **500**), and `loadAll` SCANs
pages of 500 and MGETs **each page immediately** rather than accumulating every key name
and issuing one unbounded MGET (a Full-GC/OOM risk on a large store). `loadAll` also
carries a wall-clock budget, `REDIS_LOADALL_TIMEOUT_MS` (default **30 s**), after which
it logs and returns a *partial* result — a slow or oversized Redis degrades startup
warm-up, it doesn't block it.

**A down Redis fails fast, so the JVM tier can serve stale.**
`LettuceClientFactory.failFastOptions()` sets `TimeoutOptions.enabled()` (the per-command
deadline applies even to commands queued while disconnected) plus
`DisconnectedBehavior.REJECT_COMMANDS` (commands error immediately instead of buffering).
Without this, a dead Redis would stall callers past their budget instead of tripping the
serve-stale paths in §1–§2.

The command timeout itself is capped per service, and not in one place. Model serving passes
`RECALL_REDIS_TIMEOUT_MS` (150 ms) to `LettuceClientFactory.routingFromEnv(int)`; catalog 6010
and online 7010 call the **uncapped** overload and depend on `REDIS_TIMEOUT_MS` in
`k8s/base` (200 ms each) instead, so an unset env var leaves them at the 2000 ms default —
10× the recall channel budget layered above them. Why that gap is a capacity problem rather
than a latency one, and the measurement behind it, is
[17_Scalability §2](17_Scalability.md#ortimeout-bounds-the-callers-wait-not-the-work--measured).

**Connections.** Each executor keeps one **lazily opened** shared multiplexed connection
for sync commands plus a commons-pool2 pool (`REDIS_POOL_MAX_TOTAL` 50, maxIdle 10,
minIdle 2, `REDIS_POOL_MAX_WAIT_MS` 250) for pipelines — pipelines need a dedicated
connection because auto-flush is a per-connection setting. Lazy connect is what lets a
service construct its stores at boot against a down Redis without failing startup; a
pipeline whose lifecycle was interrupted is **destroyed rather than returned**, so a
half-flushed batch can never replay into an unrelated request.

**What actually carries a TTL in Redis:**

| Key | Redis TTL | Evictable | Written by |
|---|---|---|---|
| `u2vEmb:*` (streaming) | `SETEX ttlSeconds` | yes | Flink `OnlineFeatureStreamingJob` |
| `topk:<window>` | `EXPIRE ttlSeconds` (all 5 keys, in the Lua) | yes | the Flink TopK sink |
| `sr:rec:*` / `sr:dev:*` | `EXPIRE ttlSeconds` when the writer passes one | if TTL'd | `ShardedRecordStore` |
| `svc:registry:*` | `SET … PX` renewed by heartbeat (`SERVICE_REGISTRY_TTL_MS`, 30 s) | yes | `ServiceRegistrar` |
| `i2vEmb:*` / `u2vEmb:*` (seeded) | **none** (`writeMissing(…, 0)`) | **no** | `RecSysServer.seedEmbeddings` |
| `shard:topology` | **none** (`SET … NX`) | **no** | `ShardTopologyStore` |

The pattern: **derived and liveness data expires; authoritative data has no TTL** — and
`volatile-lru` turns that convention into the eviction boundary. Note the two `u2vEmb`
rows: the same key namespace is durable when seeded from the classpath and ephemeral when
written by the streaming job, which is why the TTL, not the prefix, is what decides.

**Streaming-written values are derived, not durable.** The Flink sink writes through a
Lua script that `SETEX`s the value, its `:updated_at`, and its `:last_event` together, and
the authoritative accumulation lives in Flink keyed state (itself TTL'd). Losing a
`u2vEmb:<user>` to eviction or expiry is therefore equivalent to early expiry: the user's
next event rewrites it from Flink state.

**Seeding repairs eviction per id.** `volatile-lru` protects seeded embeddings today, but
the repair path stays because eviction is not the only way to lose them (a flush, a
restored-from-empty Redis, a policy revert).
[`RedisEmbeddingStore.writeMissing`](../../src/main/java/com/recsys/infrastructure/redis/RedisEmbeddingStore.java)
MGETs the classpath ids (batched, **on the primary** — a lagging replica would report a
live key as absent) and pipelines back **only the absent subset**, so a healthy restart
issues zero writes and a depleted one is repaired. The earlier guard re-seeded only when
the store scanned *completely empty*, which meant a partial loss left Redis non-empty and
the missing ids missing until the whole keyspace was cleared.

**Observability.** [`RedisCacheStatsProbe`](../../src/main/java/com/recsys/infrastructure/redis/RedisCacheStatsProbe.java)
samples `INFO` every `REDIS_CACHE_STATS_PROBE_SECONDS` (default **30 s**) on the online
server and publishes via
[`RedisCacheMetrics`](../../src/main/java/com/recsys/metrics/RedisCacheMetrics.java):

| Metric | Answers |
|---|---|
| `redis_cache_evicted_keys` | is Redis evicting at all? |
| `redis_cache_used_memory_bytes` / `_max_memory_bytes` | how close is it to `maxmemory`? |
| `redis_cache_keyspace_hits` / `_misses` | Redis-side hit rate, independent of the JVM tiers |
| `redis_cache_evicts_only_volatile_keys` | is the **running** policy still `volatile-*`? |
| `redis_cache_available` | was the last sample even taken? |
| `redis_unexpected_persistent_keys` | is someone writing keys that can never be evicted? |

The policy gauge and the availability flag matter most. The policy gauge catches drift the
manifest test cannot see — a manual `CONFIG SET`, an unmanaged instance, a hand-rolled
compose file. And because the cumulative counters are **retained** across an unavailable
sample (only `_available` drops to 0), a Redis gap can't masquerade as a counter reset and
corrupt `rate()`.

### Surveying the Redis tier: by consumer, not by store

A recurring mistake when reasoning about this tier — three separate attempts made it during the
2026-08-09 ACL work — is to enumerate the Redis *stores* (`RedisEmbeddingStore`,
`ShardedTopKStore`, `RecommendationCache`, …) and treat that as the keyspace. It is not. Around
**29 consumers sit behind the 35 references to `RedisExecutor`**, and the ones a store-first sweep
misses are exactly the interesting ones: classes that build keys inline, and classes that span
several stores. Each of the three store-first passes missed a *different* set.

Two consequences are worth knowing before changing anything in this tier.

**Failures here are usually silent.** `GlobalPopularityStore` catches `RuntimeException` and returns
an empty result so the caller falls back (`GlobalPopularityStore.java:40`), which is correct for a
Redis outage and indistinguishable from a permission error. So a denied `ZREVRANGE
global:item_popularity` makes Popularity and ColdStart recall quietly return nothing on all three
serving services. `SCAN` compounds this: it takes no key argument, so under a restricted ACL the
scan itself succeeds and only the per-key follow-up is refused — six of the seven access failures
found in that audit were invisible for this reason.

**Two Lua scripts touch keys they do not declare in `KEYS`** — the online rate limiter reaches
`rate:online:<bucket>:<windowId>` while declaring only `rate:online:<bucket>`, and the
sharded-record script constructs `sr:rec:…` internally. Today's key patterns cover both, but the
declared `KEYS` list will not warn anyone who narrows them. Note also that Redis requires *full
read-write* permission on every key passed to `EVAL`, even for a read-only script — so no key
reached through a script can be granted read-only access.

**Three consumers are test-only:** `RedisTopKStore`, `RedisDistributedLock` and `RedisMutex` are
constructed nowhere in `src/main/java`, so `dlock:` and `mutex:` are dead prefixes. `WatchdogLock`
is *not* in that group — it has a live construction, and an earlier survey that grouped it with the
other three was wrong.

## 9. What happens when Redis goes down

The sections above describe caches that fail open. That is true on a **warm** cache and false
on a **cold** one, and the difference is not a detail — it is the difference between a degraded
answer and an HTTP 500.

Every serve-stale path guards on a previously cached value. `OnlineFeatureStore`
([:179](../../src/main/java/com/recsys/infrastructure/store/OnlineFeatureStore.java)),
`ShardedTopKStore` ([:138](../../src/main/java/com/recsys/infrastructure/redis/ShardedTopKStore.java))
and `LogicalExpiryEmbeddingCache.loadColdMiss` all take the shape:

```java
} catch (RuntimeException ex) {
    if (cached != null && cached.staleExpiresAtMs > now) return cached;  // warm: serve stale
    throw ex;                                                            // cold: propagate
}
```

`GlobalPopularityStore.getTopIds` is the only one that catches unconditionally and returns an
empty list, letting `Channels.Popularity` fall back to its `DataManager` pool.

| Cached object | Warm cache, Redis down | Cold cache, Redis down |
|---|---|---|
| Recent history / online features | serve stale ≤ 60 s | **throws** |
| Trending top-K | serve stale ≤ 60 s | **throws** |
| User embedding (7010, `LogicalExpiry`) | serve stale indefinitely | **throws** |
| User embedding (6010, `LocalEmbeddingCache`) | heap hit | returns `null` → treated as cold-start |
| Global popularity | serve stale ≤ 60 s | returns empty → `DataManager` fallback |
| Item embedding, recall path | heap hit (classpath) | heap hit — **never reads Redis** |
| Recommendation list (8080) | serve cached result | recomputes; recall degrades per channel |

### The fallbacks are all in the wrong place

[18_Fault_Tolerance §3](18_Fault_Tolerance.md#3-graceful-degradation--a-degraded-answer-beats-no-answer)
documents five degradation layers — per-channel `exceptionally`, `ChannelHealthMonitor` backoff,
quota gap-fill, the trending response fallback, and the gateway circuit breaker. **All five sit
inside or below the channel fan-out.** `OnlineRecommendationService.recommend` makes three Redis
reads *outside* it, on the request thread:

1. `recentHistoryStore.getRecentMovieIds`
   ([OnlineRecommendationService:78](../../src/main/java/com/recsys/application/online/OnlineRecommendationService.java)) — before recall.
2. the cold-start probe inside `MultiChannelRecallService.recall`
   ([:141](../../src/main/java/com/recsys/application/retrieval/multichannel/MultiChannelRecallService.java)),
   whose `catch` names **only** `NumberFormatException` — before any channel is dispatched.
3. `topkStore.getTopKIds` for the response snapshot — after the fan-out.

A `RedisConnectionException` from any of the three bypasses all five layers. Measured against the
real server on a dead Redis port, with a valid user:

```
attempt 1: HTTP 500  {"error":"internal server error"}
attempt 2: HTTP 500  {"error":"internal server error"}
/health/ready -> HTTP 200
```

The stack named read (1) as the first to throw. A separate unit-level probe confirmed read (2)
independently: with one perfectly healthy channel and a throwing embedding store, `recall()`
propagated the exception and the healthy channel was never consulted — no degradation metric, no
`X-Recall-Degraded` header, no trending fallback. The exposure window is the **cold-cache**
window: a restarted pod, a user seen for the first time, or an entry evicted past its stale
bound. In steady state the same outage degrades correctly.

**Fixed (2026-10-07).** The stores still rethrow on a cold cache; the three callers now absorb it
on the replica path, each choosing an explicit degraded answer:

| Read | Degraded answer | Signal |
|---|---|---|
| Recent history | empty — nothing excluded, no `recentMovies` | `online_recommendation_degraded_reads_total{read="recent_history"}` + WARN |
| Cold-user probe | recall as a **cold user** (`QuotaPolicy.cold`) | degraded channel `user-embedding` in `RecallResult` + WARN |
| Trending snapshot | empty `trendingMovies` | `online_recommendation_degraded_reads_total{read="trending"}` + WARN |

The **primary** (read-your-writes) path is unchanged on purpose: it still propagates, and 7010
answers `503` with `Retry-After` rather than a guess. Re-measured against the real server and a
dead Redis port, same user, before and after:

```
main:   attempt 1: HTTP 500  {"error":"internal server error"}     /health/ready -> 200
fixed:  attempt 1: HTTP 200  {"user":{"userId":123,…},"recentMovies":[],"trendingMovies":[],
                              "recommendations":[{"id":11,…}, …]}   /health/ready -> 200
```

Global popularity still answers from its `DataManager` pool, which is why a cold user gets real
recommendations rather than an empty list. The cold-user fallback surfaces as
`recsys_recall_degradation_outcomes_total{outcome="partial"}` on both 6010 and 7010 (7010 only
registered it from 2026-10-07; before that its recall service kept a private, unexported
instance). The channel name `user-embedding` is deliberately never a metric tag on either service
— it lives in the in-process snapshot and the WARN log. Re-measured on 7010 with Redis dead:
two requests → `outcome="partial"` 2, `degraded_reads{read="recent_history"}` 2,
`{read="trending"}` 2. Pinned by `OnlineRecommendationServiceTest`,
`MultiChannelRecallDegradationTest` and `OnlineRecallMetricsWiringTest`, all in the
`-Presilience` gate.

Note the last line of the original measurement above. **Readiness stays green while the main route returns 500**, because
`OnlineHealthService` reports process and load-shedder state rather than dependency health, so
Kubernetes keeps the pod in rotation. That is the intended split — a Redis blip should not
cascade into a rolling restart — but it means the blast radius of a cold-cache outage is every
pod at once, and nothing in the readiness signal reflects it.

### 6010 cannot start at all

`RecSysServer` calls `seedEmbeddings` before building any route, and `writeMissing` issues an
`MGET` against the primary. With Redis down the process exits **code 1**, verified by running it:

```
Exception in thread "main" io.lettuce.core.RedisConnectionException: Unable to connect to …:6399
  at com.recsys.infrastructure.redis.RedisEmbeddingStore.writeMissing(RedisEmbeddingStore.java:155)
  at com.recsys.api.serving.RecSysServer.seedEmbeddings(RecSysServer.java:245)
  at com.recsys.api.serving.RecSysServer.run(RecSysServer.java:96)
```

`LocalEmbeddingCache.warmUp()` (`loadAll`) is a second such read a few lines later. This is the
opposite of what the executor was built for — `LettuceRedisExecutor`'s own javadoc explains that
connections open lazily so "startup paths that build stores up-front keep working and only fail —
fast — at request time, where callers fall back." That property holds for the executor and is
then given away by the two eager reads in `main`. 7010 boots fine under the same conditions and
fails at request time instead.

**Fixed (2026-10-07).** Both are repair operations, not preconditions — the classpath preload has
already given both heap caches every sample embedding — so `RecSysServer.repairAndWarmFromRedis`
runs the seed repair and both `warmUp()` calls best-effort, logging a WARN per skipped step. With
the same dead Redis port, 6010 now boots in ~4 s and answers `/getrecommendation` with `200`. A
step skipped at boot is not retried until the next restart. Pinned by
`RecSysServerRedisDownStartupTest` (in the `-Presilience` gate).

### The other two services

Measured the same way on 2026-10-07 (dead Redis port, user 123):

- **8080, model path** (`POST /api/v1/recommend`) — `200` with real recommendations and no Redis
  error in the log: the ONNX path does not read Redis for a request.
- **8080, retrieval path** (`/api/v1/retrieval/recommend/{user}`, `/embedding/{item}`) — always
  answered `200` with an empty result, but **silently**: `HybridRecommendationService` swallowed
  the failure at its hydration and popularity-fetch stages, and the embedding route set
  `error = false`. An outage was therefore indistinguishable from "nothing to recommend" / "no such
  embedding", and `recommendation_request_errors_total` never moved. Since 2026-10-07 both routes
  add `"degraded": true` to the body and count the request as an error (measured: 2 requests → 2 per
  endpoint; on `main` the series never appeared). This cannot cascade into a rolling restart:
  8080's readiness failure-rate gate reads `InferenceMetricsService` (ONNX inference), not these
  counters — `/health/ready` stayed `200` throughout. Pinned by `HybridRecommendationServiceTest`
  and `RetrievalRecommendationControllerTest`.
- **Gateway** (registry enabled) — boots; the registry refresh fails with "keeping last-good
  snapshot" and routing falls back to the static addresses, as documented in
  [11_Service_Discovery](11_Service_Discovery.md). With the registry off (the default) it opens no
  Redis connection at all.

## 10. Per-object inventory — what each cached thing actually gets

The class table in "The big picture" says what each cache *can* do. This says what each cached
object *has*, which service it has it in, and how it is invalidated.

| Object | Redis key | Cache in front | Pattern | TTL | Invalidation |
|---|---|---|---|---|---|
| User embeddings (7010) | `u2vEmb:<id>` | `LogicalExpiryEmbeddingCache` | cache-aside + soft-TTL serve-stale + 1 bg refresh | soft **30 s** (`ONLINE_USER_EMB_SOFT_TTL_SECONDS`); values size-bounded only | write-through |
| User embeddings (6010) | `u2vEmb:<id>` | `LocalEmbeddingCache` (+Bloom, null sentinel, single-flight) | cache-aside | none on values; sentinels 30 s | write-through |
| Item embeddings — `/similar` | `i2vEmb:<id>` | `LocalEmbeddingCache` | cache-aside | none (seeded with TTL 0) | write-through via `/setembedding`; **periodic refresh from Redis (60 s)** |
| Item embeddings — **recall** | `i2vEmb:<id>` (refresh only) | `CandidateGenerator.embeddingIndex` (heap, LSH/exact/SPANN) | classpath-built at construction, then **diff-refreshed from Redis** off the request path | n/a | `/setembedding` + **periodic refresh (60 s)** |
| Popular items — trending | `topk:{window}:value` | `ShardedTopKStore` | cache-aside | 2 s fresh / 60 s stale | TTL only |
| Popular items — global | `global:item_popularity` | `GlobalPopularityStore` → `TtlSingleFlightCache` | cache-aside, fail-open | 1 s fresh / 60 s stale | TTL only |
| Recommendation lists | — | `RecommendationCache` — **8080 only** | cache-aside | 300 s; cold-start pools 3600 s | version keying (variant + model version) |
| Candidate sets | — | **none** | recomputed per request | n/a | n/a |

Three rows deserve more than a table cell.

**Item embeddings have two consumers — and until 2026-10-07, two stale copies.** The vector index
is built in `CandidateGenerator`'s constructor from `DataLoader.loadMovieEmbeddings()` — the
**classpath** — and `/similar` reads through 6010's `LocalEmbeddingCache`, preloaded from the same
classpath at boot. Earlier versions of this section claimed `/similar` "sees the new vector
immediately"; it never did: a cache hit was returned forever (values have no TTL), and once the
Bloom filter was populated an ID absent at boot was rejected before Redis was consulted. So a
batch `i2vEmb` rewrite reached **neither** consumer until restart, and `POST /setembedding` was the
only path that updated both.

**Since 2026-10-07 both converge on Redis.** `ItemEmbeddingRefresher` (a `GuardedLoop`, every
`ITEM_EMBEDDING_REFRESH_INTERVAL_MS`, default 60 s, `0` disables) runs on every serving pod —
6010, 7010 and 8080. Each pass reads every catalog ID through a lenient chunked `MGET`, then pushes
only vectors that differ from the last one it applied: into the recall index
(`CandidateGenerator.updateEmbedding`) and, on 6010, into the `/similar` cache through a cache-only
`LocalEmbeddingCache.refresh` that also admits the ID to the Bloom filter and never writes back.
The diff is load-bearing — `SpannVectorIndex.addOrUpdate` appends a posting block per call. A
corrupt value is skipped and counted (`recsys_item_embedding_refresh_skipped_total{reason}`) rather
than failing the pass; a key absent from Redis keeps its current vector (eviction is not deletion).
Redis stays off the request path: a failed pass changes nothing in memory, and the loop's
`recsys_loop_seconds_since_success{loop="item-embedding-refresh"}` feeds the existing
`RecsysLoopStale` alert. Measured on a running 6010 against real Redis, after rewriting `i2vEmb:9`
to movie 1's vector and `i2vEmb:10` to user 123's (2 s interval):

```
main:   /similar?movieId=1 [4, 3, 2, 5, 6] → unchanged      recommend(123) unchanged
branch: /similar?movieId=1 [4, 3, 2, 5, 6] → [9, 4, 10, …]  recommend(123) → [9, 10, 5, 6, …]
        recsys_item_embedding_refresh_applied_total 2.0
```

One race is accepted: a pass that read `v1` just before `POST /setembedding` wrote `v2` re-applies
`v1`; the next pass reads `v2` and repairs it, so it lasts at most one interval. Catalog membership
is still deploy-shaped — `DataManager` loads it from the classpath and hits outside it are dropped.

**Recommendation lists are cached on exactly one of the three serving services.** 8080 has
`RecommendationCache` with version keying; 6010 and 7010 recompute every request. The same
logical object, the same user, two orders of magnitude apart in cost depending on which port
answered. That is defensible — the model path is the expensive one and the only one with a
version to key on — but it means "recommendations are cached" is true of the system only in the
sense that one implementation of it caches them.

**Candidate sets are deliberately not cached.** Recommendation pagination is stateless signed
keyset ([19_Pagination](19_Pagination.md)): the cursor carries the anchor position, and each page
re-runs the full multi-channel recall and re-ranks before slicing. Nothing holds the candidate
set between pages, so page 5 costs what page 1 cost. This is what makes the live-keyset contract
possible — a cached candidate set would freeze the ranking and defeat the point — but the cost is
real and bounded only by `maxCandidates`. It is the one object where the right answer to "why is
this not cached" is "because caching it would be a correctness change, not an optimization."

## 11. Two different cold starts

"Cold start" names two unrelated conditions here, handled with very different care.

**A cold *user*** — no embedding for this ID — is a first-class, deliberately designed case.
`MultiChannelRecallService` probes the user-embedding store, and a `null` selects
`QuotaPolicy.cold(limit)` instead of `warm(limit)`, which re-weights channel quotas toward
`ColdStartChannel` (trending + global popularity) and away from personalized channels. On 6010
the Bloom filter makes this cheap and Redis-free: `preload()` populates the filter from the
classpath, so an unknown ID short-circuits to `null` without a round trip. `LogicalExpiryEmbeddingCache`
reaches the same answer through its null-sentinel cache, one Redis miss per 30 s per ID.

**A cold *cache*** — a freshly started pod — is not designed for at all; it is what §9
describes. The two interact badly in exactly one place: 7010's `LogicalExpiryEmbeddingCache`
has neither a classpath preload nor a Bloom filter, so on a new pod every user is a cold miss
that reaches Redis, whereas 6010's cache starts pre-populated from the classpath. Same logical
object, two different caches in front of it. Until 2026-10-07 only 6010 survived
a Redis outage in its first seconds of life; 7010 now answers such a user as a cold user (§9).

**What the cold pod actually costs — measured, and not worth a preload (2026-10-07).** A fresh
7010 against a seeded local Redis, JIT warmed on one user first, then 32 requests for each of the
other five: Redis saw **exactly one** `u2vEmb` read per user, and a user's first request issued only
two per-user reads (that embedding and `user:{id}:recent_movies`); every other read is a shared
trending/popularity snapshot. First requests ran 4–13 ms against a 2.3–3.5 ms warm median, with a
Redis round trip of ~0.8 ms — so the embedding miss is one round trip, once per user per pod, and
most of the first-request delta is per-user code running for the first time, which a preload
would not touch. A preload also cannot scale past the 10,000-entry cap, and a Bloom filter would
only help IDs with no embedding, which the null sentinel already holds to one read per 30 s.
Deliberately left as is.

A third sense is worth separating out because it *is* handled: a **cold Redis**.
`seedEmbeddings` repairs a missing or partially-evicted embedding keyspace per-ID at startup
(§8), `ShardedTopKStore` falls back to the legacy unversioned key before Flink's first canonical
write, and `RecommendationCache` precomputes the `__UNK__` cold-start pool once per model
variant. An empty Redis is a supported state. An *unreachable* one, on a cold JVM, is not.

## Sharp edges — notes

1. **Most caches don't invalidate on writes.** Only the two write-through embedding
   caches (and manual CDN purges) reflect a same-version change promptly; everything else
   serves stale up to its TTL. Version keying, not invalidation, is what keeps result
   caches correct across deploys.
2. **The heap embedding cache has no TTL.** `LocalEmbeddingCache`'s value map evicts by size,
   not time, so an embedding changed in place (same ID, new vector) by anything other than
   `/setembedding` stays until evicted or the pod restarts. (This edge was first written against
   `MultiLevelEmbeddingCache`'s L1 — a class no service constructed. It was **deleted
   2026-10-07** rather than left documented as a Redis-outage defense that no service had.)
3. **Serve-stale is an availability trade — and only on a warm cache.** Under a
   backing-store outage caches serve old data rather than error; that's deliberate (AP), but
   it means a Redis incident can silently extend staleness to the stale-window bound. With
   *nothing* cached for the key, three of the four families rethrow instead, and the three
   pre-fan-out reads in `OnlineRecommendationService.recommend` turned that into an HTTP 500
   with readiness still green (§9, measured). **Resolved 2026-10-07** — each read now has an
   explicit degraded answer on the replica path (empty history, cold user, empty trending);
   the primary path still fails loudly by contract. Each fallback is countable on 7010's
   `/metrics` — `online_recommendation_degraded_reads_total{read}` and
   `recsys_recall_degradation_outcomes_total{outcome="partial"}`.
4. **LLM caching assumes determinism.** Caching a model's output is only sound because the
   demo runs at temperature 0; a nonzero-temperature deployment would serve one sampled
   answer for all identical prompts.
5. **The caches' wait budgets exceed the request deadline.** The cold-miss `SingleFlight`
   (2000 ms), `ShardedTopKStore.FETCH_WAIT_TIMEOUT_MS` (2000 ms),
   `OnlineFeatureStore.REDIS_FETCH_TIMEOUT_MS` (2000 ms) and the recommendation cache's
   compute-wait (2000 ms) were each chosen independently of online serving's 500 ms
   `ONLINE_REQUEST_TIMEOUT_MS`. None is reachable as client-visible latency there; all remain
   load-bearing as thread occupancy — see
   [17_Scalability §2](17_Scalability.md#the-request-time-budget-end-to-end), which also covers
   the pool knobs not bounding the serving path.
6. **Null sentinels are a 30 s bet.** Absent-ID sentinels expire after 30 s, so a newly
   *added* embedding for a previously-missing ID isn't visible until the sentinel lapses
   (or a write-through happens).
7. **`volatile-lru` trades silent eviction for a loud OOM.** Keys without a TTL are no
   longer eviction candidates, so once `maxmemory` is reached and the evictable set is
   exhausted, Redis rejects **writes** with an OOM error rather than dropping state. That
   is the intended failure mode for authoritative data, but it makes `maxmemory` headroom
   something to watch (`redis_cache_used_memory_bytes` vs `_max_memory_bytes`) rather than
   something the policy silently absorbs. The durable set is small and bounded — the
   catalog's embeddings plus one topology document.
8. **The eviction boundary is a writer convention, now sampled rather than assumed.**
   `volatile-lru` is only correct while *every* cache-like writer sets a TTL. Nothing at
   write time enforces that, so
   [`RedisPersistentKeyProbe`](../../src/main/java/com/recsys/infrastructure/redis/RedisPersistentKeyProbe.java)
   walks one bounded `SCAN` page per tick and publishes `redis_unexpected_persistent_keys`
   for keys with no TTL outside the declared durable prefixes (`shard:topology`, `i2vEmb:`,
   `u2vEmb:`, `sr:`, `bias:item:`). It watches the keyspace rather than the code because
   the Flink sinks — the highest-volume writer — are excluded from the Maven compile and
   write through Lua. Two residual gaps: detection is **probabilistic**, so a rarely-written
   key may take many ticks to surface; and the allow-list is itself a declaration that can
   go stale if a new durable namespace is added without updating it.
9. **The item-embedding recall index was never invalidated.** It was built from the classpath
   and nothing refreshed it — and, contrary to what this edge used to say, neither was `/similar`.
   **Resolved 2026-10-07** — `ItemEmbeddingRefresher` diff-applies Redis vectors to both every
   60 s (§10, measured). The Redis dependency it adds is the *refresh's*, not recall's: a failed
   pass leaves the in-memory index serving unchanged.
10. **6010 could not boot with Redis down.** `seedEmbeddings` and `LocalEmbeddingCache.warmUp()`
   were eager Redis reads in `main`, so the process exited code 1 (§9, verified by running it),
   giving away the lazy-connect property `LettuceRedisExecutor` was designed to provide.
   **Resolved 2026-10-07** — both now run best-effort; a step skipped at boot waits for the
   next restart.
11. **One of three serving services caches recommendation lists.** 8080 has
   `RecommendationCache`; 6010 and 7010 recompute per request (§10). Deliberate — only the
   model path has a version to key on — but "recommendations are cached" is not a
   system-level statement.
