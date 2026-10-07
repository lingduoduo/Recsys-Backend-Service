# Item-Embedding Refresh — Design

**Date:** 2026-10-07
**Status:** Approved in conversation (approach A, design parts 1–2), 2026-10-07
**Investigation:** [02_Caching](../../system_design/02_Caching.md) §10, sharp edge 9

## Problem

Item embeddings reach the serving processes once, at startup, and never again.

- **Embedding recall.** `CandidateGenerator` builds its vector index in its constructor from the
  classpath (`DataLoader.loadMovieEmbeddings()`) and nothing refreshes it. All three serving
  services build one: 6010 (`RecSysServer`), 7010 (`OnlinePredictionServer`), 8080
  (`ModelRuntimeProvider.ensureRecallInfra`, lazily, and it can close and rebuild it).
- **`/similar` on 6010.** `LocalEmbeddingCache` (prefix `i2vEmb`) is preloaded from the classpath and
  warmed from Redis at boot. A cache hit is returned forever (values have no TTL), and once the
  Bloom filter is populated, an ID not present at boot is rejected before Redis is consulted. So a
  batch rewrite of `i2vEmb:*` reaches `/similar` neither for changed vectors nor for new IDs.
  `02_Caching` §10 claims `/similar` "sees the new vector immediately"; that is wrong and is
  corrected by this work.

`ItemEmbeddingJob` (Spark; excluded from the Maven compile) writes `SET i2vEmb:<id>` with an optional
TTL and leaves no version marker. `POST /setembedding` is the only path that updates the in-memory
copies, which makes a batch embedding refresh a deploy-shaped operation.

## Goals

- A batch rewrite of item embeddings in Redis reaches embedding recall (all three services) and
  `/similar` (6010) within one refresh interval, with no restart and no change to the batch job.
- Recall keeps **no** Redis dependency on the request path: a Redis outage leaves the in-memory index
  serving exactly as today.
- A refresh that stops working is visible through the existing `RecsysLoopStale` alert.

## Non-goals

- New catalog items. `DataManager` loads the catalog from the classpath; search hits are mapped
  through `DataManager.getMovieById` and unknown IDs are dropped, so an item outside the catalog is
  useless to recall. Catalog changes remain deploys.
- Removing items from the index (`VectorIndex` has no remove; a missing Redis key is not a deletion).
- Embedding-dimension changes (a deploy-time operation; the index rejects mismatched vectors).
- The 8080 ONNX ranking path's item embeddings (`recsys.model.item-embeddings-source`), a separate
  mechanism.
- Changing `ItemEmbeddingJob`.

## Design (approach A — diff and apply)

### Components

**`ItemEmbeddingSink`** (new, `infrastructure/vectordb`): `void apply(int id, float[] vector)`.

**`ItemEmbeddingRefresher`** (new, `infrastructure/vectordb`). Inputs: a supplier of catalog IDs
(`DataManager::getAllMovieIds`), the item `RedisEmbeddingStore` (`i2vEmb`), the expected dimension,
the sinks, an initial last-applied map (the classpath vectors), and a `MeterRegistry` (nullable).

`refreshOnce()`:
1. Read every catalog ID through a **lenient** batch read (below) — chunked `MGET`s, chunk size the
   store's existing `REDIS_EMBEDDING_MGET_BATCH_SIZE` (default 500).
2. For each returned vector:
   - wrong dimension → skip, count `recsys.item_embedding.refresh.skipped{reason="dimension"}`;
   - equal (`Arrays.equals`) to the last applied vector → nothing;
   - otherwise → `apply` to every sink, record as last applied, count `…refresh.applied`.
3. A catalog ID absent from Redis keeps its current vector (keys evict and expire; absence is not a
   deletion) and is not counted.

The diff is load-bearing, not an optimization: `SpannVectorIndex.addOrUpdate` appends a posting
block per call, so re-applying unchanged vectors every interval would grow the SPANN file without
bound.

Scheduling: `GuardedLoop("item-embedding-refresh", refresher::refreshOnce)` on one daemon
single-thread scheduler, every `ITEM_EMBEDDING_REFRESH_INTERVAL_MS` (default **60000**; **0
disables**; negative rejected at construction), bound to the service registry
(`recsys.loop.seconds_since_success{loop="item-embedding-refresh"}`). The 60 s default keeps the
existing `RecsysLoopStale` rule (`> 300` s, written for 30 s loops) meaningful: it fires after about
five missed refreshes, so no new alert rule is needed.

**`RedisEmbeddingStore.getEmbeddingsLenient(Collection<Integer> ids, IntConsumer onCorrupt)`** (new).
Same chunked `MGET` as `getEmbeddings`, but a value `VectorMath.parseVector` rejects is skipped and
reported to `onCorrupt` instead of aborting the call. Without it, one corrupt `i2vEmb` key would fail
every refresh pass forever, because `getEmbeddings` throws on the first unparseable value. The
refresher counts these as `skipped{reason="corrupt"}`. `getEmbeddings` is unchanged.

**`LocalEmbeddingCache.refresh(int id, float[] vector)`** (new, cache-only). Adds the ID to the Bloom
filter, invalidates any null sentinel, and puts the value. It **never writes to the backing store**
— unlike `setEmbedding`, which writes through and would echo back to Redis what was just read.
`BloomFilterGuard` is an `AtomicLongArray`, so concurrent `add`/`mightContain` are safe.

### Wiring

| Service | Item store | Sinks | Lifecycle |
|---|---|---|---|
| 6010 `RecSysServer` | existing `embStore` (`i2vEmb`) | `CandidateGenerator::updateEmbedding`, `embCache::refresh` | started after the caches are built; stopped in the shutdown path |
| 7010 `OnlinePredictionServer` | new `RedisEmbeddingStore(jedisPool, "i2vEmb")` | `CandidateGenerator::updateEmbedding` | started after the generator is built; stopped in the shutdown path |
| 8080 `ModelRuntimeProvider` | new `RedisEmbeddingStore(recallPool, "i2vEmb")` | `CandidateGenerator::updateEmbedding` | started where `ensureRecallInfra` builds the generator; stopped where the generator is closed, so a rebuilt generator gets a fresh refresher |

`CandidateGenerator` exposes the classpath vectors it was built from (read-only) so the refresher's
last-applied map starts there and the first pass applies only genuine differences.

## Failure behavior

- **Redis unavailable:** the lenient read still throws on connection failure → `refreshOnce` throws →
  `GuardedLoop` records it; index and cache are untouched and recall serves from memory.
  `RecsysLoopStale` fires after 300 s without a success — the index has stopped tracking Redis.
- **Partial pass:** chunks already applied stay applied (each is a newer vector); the next pass
  completes the rest.
- **Corrupt value:** skipped and counted; every other ID still refreshes.
- **Wrong dimension:** skipped and counted; the index would reject it anyway.

## Concurrency

Index writes from the refresher and from `/setembedding` both go through the synchronized
`CandidateGenerator.updateEmbedding`, so they never interleave. One race is accepted: the refresher
reads `v1`, `/setembedding` writes `v2` to Redis and the index, the refresher then applies `v1`. The
index regresses to `v1` until the next pass reads `v2` (last applied is `v1`, so it differs and is
reapplied) — bounded by one interval, never permanent.

Memory: the last-applied map holds one vector per catalog item (~2.5 MB at 10k items × 64 floats).
Exact comparison was chosen over a hash so a collision can never silently skip an update.

## Configuration

`ITEM_EMBEDDING_REFRESH_INTERVAL_MS` — default 60000, 0 disables, negative rejected. Set explicitly
in `k8s/base/configmap.yaml`. Documented in `02_Caching.md` and noted for CLAUDE.md in the PR (not
committed, per project convention).

## Testing

Each test is watched failing before its implementation exists.

1. `ItemEmbeddingRefresherTest` (unit, fake store and recording sinks): a changed vector reaches every
   sink exactly once; an unchanged vector is not re-applied on the next pass; a missing key keeps the
   current vector; wrong dimension → skipped and counted; a store failure throws with sinks
   untouched; interval 0 / negative validation.
2. `RedisEmbeddingStore` lenient read: a corrupt value among valid ones is reported and skipped,
   the valid ones are returned; `getEmbeddings` keeps throwing (unchanged contract).
3. `LocalEmbeddingCache`: first, a test pinning today's defect — an ID written to the backing store
   after `preload` returns `null` (Bloom rejection); then `refresh` makes it visible, clears a null
   sentinel, and never invokes a backing-store write.
4. `CandidateGenerator`: after `updateEmbedding` through a refresh moves an item's vector,
   `byEmbedding` ranks it differently.
5. End to end on real Redis (Homebrew, throwaway port), `ITEM_EMBEDDING_REFRESH_INTERVAL_MS=2000`:
   start 6010, rewrite one `i2vEmb` vector in Redis, and observe `/similar` and recall change within
   the interval; repeat on `main` as the baseline (never changes).
6. The unit tests join the `-Presilience` gate; full suite before the PR.

## Docs

- `02_Caching.md` §10: correct the `/similar` claim; describe the refresh. Sharp edge 9 → resolved.
- `15_Eventual_Consistency.md`: the item-embedding staleness bound becomes one refresh interval.
- PR description: the CLAUDE.md line for `ITEM_EMBEDDING_REFRESH_INTERVAL_MS`.
