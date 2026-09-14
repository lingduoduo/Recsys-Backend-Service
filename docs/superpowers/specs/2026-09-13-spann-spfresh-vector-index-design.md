# SPANN / SPFresh Vector Index Design

## Objective

Add a disk-resident approximate-nearest-neighbour backend for the offline serving path's
embedding recall so that the vectors themselves leave the JVM heap. Today every backend behind
`VectorIndex` keeps the full `Map<Integer, float[]>` on the heap: `ExactVectorIndex` holds it
outright and `LshVectorIndex` sits on top of it and falls back to scanning it whenever the
bucket set is thin. SPANN's layout — a small in-memory index over centroids, with the vectors
in posting lists on disk that a query touches selectively — cuts heap to the centroid table
and the id map, and cuts per-query CPU to a centroid scan plus a handful of posting reads.
SPFresh's in-place update protocol keeps that layout balanced under `/setembedding` writes
without a rebuild.

This is a mechanism with a measured envelope, not a tuned production index: the classpath
corpus is 12 six-dimensional vectors, so every cost claim is established on synthetic data by
an opt-in benchmark, and the acceptance bar in §Acceptance Criteria is the bar the user chose.

## Current Architecture

- `VectorIndex` (`infrastructure/vectordb`) — `search(float[] query, int k, Set<Integer> excludeIds)`,
  `name()`, default no-op `addOrUpdate(int id, float[] vec)`. Results are `SearchResult(id, score)`,
  score = raw inner product (`VectorMath.innerProduct`), ordered score descending.
- `ExactVectorIndex` — `ConcurrentHashMap<Integer, float[]>`, bounded top-k heap over every entry.
- `LshVectorIndex extends ExactVectorIndex` — `EmbeddingLSH` random-hyperplane buckets with
  Hamming-1 probing for the candidate set; full scan when candidates < k.
- `ExactMultiVectorIndex` (#322/#324) — the multi-vector reranker; orders ties by id ascending.
- `CandidateGenerator` is the only consumer: `createEmbeddingIndex` switches on
  `RECSYS_VECTOR_BACKEND` / `-Drecsys.vector.backend` (`exact|flat`, `lsh|ann` default, `faiss`
  → warns and falls back to LSH). `byEmbedding` calls `search(userVec, k, watched)`;
  `updateEmbedding` is `synchronized` and calls `addOrUpdate` after a dimension check.
- `RecSysServer` builds one `CandidateGenerator` at startup and registers a shutdown hook that
  closes its owned resources; the generator has no close today.
- No `VectorIndex` has a metrics registry. `CandidateGenerator` is constructed without one.

## Scope

1. `SpannVectorIndex implements VectorIndex` and `Closeable`, in a new package
   `com.recsys.infrastructure.vectordb.spann`, selectable with `RECSYS_VECTOR_BACKEND=spann`.
2. SPANN build: k-means++-seeded, sample-based Lloyd clustering; posting lists as immutable
   blocks in one memory-mapped file; in-heap centroid table.
3. SPANN search: nearest-centroid probing with a bounded top-k heap, deterministic tie order,
   and a bounded widening fallback when fewer than `k` results are found.
4. SPFresh update on `addOrUpdate`: supersede-on-overwrite, insert into the nearest posting,
   split on overflow, bounded neighbourhood reassignment, merge on underflow, compaction of
   dead blocks.
5. Lifecycle: per-process index file under a configurable directory, removed on close and on
   JVM exit; `VectorIndex` gains a default no-op `close()`, `CandidateGenerator` gains `close()`,
   and `RecSysServer`'s shutdown hook calls it.
6. A stats snapshot (splits, merges, reassignments, compactions, file bytes, distance
   computations) for tests, the benchmark, and INFO logging on rebalancing events.
7. Unit tests in the `-Presilience` PR gate; a `@Tag("load")` synthetic benchmark asserting the
   acceptance bar.
8. Documentation folded into `docs/system_design/13_DB_Indexing.md` §5, where the existing
   index types live.

## Non-Goals

- **Persistence across restarts.** The file is rebuilt from the loaded embeddings on every boot
  and deleted on shutdown. Checkpoint, recovery, and a k8s volume are a separate lifecycle.
- **A graph or tree index over centroids.** SPANN proper uses SPTree over centroids. At the
  benchmark scale (~200k vectors, posting max 128) there are ~3k centroids and a brute-force
  L2 scan of them costs ~200k multiply-adds per query; with 8 probes of ~64 entries that is
  ~230k multiply-adds against exact's 12.8M, roughly 50× fewer. The centroid scan becomes the bottleneck at roughly 10M+
  vectors; the spec names that boundary rather than building for it.
- **Multi-vector postings.** SPANN is single-vector. `ExactMultiVectorIndex` stays the
  reranker for token bags.
- **Native or SIMD kernels.** Distances go through the existing `VectorMath` and a plain L2
  loop.
- **A delete API.** `VectorIndex` has no remove; an entry only ever goes stale because the same
  id was overwritten, which hides the previous
  vector of an overwritten id.
- **Distance pruning.** SPANN's `(1 + ε)` query-aware pruning is an L2 device; probing here is
  by inner product (§Search), so it is not implemented and there is no `RECSYS_SPANN_PRUNE_EPSILON`.
- **Prometheus metrics.** No `VectorIndex` has a registry today; threading one through
  `CandidateGenerator` is its own change. The stats snapshot is the seam it would attach to.
- **Changing the default backend.** `lsh` remains the default; no manifest sets `spann`.

## Components

All in `com.recsys.infrastructure.vectordb.spann`:

| Class | Role |
|---|---|
| `SpannVectorIndex` | `VectorIndex` + `Closeable`; owns the writer lock, the snapshot, and the SPFresh steps |
| `SpannConfig` | record of the §Configuration values; `fromEnv(EnvVars.EnvReader)` with validation |
| `CentroidTable` | in-heap arrays (centroid vectors, block offsets, live counts, alive flags), grown by doubling |
| `PostingStore` | interface: `long append(Block)`, `Block read(long offset)`, `long bytes()`, `close()`; the seam the write-failure test injects through |
| `MappedPostingStore` | `PostingStore` over the memory-mapped file: region mappings, tail writes, `CREATE_NEW`, delete on close |
| `KMeans` | k-means++ seeding + Lloyd iterations, seeded; used by build (k centroids) and split (k = 2) |
| `SpannStats` | immutable stats snapshot (§Observability) |

## On-Disk Layout

One file per index instance, `<dir>/spann-<pid>-<nanotime>.bin`, memory-mapped read-only for
searches and written through a `FileChannel` positioned at the tail.

- **Block** = one posting list, immutable once written:
  `int entryCount`, then `entryCount` entries of `int id` + `dim × float`. An entry's
  *liveness* is not stored in the block (see the id map below), so `entryCount` may exceed
  the posting's live count. Blocks are 8-byte aligned. A block is never modified in place;
  any change to a posting writes a new block at the tail and the centroid table's offset for
  that centroid is repointed.
- **Mapping**: the file is mapped in fixed-size regions (default 64 MiB) so growth is a new
  region mapping, never a remap of existing ones. A block never straddles regions: if it would,
  the tail is padded and the block starts the next region.
- **Dead bytes** = bytes of blocks no centroid points to, plus stale entries inside live
  blocks. Tracked as a counter; compaction (below) rewrites every live block's live entries into
  a new file, swaps the snapshot, and deletes the old file.

Approach chosen over (B) fixed-capacity slots mutated in place and (C) off-heap direct buffers:
(B) makes readers see torn writes without a per-posting lock on the hot path and averages
half-empty slots; (C) only moves bytes from heap to resident memory, whereas a mapped file's
pages are page cache the kernel may reclaim — which is the property "disk-resident" is meant
to buy. Note for k8s: page cache is charged to the cgroup but is reclaimable, so under a memory
limit it behaves as a soft cost, unlike heap.

## In-Heap State

- **Centroid table**: `float[][] centroid`, `long[] blockOffset`, `int[] live`, `boolean[] alive`,
  grown by doubling; dead centroids (after split/merge) keep their slot for the life of the index —
  compaction reclaims file bytes, never slot numbers, because the concurrent id map stores slot
  numbers and renumbering would make a reader's liveness test lie during the swap. A dead slot
  costs a few bytes of arrays and one shared centroid reference.
- **Id map**: id → centroid slot of the id's *current* entry (`ConcurrentHashMap`). It is
  both the locator for overwrites and the liveness test on read: an entry `(id, vec)` found in
  centroid `c`'s block is live iff `idMap.get(id) == c`. An overwrite therefore needs no
  separate tombstone structure — repointing the id map to the new posting is what kills the
  old entry, and a re-inserted id is never hidden by a stale tombstone. Stale entries are
  dropped whenever their block is rewritten, and counted as dead bytes until then. This is the
  dominant heap cost after the centroid table (a boxed map is ~40 B/id; at 200k ids ≈ 8 MB
  against ≈ 60 MB for the exact backend's `float[]`s alone — inside the 25% bar, and a
  primitive int→int map is the first lever if it is not).
- **Snapshot**: an immutable object holding the region mappings and the centroid arrays.
  Readers dereference one `volatile` snapshot for the duration of a search; writers build the
  next one and publish it. Old mappings are released by GC — Java has no safe explicit unmap.

## Build

Input: `Map<Integer, float[]>`, all of one dimension (`CandidateGenerator` already guards the
update path; the build path validates and throws `IllegalArgumentException` on a mismatch).

1. `numCentroids = max(1, ceil(N / (postingMax / 2)))` so postings start about half full and
   have headroom before the first split.
2. k-means++ seeding on a uniform sample of `min(N, 20_000)` vectors, then Lloyd iterations on
   the sample, at most 10 or until the maximum centroid movement is below 1e-4 · mean norm.
   Seeded (`RECSYS_SPANN_SEED`, default 42) so builds are reproducible in tests.
3. Assign every vector to its nearest centroid by L2; write one block per centroid.
4. Any posting above `postingMax` after assignment is split by the same routine as the update
   path (§SPFresh), so build and update share one code path and one set of tests.
5. Empty input → an index with zero centroids; `search` returns an empty list; `name()` is
   `"spann"`.

## Search

1. Score every live centroid (in heap) by **inner product** with the query and select the
   `nprobe` highest. Probing uses the scoring metric, as FAISS's inner-product IVF does: the
   inner-product top hits for a query sit at the far edge of its cluster along the query's
   direction, at a *typical* L2 distance from the query, so the L2-nearest postings would
   mostly miss them (estimated recall ≈ 0.2 on the benchmark data); the sub-postings whose
   centroids have the highest inner product with the query are exactly where they live.
   SPANN's `(1 + ε)` distance pruning is defined on L2 and has no clean inner-product form,
   so there is no pruning: `nprobe` and the widening fallback are the only controls.
2. For each selected centroid read its block from the snapshot's mapping and score every entry
   whose id is neither in `excludeIds` nor stale (id map pointing elsewhere), using
  `VectorMath.innerProduct` — the same
   score as the other backends, so `SearchResult.score` means the same thing regardless of
   backend.
3. Bounded top-k heap ordered score ascending then id descending, output reversed, exactly as
   `ExactMultiVectorIndex` does (#324): equal scores order by id ascending in both the cut and
   the output.
4. If fewer than `k` results were produced (small corpus, heavy exclusion), double `nprobe` and
   repeat, up to all live centroids. This mirrors `LshVectorIndex`'s full-scan fallback but
   costs block reads, not a heap-wide scan.

Metric note: the *partition* is L2 (k-means, insert placement, split, reassign, merge all use
L2 to the centroid) while *probing and scoring* are inner product. A vector sits in the posting
of its L2-nearest centroid, so a posting's entries are its centroid plus small noise, and the
postings with the highest ⟨query, centroid⟩ are the ones holding the highest ⟨query, entry⟩.
Recall against exact is measured by the benchmark, not assumed; `nprobe` is the knob.

## SPFresh Update

`addOrUpdate(id, vec)` runs under the index's own writer lock (a `ReentrantLock`; the
`CandidateGenerator` caller is also `synchronized`, so this is belt-and-braces for standalone
use). Every step below ends by publishing a new snapshot; a step that throws leaves the previous
snapshot in service.

Tombstones, as a structure, do not exist: liveness is the id map (§In-Heap State), so
"stale entry" below means an entry whose id the map no longer attributes to that posting.

1. **Supersede.** If `id` is in the id map, its old entry's bytes become dead the moment the
   map is repointed in step 2 (decrement the old centroid's live count; add to dead bytes).
2. **Insert.** Find the nearest live centroid by L2. Rewrite that centroid's block with the new
   entry appended and the posting's stale entries (id map no longer pointing here, or the same
   id being overwritten in place) dropped; repoint the centroid's offset; set `idMap[id]`;
   update the live count. The id map write is the linearisation point: a concurrent reader
   sees either the old entry (map still pointing at the old posting) or the new one, never
   both.
3. **Split** if the posting's live count now exceeds `postingMax`: run 2-means (k-means++
   seeded, ≤ 10 iterations) over the posting's live vectors; write two new blocks; append two
   centroids; mark the old centroid dead.
4. **Reassign** (the "lightweight" in LIRE): take the union of postings of the `reassignProbe`
   (default 4) nearest live centroids to each new centroid, excluding the two new postings
   themselves. For every entry in that union whose nearest live centroid is now one of the two
   new ones, move it: rewrite the source and destination blocks. This bounds rebalancing to a
   neighbourhood instead of a global rescan; the price is that a vector far from the split can
   stay in a now-suboptimal posting until it is next touched. That is SPFresh's trade and it is
   named as such in the doc.
5. **Merge** if any rewritten posting's live count drops below `postingMin` and there is more
   than one live centroid: append its live entries to the nearest other live centroid's block
   (that centroid's vector is left unchanged, as SPFresh does), repoint the ids, mark the
   emptied centroid dead. If the merged block exceeds `postingMax`, split it
   once (no further cascade: a second overflow is left for the next update, and counted).
6. **Compact** if `deadBytes > compactRatio × fileBytes`: write every live centroid's live
   entries into a new file, build a new snapshot over it, publish, delete the old file. The
   writer lock is held throughout; readers keep the old snapshot until publish.

Dimension mismatch on `addOrUpdate` throws `IllegalArgumentException` before any step, matching
`CandidateGenerator.updateEmbedding`'s existing contract.

## Configuration

All read once at construction via `EnvVars`; defaults are the benchmark's parameters.

| Variable | Default | Meaning |
|---|---|---|
| `RECSYS_VECTOR_BACKEND` | `lsh` | `spann` selects this index (existing switch) |
| `RECSYS_SPANN_DIR` | `java.io.tmpdir` | directory for the index file; must be writable |
| `RECSYS_SPANN_POSTING_MAX` | `128` | split threshold (live entries) |
| `RECSYS_SPANN_POSTING_MIN` | `16` | merge threshold (live entries) |
| `RECSYS_SPANN_NPROBE` | `8` | centroids probed per query before widening |
| `RECSYS_SPANN_REASSIGN_PROBE` | `4` | neighbouring centroids checked on split |
| `RECSYS_SPANN_COMPACT_RATIO` | `0.5` | compaction trigger, dead bytes / file bytes |
| `RECSYS_SPANN_SEED` | `42` | clustering seed |

Invalid values fail construction (and therefore startup) with a message naming the variable,
matching `EnvVars.readInt`'s behaviour. `postingMin` must be < `postingMax / 2`, else the
merge/split pair could oscillate; that is validated.

## Lifecycle, Failure, and Concurrency

- **Creation**: the file is created with `CREATE_NEW`; a collision is a bug and throws.
  `deleteOnExit` is registered as the last-resort cleanup.
- **Close**: `SpannVectorIndex.close()` takes the writer lock, drops the snapshot, and deletes
  the file. Subsequent `search` returns empty; subsequent `addOrUpdate` throws
  `IllegalStateException`. `CandidateGenerator.close()` closes its index (no-op for the other
  backends via the new default), and `RecSysServer`'s existing shutdown hook calls it.
- **Write failure** (disk full, I/O error): the step throws `UncheckedIOException`, the
  previous snapshot stays published, and `/setembedding` surfaces the existing 500. Reads are
  unaffected. There is no automatic retry.
- **Readers vs writers**: a search never blocks. It pins one snapshot; every block it reads
  is immutable; liveness is read through the concurrent id map. A search
  concurrent with an update may return the pre-update or post-update view of that one id, never
  a torn block and never both versions.
- **Threads**: no background threads. Every rebalancing step runs synchronously inside the
  `addOrUpdate` that triggered it, so the cost is visible on the write path and there is no
  loop to guard (cf. the `GuardedLoop` findings in 18_Fault_Tolerance §9).

## Observability

`SpannVectorIndex.stats()` returns an immutable snapshot: `centroidsLive`, `centroidsTotal`,
`entriesLive`, `fileBytes`, `deadBytes`, `splits`, `merges`, `reassigned`, `compactions`,
`distanceComputations` (centroid + entry scorings, a `LongAdder`), `fallbackWidenings`.
Split, merge, and compaction each log one INFO line with the affected counts. The benchmark
reads `distanceComputations` as its CPU proxy because wall-clock on shared hardware is not a
number a test should assert on.

## Testing Strategy

Unit tests in `src/test/java/com/recsys/infrastructure/vectordb/spann/`, every one added to the
`-Presilience` surefire include list, each written first and watched failing:

- **Build**: every id retrievable with `nprobe = all`; every posting ≤ `postingMax`; empty input
  → empty index; mixed dimensions rejected.
- **Search**: with `nprobe = all` the result equals `ExactVectorIndex.search` on the same map
  (ids and scores); exclusion honoured; `k ≤ 0` / null query → empty; equal scores order by id
  ascending in the cut and the output (bucket-colliding ids, as in #324); widening fallback
  fills `k` when the initial probe cannot.
- **SPFresh**: overwrite hides the old vector and returns the new; inserting past `postingMax`
  splits (stats.splits increments, both new postings ≤ max, old centroid dead); reassignment
  moves a vector that is provably nearer to a new centroid than to its old one; dropping below
  `postingMin` merges; compaction triggers at the ratio and every live id is still retrievable
  with identical scores; a write failure (injected via a store that throws) leaves search
  results unchanged.
- **Concurrency**: N reader threads searching continuously while one writer inserts and
  overwrites; every result id is either the original or an overwritten value, never a stale
  vector's score for a new id, and no exception.
- **Lifecycle**: the file exists after build and is gone after `close()`; `search` after close
  is empty; `addOrUpdate` after close throws.
- **Wiring**: `CandidateGenerator` with `-Drecsys.vector.backend=spann` reports `name() ==
  "spann"`; `close()` removes the file.

Benchmark `SpannBenchmarkLoadTest` (`@Tag("load")`, excluded by default, run with
`mvn test -DexcludedGroups="" -Dgroups=load -Dtest=SpannBenchmarkLoadTest`):
200 000 vectors × 64 dims from a 64-component Gaussian mixture with a fixed seed; 1 000 query
vectors drawn the same way. Builds `ExactVectorIndex`, `LshVectorIndex`, `SpannVectorIndex`
in separate phases, each preceded and followed by `System.gc()` and a `MemoryMXBean` heap
reading (the pattern `*GcObservationTest` already uses, polled rather than read once).
Computes recall@10 of LSH and SPANN against exact, and distance computations per query from
`stats()` (exact = N by construction). Prints all three rows; asserts the SPANN row only.

## Compatibility and Rollout

- Default backend unchanged; no manifest sets `spann`; the `faiss` placeholder keeps its
  current fallback.
- `VectorIndex.close()` is a default method, so the two existing implementations are
  source- and binary-compatible.
- Enabling in k8s needs a writable `RECSYS_SPANN_DIR` (an `emptyDir` is sufficient — nothing
  persists) and a memory limit that accounts for page cache as a reclaimable component; the
  doc section says so. Rollback is unsetting the variable.
- No API, Redis key, or response shape changes.

## Acceptance Criteria

1. `RECSYS_VECTOR_BACKEND=spann` serves `/getrecommendation` embedding recall with results
   whose scores are inner products identical in meaning to the other backends, and
   `/setembedding` writes are visible to the next search.
2. All unit tests above pass in the `-Presilience` gate; each was observed failing first.
3. The benchmark at 200k × 64 asserts, and passes: retained heap after SPANN build ≤ 25% of
   retained heap after exact build; recall@10 ≥ 0.90 against exact at the default `nprobe`;
   mean distance computations per query < N. The LSH row is printed, not asserted.
4. The index file is absent after `RecSysServer`'s shutdown hook runs.
5. `13_DB_Indexing.md` §5 documents the layout, the L2-partition / inner-product-probe metric note, the
   SPFresh bounded-reassignment trade, the configuration table, the page-cache note for k8s,
   and the measured benchmark numbers with the date and machine they were taken on.
