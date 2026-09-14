# Database Indexing in Recsys-Backend-Service

An investigation of how the relational read model earns its query performance:
which secondary indexes exist and why, how every query is *pinned* to its index
and kept honest by contract tests plus a real `EXPLAIN`, and the three index-access
patterns (covering count, keyset seek, delayed-join deep-offset) the code emits.
This is the indexing counterpart to the
[Partitioning investigation](14_Partitioning.md) — where that doc treats a keyset
cursor as a way to *partition a result set*, this one treats the same query as an
*index range-scan* and asks which B-tree makes it cheap.

## The big picture

MySQL here is a deliberately small, opt-in read model (the serving hot paths stay
on Redis/ONNX), so indexing is governed by two rules:

- **An index is a contract, not a hope.** Every fixed query pins its plan with
  `FORCE INDEX (...)` rather than trusting the optimizer, and that pinning is
  guarded by a static contract test (hint + exact column order) *and* a
  Docker-tagged `EXPLAIN` that asserts the optimizer actually chose the index.
- **Widen an index only on evidence.** The secondary indexes deliberately omit
  projected payload columns; covering is added only when `EXPLAIN ANALYZE`,
  slow-query logs, or representative benchmarks show clustered-row lookup is the
  bottleneck — trading a smaller, cheaper-to-maintain B-tree for an occasional
  clustered lookup until proven otherwise.

The full secondary-index inventory (7 indexes across three migrations):

| Index | Table | Columns | Serves |
|---|---|---|---|
| `idx_movies_genre_popularity_id` | `movies` | `(genre, popularity_score DESC, id DESC)` | Genre-filtered catalog page |
| `idx_movies_popularity_id` | `movies` | `(popularity_score DESC, id DESC)` | Global (unfiltered) catalog page |
| `idx_outbox_claim` | `event_outbox` | `(status, next_attempt_at, created_at)` | Dispatcher claim scan |
| `idx_outbox_lease` | `event_outbox` | `(status, lease_expires_at)` | Expired-lease reclaim |
| `idx_outbox_reconcile` | `event_outbox` | `(destination, status, created_at, broker_acknowledged_at)` | Reconciliation sweep |
| `idx_outbox_aggregate` | `event_outbox` | `(aggregate_type, aggregate_id, created_at)` | Per-aggregate history |
| `idx_saga_correlation` | `saga_instance` | `(correlation_id, created_at)` | Saga lookup by correlation |

An allowlist test (`MySqlIndexContractTest`) asserts the migrations define
*exactly* these seven and no stray indexes.

## 1. The catalog secondary indexes

`V1__create_movies_catalog.sql`
([src/main/resources/db/migration/V1__create_movies_catalog.sql](../../src/main/resources/db/migration/V1__create_movies_catalog.sql))
creates `movies` (`id`, `title`, `year`, `genre`, `popularity_score DECIMAL(12,6)`,
`updated_at`) and two composite indexes:

```sql
-- inline in CREATE TABLE movies
INDEX idx_movies_genre_popularity_id (genre, popularity_score DESC, id DESC)

-- standalone
CREATE INDEX idx_movies_popularity_id ON movies (popularity_score DESC, id DESC)
```

Both catalog queries order by `(popularity_score DESC, id DESC)` — popularity is
the sort key and `id` is the deterministic tiebreaker that also anchors the keyset
cursor (§3).

**Why both are needed.** MySQL's leftmost-prefix rule means the
genre-leading B-tree (`idx_movies_genre_popularity_id`) is ordered by `genre`
first, so it can only give a popularity order *within a genre*. Serving a
**global** `ORDER BY popularity_score` from it would require scanning every genre
partition and merge-sorting — so a dedicated `(popularity_score DESC, id DESC)`
index provides the global order directly. The genre index is not redundant with
the global one, and vice versa.

**Why payload columns are omitted.** Appending `title` / `year` / `updated_at`
would make either index *covering* (the query answered entirely from the index,
no clustered-row lookup), but it would unconditionally grow the B-tree, add
buffer-pool pressure, and amplify writes. With no repository-level evidence that
the clustered lookup is the bottleneck, covering is deliberately deferred — see
the [MySQL index audit](../superpowers/specs/2026-07-18-mysql-index-audit-design.md)
and [robust catalog querying](../superpowers/specs/2026-07-15-robust-mysql-catalog-querying-design.md)
designs. The verification evidence is defined in [§6](#6-testing-the-indexes): the
static index contracts and the Docker-tagged optimizer assertion are both required.

## 2. Plan pinning and query-to-index contracts

The two catalog queries are not hand-tuned per call — they live as fixed plans in
[`MovieCatalogRepository`](../../src/main/java/com/recsys/infrastructure/persistence/MovieCatalogRepository.java),
each carrying a `FORCE INDEX` hint so the optimizer can't drift to a worse plan:

```sql
-- FILTERED_SQL
FROM movies FORCE INDEX (idx_movies_genre_popularity_id)
WHERE genre = ? AND (popularity_score, id) < (?, ?)
ORDER BY popularity_score DESC, id DESC LIMIT ?

-- UNFILTERED_SQL
FROM movies FORCE INDEX (idx_movies_popularity_id)
WHERE (popularity_score, id) < (?, ?)
ORDER BY popularity_score DESC, id DESC LIMIT ?
```

The row-tuple comparison `(popularity_score, id) < (?, ?)` is the keyset seek
predicate (§3); the opening page uses a sentinel "before the first row" anchor
(`BEFORE_FIRST_SCORE = 1000000.000000`, `id = Long.MAX_VALUE`).

That pinning is enforced at two levels — both required for **every** new
production query:

- **Static contract** —
  [`MySqlIndexContractTest`](../../src/test/java/com/recsys/infrastructure/persistence/MySqlIndexContractTest.java)
  via
  [`MySqlIndexContractAssertions`](../../src/test/java/com/recsys/infrastructure/persistence/MySqlIndexContractAssertions.java)
  checks, with no database, that (a) the migration declares the index *exactly
  once*, (b) with the exact ordered column list (inline or standalone form), and
  (c) the repository plan contains `FORCE INDEX (<name>)`, the expected equality
  predicate, and the expected `ORDER BY`. A separate allowlist test asserts the
  migration set defines only the seven workload-required indexes.
- **Real `EXPLAIN`** —
  [`MovieCatalogMySqlIntegrationTest`](../../src/test/java/com/recsys/infrastructure/persistence/MovieCatalogMySqlIntegrationTest.java)
  (`@Tag("docker")`, Testcontainers `mysql:8.4`) runs the real Flyway migration,
  seeds rows with score ties (to exercise the `id` tiebreaker), and asserts the
  optimizer's chosen `key` equals the expected index. This is the authoritative
  check — the static test proves the *hint* is present; `EXPLAIN` proves the
  *engine* honors it.

So a new catalog query ships three things together: a Flyway-managed index, a
static FORCE-INDEX/column-order contract, and a Docker-tagged `EXPLAIN` assertion
([index audit design](../superpowers/specs/2026-07-18-mysql-index-audit-design.md)).
Plans execute through
[`MySqlClient`](../../src/main/java/com/recsys/infrastructure/persistence/MySqlClient.java) —
a **read-only** HikariCP pool (`setReadOnly(true)`, max 5) that applies a
per-statement `setQueryTimeout` (`MYSQL_QUERY_TIMEOUT_SECONDS`, default 2) and
binds every request value as a positional parameter.

## 3. Index-access patterns via `MillionScalePaginationSql`

> **What is live here, and what is not.** The builder methods below are a **reference
> implementation** of the access patterns — nothing in `src/main` calls them. The live
> catalog path,
> [`MovieCatalogRepository`](../../src/main/java/com/recsys/infrastructure/persistence/MovieCatalogRepository.java),
> hand-writes its two `FORCE INDEX` statements as tuned constants and constructs a
> `SqlPlan` directly; it borrows the `SqlPlan` / `SeekCursor` *types* from this class but
> none of its SQL generation. So editing a builder method below will not change any query
> the service issues. This is asserted by `DocumentedMechanismTest`.

[`MillionScalePaginationSql`](../../src/main/java/com/recsys/application/pagination/MillionScalePaginationSql.java)
is a reusable builder that turns a table + index into MySQL-friendly SQL. Every
request value is a bind parameter; only identifiers are validated (regex-checked),
predicates rejecting blank/`;` fragments, page size bounded 1–1000, and a `SqlPlan`
record that enforces placeholder-count == bind-count. It demonstrates three access
patterns, each mapped to how it rides an index:

- **Covering-index count** — `countWithCoveringIndex(...)` emits
  `SELECT COUNT(*) FROM t FORCE INDEX (<covering>) <where>`, forcing the narrow
  secondary index instead of the clustered PK so `COUNT` walks the smaller B-tree
  (materially cheaper on large tables).
- **Keyset seek** — `cursorPage(...)` emits `FORCE INDEX (<covering>)` plus the
  seek predicate `(sort <op> ? OR (sort = ? AND id <op> ?))` and
  `ORDER BY sort dir, id dir LIMIT ?` — **no `OFFSET`**. This is an index
  range-scan that starts *after* the last returned tuple, so page N costs the same
  as page 1. `cursorPageBefore(...)` walks backward with reversed operators. The
  cursor/result-window semantics — the seek-anchor model, HMAC-signed catalog
  cursors — are covered from the partitioning angle in
  [14_Partitioning §4](14_Partitioning.md#4-keyset--cursor-pagination--partitioning-a-result-set);
  here the point is simply that the `(sort DESC, id DESC)` composite index makes
  the seek an index range-scan rather than a sort.
- **Delayed-join deep-offset** — `delayedJoinPage(...)` handles genuine
  random-access deep pages (admin "jump to page 1000"): an inner query walks
  *only* the covering index returning `(id, sort)` with `LIMIT ? OFFSET ?`, then an
  outer query joins those page keys back to the base table
  (`JOIN (inner) page_keys ON t.id = page_keys.id`). The expensive offset walk is
  index-only; the clustered lookup happens for just the ~`limit` rows on the page.
- **Covering DDL** — `coveringIndexDdl(...)` emits the matching
  `CREATE INDEX` with column order `equality-filters → sort → id → extra covered
  columns`, so a covering index (when justified by evidence) is generated the same
  way the queries expect it.

The helper methods above are the authoritative API for these three access patterns;
their unit tests exercise placeholder/bind parity, keyset seek semantics, and the
delayed-join shape.

## 4. Indexes beyond the catalog — the outbox and saga workload

`V2__create_event_outbox_and_sagas.sql` indexes the transactional-outbox and saga
tables around their access patterns rather than their identity columns:
`idx_outbox_claim (status, next_attempt_at, created_at)` for the dispatcher's
"claim the next due batch" scan, `idx_outbox_lease (status, lease_expires_at)` for
reclaiming expired leases, `idx_outbox_reconcile (destination, status, created_at,
broker_acknowledged_at)` for the reconciliation sweep, `idx_outbox_aggregate
(aggregate_type, aggregate_id, created_at)` for per-aggregate history, and
`idx_saga_correlation (correlation_id, created_at)` for saga lookup. Each leads with
the equality/selectivity column (`status`, `destination`, `aggregate_type`) and
trails with a time column so the index serves both the filter and the order. How
these tables are used is covered in the
[Eventual Consistency investigation](15_Eventual_Consistency.md); `V3` adds JSON
progress columns only, no indexes.

## 5. Other index types (for context)

Relational B-trees are one of several "indexes" in the system. Ordered structures first,
then the hash family — which is where most of this system's lookups actually happen.

### Ordered and inverted

- **Redis ZSET-as-index** — `ShardedRecordStore` maintains a per-device sorted-set
  index (`ZADD NX` / `ZADD XX GT` keyed by sequence number) so per-device reads
  page in order — a Redis-side ordered index, discussed in
  [14_Partitioning §1](14_Partitioning.md#1-consistent-hash-record-sharding).
- **In-memory inverted index** —
  [`DataManager`](../../src/main/java/com/recsys/infrastructure/dataloading/DataManager.java)
  holds `moviesByGenre` (`Map<String, List<Movie>>`), an in-memory genre index for
  the non-SQL serving path.

### The hash family

Every relational index above is a B-tree, but B-trees are a minority of this system's
lookups — most of them hash. **The full map of where hashing is used, and which document
owns each, is the table under "Hashing elsewhere in the system" in
[06_Consistent_Hashing](06_Consistent_Hashing.md).** It is not repeated here; that would
give it two homes and one would eventually be wrong.

Two entries in it are *indexes* in this document's sense, and their names hide that:

- [`EmbeddingLSH`](../../src/main/java/com/recsys/infrastructure/vectordb/EmbeddingLSH.java)
  — LSH is locality-sensitive **hashing**, and it is a hash index whose collisions are the
  *feature*. It hashes each embedding to a 16-bit mask by the sign of its dot product
  against random Gaussian hyperplanes, so vectors at a similar cosine angle deliberately
  land in the same bucket; Hamming-1 probing widens recall. That is the opposite of what a
  hash index normally wants, which is why it answers "what is this near?" rather than
  "where is this row?". 06's table points here for it, so this is its home.
- [`BloomFilterGuard`](../../src/main/java/com/recsys/infrastructure/resilience/BloomFilterGuard.java)
  — a **negative index**. It cannot tell you where a row is, only that it certainly is not
  there. That is exactly what makes it cheap enough to sit in front of Redis and skip a
  round-trip for a known-absent id.

### Why no hash index in MySQL

A reasonable question, given how much of the system hashes: why is every relational index
here a B-tree?

Because InnoDB does not offer a user-declarable one. `CREATE INDEX ... USING HASH` is
accepted and **silently produces a B-tree**; the only true hash index InnoDB has is the
*adaptive hash index*, which the engine builds and discards on its own from observed access
patterns. It cannot be created, targeted by `FORCE INDEX`, or relied upon — so it is
invisible to the plan-pinning discipline in §2, and pinning a query to it is not
expressible. MySQL's `MEMORY` engine does support real `USING HASH` indexes, but only for
equality on non-persistent tables, which is not this workload.

That is a constraint, not a preference — and it would not hold on other engines:

- **PostgreSQL** has genuine hash indexes (WAL-logged and crash-safe since v10). They serve
  only `=`, so they could not serve any query here anyway: every catalog query needs a
  *range* scan in `(sort, id)` order for keyset pagination. Postgres is not used in this
  project regardless — `MySqlConnectionSettings` rejects any URL that is not
  `jdbc:mysql://`, and a test pins that.
- **DynamoDB** makes the hash index the primary access path: the partition key *is* a hash
  key, with the sort key providing range access within a partition — essentially the
  `(hash, range)` split this system implements by hand with `ConsistentHashRing` plus a
  Redis ZSET. DynamoDB is not used here; the equivalent role is played by Redis.

So the honest summary is that this system does plenty of hash-based indexing — just none of
it inside the relational store, where the engine gives it nowhere to live.

### Multi-vector scoring: Sum of MaxSim

Everything above scores an item by **one** vector — one pooled `float[]` per movie in
`i2vEmb:<id>`, one per user from the two-tower model, compared by
[`VectorMath.innerProduct`](../../src/main/java/com/recsys/infrastructure/vectordb/VectorMath.java).
That is the "two tower" regime: the query is collapsed to a single point before it ever
meets the corpus, so any token-level structure in the query is gone by retrieval time.

The late-interaction alternative (ColBERT; Pinterest's Manas multi-embedding queries) keeps
a **bag of token vectors** on both sides and scores a document as

```
SumOfMaxSim(q, d) = Σ_i max_j ⟨q_i, d_j⟩
```

— for each query token, the best-matching document token, summed. Two consequences that
matter for a serving system:

- **Cost is `|q| × |d| × dim` per document**, not `dim`. At corpus scale this is a reranker
  over a candidate set, not something to run exhaustively; the ANN stage stays
  single-vector (one search per query token, candidates unioned) and MaxSim decides the
  final order.
- **The score is order-free but not pooled.** Shuffling a document's tokens changes
  nothing; averaging them into one vector would, which is precisely the information a
  two-tower model throws away.

The primitives live beside the single-vector ones and copy their contract exactly:

- [`MultiVectorMath.sumOfMaxSim`](../../src/main/java/com/recsys/infrastructure/vectordb/MultiVectorMath.java)
  — raw inner product, no normalisation (callers wanting cosine L2-normalise tokens first,
  as ColBERT assumes). Null, empty, a null token, or a width mismatch on *any* pair returns
  `-∞` and never throws, so index code drops the document the same way `ExactVectorIndex`
  drops an unscorable vector. A width mismatch poisons the whole score rather than the one
  pair: a partial comparison would still rank the document, silently.
- [`MultiVectorIndex`](../../src/main/java/com/recsys/infrastructure/vectordb/MultiVectorIndex.java)
  / [`ExactMultiVectorIndex`](../../src/main/java/com/recsys/infrastructure/vectordb/ExactMultiVectorIndex.java)
  — the `float[][]` twin of `VectorIndex` / `ExactVectorIndex`: same bounded top-K heap,
  same exclusion set, same static one-shot `search` over an ad hoc candidate map.

**Where the token bags come from: the Word2Vec vectors, not a new model.** There is no
token-level embedding artifact here (Word2Vec yields one vector per movie, the ONNX towers
one per user), so
[`CoRatedTokenBags`](../../src/main/java/com/recsys/infrastructure/vectordb/CoRatedTokenBags.java)
manufactures a bag per movie out of vectors the system already has: the movie's own vector
followed by the vectors of its co-rated neighbours (`DataManager.getSimilarMovies`, at most
5). Bags are read from the `EmbeddingStore`, not the classpath map, in one bulk read over
items ∪ neighbours, so a vector rewritten by `POST /setembedding` is honoured. A movie with
no vector of its own is omitted rather than ranked on borrowed neighbour tokens.

**The one consumer: `GET /similar`, behind `RECSYS_SIMILAR_SCORING`.** Default
`inner_product` is the original single-vector path, byte for byte. `sum_of_maxsim` makes the
seed's bag the query and the candidates' bags the documents, scored by
`ExactMultiVectorIndex`; candidate selection, response shape and cache headers do not change.
This is a deployment-level env var and deliberately **not** a query parameter: the CDN cache
key for this route whitelists only `movieId` and `k`, so a per-request switch would let one
mode's body be served under the other mode's key. Flipping it therefore needs a CDN
invalidation — see the runbook. An unknown value fails startup rather than silently serving
the default under a flag that says otherwise.

**What that buys, and what it cannot claim.** With single-vector items Sum of MaxSim
collapses to a pooled query (the max over one document token is trivial, and the sum over
query tokens is then an inner product against the summed query), so the neighbour tokens on
the *item* side are what carry any signal: a candidate whose neighbours overlap the seed's
can outrank one whose own vector is closer — `SimilarMaxSimScoringTest` pins a fixture where
the two scorers disagree. The classpath dataset is 12 hand-authored 6-dimensional vectors,
so this is the mechanism, wired and tested, not a measured ranking improvement.

**Measured on a live 6010 against Redis (2026-09-13), one thing the unit tests could not
show.** With 20 ratings over 12 movies almost everything is co-rated with everything, so the
bags overlap heavily and the own vector is one token in up to six: for seed 5, three
candidates scored *exactly* equal under MaxSim (default mode had them 0.86 / 0.57 / 0.36
apart). Two consequences. First, equal scores must be ordered deterministically, or two pods
emit different bodies — and different ETags — for the same CDN cache key; `ExactMultiVectorIndex`
therefore breaks ties by id ascending, in the top-k cut as well as the final sort
(`ExactVectorIndex` still sorts by score alone, a pre-existing property that only matters
when ties are common). Second, on a corpus where neighbourhoods are this dense the neighbour
tokens swamp the own vector; on real data that is a tuning question (weight the own token,
cap the bag) that should be settled by measurement, not by picking a constant here. Part of
the tie mechanism is co-rating symmetry: the seed is usually among each co-rated candidate's
own neighbours, so the seed's first query token finds itself in every such bag and contributes
the same `‖seed‖²` to each — exact ties then need only the remaining tokens to coincide, which
on overlapping bags they do.

Two operational edges of the same mode. **Bag construction reads more keys.** The default mode
bulk-reads the candidate set (up to `k × 5`); MaxSim mode reads that set plus up to five
neighbours each, so a cold `k=200` request issues roughly six times the Redis reads (chunked
by `REDIS_EMBEDDING_MGET_BATCH_SIZE`, fronted by `LocalEmbeddingCache`, so it is the first
request after a flip that shows on a latency graph, not the steady state). **A wrong-width
vector is contained, not fatal.** `sumOfMaxSim` scores a bag `-∞` on *any* mismatched pair, and
`POST /setembedding` persists the vector before the dimension check rejects it, so a bad
vector can sit in Redis; `CoRatedTokenBags` drops a neighbour token whose width differs from
the item's own vector, so it degrades that bag rather than poisoning the seed's query bag and
returning an empty, publicly cached 200 for every candidate.

**What still does not exist, deliberately.** No request type carries more than one query
vector, and there is no per-token ANN fan-out: feeding a user's watched-history vectors into
`/getrecommendation` as a multi-vector query would need one ANN search per token against the
LSH index merged before a MaxSim rerank — the Manas-style retrieval change, and its own
design. `MultiVectorMathTest`, `ExactMultiVectorIndexTest`, `CoRatedTokenBagsTest` and
`SimilarMaxSimScoringTest` run in the `-Presilience` PR gate.

### Disk-resident ANN: SPANN layout with SPFresh updates

Both heap backends above keep every vector on the heap: `ExactVectorIndex` holds the full map
and `LshVectorIndex` sits on top of it and falls back to scanning it whenever the bucket set
is thin. `RECSYS_VECTOR_BACKEND=spann` selects
[`SpannVectorIndex`](../../src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java),
which keeps only a centroid table and an id→posting map in heap and puts the vectors in
posting lists in a memory-mapped file. Design:
[2026-09-13-spann-spfresh-vector-index-design.md](../superpowers/specs/2026-09-13-spann-spfresh-vector-index-design.md).

**Layout.** Build runs k-means++/Lloyd on a sample and assigns every vector to its nearest
centroid by L2; each posting is an immutable block at the file tail; the table maps a centroid
to its block offset. A query scores all live centroids (in heap) by **inner product**, probes
the `nprobe` highest, reads those blocks, and scores their entries by inner product through
`VectorMath`, so a `SearchResult.score` means the same thing as in the other backends. The
partition is L2 and the probe is inner product on purpose, as in FAISS's inner-product IVF:
a posting's entries are its centroid plus small noise, so the postings with the highest
⟨query, centroid⟩ are the ones holding the highest ⟨query, entry⟩ — whereas the inner-product
top hits sit at the far edge of a cluster along the query's direction, at a *typical* L2
distance from the query, so L2-nearest probing would mostly miss them. SPANN's `(1 + ε)`
distance pruning is an L2 device with no clean inner-product form, so there is none; `nprobe`
is the knob and the benchmark measures recall against exact rather than assuming it. Equal
scores order by id ascending in both the top-k cut and the output, as `ExactMultiVectorIndex`
does. If a probe returns fewer than `k`, the probe count doubles up to every centroid — the
same fallback `LshVectorIndex` has, but paid in block reads instead of a heap-wide scan.

**Liveness is the id map, not a tombstone.** An entry read from posting `c` counts iff the
concurrent id map says `id → c`. An overwrite appends a new block, publishes the table, and
then flips the map, so a reader never sees two versions of an id and never sees a torn block;
search also dedups by id. Readers never block and pin one snapshot (table + store) per search.

**One measured consequence, and it is a real serving property.** Blocks come from the pinned
snapshot but liveness comes from the live id map, so a search that pinned its snapshot *before*
a cross-posting move and scans *after* the flip finds the id stale in its old posting and
absent from the new posting's pre-move block: that one in-flight search misses the id entirely.
`SpannVectorIndexPinnedReaderTest` forces the interleaving with a blocking store and measures
it — 0 occurrences, never duplicated. The window is one search long and self-heals, and an id
is never returned twice or with a torn vector, but a candidate can silently drop out of a
single response while its posting is being rebalanced. So the guarantee this backend offers is
"never two versions, never a torn block" — not "always visible": a candidate can vanish from
one response during rebalancing and reappear on the next request.
`SpannVectorIndexConcurrencyTest` cannot see this (its violation set checks duplicates,
ordering, unknown ids and scores, not absence), which is why the property is measured
separately rather than asserted as an invariant.

**SPFresh, bounded on purpose.** Insert rewrites the nearest posting. Past
`RECSYS_SPANN_POSTING_MAX` the posting 2-means-splits into two new centroids, and
*reassignment* then checks only the postings of the `RECSYS_SPANN_REASSIGN_PROBE` centroids
nearest each new one, moving an entry iff its nearest centroid is now one of the two. That is
the "lightweight" in SPFresh's LIRE: a far-away entry in a now-suboptimal posting stays until
it is next touched — the trade the paper makes, named here so nobody reads it as a bug. Below
`RECSYS_SPANN_POSTING_MIN` a posting merges into its nearest neighbour (which may then split
once, without cascading). When dead bytes pass `RECSYS_SPANN_COMPACT_RATIO` of the file the
live entries are rewritten into a fresh file and the old one deleted; slot numbers survive
compaction so the id map needs no rewrite. Every rebalancing step runs synchronously inside
the `/setembedding` that triggered it — no background loop to guard.

**Configuration.** All read once at startup; an invalid value fails the boot, naming the
variable. `RECSYS_SPANN_DIR` (default `java.io.tmpdir`; the file is per process, deleted on
shutdown, and nothing persists), `RECSYS_SPANN_POSTING_MAX` (128), `RECSYS_SPANN_POSTING_MIN`
(16; must be below half of max or merge and split would oscillate), `RECSYS_SPANN_NPROBE`
(128), `RECSYS_SPANN_REASSIGN_PROBE` (4), `RECSYS_SPANN_COMPACT_RATIO` (0.5), `RECSYS_SPANN_SEED`
(42). In k8s an `emptyDir` is enough for the directory; note that a mapped file's pages are
page cache, which the cgroup charges but can reclaim — a soft cost, unlike heap. That holds for
the *live* file only: after a compaction, the previous store's mappings stay charged to the
cgroup until the old snapshot becomes unreachable and is garbage-collected, so under write
churn a memory limit should budget for both the live and the about-to-be-collected file, not
just the live one.

**Why 128, not the textbook-small 8.** A first pass at `nprobe = 8` measured **0.143** recall
on the 200 000-vector benchmark corpus: at `postingMax` 128, 200 000 vectors build roughly
4 200 centroids, so 8 probes reach only 0.2% of them. `RECSYS_SPANN_NPROBE`'s default was set
from a measured probe/recall curve on that corpus, not guessed —
`SpannProbeCurveLoadTest` (4 184 live centroids):

| nprobe | recall@10 | distances/query |
|---|---|---|
| 8 | 0.143 | 4 300 |
| 32 | 0.658 | 5 405 |
| 128 | 0.986 | 10 084 |
| 512 | 1.000 | 28 461 |
| 3 125 | 1.000 | 153 704 |

`Math.min(nprobe, liveCentroids)` makes a probe count of 128 harmless on small corpora: the
12-movie classpath corpus builds one or two centroids, so a search simply probes all of them
regardless of the configured `nprobe`. Per-query cost is roughly `liveCentroids +
nprobe × postingSize`, which is why the centroid scan dominates at small probe counts (the
step from 8 to 32 probes is a +25.7% change in distances/query — 4 300 to 5 405, not the
negligible move "barely moves" would suggest) and probing starts to dominate once `nprobe`
approaches or exceeds the centroid count. That cost model counts distance computations only:
selecting the `nprobe` best centroids also sorts every live centroid through boxed `Integer`
comparisons on every query, which the `distanceComputations` counter does not measure, so the
reported per-query figures above understate real CPU. Replacing that sort with a bounded
primitive selection (a partial selection instead of a full `Arrays.sort`) is the next
optimization and is deliberately not done in this change.

**Measured envelope (synthetic; the classpath corpus is 12 six-dimensional vectors and can
show nothing).** `SpannBenchmarkLoadTest` (`@Tag("load")`, run with
`mvn test -DexcludedGroups="" -Dgroups=load -Dtest=SpannBenchmarkLoadTest`) builds exact, LSH
and SPANN over 200 000 × 64-dim Gaussian-mixture vectors and asserts, for SPANN only: retained
heap ≤ 25% of exact, recall@10 ≥ 0.90 against exact at the default `nprobe`, and fewer
distance computations per query than exact's 200 000. Measured 2026-09-14, x86_64, JVM 17.0.12,
heap max 2048 MB, 1 000 queries, k=10:

| index | retained heap MB | recall@10 | distances/query | build ms | query ms (1 000) |
|---|---|---|---|---|---|
| exact | 66.1 | 1.000 | 200 000 | 34 | 16 029 |
| lsh | 76.4 | 0.718 | — | 545 | 10 643 |
| spann | 14.9 | 0.986 | 10 084 | 373 550 | 2 261 |

SPANN retains 22.5% of exact's heap (bar: ≤ 25%), recall@10 0.986 (bar: ≥ 0.90), and 10 084
distance computations per query against exact's 200 000 (bar: below that). All three bars pass.
The 373 550 ms (~374 s) build cost is real: it is sample k-means plus assigning 200 000 vectors
across ~4 200 centroids, and nothing persists across restarts (see Configuration above), so an
operator turning this backend on pays that cost on every boot, not once.

**Getting under the heap bar took one specific fix.** The id map's values are slot numbers,
and `Integer.valueOf` caches only −128..127, so a naive `Map<Integer, Integer>` on a
200 000-entry index would box a distinct `Integer` per entry (~3.2 MB) where ~4 200 shared
boxes, one per centroid slot, would do (~67 KB). `SpannVectorIndex` interns slot boxes itself
(`box(int slot)`, an array of cached boxes touched only from the constructor or under the
writer lock); before that fix retained heap measured 17.8 MB (27.0% of exact, over the 25%
bar), after it 14.9 MB (22.5%, under it). A primitive `int→int` map (e.g. an open-addressed
array-backed map) remains the next lever if a larger corpus needs to shave further, but it was
deliberately **not** taken here: it would put a hand-written concurrent structure under the
same liveness rule every read depends on (see above), for a saving the interned-box fix already
delivered at far lower risk.

**What deliberately does not exist.** Persistence across restarts (rebuild on boot, delete on
shutdown); a graph or tree over centroids — at ~3k centroids a brute-force scan is ~200k
multiply-adds and the scan becomes the bottleneck only around 10M+ vectors; multi-vector
postings; native or SIMD kernels; a delete API; Prometheus metrics (the `stats()` snapshot is
the seam). The default backend stays `lsh` and no manifest sets `spann`.

## 6. Testing the indexes

- **Static contracts** — `MySqlIndexContractTest` (FORCE INDEX + exact column
  order per query; the 7-index allowlist), `MySqlIndexContractAssertions` (the
  assertion engine), `MovieCatalogRepositoryTest` (exact plan SQL incl. the
  `FORCE INDEX (...)` clause and bind values).
- **Real optimizer** — `MovieCatalogMySqlIntegrationTest` (`@Tag("docker")`,
  Testcontainers `mysql:8.4`, `EXPLAIN` asserts the chosen `key`).
- **Access patterns** — `MillionScalePaginationSqlTest` pins covering-DDL column
  order, keyset-not-offset, placeholder/bind parity, delayed-join offset-inside-
  subquery, `countWithCoveringIndex`, and reversed `cursorPageBefore` ordering.
- **Execution** — `MySqlClientTest` (query execution over the FORCE-INDEX plans).

## Sharp edges — notes

1. **Covering is deferred by policy, not oversight.** The catalog indexes omit
   payload columns on purpose; adding them is gated behind `EXPLAIN ANALYZE` /
   slow-query / benchmark evidence, not added speculatively.
2. **`FORCE INDEX` is a guarantee that must be proven.** A hint alone can be
   silently ignored by the engine, so the static contract test is always paired
   with the Docker `EXPLAIN` assertion — never one without the other.
3. **New queries can't skip the discipline.** The allowlist test fails if a
   migration adds an index without the matching contract, and a query without a
   `FORCE INDEX`/`EXPLAIN` pair has no home in the pattern — the three-part
   requirement is enforced by tests, not convention.
4. **Deep `OFFSET` is only for true random access.** Keyset paging is the default
   (constant cost per page); `delayedJoinPage` exists for admin "jump to page N"
   and still pays an index-only offset walk — it bounds the cost, it does not
   erase it.
5. **No MySQL table partitioning yet.** Indexing scales the single `movies` table;
   native table partitioning / sharding is deferred — see
   [14_Partitioning](14_Partitioning.md) sharp edges.
