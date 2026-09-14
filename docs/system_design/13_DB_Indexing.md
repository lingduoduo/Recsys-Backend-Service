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

**What does not exist yet, deliberately.** No route consumes these. There is no token-level
embedding store (Word2Vec yields one vector per movie, the ONNX towers one per user), no
request type that carries more than one query vector, and no per-token ANN fan-out. Those
are the retrieval-stack changes Pinterest describes for Manas — a new query type, a parser
for it, parallel ANN searches merged before rerank — and each is its own design, not a
side-effect of adding the scorer. `MultiVectorMathTest` and `ExactMultiVectorIndexTest`
run in the `-Presilience` PR gate.

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
