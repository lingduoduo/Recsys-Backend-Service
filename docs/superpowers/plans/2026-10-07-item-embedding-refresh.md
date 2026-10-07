# Item-Embedding Refresh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Periodically re-read catalog item embeddings from Redis on every serving pod and apply the changed ones to the in-memory recall index (6010, 7010, 8080) and 6010's `/similar` heap cache, so a batch embedding rewrite takes effect within one interval with no restart.

**Architecture:** A new `ItemEmbeddingRefresher` (a `GuardedLoop` on one daemon thread) reads every catalog ID through a new lenient chunked-`MGET` read, diffs each vector against the last one it applied, and pushes only changes to a list of `ItemEmbeddingSink`s — `CandidateGenerator::updateEmbedding` everywhere, plus a new cache-only `LocalEmbeddingCache::refresh` on 6010. Redis stays off the request path: a failed pass leaves memory untouched and surfaces through the existing `RecsysLoopStale` alert.

**Tech Stack:** Java 17, Lettuce, Caffeine, Micrometer, JUnit 5 + AssertJ + Mockito, Maven (`JAVA_HOME=$(/usr/libexec/java_home -v 17)`).

**Spec:** [docs/superpowers/specs/2026-10-07-item-embedding-refresh-design.md](../specs/2026-10-07-item-embedding-refresh-design.md)

## Global Constraints

- Env var `ITEM_EMBEDDING_REFRESH_INTERVAL_MS`: default `60000`; `0` disables; negative rejected at construction with a message naming the variable.
- Metric names: `recsys.item_embedding.refresh.applied` (no tags), `recsys.item_embedding.refresh.skipped{reason="dimension"|"corrupt"}`; loop name `item-embedding-refresh` (→ `recsys_loop_seconds_since_success{loop="item-embedding-refresh"}`).
- No Redis read on the recall request path; no change to `ItemEmbeddingJob`; no new alert rule.
- `LocalEmbeddingCache.refresh` must never write to the backing store.
- A catalog ID absent from Redis is never un-indexed.
- Do not commit `.claude/CLAUDE.md`. New merge-blocking tests are non-docker and listed in `-Presilience` with a comment.

## Review Focus

1. **One corrupt value among thousands** — every other item must still refresh. Pinned by Task 1's lenient-read test and Task 4's `corruptValueIsSkippedAndCountedOthersStillApply`.
2. **SPANN file growth** — an unchanged vector must never be re-applied, because `SpannVectorIndex.addOrUpdate` appends a block per call. Pinned by Task 4's `unchangedVectorIsNotReappliedOnTheNextPass`.
3. **A sink that throws midway** (e.g. index closed during shutdown) — the item must not be recorded as applied, so the next pass retries it. Pinned by Task 4's `sinkFailureLeavesTheItemPendingForTheNextPass`.
4. **Redis down for a long time** — memory untouched, loop failure recorded. Pinned by Task 4's `storeFailureThrowsAndTouchesNothing`; alerting is the existing rule.
5. **8080 binds loop metrics once** — `ensureRecallInfra` builds the generator once per provider lifetime (closed only in `@PreDestroy`); verified by code review in Task 5.

---

### Task 1: Lenient batch read on `RedisEmbeddingStore`

**Files:**
- Modify: `src/main/java/com/recsys/infrastructure/redis/RedisEmbeddingStore.java` (`getEmbeddings`, lines ~170–195)
- Test: `src/test/java/com/recsys/infrastructure/redis/RedisEmbeddingStoreTest.java`

**Interfaces:**
- Produces: `public Map<Integer, float[]> getEmbeddingsLenient(Collection<Integer> ids, IntConsumer onCorrupt)` — same chunked `MGET` as `getEmbeddings`; an unparseable value is skipped and its ID passed to `onCorrupt`. `getEmbeddings` keeps throwing on the first unparseable value.

- [ ] **Step 1: Write the failing tests** (append to `RedisEmbeddingStoreTest`; uses the file's existing `execFor` and `kvs` helpers; add `import java.util.ArrayList;` and `import static org.assertj.core.api.Assertions.assertThatThrownBy;` if absent):

```java
    @Test
    void lenientBatchReadSkipsAndReportsCorruptValues() {
        RedisCommands<String, String> cmd = mock(RedisCommands.class);
        when(cmd.mget(any(String[].class))).thenReturn(kvs("1.0 2.0", "not-a-vector", null, "3.0 4.0"));
        RedisEmbeddingStore store = new RedisEmbeddingStore(execFor(cmd), "i2vEmb");
        List<Integer> corrupt = new ArrayList<>();

        Map<Integer, float[]> got = store.getEmbeddingsLenient(List.of(1, 2, 3, 4), corrupt::add);

        assertThat(got).containsOnlyKeys(1, 4);
        assertThat(got.get(1)).containsExactly(1f, 2f);
        assertThat(corrupt).containsExactly(2);
    }

    @Test
    void strictBatchReadStillThrowsOnACorruptValue() {
        RedisCommands<String, String> cmd = mock(RedisCommands.class);
        when(cmd.mget(any(String[].class))).thenReturn(kvs("1.0 2.0", "not-a-vector"));
        RedisEmbeddingStore store = new RedisEmbeddingStore(execFor(cmd), "i2vEmb");

        assertThatThrownBy(() -> store.getEmbeddings(List.of(1, 2)))
                .isInstanceOf(IllegalArgumentException.class);
    }
```

- [ ] **Step 2: Add a stub so the RED is behavioural, not a compile error** — in `RedisEmbeddingStore`:

```java
    public Map<Integer, float[]> getEmbeddingsLenient(Collection<Integer> ids, IntConsumer onCorrupt) {
        return new HashMap<>();
    }
```

(import `java.util.function.IntConsumer`).

- [ ] **Step 3: Run to verify RED**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=RedisEmbeddingStoreTest`
Expected: `lenientBatchReadSkipsAndReportsCorruptValues` FAILS (empty map); `strictBatchReadStillThrowsOnACorruptValue` PASSES (pins the unchanged contract).

- [ ] **Step 4: Implement** — replace `getEmbeddings` and the stub with one shared loop:

```java
    public Map<Integer, float[]> getEmbeddings(Collection<Integer> movieIds) {
        return readBatched(movieIds, null);
    }

    /**
     * Like {@link #getEmbeddings} but a value that does not parse is skipped and its id handed to
     * {@code onCorrupt}, instead of aborting the whole read. For the periodic item-embedding refresh:
     * with the strict read, one corrupt key would fail every pass forever.
     */
    public Map<Integer, float[]> getEmbeddingsLenient(Collection<Integer> ids, IntConsumer onCorrupt) {
        return readBatched(ids, Objects.requireNonNull(onCorrupt, "onCorrupt"));
    }

    private Map<Integer, float[]> readBatched(Collection<Integer> movieIds, IntConsumer onCorrupt) {
        Map<Integer, float[]> embeddings = new HashMap<>();
        if (movieIds == null || movieIds.isEmpty()) return embeddings;

        List<Integer> ids = new ArrayList<>(new LinkedHashSet<>(movieIds));
        for (int start = 0; start < ids.size(); start += mgetBatchSize) {
            int end = Math.min(ids.size(), start + mgetBatchSize);
            String[] keys = new String[end - start];
            for (int i = start; i < end; i++) {
                keys[i - start] = keyPrefix + ":" + ids.get(i);
            }
            final int batchStart = start;
            List<KeyValue<String, String>> values = exec.executeRead(c -> c.mget(keys));
            for (int j = 0; j < values.size(); j++) {
                String value = values.get(j).getValueOrElse(null);
                if (value == null || value.isBlank()) continue;
                int id = ids.get(batchStart + j);
                try {
                    embeddings.put(id, VectorMath.parseVector(value));
                } catch (IllegalArgumentException e) {   // NumberFormatException included
                    if (onCorrupt == null) throw e;
                    onCorrupt.accept(id);
                }
            }
        }
        return embeddings;
    }
```

(import `java.util.Objects` if absent.)

- [ ] **Step 5: Run to verify GREEN** — same command. Expected: PASS (all `RedisEmbeddingStoreTest`).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/recsys/infrastructure/redis/RedisEmbeddingStore.java src/test/java/com/recsys/infrastructure/redis/RedisEmbeddingStoreTest.java
git commit -m "feat(redis): lenient batch embedding read that skips corrupt values"
```

---

### Task 2: Cache-only `LocalEmbeddingCache.refresh`

**Files:**
- Modify: `src/main/java/com/recsys/infrastructure/cache/LocalEmbeddingCache.java`
- Test: `src/test/java/com/recsys/infrastructure/cache/LocalEmbeddingCacheTest.java`

**Interfaces:**
- Produces: `public void refresh(int id, float[] vector)` — adds `id` to the Bloom filter, invalidates its null sentinel, puts the value; never touches the backing store. Null vector → no-op.

- [ ] **Step 1: Write the tests** (append; Mockito `mock`, `verify`, `never`, `any`, `anyInt`, `anyLong`, `anyMap` imports as needed):

```java
    // --- refresh(): how a periodic Redis re-read reaches /similar ---

    @Test
    void anIdWrittenToTheStoreAfterPreloadIsInvisible_theDefectRefreshFixes() {
        EmbeddingStore store = mock(EmbeddingStore.class);
        when(store.getEmbedding(99)).thenReturn(new float[]{1f, 2f});
        LocalEmbeddingCache c = new LocalEmbeddingCache(store);
        c.preload(Map.of(1, new float[]{0f, 1f}));   // Bloom now knows only id 1

        assertThat(c.getEmbedding(99)).as("Bloom rejects ids absent at preload").isNull();
    }

    @Test
    void refreshMakesANewIdVisibleWithoutTouchingTheStore() {
        EmbeddingStore store = mock(EmbeddingStore.class);
        LocalEmbeddingCache c = new LocalEmbeddingCache(store);
        c.preload(Map.of(1, new float[]{0f, 1f}));

        c.refresh(99, new float[]{1f, 2f});

        assertThat(c.getEmbedding(99)).containsExactly(1f, 2f);
        verify(store, never()).setEmbedding(anyInt(), any(), anyLong());
        verify(store, never()).setEmbeddings(anyMap(), anyLong());
    }

    @Test
    void refreshReplacesAChangedVector() {
        LocalEmbeddingCache c = new LocalEmbeddingCache(mock(EmbeddingStore.class));
        c.preload(Map.of(1, new float[]{0f, 1f}));

        c.refresh(1, new float[]{5f, 5f});

        assertThat(c.getEmbedding(1)).containsExactly(5f, 5f);
    }

    @Test
    void refreshClearsANullSentinel() {
        EmbeddingStore store = mock(EmbeddingStore.class);   // returns null: id 7 is "known absent"
        LocalEmbeddingCache c = new LocalEmbeddingCache(store);
        assertThat(c.getEmbedding(7)).isNull();              // caches a null sentinel

        c.refresh(7, new float[]{1f, 1f});

        assertThat(c.getEmbedding(7)).containsExactly(1f, 1f);
    }
```

- [ ] **Step 2: Add a no-op stub** `public void refresh(int id, float[] vector) {}` so the run is behavioural.

- [ ] **Step 3: Run to verify RED**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=LocalEmbeddingCacheTest`
Expected: the defect-pinning test PASSES (it documents today's behaviour); the three `refresh…` tests FAIL.

- [ ] **Step 4: Implement**:

```java
    /**
     * Cache-only update for a value just read from the backing store (the periodic item-embedding
     * refresh). Unlike {@link #setEmbedding} it never writes back — that would echo to Redis the value
     * just read from it. Adds the id to the Bloom filter so an item first written after startup is no
     * longer rejected, and clears any null sentinel.
     */
    public void refresh(int id, float[] vector) {
        if (vector == null) return;
        bloom.add(id);
        nullSentinels.invalidate(id);
        put(id, vector);
    }
```

- [ ] **Step 5: Run to verify GREEN** — same command; all pass.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/recsys/infrastructure/cache/LocalEmbeddingCache.java src/test/java/com/recsys/infrastructure/cache/LocalEmbeddingCacheTest.java
git commit -m "feat(cache): cache-only LocalEmbeddingCache.refresh for store re-reads"
```

---

### Task 3: `CandidateGenerator.seedEmbeddings()`

**Files:**
- Modify: `src/main/java/com/recsys/infrastructure/vectordb/CandidateGenerator.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/CandidateGeneratorDimensionTest.java`

**Interfaces:**
- Produces: `public Map<Integer, float[]> seedEmbeddings()` — unmodifiable view of the classpath vectors the index was built from (the refresher's starting last-applied map).

- [ ] **Step 1: Test** (append):

```java
    @Test
    void seedEmbeddingsExposeTheClasspathVectorsReadOnly() {
        assertThat(generator.seedEmbeddings()).isNotEmpty();
        assertThat(generator.seedEmbeddings().values().iterator().next()).hasSize(generator.embeddingDimension());
        assertThatThrownBy(() -> generator.seedEmbeddings().put(-1, new float[0]))
                .isInstanceOf(UnsupportedOperationException.class);
    }
```

- [ ] **Step 2: RED** — `mvn test -Dtest=CandidateGeneratorDimensionTest` fails to compile (method missing). This one is a pure accessor; compile-RED is acceptable here and is ledgered.

- [ ] **Step 3: Implement**:

```java
    /** The classpath vectors the index was built from; read-only. */
    public Map<Integer, float[]> seedEmbeddings() {
        return Collections.unmodifiableMap(movieEmbeddings);
    }
```

- [ ] **Step 4: GREEN** — same command passes.

- [ ] **Step 5: Commit** — `git commit -am "feat(vectordb): expose CandidateGenerator seed embeddings read-only"`

---

### Task 4: `ItemEmbeddingRefresher`

**Files:**
- Create: `src/main/java/com/recsys/infrastructure/vectordb/ItemEmbeddingSink.java`
- Create: `src/main/java/com/recsys/infrastructure/vectordb/ItemEmbeddingReader.java`
- Create: `src/main/java/com/recsys/infrastructure/vectordb/ItemEmbeddingRefresher.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/ItemEmbeddingRefresherTest.java`

**Interfaces:**
- Consumes: `RedisEmbeddingStore.getEmbeddingsLenient` (Task 1), `CandidateGenerator.seedEmbeddings()` / `updateEmbedding` / `embeddingDimension()` (Task 3).
- Produces:
  ```java
  @FunctionalInterface public interface ItemEmbeddingSink { void apply(int id, float[] vector); }
  @FunctionalInterface public interface ItemEmbeddingReader {
      Map<Integer, float[]> read(Collection<Integer> ids, IntConsumer onCorrupt); }
  public final class ItemEmbeddingRefresher implements AutoCloseable {
      public static final String LOOP_NAME = "item-embedding-refresh";
      public static final String APPLIED = "recsys.item_embedding.refresh.applied";
      public static final String SKIPPED = "recsys.item_embedding.refresh.skipped";
      public static final String INTERVAL_ENV = "ITEM_EMBEDDING_REFRESH_INTERVAL_MS";
      public static final long DEFAULT_INTERVAL_MS = 60_000L;
      public ItemEmbeddingRefresher(Supplier<? extends Collection<Integer>> catalogIds, ItemEmbeddingReader reader,
              int dimension, Map<Integer, float[]> initiallyApplied, List<ItemEmbeddingSink> sinks, MeterRegistry registry);
      public static ItemEmbeddingRefresher forGenerator(CandidateGenerator generator,
              Supplier<? extends Collection<Integer>> catalogIds, RedisEmbeddingStore itemStore,
              List<ItemEmbeddingSink> extraSinks, MeterRegistry registry);
      public static long intervalFromEnv(EnvVars.EnvReader env);   // 0 = disabled; negative -> IAE naming INTERVAL_ENV
      public void refreshOnce();
      public GuardedLoop loop();
      public void start(long intervalMs);   // 0 -> no-op; binds loop metrics, schedules with fixed delay
      public void close();
  }
  ```

- [ ] **Step 1: Write the failing tests** — `ItemEmbeddingRefresherTest.java`:

```java
package com.recsys.infrastructure.vectordb;

import com.recsys.domain.item.Movie;
import com.recsys.infrastructure.dataloading.DataManager;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntConsumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ItemEmbeddingRefresherTest {

    private static final float[] A = {1f, 0f};
    private static final float[] B = {0f, 1f};

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final List<String> applied = new ArrayList<>();
    private final ItemEmbeddingSink recording = (id, v) -> applied.add(id + "=" + v[0] + "," + v[1]);

    /** A Redis stand-in: the current values, ids to report as corrupt, and an optional failure. */
    private final Map<Integer, float[]> redis = new HashMap<>();
    private final Set<Integer> corrupt = new java.util.HashSet<>();
    private RuntimeException failure;
    private int reads;
    private final ItemEmbeddingReader reader = (Collection<Integer> ids, IntConsumer onCorrupt) -> {
        reads++;
        if (failure != null) throw failure;
        Map<Integer, float[]> out = new HashMap<>();
        for (int id : ids) {
            if (corrupt.contains(id)) onCorrupt.accept(id);
            else if (redis.containsKey(id)) out.put(id, redis.get(id));
        }
        return out;
    };

    private ItemEmbeddingRefresher refresher(Map<Integer, float[]> initial, ItemEmbeddingSink... sinks) {
        return new ItemEmbeddingRefresher(() -> List.of(1, 2, 3), reader, 2, initial, List.of(sinks), registry);
    }

    private double count(String name, String... tags) {
        var search = registry.find(name);
        if (tags.length > 0) search = search.tags(tags);
        var c = search.counter();
        return c == null ? 0 : c.count();
    }

    @Test
    void changedVectorReachesEverySinkExactlyOnce() {
        List<String> second = new ArrayList<>();
        redis.put(1, B);
        ItemEmbeddingRefresher r = refresher(Map.of(1, A), recording, (id, v) -> second.add("" + id));

        r.refreshOnce();

        assertThat(applied).containsExactly("1=0.0,1.0");
        assertThat(second).containsExactly("1");
        assertThat(count(ItemEmbeddingRefresher.APPLIED)).isEqualTo(1.0);
    }

    @Test
    void unchangedVectorIsNotReappliedOnTheNextPass() {
        // SpannVectorIndex.addOrUpdate appends a posting block per call: re-applying unchanged
        // vectors every interval would grow the index file without bound.
        redis.put(1, A);
        redis.put(2, B);
        ItemEmbeddingRefresher r = refresher(Map.of(1, A), recording);

        r.refreshOnce();   // id 2 is new relative to the seed -> applied once
        r.refreshOnce();   // nothing changed

        assertThat(applied).containsExactly("2=0.0,1.0");
    }

    @Test
    void missingKeyKeepsTheCurrentVector() {
        ItemEmbeddingRefresher r = refresher(Map.of(1, A), recording);   // redis is empty

        r.refreshOnce();

        assertThat(applied).isEmpty();
    }

    @Test
    void wrongDimensionIsSkippedAndCounted() {
        redis.put(1, new float[]{1f, 2f, 3f});
        ItemEmbeddingRefresher r = refresher(Map.of(), recording);

        r.refreshOnce();

        assertThat(applied).isEmpty();
        assertThat(count(ItemEmbeddingRefresher.SKIPPED, "reason", "dimension")).isEqualTo(1.0);
    }

    @Test
    void corruptValueIsSkippedAndCountedOthersStillApply() {
        corrupt.add(1);
        redis.put(2, B);
        ItemEmbeddingRefresher r = refresher(Map.of(), recording);

        r.refreshOnce();

        assertThat(applied).containsExactly("2=0.0,1.0");
        assertThat(count(ItemEmbeddingRefresher.SKIPPED, "reason", "corrupt")).isEqualTo(1.0);
    }

    @Test
    void storeFailureThrowsAndTouchesNothing() {
        failure = new IllegalStateException("redis down");
        ItemEmbeddingRefresher r = refresher(Map.of(1, A), recording);

        assertThatThrownBy(r::refreshOnce).isSameAs(failure);
        assertThat(applied).isEmpty();
    }

    @Test
    void sinkFailureLeavesTheItemPendingForTheNextPass() {
        redis.put(1, B);
        boolean[] fail = {true};
        ItemEmbeddingRefresher r = refresher(Map.of(1, A), (id, v) -> {
            if (fail[0]) throw new IllegalStateException("index closed");
            applied.add("" + id);
        });

        assertThatThrownBy(r::refreshOnce).hasMessage("index closed");
        fail[0] = false;
        r.refreshOnce();

        assertThat(applied).containsExactly("1");
    }

    @Test
    void loopRecordsAFailedPassInsteadOfDying() {
        failure = new IllegalStateException("redis down");
        ItemEmbeddingRefresher r = refresher(Map.of(), recording);

        r.loop().run();   // must not throw

        assertThat(r.loop().failureCount()).isEqualTo(1);
    }

    @Test
    void intervalDefaultsTo60sZeroDisablesNegativeIsRejected() {
        assertThat(ItemEmbeddingRefresher.intervalFromEnv(name -> null)).isEqualTo(60_000L);
        assertThat(ItemEmbeddingRefresher.intervalFromEnv(Map.of(ItemEmbeddingRefresher.INTERVAL_ENV, "0")::get))
                .isZero();
        assertThatThrownBy(() -> ItemEmbeddingRefresher.intervalFromEnv(
                Map.of(ItemEmbeddingRefresher.INTERVAL_ENV, "-1")::get))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(ItemEmbeddingRefresher.INTERVAL_ENV);
    }

    @Test
    void aRefreshedVectorChangesWhatRecallReturns() {
        System.setProperty("recsys.vector.backend", "exact");   // deterministic ranking for the assertion
        try {
            DataManager dm = mock(DataManager.class);
            when(dm.getMovieById(anyInt())).thenAnswer(i -> new Movie(i.getArgument(0), "m", 2020, List.of("Drama")));
            when(dm.getWatchedMovieIds(anyInt())).thenReturn(Set.of());
            EmbeddingStore users = mock(EmbeddingStore.class);
            CandidateGenerator gen = new CandidateGenerator(dm, users);
            float[] query = new float[gen.embeddingDimension()];
            query[0] = 1f;
            when(users.getEmbedding(5)).thenReturn(query);
            int before = gen.byEmbedding(5, 1).get(0).id();
            int target = gen.seedEmbeddings().keySet().stream().filter(id -> id != before).findFirst().orElseThrow();
            ItemEmbeddingReader moved = (ids, onCorrupt) -> Map.of(target, query.clone());   // cosine 1.0
            ItemEmbeddingRefresher r = ItemEmbeddingRefresher.forGenerator(
                    gen, () -> gen.seedEmbeddings().keySet(), null, List.of(), registry, moved);

            r.refreshOnce();

            assertThat(gen.byEmbedding(5, 1).get(0).id()).isEqualTo(target);
        } finally {
            System.clearProperty("recsys.vector.backend");
        }
    }
}
```

> Note for the implementer: `aRefreshedVectorChangesWhatRecallReturns` uses the package-private
> `forGenerator(..., ItemEmbeddingReader reader)` overload so the test needs no Redis; the public
> overload passes `itemStore::getEmbeddingsLenient`. The refresher depends on the small
> `ItemEmbeddingReader` interface rather than on `RedisEmbeddingStore` directly (a refinement of the
> spec's wording) so unit tests can use a fake reader.

- [ ] **Step 2: Create the two interfaces and a skeleton refresher whose `refreshOnce` does nothing** (constants, constructor storing fields, `intervalFromEnv` returning `DEFAULT_INTERVAL_MS`, `loop()` returning a `GuardedLoop(LOOP_NAME, this::refreshOnce)`, empty `start`/`close`, both `forGenerator` overloads), so the run is behavioural.

- [ ] **Step 3: Run to verify RED**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=ItemEmbeddingRefresherTest`
Expected: every test except `missingKeyKeepsTheCurrentVector` FAILS (that one passes trivially against a no-op; it guards against over-applying).

- [ ] **Step 4: Implement** `ItemEmbeddingRefresher`:

```java
package com.recsys.infrastructure.vectordb;

import com.recsys.config.EnvVars;
import com.recsys.infrastructure.redis.RedisEmbeddingStore;
import com.recsys.resilience.GuardedLoop;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Keeps in-memory item embeddings (the recall index; 6010's /similar cache) tracking Redis. Every
 * interval it reads every catalog id from Redis, and applies to its sinks only vectors that differ
 * from the last one it applied — load-bearing for SPANN, whose addOrUpdate appends a posting block
 * per call. Redis stays off the request path: a failed pass changes nothing in memory and is
 * recorded by the GuardedLoop, which the RecsysLoopStale alert watches.
 *
 * <p>A catalog id absent from Redis keeps its current vector (keys evict and expire; absence is not
 * deletion). One accepted race with /setembedding: a pass that read v1 before /setembedding wrote v2
 * can re-apply v1 after it; the next pass reads v2 and repairs it, so it lasts at most one interval.
 */
public final class ItemEmbeddingRefresher implements AutoCloseable {

    public static final String LOOP_NAME = "item-embedding-refresh";
    public static final String APPLIED = "recsys.item_embedding.refresh.applied";
    public static final String SKIPPED = "recsys.item_embedding.refresh.skipped";
    public static final String INTERVAL_ENV = "ITEM_EMBEDDING_REFRESH_INTERVAL_MS";
    public static final long DEFAULT_INTERVAL_MS = 60_000L;

    private final Supplier<? extends Collection<Integer>> catalogIds;
    private final ItemEmbeddingReader reader;
    private final int dimension;
    private final List<ItemEmbeddingSink> sinks;
    private final Map<Integer, float[]> lastApplied;
    private final MeterRegistry registry;
    private final Counter applied;
    private final Counter skippedDimension;
    private final Counter skippedCorrupt;
    private final GuardedLoop loop = new GuardedLoop(LOOP_NAME, this::refreshOnce);
    private ScheduledExecutorService scheduler;

    public ItemEmbeddingRefresher(Supplier<? extends Collection<Integer>> catalogIds, ItemEmbeddingReader reader,
                                  int dimension, Map<Integer, float[]> initiallyApplied,
                                  List<ItemEmbeddingSink> sinks, MeterRegistry registry) {
        this.catalogIds = Objects.requireNonNull(catalogIds, "catalogIds");
        this.reader = Objects.requireNonNull(reader, "reader");
        this.dimension = dimension;
        this.sinks = List.copyOf(sinks);
        this.lastApplied = new ConcurrentHashMap<>(initiallyApplied == null ? Map.of() : initiallyApplied);
        this.registry = registry == null ? new SimpleMeterRegistry() : registry;
        this.applied = Counter.builder(APPLIED)
                .description("Item embeddings re-read from Redis that differed and were applied in memory")
                .register(this.registry);
        this.skippedDimension = skipped("dimension");
        this.skippedCorrupt = skipped("corrupt");
    }

    /** Wires the refresher to a generator: its index is always a sink, its seed vectors the baseline. */
    public static ItemEmbeddingRefresher forGenerator(CandidateGenerator generator,
                                                      Supplier<? extends Collection<Integer>> catalogIds,
                                                      RedisEmbeddingStore itemStore,
                                                      List<ItemEmbeddingSink> extraSinks,
                                                      MeterRegistry registry) {
        Objects.requireNonNull(itemStore, "itemStore");
        return forGenerator(generator, catalogIds, itemStore, extraSinks, registry, itemStore::getEmbeddingsLenient);
    }

    static ItemEmbeddingRefresher forGenerator(CandidateGenerator generator,
                                               Supplier<? extends Collection<Integer>> catalogIds,
                                               RedisEmbeddingStore itemStore,
                                               List<ItemEmbeddingSink> extraSinks,
                                               MeterRegistry registry,
                                               ItemEmbeddingReader reader) {
        List<ItemEmbeddingSink> sinks = new ArrayList<>();
        sinks.add(generator::updateEmbedding);
        sinks.addAll(extraSinks);
        return new ItemEmbeddingRefresher(catalogIds, reader, generator.embeddingDimension(),
                generator.seedEmbeddings(), sinks, registry);
    }

    public static long intervalFromEnv(EnvVars.EnvReader env) {
        long ms = EnvVars.readLong(env, INTERVAL_ENV, DEFAULT_INTERVAL_MS);
        if (ms < 0) {
            throw new IllegalArgumentException(INTERVAL_ENV + " must be >= 0 (0 disables), was " + ms);
        }
        return ms;
    }

    public void refreshOnce() {
        Map<Integer, float[]> current = reader.read(catalogIds.get(), id -> skippedCorrupt.increment());
        for (Map.Entry<Integer, float[]> e : current.entrySet()) {
            int id = e.getKey();
            float[] vector = e.getValue();
            if (dimension > 0 && vector.length != dimension) {
                skippedDimension.increment();
                continue;
            }
            if (Arrays.equals(vector, lastApplied.get(id))) continue;
            for (ItemEmbeddingSink sink : sinks) {
                sink.apply(id, vector);   // a throw leaves lastApplied untouched: retried next pass
            }
            lastApplied.put(id, vector);
            applied.increment();
        }
    }

    public GuardedLoop loop() {
        return loop;
    }

    public synchronized void start(long intervalMs) {
        if (intervalMs <= 0 || scheduler != null) return;
        loop.bindTo(registry);
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, LOOP_NAME);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(loop, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    private Counter skipped(String reason) {
        return Counter.builder(SKIPPED)
                .description("Item embeddings re-read from Redis that were not applied, by reason")
                .tag("reason", reason)
                .register(registry);
    }
}
```

- [ ] **Step 5: Run to verify GREEN** — same command; all pass. Then watch the diff matter: temporarily delete the `Arrays.equals` line, rerun, confirm `unchangedVectorIsNotReappliedOnTheNextPass` FAILS, restore.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/recsys/infrastructure/vectordb/ItemEmbedding*.java src/test/java/com/recsys/infrastructure/vectordb/ItemEmbeddingRefresherTest.java
git commit -m "feat(vectordb): ItemEmbeddingRefresher diffs Redis item vectors into memory"
```

---

### Task 5: Wire all three services; config; gate

**Files:**
- Modify: `src/main/java/com/recsys/api/serving/RecSysServer.java` (after `RequestDurationHistogram.configure(registry)`; shutdown hook ~line 233)
- Modify: `src/main/java/com/recsys/api/online/OnlinePredictionServer.java` (after `RequestDurationHistogram.configure(registry)`; shutdown hook ~line 308)
- Modify: `src/main/java/com/recsys/application/model/ModelRuntimeProvider.java` (`ensureRecallInfra` ~line 244; `close()` ~line 370)
- Modify: `k8s/base/configmap.yaml`, `pom.xml` (`-Presilience` includes)

**Interfaces:**
- Consumes: `ItemEmbeddingRefresher.forGenerator(...)`, `intervalFromEnv(System::getenv)`, `start`, `close` (Task 4); `LocalEmbeddingCache::refresh` (Task 2).

- [ ] **Step 1: 6010** — after `RequestDurationHistogram.configure(registry);`:

```java
            // Keeps the recall index and the /similar cache tracking batch rewrites of i2vEmb in Redis
            // (02_Caching §10). Off the request path; a failed pass changes nothing in memory.
            ItemEmbeddingRefresher itemRefresher = ItemEmbeddingRefresher.forGenerator(
                    candidateGenerator, dataManager::getAllMovieIds, embStore,
                    List.of(embCache::refresh), registry);
            itemRefresher.start(ItemEmbeddingRefresher.intervalFromEnv(System::getenv));
```

and in the shutdown hook, before `candidateGenerator.close()`: `itemRefresher.close();`.

- [ ] **Step 2: 7010** — after `RequestDurationHistogram.configure(registry);`:

```java
            // Keeps the recall index tracking batch rewrites of i2vEmb in Redis (02_Caching §10).
            ItemEmbeddingRefresher itemRefresher = ItemEmbeddingRefresher.forGenerator(
                    candidateGenerator, dataManager::getAllMovieIds,
                    new RedisEmbeddingStore(jedisPool, "i2vEmb"), List.of(), registry);
            itemRefresher.start(ItemEmbeddingRefresher.intervalFromEnv(System::getenv));
```

and `itemRefresher.close();` in the shutdown hook before `candidateGenerator.close()`.

- [ ] **Step 3: 8080** — field `private ItemEmbeddingRefresher itemRefresher;`. In `ensureRecallInfra`, right after `candidateGenerator = new CandidateGenerator(...)`:

```java
            itemRefresher = ItemEmbeddingRefresher.forGenerator(
                    candidateGenerator, dataManager::getAllMovieIds,
                    new RedisEmbeddingStore(recallPool, "i2vEmb"), List.of(), meterRegistry);
            itemRefresher.start(ItemEmbeddingRefresher.intervalFromEnv(System::getenv));
```

In `close()`, before the generator is closed: `if (itemRefresher != null) { itemRefresher.close(); itemRefresher = null; }`. Confirm by reading `ModelRuntimeProvider` that `candidateGenerator` is only nulled in `close()` (`@PreDestroy`), so loop metrics bind once per provider.

- [ ] **Step 4: Config and gate.** `k8s/base/configmap.yaml`:

```yaml
  # Re-read catalog item embeddings from Redis into the recall index (all serving services) and
  # 6010's /similar cache; 0 disables. Keep well under 300000 — RecsysLoopStale fires at 300 s.
  ITEM_EMBEDDING_REFRESH_INTERVAL_MS: "60000"
```

`pom.xml`, `-Presilience`, beside the cache tests:

```xml
                <!-- Item-embedding refresh: diff-only apply (SPANN appends per update), corrupt-value
                     isolation, missing-key retention, sink-failure retry, and the cache-only path that
                     must never echo back to Redis. -->
                <include>**/vectordb/ItemEmbeddingRefresherTest.java</include>
```

(`LocalEmbeddingCacheTest` and `RedisEmbeddingStoreTest`: check whether already included; add with the same comment if not.)

- [ ] **Step 5: Verify** — `mvn -q compile`; `mvn test -Dtest='ItemEmbeddingRefresherTest,LocalEmbeddingCacheTest,RedisEmbeddingStoreTest,CandidateGenerator*Test,RecSysServer*Test,OnlinePrediction*Test,ModelRuntimeProvider*Test'`; then `mvn test -Presilience`.

- [ ] **Step 6: Commit** — `git commit -am "feat(serving): refresh item embeddings from Redis on 6010, 7010 and 8080"`

---

### Task 6: Measure end to end, document, full suite, PR

- [ ] **Step 1: Before/after on real Redis.** Throwaway `redis-server --port 6390`; run 6010 with `REDIS_PORT=6390 ITEM_EMBEDDING_REFRESH_INTERVAL_MS=2000` (plus `REDIS_ALLOW_NO_AUTH=true`, `RECOMMENDATION_CURSOR_SIGNING_KEY`). Record `GET /similar?movieId=<id>` and `GET /getrecommendation?userId=123&k=5`. Then `redis-cli -p 6390 SET i2vEmb:<other-id> "<the query item's vector>"` so `<other-id>` should become the top similar item; wait 5 s; re-request. Expected: both answers change; `recsys_item_embedding_refresh_applied_total` ≥ 1 on `/metrics`. Repeat on `main` (stash) — expected: no change. Stop everything.
- [ ] **Step 2: Docs.** `02_Caching.md` §10: replace "a batch `i2vEmb` rewrite reaches `/similar` immediately" with the measured truth and the refresh; row "Item embeddings — recall" invalidation → "periodic refresh (60 s)"; sharp edge 9 → **Resolved** with the measurement. `15_Eventual_Consistency.md`: item-embedding staleness bound = one refresh interval. Run `mvn test -Dtest='com.recsys.docs.**'`.
- [ ] **Step 3: Full suite** — `rm -rf target/surefire-reports && mvn clean test`; record counts.
- [ ] **Step 4: PR** — push the branch, open a PR to `main` with the measurement, the CLAUDE.md line for `ITEM_EMBEDDING_REFRESH_INTERVAL_MS`, and the accepted race.
