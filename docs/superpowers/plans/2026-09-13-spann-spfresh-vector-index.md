# SPANN / SPFresh Vector Index Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A disk-resident SPANN-style `VectorIndex` backend with SPFresh in-place updates, selectable with `RECSYS_VECTOR_BACKEND=spann`, whose heap and CPU envelope is measured by an opt-in benchmark.

**Architecture:** An in-heap centroid table (immutable, copy-on-write, published through one `volatile` snapshot together with its posting store) points at immutable posting blocks in a memory-mapped file. Search scans centroids by L2, probes the nearest postings, and scores entries by inner product through the existing `VectorMath`. Updates rewrite whole blocks at the file tail and repoint; liveness is "the id map points at this posting"; split / bounded reassign / merge / compaction keep postings balanced.

**Tech Stack:** Java 17, Maven, JUnit 5 + AssertJ + Mockito (already in `pom.xml`), `java.nio` `FileChannel` / `MappedByteBuffer`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-13-spann-spfresh-vector-index-design.md`

## Global Constraints

- JDK 17 for every Maven command: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn ...`.
- Every new unit test class is added to the `-Presilience` surefire `<includes>` list in `pom.xml` in the same commit that adds it (the PR gate is an allow-list; a test not listed there never gates a merge). The benchmark is `@Tag("load")` and is NOT added there.
- Scores returned by the index are raw inner products from `VectorMath.innerProduct`; equal scores order by id ascending in both the top-k cut and the output (the `WORST_FIRST` pattern from `ExactMultiVectorIndex`).
- Every test is written first and watched failing for the right reason before its implementation exists.
- `.claude/CLAUDE.md` is never committed. Env vars are documented in `docs/system_design/13_DB_Indexing.md`.
- Default backend stays `lsh`; no k8s manifest changes.
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

## File Structure

| File | Responsibility |
|---|---|
| `src/main/java/com/recsys/infrastructure/vectordb/spann/SpannConfig.java` | Immutable config record; env parsing + validation; `with*` copies for tests |
| `.../spann/KMeans.java` | k-means++ seeding, Lloyd iterations, L2 helpers, nearest-centroid lookup |
| `.../spann/PostingStore.java` | Interface + `Block` record; the injection seam |
| `.../spann/MappedPostingStore.java` | Memory-mapped append-only block file: regions, tail writes, delete on close |
| `.../spann/CentroidTable.java` | Immutable centroid arrays with copy-on-write mutators and nearest-N queries |
| `.../spann/SpannStats.java` | Immutable stats snapshot record |
| `.../spann/SpannVectorIndex.java` | The `VectorIndex`: build, search, SPFresh steps, compaction, close |
| `src/main/java/com/recsys/infrastructure/vectordb/VectorIndex.java` | + default no-op `close()` |
| `src/main/java/com/recsys/infrastructure/vectordb/CandidateGenerator.java` | + `spann` case, + `close()` |
| `src/main/java/com/recsys/api/serving/RecSysServer.java` | shutdown hook calls `candidateGenerator.close()` |
| `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannTestVectors.java` | Seeded Gaussian-mixture vector generator shared by tests and the benchmark |
| `src/test/java/com/recsys/infrastructure/vectordb/spann/*Test.java` | One test class per task below |
| `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannBenchmarkLoadTest.java` | `@Tag("load")` benchmark asserting the acceptance bar |
| `docs/system_design/13_DB_Indexing.md` | §5 subsection documenting the backend |
| `pom.xml` | `-Presilience` includes |

---

### Task 1: SpannConfig

**Files:**
- Create: `src/main/java/com/recsys/infrastructure/vectordb/spann/SpannConfig.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannConfigTest.java`
- Modify: `pom.xml` (add `**/spann/SpannConfigTest.java` to the resilience includes, after the `**/vectordb/ExactMultiVectorIndexTest.java` line)

**Interfaces:**
- Consumes: `com.recsys.config.EnvVars` (`readInt(EnvReader, String, int)`, `readDouble(...)`, `readLong(...)`, nested `EnvReader` functional interface `String get(String name)`).
- Produces: `record SpannConfig(Path dir, int postingMax, int postingMin, int nprobe, int reassignProbe, double compactRatio, long seed, int regionBytes)`; `static SpannConfig defaults()`; `static SpannConfig fromEnv(EnvVars.EnvReader env)`; `withPostingMax(int)`, `withPostingMin(int)`, `withNprobe(int)`, `withReassignProbe(int)`, `withCompactRatio(double)`, `withDir(Path)`, `withRegionBytes(int)`, `withSeed(long)`.

- [ ] **Step 1: Write the failing test**

```java
package com.recsys.infrastructure.vectordb.spann;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannConfigTest {

    @Test
    void defaults_matchTheSpecTable() {
        SpannConfig c = SpannConfig.defaults();
        assertThat(c.dir()).isEqualTo(Path.of(System.getProperty("java.io.tmpdir")));
        assertThat(c.postingMax()).isEqualTo(128);
        assertThat(c.postingMin()).isEqualTo(16);
        assertThat(c.nprobe()).isEqualTo(8);
        assertThat(c.reassignProbe()).isEqualTo(4);
        assertThat(c.compactRatio()).isEqualTo(0.5);
        assertThat(c.seed()).isEqualTo(42L);
        assertThat(c.regionBytes()).isEqualTo(64 * 1024 * 1024);
    }

    @Test
    void fromEnv_readsEveryVariable() {
        Map<String, String> env = Map.of(
                "RECSYS_SPANN_DIR", "/tmp/spann-test",
                "RECSYS_SPANN_POSTING_MAX", "64",
                "RECSYS_SPANN_POSTING_MIN", "8",
                "RECSYS_SPANN_NPROBE", "3",
                "RECSYS_SPANN_REASSIGN_PROBE", "2",
                "RECSYS_SPANN_COMPACT_RATIO", "0.4",
                "RECSYS_SPANN_SEED", "7");
        SpannConfig c = SpannConfig.fromEnv(env::get);
        assertThat(c.dir()).isEqualTo(Path.of("/tmp/spann-test"));
        assertThat(c.postingMax()).isEqualTo(64);
        assertThat(c.postingMin()).isEqualTo(8);
        assertThat(c.nprobe()).isEqualTo(3);
        assertThat(c.reassignProbe()).isEqualTo(2);
        assertThat(c.compactRatio()).isEqualTo(0.4);
        assertThat(c.seed()).isEqualTo(7L);
    }

    @Test
    void fromEnv_blankMeansDefault() {
        assertThat(SpannConfig.fromEnv(name -> null)).isEqualTo(SpannConfig.defaults());
    }

    @Test
    void postingMin_mustBeBelowHalfOfPostingMax() {
        // 8 >= 16/2 would let merge and split oscillate on the same posting.
        assertThatThrownBy(() -> SpannConfig.defaults().withPostingMax(16).withPostingMin(8))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RECSYS_SPANN_POSTING_MIN");
    }

    @Test
    void rejectsNonPositiveNprobeAndPostingMax() {
        assertThatThrownBy(() -> SpannConfig.defaults().withNprobe(0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("RECSYS_SPANN_NPROBE");
        assertThatThrownBy(() -> SpannConfig.defaults().withPostingMax(1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("RECSYS_SPANN_POSTING_MAX");
    }

    @Test
    void rejectsCompactRatioOutsideZeroToOne() {
        assertThatThrownBy(() -> SpannConfig.defaults().withCompactRatio(0.0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("RECSYS_SPANN_COMPACT_RATIO");
        assertThatThrownBy(() -> SpannConfig.defaults().withCompactRatio(1.5))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("RECSYS_SPANN_COMPACT_RATIO");
    }

    @Test
    void fromEnv_nonNumericFailsNamingTheVariable() {
        assertThatThrownBy(() -> SpannConfig.fromEnv(name -> "RECSYS_SPANN_NPROBE".equals(name) ? "many" : null))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("RECSYS_SPANN_NPROBE");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=SpannConfigTest 2>&1 | grep -E "cannot find symbol|symbol:" | sort -u | head`
Expected: `symbol: class SpannConfig` (compilation failure, class missing).

- [ ] **Step 3: Write minimal implementation**

```java
package com.recsys.infrastructure.vectordb.spann;

import com.recsys.config.EnvVars;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Configuration for {@link SpannVectorIndex}. Read once at construction; every validation
 * failure names the environment variable so a bad value is a startup crash, not a silent default.
 */
public record SpannConfig(Path dir, int postingMax, int postingMin, int nprobe,
                          int reassignProbe, double compactRatio, long seed, int regionBytes) {

    public static final int DEFAULT_REGION_BYTES = 64 * 1024 * 1024;

    public SpannConfig {
        Objects.requireNonNull(dir, "RECSYS_SPANN_DIR");
        if (postingMax < 2) throw new IllegalArgumentException("RECSYS_SPANN_POSTING_MAX must be >= 2, got " + postingMax);
        if (postingMin < 0 || postingMin >= postingMax / 2) {
            throw new IllegalArgumentException("RECSYS_SPANN_POSTING_MIN must be in [0, RECSYS_SPANN_POSTING_MAX/2), got "
                    + postingMin + " with max " + postingMax);
        }
        if (nprobe < 1) throw new IllegalArgumentException("RECSYS_SPANN_NPROBE must be >= 1, got " + nprobe);
        if (reassignProbe < 0) throw new IllegalArgumentException("RECSYS_SPANN_REASSIGN_PROBE must be >= 0, got " + reassignProbe);
        if (!(compactRatio > 0 && compactRatio <= 1)) {
            throw new IllegalArgumentException("RECSYS_SPANN_COMPACT_RATIO must be in (0, 1], got " + compactRatio);
        }
        if (regionBytes < 4096) throw new IllegalArgumentException("regionBytes must be >= 4096, got " + regionBytes);
    }

    public static SpannConfig defaults() {
        return new SpannConfig(Path.of(System.getProperty("java.io.tmpdir")), 128, 16, 8, 4, 0.5, 42L,
                DEFAULT_REGION_BYTES);
    }

    public static SpannConfig fromEnv(EnvVars.EnvReader env) {
        SpannConfig d = defaults();
        String dir = env.get("RECSYS_SPANN_DIR");
        return new SpannConfig(
                dir == null || dir.isBlank() ? d.dir() : Path.of(dir.trim()),
                EnvVars.readInt(env, "RECSYS_SPANN_POSTING_MAX", d.postingMax()),
                EnvVars.readInt(env, "RECSYS_SPANN_POSTING_MIN", d.postingMin()),
                EnvVars.readInt(env, "RECSYS_SPANN_NPROBE", d.nprobe()),
                EnvVars.readInt(env, "RECSYS_SPANN_REASSIGN_PROBE", d.reassignProbe()),
                EnvVars.readDouble(env, "RECSYS_SPANN_COMPACT_RATIO", d.compactRatio()),
                EnvVars.readLong(env, "RECSYS_SPANN_SEED", d.seed()),
                d.regionBytes());
    }

    public SpannConfig withPostingMax(int v) { return new SpannConfig(dir, v, postingMin, nprobe, reassignProbe, compactRatio, seed, regionBytes); }
    public SpannConfig withPostingMin(int v) { return new SpannConfig(dir, postingMax, v, nprobe, reassignProbe, compactRatio, seed, regionBytes); }
    public SpannConfig withNprobe(int v) { return new SpannConfig(dir, postingMax, postingMin, v, reassignProbe, compactRatio, seed, regionBytes); }
    public SpannConfig withReassignProbe(int v) { return new SpannConfig(dir, postingMax, postingMin, nprobe, v, compactRatio, seed, regionBytes); }
    public SpannConfig withCompactRatio(double v) { return new SpannConfig(dir, postingMax, postingMin, nprobe, reassignProbe, v, seed, regionBytes); }
    public SpannConfig withDir(Path v) { return new SpannConfig(v, postingMax, postingMin, nprobe, reassignProbe, compactRatio, seed, regionBytes); }
    public SpannConfig withRegionBytes(int v) { return new SpannConfig(dir, postingMax, postingMin, nprobe, reassignProbe, compactRatio, seed, v); }
    public SpannConfig withSeed(long v) { return new SpannConfig(dir, postingMax, postingMin, nprobe, reassignProbe, compactRatio, v, regionBytes); }
}
```

Note `EnvVars.readInt(EnvReader, String, int)` throws `IllegalStateException` naming the variable on a non-numeric value; that is the behaviour `fromEnv_nonNumericFailsNamingTheVariable` pins. `withPostingMax(16)` alone is valid (16 > 2·16... no: default `postingMin` 16 is not `< 16/2`), so the oscillation test's `withPostingMax(16)` already throws before `withPostingMin(8)` runs — that is fine, the message names `RECSYS_SPANN_POSTING_MIN` either way.

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=SpannConfigTest 2>&1 | grep -E "Tests run:|BUILD"`
Expected: `Tests run: 7, Failures: 0, Errors: 0` and `BUILD SUCCESS`.

- [ ] **Step 5: Add to the PR gate and commit**

In `pom.xml`, after the line `<include>**/serving/SimilarMaxSimScoringTest.java</include>` add
`<include>**/spann/SpannConfigTest.java</include>`.

```bash
git add pom.xml src/main/java/com/recsys/infrastructure/vectordb/spann/SpannConfig.java src/test/java/com/recsys/infrastructure/vectordb/spann/SpannConfigTest.java
git commit -m "feat(spann): SpannConfig with env parsing and fail-fast validation

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 2: KMeans

**Files:**
- Create: `src/main/java/com/recsys/infrastructure/vectordb/spann/KMeans.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/KMeansTest.java`
- Modify: `pom.xml` (add `**/spann/KMeansTest.java`)

**Interfaces:**
- Produces: `static double l2sq(float[] a, float[] b)`; `static float[][] cluster(List<float[]> vectors, int k, long seed, int maxIterations)` returning `min(k, vectors.size())` centroids; `static int nearest(float[] v, float[][] centroids, boolean[] alive)` returning the index of the nearest centroid with `alive[i]` true, or `-1` if none. `alive == null` means all alive.

- [ ] **Step 1: Write the failing test**

```java
package com.recsys.infrastructure.vectordb.spann;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class KMeansTest {

    @Test
    void l2sq_isSquaredEuclideanDistance() {
        assertThat(KMeans.l2sq(new float[]{0f, 0f}, new float[]{3f, 4f})).isCloseTo(25.0, within(1e-9));
    }

    @Test
    void cluster_twoWellSeparatedBlobs_findsBothCentres() {
        Random rng = new Random(1);
        List<float[]> v = new ArrayList<>();
        for (int i = 0; i < 100; i++) v.add(new float[]{(float) rng.nextGaussian() * 0.1f, (float) rng.nextGaussian() * 0.1f});
        for (int i = 0; i < 100; i++) v.add(new float[]{10f + (float) rng.nextGaussian() * 0.1f, (float) rng.nextGaussian() * 0.1f});

        float[][] c = KMeans.cluster(v, 2, 42L, 10);

        assertThat(c).hasDimensions(2, 2);
        double[] xs = {c[0][0], c[1][0]};
        java.util.Arrays.sort(xs);
        assertThat(xs[0]).isCloseTo(0.0, within(0.2));
        assertThat(xs[1]).isCloseTo(10.0, within(0.2));
    }

    @Test
    void cluster_isDeterministicForASeed() {
        Random rng = new Random(2);
        List<float[]> v = new ArrayList<>();
        for (int i = 0; i < 50; i++) v.add(new float[]{rng.nextFloat(), rng.nextFloat(), rng.nextFloat()});
        assertThat(KMeans.cluster(v, 5, 9L, 5)).isDeepEqualTo(KMeans.cluster(v, 5, 9L, 5));
    }

    @Test
    void cluster_kAtLeastN_returnsTheVectorsThemselves() {
        List<float[]> v = List.of(new float[]{1f, 2f}, new float[]{3f, 4f});
        float[][] c = KMeans.cluster(v, 5, 0L, 3);
        assertThat(c).hasNumberOfRows(2);
        assertThat(c[0]).containsExactly(1f, 2f);
        assertThat(c[1]).containsExactly(3f, 4f);
    }

    @Test
    void nearest_skipsDeadCentroidsAndReturnsMinusOneWhenNoneAlive() {
        float[][] c = {{0f, 0f}, {10f, 0f}, {20f, 0f}};
        assertThat(KMeans.nearest(new float[]{9f, 0f}, c, null)).isEqualTo(1);
        assertThat(KMeans.nearest(new float[]{9f, 0f}, c, new boolean[]{true, false, true})).isEqualTo(0);
        assertThat(KMeans.nearest(new float[]{9f, 0f}, c, new boolean[]{false, false, false})).isEqualTo(-1);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=KMeansTest 2>&1 | grep -E "symbol:" | sort -u`
Expected: `symbol: variable KMeans`.

- [ ] **Step 3: Write minimal implementation**

```java
package com.recsys.infrastructure.vectordb.spann;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/** Seeded k-means++ / Lloyd clustering on L2 distance. Small and dependency-free by design. */
final class KMeans {

    private KMeans() {}

    static double l2sq(float[] a, float[] b) {
        double s = 0;
        for (int i = 0; i < a.length; i++) {
            double d = (double) a[i] - b[i];
            s += d * d;
        }
        return s;
    }

    /** Returns {@code min(k, n)} centroids. Empty clusters keep their previous centroid. */
    static float[][] cluster(List<float[]> vectors, int k, long seed, int maxIterations) {
        int n = vectors.size();
        if (n == 0 || k <= 0) return new float[0][];
        if (k >= n) {
            float[][] out = new float[n][];
            for (int i = 0; i < n; i++) out[i] = vectors.get(i).clone();
            return out;
        }
        int dim = vectors.get(0).length;
        Random rng = new Random(seed);
        float[][] centroids = seedPlusPlus(vectors, k, rng);
        int[] assign = new int[n];
        for (int iter = 0; iter < maxIterations; iter++) {
            boolean changed = false;
            for (int i = 0; i < n; i++) {
                int c = nearest(vectors.get(i), centroids, null);
                if (c != assign[i]) { assign[i] = c; changed = true; }
            }
            if (!changed && iter > 0) break;
            double[][] sum = new double[k][dim];
            int[] count = new int[k];
            for (int i = 0; i < n; i++) {
                float[] v = vectors.get(i);
                int c = assign[i];
                count[c]++;
                for (int d = 0; d < dim; d++) sum[c][d] += v[d];
            }
            for (int c = 0; c < k; c++) {
                if (count[c] == 0) continue;
                for (int d = 0; d < dim; d++) centroids[c][d] = (float) (sum[c][d] / count[c]);
            }
        }
        return centroids;
    }

    static int nearest(float[] v, float[][] centroids, boolean[] alive) {
        int best = -1;
        double bestD = Double.POSITIVE_INFINITY;
        for (int i = 0; i < centroids.length; i++) {
            if (alive != null && !alive[i]) continue;
            double d = l2sq(v, centroids[i]);
            if (d < bestD) { bestD = d; best = i; }
        }
        return best;
    }

    private static float[][] seedPlusPlus(List<float[]> vectors, int k, Random rng) {
        int n = vectors.size();
        float[][] centroids = new float[k][];
        centroids[0] = vectors.get(rng.nextInt(n)).clone();
        double[] minD = new double[n];
        Arrays.fill(minD, Double.POSITIVE_INFINITY);
        for (int c = 1; c < k; c++) {
            double total = 0;
            for (int i = 0; i < n; i++) {
                double d = l2sq(vectors.get(i), centroids[c - 1]);
                if (d < minD[i]) minD[i] = d;
                total += minD[i];
            }
            int chosen = n - 1;
            if (total > 0) {
                double r = rng.nextDouble() * total, acc = 0;
                for (int i = 0; i < n; i++) {
                    acc += minD[i];
                    if (acc >= r) { chosen = i; break; }
                }
            } else {
                chosen = rng.nextInt(n);
            }
            centroids[c] = vectors.get(chosen).clone();
        }
        return centroids;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=KMeansTest 2>&1 | grep -E "Tests run:|BUILD"`
Expected: `Tests run: 5, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 5: Add to the PR gate and commit**

Add `<include>**/spann/KMeansTest.java</include>` after the `SpannConfigTest` include in `pom.xml`.

```bash
git add pom.xml src/main/java/com/recsys/infrastructure/vectordb/spann/KMeans.java src/test/java/com/recsys/infrastructure/vectordb/spann/KMeansTest.java
git commit -m "feat(spann): seeded k-means++ / Lloyd clustering with L2 helpers

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 3: PostingStore and MappedPostingStore

**Files:**
- Create: `src/main/java/com/recsys/infrastructure/vectordb/spann/PostingStore.java`
- Create: `src/main/java/com/recsys/infrastructure/vectordb/spann/MappedPostingStore.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/MappedPostingStoreTest.java`
- Modify: `pom.xml` (add `**/spann/MappedPostingStoreTest.java`)

**Interfaces:**
- Produces:
  ```java
  public interface PostingStore extends java.io.Closeable {
      record Block(int[] ids, float[][] vectors) { public int count() { return ids.length; } }
      long append(Block block);          // offset of the written block; throws UncheckedIOException
      Block read(long offset);           // the block written at that offset
      long bytes();                      // bytes used so far (tail), incl. padding
      static int encodedBytes(int count, int dim) { return 8 + count * (4 + 4 * dim); }
      @Override void close();            // idempotent; deletes the backing file if any
  }
  ```
  `MappedPostingStore(Path dir, int regionBytes)`; `Path path()`.

Block encoding at `offset`: `int count`, `int dim`, then `count` × (`int id`, `dim` × `float`). Big-endian (ByteBuffer default). Blocks are 8-byte aligned; a block never straddles a region boundary — the tail is padded to the next region start instead.

- [ ] **Step 1: Write the failing test**

```java
package com.recsys.infrastructure.vectordb.spann;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MappedPostingStoreTest {

    @TempDir Path dir;

    private static PostingStore.Block block(int... ids) {
        float[][] v = new float[ids.length][];
        for (int i = 0; i < ids.length; i++) v[i] = new float[]{ids[i], ids[i] * 0.5f, -ids[i]};
        return new PostingStore.Block(ids, v);
    }

    @Test
    void appendThenRead_roundTrips() {
        try (MappedPostingStore s = new MappedPostingStore(dir, 4096)) {
            long off = s.append(block(1, 2, 3));
            PostingStore.Block b = s.read(off);
            assertThat(b.ids()).containsExactly(1, 2, 3);
            assertThat(b.vectors()[1]).containsExactly(2f, 1f, -2f);
            assertThat(s.bytes()).isEqualTo(PostingStore.encodedBytes(3, 3));
        }
    }

    @Test
    void blocksAreEightByteAligned() {
        try (MappedPostingStore s = new MappedPostingStore(dir, 4096)) {
            s.append(block(1));                       // 8 + 16 = 24 bytes
            long second = s.append(block(2));
            assertThat(second % 8).isZero();
            assertThat(second).isEqualTo(24);
        }
    }

    @Test
    void blockNeverStraddlesARegion() {
        // region 4096; each 3-dim block of 100 entries is 8 + 100*16 = 1608 bytes; the third would
        // cross the boundary at 4096, so it must start at 4096.
        try (MappedPostingStore s = new MappedPostingStore(dir, 4096)) {
            int[] ids = new int[100];
            for (int i = 0; i < 100; i++) ids[i] = i;
            s.append(block(ids));
            s.append(block(ids));
            long third = s.append(block(ids));
            assertThat(third).isEqualTo(4096);
            assertThat(s.read(third).ids()).hasSize(100);
            assertThat(s.bytes()).isEqualTo(4096 + 1608);
        }
    }

    @Test
    void blockLargerThanARegionIsRejected() {
        try (MappedPostingStore s = new MappedPostingStore(dir, 4096)) {
            int[] ids = new int[300];
            assertThatThrownBy(() -> s.append(block(ids))).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void emptyBlockRoundTrips() {
        try (MappedPostingStore s = new MappedPostingStore(dir, 4096)) {
            long off = s.append(new PostingStore.Block(new int[0], new float[0][]));
            assertThat(s.read(off).count()).isZero();
        }
    }

    @Test
    void fileExistsWhileOpenAndIsDeletedOnClose() {
        MappedPostingStore s = new MappedPostingStore(dir, 4096);
        Path p = s.path();
        assertThat(Files.exists(p)).isTrue();
        assertThat(p.getFileName().toString()).startsWith("spann-").endsWith(".bin");
        s.close();
        assertThat(Files.exists(p)).isFalse();
        s.close(); // idempotent
    }

    @Test
    void appendAfterCloseThrows() {
        MappedPostingStore s = new MappedPostingStore(dir, 4096);
        s.close();
        assertThatThrownBy(() -> s.append(block(1))).isInstanceOf(IllegalStateException.class);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=MappedPostingStoreTest 2>&1 | grep -E "symbol:" | sort -u`
Expected: `symbol: class MappedPostingStore` and `symbol: class PostingStore` (or `variable PostingStore`).

- [ ] **Step 3: Write minimal implementation**

`PostingStore.java`:

```java
package com.recsys.infrastructure.vectordb.spann;

import java.io.Closeable;

/**
 * Append-only store of immutable posting blocks. The seam between {@link SpannVectorIndex} and
 * the file system; tests inject a throwing wrapper through it.
 */
public interface PostingStore extends Closeable {

    record Block(int[] ids, float[][] vectors) {
        public int count() { return ids.length; }
    }

    /** Writes the block and returns its offset. Never modifies an existing block. */
    long append(Block block);

    Block read(long offset);

    /** Bytes used so far, padding included — the denominator of the dead-bytes ratio. */
    long bytes();

    static int encodedBytes(int count, int dim) {
        return 8 + count * (4 + 4 * dim);
    }

    @Override
    void close();
}
```

`MappedPostingStore.java`:

```java
package com.recsys.infrastructure.vectordb.spann;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * One memory-mapped file per index instance, mapped READ_WRITE in fixed-size regions. The
 * single writer puts bytes through the mapping and readers do absolute gets on the same
 * buffers, so there is no channel-write/page-cache coherence question; the index's
 * {@code volatile} snapshot publish is the happens-before edge that makes a block visible.
 *
 * <p>Regions are never remapped: growth maps the next region and appends it to a
 * copy-on-write list, so a reader holding an earlier region is unaffected. Old buffers are
 * released by GC — Java offers no safe explicit unmap.
 */
public final class MappedPostingStore implements PostingStore {

    private final Path path;
    private final int regionBytes;
    private final FileChannel channel;
    private final List<MappedByteBuffer> regions = new CopyOnWriteArrayList<>();
    private long tail;            // written only under the index's writer lock
    private volatile boolean closed;

    public MappedPostingStore(Path dir, int regionBytes) {
        this.regionBytes = regionBytes;
        this.path = dir.resolve("spann-" + ProcessHandle.current().pid() + "-" + System.nanoTime() + ".bin");
        try {
            Files.createDirectories(dir);
            this.channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            path.toFile().deleteOnExit();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create SPANN index file in " + dir, e);
        }
    }

    public Path path() {
        return path;
    }

    @Override
    public long append(Block block) {
        if (closed) throw new IllegalStateException("posting store is closed: " + path);
        int dim = block.count() == 0 ? 0 : block.vectors()[0].length;
        int size = PostingStore.encodedBytes(block.count(), dim);
        if (size > regionBytes) {
            throw new IllegalArgumentException("block of " + size + " bytes exceeds region size " + regionBytes);
        }
        long regionStart = (tail / regionBytes) * regionBytes;
        if (tail + size > regionStart + regionBytes) {
            tail = regionStart + regionBytes;            // pad to next region; a block never straddles
        }
        int regionIdx = (int) (tail / regionBytes);
        while (regions.size() <= regionIdx) mapRegion(regions.size());
        MappedByteBuffer buf = regions.get(regionIdx);
        int pos = (int) (tail - (long) regionIdx * regionBytes);
        long offset = tail;
        buf.putInt(pos, block.count());
        buf.putInt(pos + 4, dim);
        int p = pos + 8;
        for (int i = 0; i < block.count(); i++) {
            buf.putInt(p, block.ids()[i]);
            p += 4;
            float[] v = block.vectors()[i];
            for (int d = 0; d < dim; d++) { buf.putFloat(p, v[d]); p += 4; }
        }
        tail = align8(tail + size);
        return offset;
    }

    @Override
    public Block read(long offset) {
        int regionIdx = (int) (offset / regionBytes);
        MappedByteBuffer buf = regions.get(regionIdx);
        int pos = (int) (offset - (long) regionIdx * regionBytes);
        int count = buf.getInt(pos);
        int dim = buf.getInt(pos + 4);
        int[] ids = new int[count];
        float[][] vectors = new float[count][];
        int p = pos + 8;
        for (int i = 0; i < count; i++) {
            ids[i] = buf.getInt(p);
            p += 4;
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) { v[d] = buf.getFloat(p); p += 4; }
            vectors[i] = v;
        }
        return new Block(ids, vectors);
    }

    @Override
    public long bytes() {
        return tail;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        regions.clear();
        try {
            channel.close();
        } catch (IOException ignored) {
            // nothing to do: the file is deleted below either way
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot delete SPANN index file " + path, e);
        }
    }

    private void mapRegion(int idx) {
        try {
            regions.add(channel.map(FileChannel.MapMode.READ_WRITE, (long) idx * regionBytes, regionBytes));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot map region " + idx + " of " + path, e);
        }
    }

    private static long align8(long v) {
        return (v + 7) & ~7L;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=MappedPostingStoreTest 2>&1 | grep -E "Tests run:|BUILD"`
Expected: `Tests run: 7, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 5: Add to the PR gate and commit**

Add `<include>**/spann/MappedPostingStoreTest.java</include>` after the `KMeansTest` include.

```bash
git add pom.xml src/main/java/com/recsys/infrastructure/vectordb/spann/PostingStore.java src/main/java/com/recsys/infrastructure/vectordb/spann/MappedPostingStore.java src/test/java/com/recsys/infrastructure/vectordb/spann/MappedPostingStoreTest.java
git commit -m "feat(spann): append-only memory-mapped posting store

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 4: CentroidTable

**Files:**
- Create: `src/main/java/com/recsys/infrastructure/vectordb/spann/CentroidTable.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/CentroidTableTest.java`
- Modify: `pom.xml` (add `**/spann/CentroidTableTest.java`)

**Interfaces:**
- Produces (immutable; every mutator returns a new table, slot numbers are stable forever):
  ```java
  final class CentroidTable {
      static CentroidTable empty();
      int size();                       // slots ever allocated, dead included
      int aliveCount();
      boolean alive(int slot);
      float[] centroid(int slot);
      long offset(int slot);
      int live(int slot);               // live entry count
      int[] aliveSlots();
      CentroidTable withAdded(float[] centroid, long offset, int live);   // new slot = old size()
      CentroidTable withRepointed(int slot, long offset, int live);
      CentroidTable withLive(int slot, int live);
      CentroidTable withKilled(int slot);
      int nearest(float[] q);                                   // alive only; -1 if none
      int[] nearestN(float[] q, int n, int... exclude);         // alive only, ascending L2, up to n
      double[] distancesToAlive(float[] q, int[] aliveSlots);   // same order as aliveSlots
  }
  ```

- [ ] **Step 1: Write the failing test**

```java
package com.recsys.infrastructure.vectordb.spann;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CentroidTableTest {

    private static CentroidTable three() {
        return CentroidTable.empty()
                .withAdded(new float[]{0f, 0f}, 0L, 5)
                .withAdded(new float[]{10f, 0f}, 100L, 6)
                .withAdded(new float[]{20f, 0f}, 200L, 7);
    }

    @Test
    void withAdded_allocatesSlotsInOrder() {
        CentroidTable t = three();
        assertThat(t.size()).isEqualTo(3);
        assertThat(t.aliveCount()).isEqualTo(3);
        assertThat(t.centroid(1)).containsExactly(10f, 0f);
        assertThat(t.offset(2)).isEqualTo(200L);
        assertThat(t.live(0)).isEqualTo(5);
        assertThat(t.aliveSlots()).containsExactly(0, 1, 2);
    }

    @Test
    void mutatorsDoNotChangeTheOriginal() {
        CentroidTable t = three();
        CentroidTable t2 = t.withRepointed(1, 999L, 1).withKilled(0);
        assertThat(t.offset(1)).isEqualTo(100L);
        assertThat(t.alive(0)).isTrue();
        assertThat(t2.offset(1)).isEqualTo(999L);
        assertThat(t2.live(1)).isEqualTo(1);
        assertThat(t2.alive(0)).isFalse();
        assertThat(t2.aliveCount()).isEqualTo(2);
        assertThat(t2.aliveSlots()).containsExactly(1, 2);
    }

    @Test
    void killedSlotsKeepTheirNumberAndNewSlotsAppend() {
        CentroidTable t = three().withKilled(1).withAdded(new float[]{30f, 0f}, 300L, 1);
        assertThat(t.size()).isEqualTo(4);
        assertThat(t.alive(1)).isFalse();
        assertThat(t.centroid(3)).containsExactly(30f, 0f);
    }

    @Test
    void nearest_ignoresDeadSlots() {
        CentroidTable t = three();
        assertThat(t.nearest(new float[]{9f, 0f})).isEqualTo(1);
        assertThat(t.withKilled(1).nearest(new float[]{9f, 0f})).isEqualTo(0);
        assertThat(CentroidTable.empty().nearest(new float[]{9f, 0f})).isEqualTo(-1);
    }

    @Test
    void nearestN_ordersAscendingAndHonoursExclusions() {
        CentroidTable t = three();
        assertThat(t.nearestN(new float[]{12f, 0f}, 3)).containsExactly(1, 2, 0);
        assertThat(t.nearestN(new float[]{12f, 0f}, 2, 1)).containsExactly(2, 0);
        assertThat(t.nearestN(new float[]{12f, 0f}, 10, 1, 2)).containsExactly(0);
    }

    @Test
    void distancesToAlive_matchesL2sqInSlotOrder() {
        CentroidTable t = three();
        double[] d = t.distancesToAlive(new float[]{0f, 0f}, t.aliveSlots());
        assertThat(d).containsExactly(0.0, 100.0, 400.0);
    }

    @Test
    void growsPastInitialCapacity() {
        CentroidTable t = CentroidTable.empty();
        for (int i = 0; i < 100; i++) t = t.withAdded(new float[]{i}, i, 1);
        assertThat(t.size()).isEqualTo(100);
        assertThat(t.centroid(99)).containsExactly(99f);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=CentroidTableTest 2>&1 | grep -E "symbol:" | sort -u`
Expected: `symbol: variable CentroidTable`.

- [ ] **Step 3: Write minimal implementation**

```java
package com.recsys.infrastructure.vectordb.spann;

import java.util.Arrays;

/**
 * Immutable in-heap centroid arrays. Every mutator copies the (small) arrays and returns a new
 * table; the index publishes tables through one {@code volatile} reference so a reader sees a
 * consistent set of offsets. Slot numbers are stable for the life of the index — dead slots
 * are never reused or renumbered, because the concurrent id map stores slot numbers and
 * renumbering would make a reader's liveness test lie during the swap.
 */
final class CentroidTable {

    private final float[][] centroid;
    private final long[] offset;
    private final int[] live;
    private final boolean[] alive;
    private final int size;
    private final int aliveCount;

    private CentroidTable(float[][] centroid, long[] offset, int[] live, boolean[] alive, int size, int aliveCount) {
        this.centroid = centroid;
        this.offset = offset;
        this.live = live;
        this.alive = alive;
        this.size = size;
        this.aliveCount = aliveCount;
    }

    static CentroidTable empty() {
        return new CentroidTable(new float[8][], new long[8], new int[8], new boolean[8], 0, 0);
    }

    int size() { return size; }
    int aliveCount() { return aliveCount; }
    boolean alive(int slot) { return alive[slot]; }
    float[] centroid(int slot) { return centroid[slot]; }
    long offset(int slot) { return offset[slot]; }
    int live(int slot) { return live[slot]; }

    int[] aliveSlots() {
        int[] out = new int[aliveCount];
        int j = 0;
        for (int i = 0; i < size; i++) if (alive[i]) out[j++] = i;
        return out;
    }

    CentroidTable withAdded(float[] c, long off, int liveCount) {
        int cap = centroid.length;
        int newCap = size == cap ? cap * 2 : cap;
        float[][] nc = Arrays.copyOf(centroid, newCap);
        long[] no = Arrays.copyOf(offset, newCap);
        int[] nl = Arrays.copyOf(live, newCap);
        boolean[] na = Arrays.copyOf(alive, newCap);
        nc[size] = c;
        no[size] = off;
        nl[size] = liveCount;
        na[size] = true;
        return new CentroidTable(nc, no, nl, na, size + 1, aliveCount + 1);
    }

    CentroidTable withRepointed(int slot, long off, int liveCount) {
        long[] no = offset.clone();
        int[] nl = live.clone();
        no[slot] = off;
        nl[slot] = liveCount;
        return new CentroidTable(centroid, no, nl, alive, size, aliveCount);
    }

    CentroidTable withLive(int slot, int liveCount) {
        int[] nl = live.clone();
        nl[slot] = liveCount;
        return new CentroidTable(centroid, offset, nl, alive, size, aliveCount);
    }

    CentroidTable withKilled(int slot) {
        if (!alive[slot]) return this;
        boolean[] na = alive.clone();
        int[] nl = live.clone();
        na[slot] = false;
        nl[slot] = 0;
        return new CentroidTable(centroid, offset, nl, na, size, aliveCount - 1);
    }

    int nearest(float[] q) {
        int best = -1;
        double bestD = Double.POSITIVE_INFINITY;
        for (int i = 0; i < size; i++) {
            if (!alive[i]) continue;
            double d = KMeans.l2sq(q, centroid[i]);
            if (d < bestD) { bestD = d; best = i; }
        }
        return best;
    }

    int[] nearestN(float[] q, int n, int... exclude) {
        int[] slots = aliveSlots();
        double[] d = distancesToAlive(q, slots);
        Integer[] order = new Integer[slots.length];
        for (int i = 0; i < slots.length; i++) order[i] = i;
        Arrays.sort(order, (a, b) -> Double.compare(d[a], d[b]));
        int[] out = new int[Math.min(n, slots.length)];
        int j = 0;
        for (Integer i : order) {
            if (j == out.length) break;
            int slot = slots[i];
            boolean excluded = false;
            for (int e : exclude) if (e == slot) { excluded = true; break; }
            if (!excluded) out[j++] = slot;
        }
        return j == out.length ? out : Arrays.copyOf(out, j);
    }

    double[] distancesToAlive(float[] q, int[] aliveSlots) {
        double[] d = new double[aliveSlots.length];
        for (int i = 0; i < aliveSlots.length; i++) d[i] = KMeans.l2sq(q, centroid[aliveSlots[i]]);
        return d;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=CentroidTableTest 2>&1 | grep -E "Tests run:|BUILD"`
Expected: `Tests run: 7, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 5: Add to the PR gate and commit**

Add `<include>**/spann/CentroidTableTest.java</include>` after the `MappedPostingStoreTest` include.

```bash
git add pom.xml src/main/java/com/recsys/infrastructure/vectordb/spann/CentroidTable.java src/test/java/com/recsys/infrastructure/vectordb/spann/CentroidTableTest.java
git commit -m "feat(spann): immutable copy-on-write centroid table

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 5: SpannVectorIndex — build, search, stats

**Files:**
- Create: `src/main/java/com/recsys/infrastructure/vectordb/spann/SpannStats.java`
- Create: `src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java`
- Create: `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannTestVectors.java` (test helper)
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexSearchTest.java`
- Modify: `pom.xml` (add `**/spann/SpannVectorIndexSearchTest.java`)

**Interfaces:**
- Consumes: `SpannConfig`, `KMeans`, `PostingStore`/`MappedPostingStore`, `CentroidTable`, `com.recsys.infrastructure.vectordb.VectorIndex`, `SearchResult`, `VectorMath.innerProduct`.
- Produces:
  ```java
  public record SpannStats(int centroidsLive, int centroidsTotal, int entriesLive, long fileBytes, long deadBytes,
                           long splits, long merges, long reassigned, long compactions,
                           long distanceComputations, long fallbackWidenings) {}
  public final class SpannVectorIndex implements VectorIndex, java.io.Closeable {
      public SpannVectorIndex(Map<Integer, float[]> embeddings, SpannConfig cfg);
      public SpannVectorIndex(Map<Integer, float[]> embeddings, SpannConfig cfg, Supplier<PostingStore> storeFactory);
      public List<SearchResult> search(float[] query, int k, Set<Integer> excludeIds);
      public String name();            // "spann"
      public SpannStats stats();
      public int dimension();          // 0 when empty
      public void close();
  }
  ```
  `addOrUpdate` is added in Task 6; until then the interface default (no-op) applies.

Search liveness rule: an entry `(id, vec)` read from centroid slot `c` counts only if `idMap.get(id) == c`. Search also dedups by id (a reader can observe the same id in two blocks during a move — Task 7 explains why); the first occurrence wins.

- [ ] **Step 1: Write the test helper and the failing test**

`SpannTestVectors.java`:

```java
package com.recsys.infrastructure.vectordb.spann;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/** Seeded Gaussian-mixture vectors: clustering is meaningful, unlike uniform noise. */
final class SpannTestVectors {

    private SpannTestVectors() {}

    static List<float[]> mixture(int n, int dim, int components, long seed) {
        Random rng = new Random(seed);
        float[][] centres = new float[components][dim];
        for (float[] c : centres) for (int d = 0; d < dim; d++) c[d] = (float) (rng.nextGaussian() * 5.0);
        List<float[]> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            float[] c = centres[rng.nextInt(components)];
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) v[d] = c[d] + (float) rng.nextGaussian();
            out.add(v);
        }
        return out;
    }

    /** ids 0..n-1 in insertion order. */
    static Map<Integer, float[]> asMap(List<float[]> vectors) {
        Map<Integer, float[]> m = new LinkedHashMap<>();
        for (int i = 0; i < vectors.size(); i++) m.put(i, vectors.get(i));
        return m;
    }
}
```

`SpannVectorIndexSearchTest.java`:

```java
package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.ExactVectorIndex;
import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorMath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannVectorIndexSearchTest {

    @TempDir Path dir;

    private SpannConfig cfg() {
        return SpannConfig.defaults().withDir(dir).withPostingMax(16).withPostingMin(2).withRegionBytes(1 << 20);
    }

    /** nprobe covering every centroid: SPANN must equal exact. */
    private SpannConfig exhaustive() {
        return cfg().withNprobe(1_000_000);
    }

    @Test
    void exhaustiveProbe_equalsExactIndex() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(500, 8, 6, 1L));
        List<float[]> queries = SpannTestVectors.mixture(20, 8, 6, 2L);
        ExactVectorIndex exact = new ExactVectorIndex(data);
        try (SpannVectorIndex spann = new SpannVectorIndex(data, exhaustive())) {
            for (float[] q : queries) {
                List<SearchResult> e = exact.search(q, 10, Set.of());
                List<SearchResult> s = spann.search(q, 10, Set.of());
                assertThat(s).usingRecursiveFieldByFieldElementComparator().containsExactlyElementsOf(e);
            }
        }
    }

    @Test
    void everyIdIsRetrievableAndStatsCountPostings() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(200, 4, 4, 3L));
        try (SpannVectorIndex spann = new SpannVectorIndex(data, exhaustive())) {
            for (Map.Entry<Integer, float[]> e : data.entrySet()) {
                // Inner product does not make a vector its own nearest neighbour (a longer aligned
                // vector scores higher), so "retrievable" means: present, with its own score.
                float[] v = e.getValue();
                List<SearchResult> all = spann.search(v, 200, Set.of());
                assertThat(all).hasSize(200).contains(new SearchResult(e.getKey(), VectorMath.innerProduct(v, v)));
            }
            SpannStats st = spann.stats();
            assertThat(st.entriesLive()).isEqualTo(200);
            assertThat(st.centroidsLive()).isBetween(1, 200);
            assertThat(st.fileBytes()).isPositive();
            assertThat(spann.dimension()).isEqualTo(4);
            assertThat(spann.name()).isEqualTo("spann");
        }
    }

    @Test
    void defaultProbe_findsTheNearestOfAWellClusteredCorpus() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(2000, 16, 8, 4L));
        List<float[]> queries = SpannTestVectors.mixture(50, 16, 8, 5L);
        ExactVectorIndex exact = new ExactVectorIndex(data);
        int hits = 0;
        try (SpannVectorIndex spann = new SpannVectorIndex(data, cfg().withNprobe(8))) {
            for (float[] q : queries) {
                Set<Integer> truth = new java.util.HashSet<>();
                for (SearchResult r : exact.search(q, 10, Set.of())) truth.add(r.id());
                for (SearchResult r : spann.search(q, 10, Set.of())) if (truth.contains(r.id())) hits++;
            }
            long computed = spann.stats().distanceComputations();
            assertThat(hits / 500.0).isGreaterThanOrEqualTo(0.9);
            // CPU proxy: far fewer scorings than exact's 2000 per query (build-time assignment excluded).
            assertThat(computed).isLessThan(50L * 2000);
        }
    }

    @Test
    void exclusionsAreHonoured() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(100, 4, 2, 6L));
        try (SpannVectorIndex spann = new SpannVectorIndex(data, exhaustive())) {
            float[] q = data.get(7);
            assertThat(spann.search(q, 1, Set.of()).get(0).id()).isEqualTo(7);
            assertThat(spann.search(q, 5, Set.of(7))).extracting(SearchResult::id).doesNotContain(7);
            assertThat(spann.search(q, 5, null)).hasSize(5);
        }
    }

    @Test
    void equalScoresOrderByIdAscendingInCutAndOutput() {
        // Four identical vectors with ids chosen to collide in the id map's hash buckets (cf. #324).
        float[] same = {1f, 0f};
        Map<Integer, float[]> data = Map.of(17, same, 33, same, 2, same, 20, same, 99, new float[]{-1f, 0f});
        try (SpannVectorIndex spann = new SpannVectorIndex(data, exhaustive())) {
            assertThat(spann.search(new float[]{1f, 0f}, 4, Set.of())).extracting(SearchResult::id).containsExactly(2, 17, 20, 33);
            assertThat(spann.search(new float[]{1f, 0f}, 2, Set.of())).extracting(SearchResult::id).containsExactly(2, 17);
        }
    }

    @Test
    void wideningFallback_fillsKWhenTheFirstProbeCannot() {
        // Two far-apart blobs; nprobe=1 lands in one blob; asking for more than that blob holds
        // must widen to the other blob rather than return short.
        Map<Integer, float[]> data = new java.util.LinkedHashMap<>();
        for (int i = 0; i < 10; i++) data.put(i, new float[]{i * 0.01f, 0f});
        for (int i = 10; i < 20; i++) data.put(i, new float[]{100f + i * 0.01f, 0f});
        try (SpannVectorIndex spann = new SpannVectorIndex(data, cfg().withPostingMax(12).withNprobe(1))) {
            assertThat(spann.stats().centroidsLive()).isGreaterThanOrEqualTo(2);
            List<SearchResult> hits = spann.search(new float[]{0f, 0f}, 15, Set.of());
            assertThat(hits).hasSize(15);
            assertThat(spann.stats().fallbackWidenings()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void emptyIndex_searchesEmpty() {
        try (SpannVectorIndex spann = new SpannVectorIndex(Map.of(), cfg())) {
            assertThat(spann.search(new float[]{1f}, 3, Set.of())).isEmpty();
            assertThat(spann.dimension()).isZero();
            assertThat(spann.stats().centroidsLive()).isZero();
        }
    }

    @Test
    void nullQueryOrNonPositiveKOrWrongDimension_isEmpty() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(30, 3, 2, 7L));
        try (SpannVectorIndex spann = new SpannVectorIndex(data, exhaustive())) {
            assertThat(spann.search(null, 3, Set.of())).isEmpty();
            assertThat(spann.search(new float[]{1f, 2f, 3f}, 0, Set.of())).isEmpty();
            assertThat(spann.search(new float[]{1f, 2f}, 3, Set.of())).isEmpty();
        }
    }

    @Test
    void mixedDimensionsAreRejectedAtBuild() {
        Map<Integer, float[]> bad = Map.of(1, new float[]{1f, 2f}, 2, new float[]{1f});
        assertThatThrownBy(() -> new SpannVectorIndex(bad, cfg()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dimension");
        // No index file may be left behind by a failed build.
        assertThat(dir.toFile().listFiles()).isEmpty();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=SpannVectorIndexSearchTest 2>&1 | grep -E "symbol:" | sort -u`
Expected: `symbol: class SpannVectorIndex` (and `SpannStats`).

- [ ] **Step 3: Write minimal implementation**

`SpannStats.java`:

```java
package com.recsys.infrastructure.vectordb.spann;

/** Point-in-time counters of a {@link SpannVectorIndex}; the seam Prometheus would attach to. */
public record SpannStats(int centroidsLive, int centroidsTotal, int entriesLive, long fileBytes, long deadBytes,
                         long splits, long merges, long reassigned, long compactions,
                         long distanceComputations, long fallbackWidenings) {}
```

`SpannVectorIndex.java` (build + search; SPFresh methods are added in Tasks 6–9):

```java
package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorIndex;
import com.recsys.infrastructure.vectordb.VectorMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * SPANN-style disk-resident index: centroids in heap, posting lists as immutable blocks in a
 * {@link PostingStore}, SPFresh-style in-place updates. See
 * docs/superpowers/specs/2026-09-13-spann-spfresh-vector-index-design.md.
 *
 * <p>Liveness rule: an entry read from centroid slot {@code c} is live iff {@code idMap.get(id) == c}.
 * Readers pin one {@link Snapshot} (table + store) per search and never block; the single writer
 * publishes a new snapshot after every step and flips {@code idMap} as the linearisation point.
 */
public final class SpannVectorIndex implements VectorIndex, Closeable {

    private static final Logger log = LoggerFactory.getLogger(SpannVectorIndex.class);
    private static final int SAMPLE_LIMIT = 20_000;
    private static final int BUILD_ITERATIONS = 10;
    private static final int SPLIT_ITERATIONS = 10;

    /** Heap head is the worst retained hit: lowest score, then highest id (cf. ExactMultiVectorIndex). */
    static final Comparator<SearchResult> WORST_FIRST =
            Comparator.comparingDouble(SearchResult::score).thenComparing(SearchResult::id, Comparator.reverseOrder());

    record Snapshot(CentroidTable table, PostingStore store) {}

    private final SpannConfig cfg;
    private final Supplier<PostingStore> storeFactory;
    private final ReentrantLock writer = new ReentrantLock();
    private final ConcurrentHashMap<Integer, Integer> idMap = new ConcurrentHashMap<>();
    private final LongAdder distanceComputations = new LongAdder();
    private final AtomicLong splits = new AtomicLong();
    private final AtomicLong merges = new AtomicLong();
    private final AtomicLong reassigned = new AtomicLong();
    private final AtomicLong compactions = new AtomicLong();
    private final AtomicLong fallbackWidenings = new AtomicLong();
    private volatile Snapshot snap;
    private volatile int dim;
    private volatile long deadBytes;       // written under the writer lock only
    private volatile boolean closed;

    public SpannVectorIndex(Map<Integer, float[]> embeddings, SpannConfig cfg) {
        this(embeddings, cfg, () -> new MappedPostingStore(cfg.dir(), cfg.regionBytes()));
    }

    public SpannVectorIndex(Map<Integer, float[]> embeddings, SpannConfig cfg, Supplier<PostingStore> storeFactory) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.storeFactory = Objects.requireNonNull(storeFactory, "storeFactory");
        PostingStore store = storeFactory.get();
        try {
            this.snap = new Snapshot(build(embeddings, store), store);
        } catch (RuntimeException e) {
            store.close();
            throw e;
        }
        log.info("SPANN index built: {} entries, {} centroids, {} bytes on disk", idMap.size(),
                snap.table().aliveCount(), store.bytes());
    }

    // ---------------------------------------------------------------- build

    private CentroidTable build(Map<Integer, float[]> embeddings, PostingStore store) {
        CentroidTable table = CentroidTable.empty();
        if (embeddings.isEmpty()) {
            dim = 0;
            return table;
        }
        List<Integer> ids = new ArrayList<>(embeddings.keySet());
        Collections.sort(ids);
        int d = embeddings.get(ids.get(0)).length;
        for (int id : ids) {
            float[] v = embeddings.get(id);
            if (v == null || v.length != d) {
                throw new IllegalArgumentException("vector dimension mismatch for id " + id + ": expected " + d
                        + ", got " + (v == null ? "null" : v.length));
            }
        }
        dim = d;
        int n = ids.size();
        int k = Math.max(1, (int) Math.ceil(n / (cfg.postingMax() / 2.0)));
        List<float[]> sample = new ArrayList<>();
        if (n <= SAMPLE_LIMIT) {
            for (int id : ids) sample.add(embeddings.get(id));
        } else {
            List<Integer> shuffled = new ArrayList<>(ids);
            Collections.shuffle(shuffled, new Random(cfg.seed()));
            for (int i = 0; i < SAMPLE_LIMIT; i++) sample.add(embeddings.get(shuffled.get(i)));
        }
        float[][] centroids = KMeans.cluster(sample, k, cfg.seed(), BUILD_ITERATIONS);
        List<List<Integer>> members = new ArrayList<>(centroids.length);
        for (int c = 0; c < centroids.length; c++) members.add(new ArrayList<>());
        for (int id : ids) {
            members.get(KMeans.nearest(embeddings.get(id), centroids, null)).add(id);
        }
        distanceComputations.add((long) n * centroids.length);
        for (int c = 0; c < centroids.length; c++) {
            List<Integer> m = members.get(c);
            if (m.isEmpty()) continue;
            int[] bid = new int[m.size()];
            float[][] bvec = new float[m.size()][];
            for (int i = 0; i < m.size(); i++) {
                bid[i] = m.get(i);
                bvec[i] = embeddings.get(m.get(i));
            }
            int slot = table.size();
            long off = store.append(new PostingStore.Block(bid, bvec));
            table = table.withAdded(centroids[c], off, m.size());
            for (int id : m) idMap.put(id, slot);
        }
        return table;
    }

    // ---------------------------------------------------------------- search

    @Override
    public List<SearchResult> search(float[] query, int k, Set<Integer> excludeIds) {
        Snapshot s = snap;
        if (closed || s == null || query == null || k <= 0 || s.table().aliveCount() == 0) return List.of();
        if (query.length != dim) return List.of();
        Set<Integer> excluded = Objects.requireNonNullElse(excludeIds, Set.of());
        CentroidTable t = s.table();
        int[] slots = t.aliveSlots();
        // Probe by the SCORING metric (inner product), as FAISS's inner-product IVF does: the
        // partition is L2 (k-means), but the postings holding the highest <query, entry> are the
        // ones whose centroids have the highest <query, centroid>. L2-nearest postings would miss
        // them — see the spec's Search section.
        double[] ip = new double[slots.length];
        for (int i = 0; i < slots.length; i++) ip[i] = VectorMath.innerProduct(query, t.centroid(slots[i]));
        distanceComputations.add(slots.length);
        Integer[] order = new Integer[slots.length];
        for (int i = 0; i < slots.length; i++) order[i] = i;
        Arrays.sort(order, (a, b) -> Double.compare(ip[b], ip[a]));          // descending

        PriorityQueue<SearchResult> best = new PriorityQueue<>(WORST_FIRST);
        Set<Integer> seen = new HashSet<>();
        int scanned = 0;
        int probe = Math.min(cfg.nprobe(), slots.length);
        while (true) {
            for (; scanned < probe; scanned++) {
                scanBlock(s, slots[order[scanned]], query, k, excluded, seen, best);
            }
            if (best.size() >= k || probe == slots.length) break;
            probe = Math.min(probe * 2, slots.length);                          // widening fallback
            fallbackWidenings.incrementAndGet();
        }
        List<SearchResult> results = new ArrayList<>(best);
        results.sort(WORST_FIRST.reversed());
        return results;
    }

    private void scanBlock(Snapshot s, int slot, float[] query, int k, Set<Integer> excluded,
                           Set<Integer> seen, PriorityQueue<SearchResult> best) {
        // Read through the pinned snapshot: its offset and its store were published together.
        PostingStore.Block b = s.store().read(s.table().offset(slot));
        for (int i = 0; i < b.count(); i++) {
            int id = b.ids()[i];
            if (excluded.contains(id)) continue;
            Integer owner = idMap.get(id);
            if (owner == null || owner != slot) continue;      // stale: superseded or moved
            if (!seen.add(id)) continue;                        // observed in another block mid-move
            double score = VectorMath.innerProduct(query, b.vectors()[i]);
            distanceComputations.increment();
            if (score == Double.NEGATIVE_INFINITY) continue;
            SearchResult hit = new SearchResult(id, score);
            if (best.size() < k) {
                best.offer(hit);
            } else if (WORST_FIRST.compare(hit, best.peek()) > 0) {
                best.poll();
                best.offer(hit);
            }
        }
    }

    // ---------------------------------------------------------------- misc

    @Override
    public String name() {
        return "spann";
    }

    public int dimension() {
        return dim;
    }

    public SpannStats stats() {
        Snapshot s = snap;
        CentroidTable t = s == null ? CentroidTable.empty() : s.table();
        long bytes = s == null ? 0 : s.store().bytes();
        return new SpannStats(t.aliveCount(), t.size(), idMap.size(), bytes, deadBytes, splits.get(), merges.get(),
                reassigned.get(), compactions.get(), distanceComputations.sum(), fallbackWidenings.get());
    }

    @Override
    public void close() {
        writer.lock();
        try {
            if (closed) return;
            closed = true;
            Snapshot s = snap;
            snap = null;
            idMap.clear();
            if (s != null) s.store().close();
        } finally {
            writer.unlock();
        }
    }

    private static int entryBytes(int dim) {
        return 4 + 4 * dim;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=SpannVectorIndexSearchTest 2>&1 | grep -E "Tests run:|BUILD|FAIL"`
Expected: `Tests run: 9, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

If `defaultProbe_findsTheNearestOfAWellClusteredCorpus` fails on recall: the corpus is 8 well-separated components, 2000 points, posting max 16 → ~250 centroids; nprobe 8 by inner product should exceed 0.9. Do not loosen the assertion; check that `order` is sorted by inner product *descending* and the widening loop first.

- [ ] **Step 5: Add to the PR gate and commit**

Add `<include>**/spann/SpannVectorIndexSearchTest.java</include>` after the `CentroidTableTest` include.

```bash
git add pom.xml src/main/java/com/recsys/infrastructure/vectordb/spann/SpannStats.java src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java src/test/java/com/recsys/infrastructure/vectordb/spann/SpannTestVectors.java src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexSearchTest.java
git commit -m "feat(spann): SpannVectorIndex build and probe search with deterministic tie order

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 6: SPFresh insert and overwrite

**Files:**
- Modify: `src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexUpdateTest.java`
- Modify: `pom.xml` (add `**/spann/SpannVectorIndexUpdateTest.java`)

**Interfaces:**
- Produces: `public void addOrUpdate(int id, float[] vec)` (overrides the `VectorIndex` default); private helpers `liveEntries(Snapshot, int slot)`, `rewrite(Snapshot, CentroidTable, int slot, List<int[]> ..)` as written below; `publish(CentroidTable)`.
- Ordering contract used by Tasks 7–9: **append → publish table → flip idMap → (optionally) publish again**. Publishing before the flip is what keeps a moved id visible in exactly one place at every instant (old block until the flip, new block after).

- [ ] **Step 1: Write the failing test**

```java
package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorMath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

class SpannVectorIndexUpdateTest {

    @TempDir Path dir;

    private SpannConfig cfg() {
        return SpannConfig.defaults().withDir(dir).withPostingMax(100).withPostingMin(0)
                .withNprobe(1_000_000).withRegionBytes(1 << 20);
    }

    private static Map<Integer, float[]> twoBlobs() {
        Map<Integer, float[]> m = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) m.put(i, new float[]{i * 0.1f, 0f});
        for (int i = 10; i < 20; i++) m.put(i, new float[]{50f + i * 0.1f, 0f});
        return m;
    }

    @Test
    void insertNewId_isSearchable() {
        try (SpannVectorIndex idx = new SpannVectorIndex(twoBlobs(), cfg())) {
            float[] v = {0.35f, 0f};
            idx.addOrUpdate(42, v);
            List<SearchResult> hits = idx.search(v, 100, Set.of());
            assertThat(hits).hasSize(21).contains(new SearchResult(42, VectorMath.innerProduct(v, v)));
            assertThat(idx.stats().entriesLive()).isEqualTo(21);
        }
    }

    @Test
    void overwriteInSamePosting_hidesTheOldVector() {
        try (SpannVectorIndex idx = new SpannVectorIndex(twoBlobs(), cfg())) {
            idx.addOrUpdate(3, new float[]{0.95f, 0f});            // still nearest to the first blob
            List<SearchResult> hits = idx.search(new float[]{1f, 0f}, 100, Set.of());
            assertThat(hits).hasSize(20).extracting(SearchResult::id).doesNotHaveDuplicates();
            // id 3 scores with its NEW vector (0.95); the old (0.3) is gone.
            assertThat(scoreOf(hits, 3)).isCloseTo(0.95, within(1e-6));
            assertThat(idx.stats().entriesLive()).isEqualTo(20);
        }
    }

    @Test
    void overwriteMovingToAnotherPosting_isVisibleExactlyOnce() {
        // postingMax 16 -> ceil(20 / 8) = 3 centroids over two blobs, so a move can cross postings.
        try (SpannVectorIndex idx = new SpannVectorIndex(twoBlobs(), cfg().withPostingMax(16))) {
            int before = idx.stats().centroidsLive();
            assertThat(before).isGreaterThanOrEqualTo(2);
            idx.addOrUpdate(3, new float[]{51.05f, 0f});           // moves from blob A to blob B
            List<SearchResult> all = idx.search(new float[]{1f, 0f}, 30, Set.of());
            assertThat(all).hasSize(20).extracting(SearchResult::id).doesNotHaveDuplicates().contains(3);
            assertThat(scoreOf(all, 3)).isCloseTo(51.05, within(1e-4));
            assertThat(idx.stats().deadBytes()).isPositive();
        }
    }

    private static double scoreOf(List<SearchResult> hits, int id) {
        return hits.stream().filter(r -> r.id() == id).mapToDouble(SearchResult::score).findFirst().orElseThrow();
    }

    @Test
    void firstInsertIntoEmptyIndex_setsTheDimension() {
        try (SpannVectorIndex idx = new SpannVectorIndex(Map.of(), cfg())) {
            idx.addOrUpdate(1, new float[]{1f, 2f, 3f});
            assertThat(idx.dimension()).isEqualTo(3);
            assertThat(idx.search(new float[]{1f, 2f, 3f}, 1, Set.of()).get(0).id()).isEqualTo(1);
            idx.addOrUpdate(2, new float[]{-1f, -2f, -3f});
            assertThat(idx.search(new float[]{-1f, -2f, -3f}, 1, Set.of()).get(0).id()).isEqualTo(2);
        }
    }

    @Test
    void dimensionMismatchIsRejectedBeforeAnyWrite() {
        try (SpannVectorIndex idx = new SpannVectorIndex(twoBlobs(), cfg())) {
            long bytes = idx.stats().fileBytes();
            assertThatThrownBy(() -> idx.addOrUpdate(7, new float[]{1f, 2f, 3f}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("dimension");
            assertThatThrownBy(() -> idx.addOrUpdate(7, null)).isInstanceOf(IllegalArgumentException.class);
            assertThat(idx.stats().fileBytes()).isEqualTo(bytes);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=SpannVectorIndexUpdateTest 2>&1 | grep -E "Tests run:|insertNewId|overwrite|firstInsert|dimensionMismatch" | head`
Expected: compiles (the interface default exists) but `insertNewId_isSearchable`, both `overwrite*` and `firstInsert*` FAIL (the default `addOrUpdate` is a no-op, so the inserted ids are never found); `dimensionMismatch*` fails because nothing throws.

- [ ] **Step 3: Write minimal implementation**

Add to `SpannVectorIndex` (after the `// ---- search` section):

```java
    // ---------------------------------------------------------------- SPFresh: insert / overwrite

    @Override
    public void addOrUpdate(int id, float[] vec) {
        if (vec == null) throw new IllegalArgumentException("vector must not be null");
        writer.lock();
        try {
            if (closed) throw new IllegalStateException("SPANN index is closed");
            Snapshot s = snap;
            if (s.table().aliveCount() == 0) {
                dim = vec.length;
                long off = s.store().append(new PostingStore.Block(new int[]{id}, new float[][]{vec.clone()}));
                CentroidTable t = s.table().withAdded(vec.clone(), off, 1);
                publish(new Snapshot(t, s.store()));
                idMap.put(id, t.size() - 1);
                return;
            }
            if (vec.length != dim) {
                throw new IllegalArgumentException("vector dimension mismatch: expected " + dim + ", got " + vec.length);
            }
            Integer old = idMap.get(id);
            int target = s.table().nearest(vec);
            distanceComputations.add(s.table().aliveCount());

            List<int[]> ids = new ArrayList<>();
            List<float[]> vecs = new ArrayList<>();
            int liveBefore = collectLive(s, target, id, ids, vecs);
            ids.add(new int[]{id});
            vecs.add(vec.clone());
            long off = s.store().append(block(ids, vecs));
            CentroidTable t = s.table().withRepointed(target, off, ids.size());
            deadBytes += 8L + (long) liveBefore * entryBytes(dim);       // the old block is unreferenced now
            if (old != null && old != target) {
                t = t.withLive(old, t.live(old) - 1);
                deadBytes += entryBytes(dim);                            // stale entry left inside old's block
            }
            publish(new Snapshot(t, s.store()));
            idMap.put(id, target);                                      // linearisation point
        } finally {
            writer.unlock();
        }
    }

    /**
     * Collects the live entries of {@code slot} (those the id map attributes to it), skipping
     * {@code skipId}. Returns the number of entries that were live before the skip, i.e. the
     * live count the old block carried, for dead-bytes accounting.
     */
    private int collectLive(Snapshot s, int slot, int skipId, List<int[]> ids, List<float[]> vecs) {
        PostingStore.Block b = s.store().read(s.table().offset(slot));
        int liveBefore = 0;
        for (int i = 0; i < b.count(); i++) {
            int eid = b.ids()[i];
            Integer owner = idMap.get(eid);
            if (owner == null || owner != slot) continue;
            liveBefore++;
            if (eid == skipId) continue;
            ids.add(new int[]{eid});
            vecs.add(b.vectors()[i]);
        }
        return liveBefore;
    }

    private static PostingStore.Block block(List<int[]> ids, List<float[]> vecs) {
        int[] bid = new int[ids.size()];
        float[][] bvec = new float[ids.size()][];
        for (int i = 0; i < ids.size(); i++) {
            bid[i] = ids.get(i)[0];
            bvec[i] = vecs.get(i);
        }
        return new PostingStore.Block(bid, bvec);
    }

    private void publish(Snapshot s) {
        snap = s;
    }
```

(`ids` is a `List<int[]>` of one-element arrays rather than `List<Integer>` so the same `block(...)` helper serves Tasks 7–9 without boxing churn in the hot rewrite path; keep it as written.)

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='SpannVectorIndexUpdateTest,SpannVectorIndexSearchTest' 2>&1 | grep -E "Tests run:|BUILD|FAIL"`
Expected: `Tests run: 14, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 5: Add to the PR gate and commit**

Add `<include>**/spann/SpannVectorIndexUpdateTest.java</include>` after the `SpannVectorIndexSearchTest` include.

```bash
git add pom.xml src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexUpdateTest.java
git commit -m "feat(spann): SPFresh insert and overwrite with id-map liveness

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 7: SPFresh split and bounded reassignment (also applied after build)

**Files:**
- Modify: `src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexSplitTest.java`
- Modify: `pom.xml` (add `**/spann/SpannVectorIndexSplitTest.java`)

**Interfaces:**
- Produces: private `CentroidTable split(Snapshot s, CentroidTable t, int slot, boolean cascade)`; private `CentroidTable reassign(Snapshot s, CentroidTable t, int a, int b, boolean cascade)`; private `CentroidTable rebalanceAfterBuild(...)`. `cascade=false` means "do not trigger a merge from this reassignment" (Task 8 uses it to bound merge→split→merge chains). The `addOrUpdate` tail gains `if (t.live(target) > cfg.postingMax()) t = split(...)`.

Move protocol (from Task 6): append new blocks → publish (old block still referenced, new blocks referenced but their entries stale) → flip idMap → publish with the old block dropped. A reader may see an id in both the old and the new block between the two publishes; search dedups by id, so it returns the id once.

- [ ] **Step 1: Write the failing test**

```java
package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorMath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SpannVectorIndexSplitTest {

    @TempDir Path dir;

    private SpannConfig cfg(int postingMax) {
        return SpannConfig.defaults().withDir(dir).withPostingMax(postingMax).withPostingMin(0)
                .withNprobe(1_000_000).withRegionBytes(1 << 20);
    }

    @Test
    void insertingPastPostingMax_splitsAndKeepsEveryPostingBounded() {
        try (SpannVectorIndex idx = new SpannVectorIndex(Map.of(0, new float[]{0f, 0f}), cfg(4))) {
            for (int i = 1; i < 30; i++) idx.addOrUpdate(i, new float[]{i * 0.1f, (i % 3) * 0.1f});
            SpannStats st = idx.stats();
            assertThat(st.splits()).isGreaterThanOrEqualTo(1);
            assertThat(st.entriesLive()).isEqualTo(30);
            assertThat(st.centroidsLive()).isGreaterThanOrEqualTo(8);          // 30 entries / max 4
            for (int i = 0; i < 30; i++) {
                float[] v = {i * 0.1f, (i % 3) * 0.1f};
                assertThat(idx.search(v, 100, Set.of())).contains(new SearchResult(i, VectorMath.innerProduct(v, v)));
            }
            List<SearchResult> all = idx.search(new float[]{1f, 0f}, 100, Set.of());
            assertThat(all).hasSize(30).extracting(SearchResult::id).doesNotHaveDuplicates();
        }
    }

    @Test
    void build_splitsOversizedPostings() {
        // 40 near-identical points force k-means to put most of them in one posting of max 8.
        Map<Integer, float[]> m = new LinkedHashMap<>();
        for (int i = 0; i < 40; i++) m.put(i, new float[]{i * 0.001f, 0f});
        try (SpannVectorIndex idx = new SpannVectorIndex(m, cfg(8))) {
            assertThat(idx.stats().entriesLive()).isEqualTo(40);
            assertThat(idx.stats().centroidsLive()).isGreaterThanOrEqualTo(5);
            for (int i = 0; i < 40; i++) {
                float[] v = {i * 0.001f, 0f};
                assertThat(idx.search(v, 100, Set.of())).contains(new SearchResult(i, VectorMath.innerProduct(v, v)));
            }
        }
    }

    @Test
    void reassignment_movesAVectorThatIsNowNearerToANewCentroid() {
        // Nine points, postingMax 9 -> ceil(9 / 4.5) = 2 centroids: A = four points around the
        // origin plus p = (4.5, 0) (nearer A's mean (0.9, 0.2) than B's (10.25, 0)), B = four
        // points around (10, 0). Six inserts around (7.5, 0) all land in B (nearer than A),
        // overflow it at the 10th entry, and its 2-means split yields centroids ~(7.5, 0) and
        // ~(10.25, 0). p is now 3.0 from (7.5, 0) but 3.6 from A's mean, so reassignment must
        // move it; A's other four points stay.
        Map<Integer, float[]> m = new LinkedHashMap<>();
        m.put(0, new float[]{0f, 0f}); m.put(1, new float[]{1f, 0f}); m.put(2, new float[]{0f, 1f}); m.put(3, new float[]{-1f, 0f});
        m.put(4, new float[]{4.5f, 0f});                                                // p
        m.put(10, new float[]{10f, 0f}); m.put(11, new float[]{10f, 1f}); m.put(12, new float[]{10f, -1f}); m.put(13, new float[]{11f, 0f});
        try (SpannVectorIndex idx = new SpannVectorIndex(m, cfg(9).withReassignProbe(4))) {
            assertThat(idx.stats().centroidsLive()).isEqualTo(2);          // premise guard
            int id = 20;
            for (float[] v : new float[][]{{7f, 0f}, {7f, 1f}, {7f, -1f}, {8f, 0f}, {8f, 1f}, {8f, -1f}}) {
                idx.addOrUpdate(id++, v);
            }
            assertThat(idx.stats().splits()).isGreaterThanOrEqualTo(1);
            assertThat(idx.stats().reassigned()).isGreaterThanOrEqualTo(1);
            assertThat(idx.stats().entriesLive()).isEqualTo(15);
            // p is still exactly once retrievable, with its own vector (4.5^2 = 20.25).
            List<SearchResult> all = idx.search(new float[]{4.5f, 0f}, 50, Set.of());
            assertThat(all).hasSize(15).contains(new SearchResult(4, 20.25));
            assertThat(all).extracting(SearchResult::id).doesNotHaveDuplicates();
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=SpannVectorIndexSplitTest 2>&1 | grep -E "Tests run:|splits|reassigned|centroidsLive|Expecting" | head`
Expected: all three FAIL — `splits()` is 0 and `centroidsLive()` is below the bound because nothing splits yet.

- [ ] **Step 3: Write minimal implementation**

In `addOrUpdate`, replace the two lines

```java
            publish(new Snapshot(t, s.store()));
            idMap.put(id, target);                                      // linearisation point
```

with

```java
            publish(new Snapshot(t, s.store()));
            idMap.put(id, target);                                      // linearisation point
            if (t.live(target) > cfg.postingMax()) {
                t = split(snap, t, target, true);
            }
```

In the constructor, after `this.snap = new Snapshot(build(embeddings, store), store);` add `rebalanceAfterBuild();` (inside the same `try`). Then add the methods:

```java
    // ---------------------------------------------------------------- SPFresh: split / reassign

    private void rebalanceAfterBuild() {
        CentroidTable t = snap.table();
        for (int slot = 0; slot < t.size(); slot++) {
            if (t.alive(slot) && t.live(slot) > cfg.postingMax()) {
                t = split(snap, t, slot, false);
            }
        }
    }

    /**
     * 2-means the live entries of {@code slot} into two new centroids, then reassigns the
     * neighbourhood. Returns the table in service afterwards. A posting whose entries cannot be
     * separated (fewer than two live, or all identical) is left as is.
     */
    private CentroidTable split(Snapshot s, CentroidTable t, int slot, boolean cascade) {
        List<int[]> ids = new ArrayList<>();
        List<float[]> vecs = new ArrayList<>();
        collectLive(new Snapshot(t, s.store()), slot, Integer.MIN_VALUE, ids, vecs);
        // MIN_VALUE is never an id in practice; collectLive's skip is a no-op here.
        if (ids.size() < 2) return t;
        float[][] c = KMeans.cluster(vecs, 2, cfg.seed(), SPLIT_ITERATIONS);
        if (c.length < 2) return t;
        List<int[]> idsA = new ArrayList<>(), idsB = new ArrayList<>();
        List<float[]> vecA = new ArrayList<>(), vecB = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            if (KMeans.l2sq(vecs.get(i), c[0]) <= KMeans.l2sq(vecs.get(i), c[1])) { idsA.add(ids.get(i)); vecA.add(vecs.get(i)); }
            else { idsB.add(ids.get(i)); vecB.add(vecs.get(i)); }
        }
        distanceComputations.add(2L * ids.size());
        if (idsA.isEmpty() || idsB.isEmpty()) return t;

        long offA = s.store().append(block(idsA, vecA));
        long offB = s.store().append(block(idsB, vecB));
        t = t.withAdded(c[0], offA, idsA.size()).withAdded(c[1], offB, idsB.size());
        int a = t.size() - 2, b = t.size() - 1;
        publish(new Snapshot(t, s.store()));                            // old slot still referenced
        for (int[] e : idsA) idMap.put(e[0], a);
        for (int[] e : idsB) idMap.put(e[0], b);
        t = t.withKilled(slot);
        deadBytes += 8L + (long) ids.size() * entryBytes(dim);
        publish(new Snapshot(t, s.store()));
        splits.incrementAndGet();
        log.info("SPANN split: slot {} ({} entries) -> {} ({}) + {} ({})", slot, ids.size(), a, idsA.size(), b, idsB.size());
        return reassign(s, t, a, b, cascade);
    }

    /**
     * SPFresh's bounded rebalancing: only the postings of the {@code reassignProbe} centroids
     * nearest to each new centroid are examined; an entry moves iff its nearest live centroid
     * is now one of the two new ones. A far-away entry in a now-suboptimal posting stays until
     * it is next touched — that is the trade, and 13_DB_Indexing §5 names it.
     */
    private CentroidTable reassign(Snapshot s, CentroidTable t, int a, int b, boolean cascade) {
        if (cfg.reassignProbe() == 0) return t;
        Set<Integer> neighbours = new java.util.LinkedHashSet<>();
        for (int n : t.nearestN(t.centroid(a), cfg.reassignProbe(), a, b)) neighbours.add(n);
        for (int n : t.nearestN(t.centroid(b), cfg.reassignProbe(), a, b)) neighbours.add(n);
        for (int n : neighbours) {
            if (!t.alive(n)) continue;
            List<int[]> ids = new ArrayList<>();
            List<float[]> vecs = new ArrayList<>();
            collectLive(new Snapshot(t, s.store()), n, Integer.MIN_VALUE, ids, vecs);
            List<int[]> stayIds = new ArrayList<>(), toA = new ArrayList<>(), toB = new ArrayList<>();
            List<float[]> stayVec = new ArrayList<>(), vecToA = new ArrayList<>(), vecToB = new ArrayList<>();
            for (int i = 0; i < ids.size(); i++) {
                int nearest = t.nearest(vecs.get(i));
                if (nearest == a) { toA.add(ids.get(i)); vecToA.add(vecs.get(i)); }
                else if (nearest == b) { toB.add(ids.get(i)); vecToB.add(vecs.get(i)); }
                else { stayIds.add(ids.get(i)); stayVec.add(vecs.get(i)); }
            }
            distanceComputations.add((long) ids.size() * t.aliveCount());
            if (toA.isEmpty() && toB.isEmpty()) continue;

            // Destination blocks first (moved entries are stale there until the flip)…
            if (!toA.isEmpty()) t = appendTo(s, t, a, toA, vecToA);
            if (!toB.isEmpty()) t = appendTo(s, t, b, toB, vecToB);
            long offStay = s.store().append(block(stayIds, stayVec));
            publish(new Snapshot(t, s.store()));                        // n still points at its old block
            for (int[] e : toA) idMap.put(e[0], a);
            for (int[] e : toB) idMap.put(e[0], b);
            deadBytes += 8L + (long) ids.size() * entryBytes(dim);
            t = t.withRepointed(n, offStay, stayIds.size());
            publish(new Snapshot(t, s.store()));
            reassigned.addAndGet(toA.size() + toB.size());
            if (cascade && stayIds.size() < cfg.postingMin()) {
                t = mergeIfUnderfull(s, t, n);
            }
        }
        return t;
    }

    /** Rewrites {@code slot}'s block as its current live entries plus {@code extra}; no id-map change. */
    private CentroidTable appendTo(Snapshot s, CentroidTable t, int slot, List<int[]> extraIds, List<float[]> extraVecs) {
        List<int[]> ids = new ArrayList<>();
        List<float[]> vecs = new ArrayList<>();
        int liveBefore = collectLive(new Snapshot(t, s.store()), slot, Integer.MIN_VALUE, ids, vecs);
        ids.addAll(extraIds);
        vecs.addAll(extraVecs);
        long off = s.store().append(block(ids, vecs));
        deadBytes += 8L + (long) liveBefore * entryBytes(dim);
        return t.withRepointed(slot, off, ids.size());
    }

    /** Task 8 replaces this stub with the real merge. Until then an underfull posting is left alone. */
    private CentroidTable mergeIfUnderfull(Snapshot s, CentroidTable t, int slot) {
        return t;
    }
```

The stub is the one place this plan defers behaviour, and Task 8 replaces it in the very next commit; it exists so that Task 7 compiles and its tests pin split/reassign in isolation.

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='SpannVectorIndexSplitTest,SpannVectorIndexUpdateTest,SpannVectorIndexSearchTest' 2>&1 | grep -E "Tests run:|BUILD|FAIL"`
Expected: `Tests run: 17, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

If `reassignment_movesAVectorThatIsNowNearerToANewCentroid` fails on the premise guard, k-means++ with seed 42 separated the nine points differently than A/B; report that with the actual centroids rather than loosening the test — the fixture is built so that exactly two centroids exist and the arithmetic in the comment holds.

- [ ] **Step 5: Add to the PR gate and commit**

Add `<include>**/spann/SpannVectorIndexSplitTest.java</include>` after the `SpannVectorIndexUpdateTest` include.

```bash
git add pom.xml src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexSplitTest.java
git commit -m "feat(spann): SPFresh split with bounded neighbourhood reassignment

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 8: SPFresh merge

**Files:**
- Modify: `src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexMergeTest.java`
- Modify: `pom.xml` (add `**/spann/SpannVectorIndexMergeTest.java`)

**Interfaces:**
- Produces: `mergeIfUnderfull(Snapshot, CentroidTable, int slot)` becomes real: merges into the nearest other live centroid, kills the slot, and if the destination overflows splits it once with `cascade=false`. `addOrUpdate` calls it for the `old` slot after a cross-posting overwrite.

- [ ] **Step 1: Write the failing test**

```java
package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorMath;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class SpannVectorIndexMergeTest {

    @TempDir Path dir;

    private SpannConfig cfg() {
        return SpannConfig.defaults().withDir(dir).withPostingMax(16).withPostingMin(3)
                .withNprobe(1_000_000).withRegionBytes(1 << 20);
    }

    private static Map<Integer, float[]> twoBlobs() {
        Map<Integer, float[]> m = new LinkedHashMap<>();
        for (int i = 0; i < 8; i++) m.put(i, new float[]{i * 0.1f, 0f});
        for (int i = 10; i < 18; i++) m.put(i, new float[]{50f + i * 0.1f, 0f});
        return m;
    }

    @Test
    void shrinkingBelowPostingMin_mergesIntoTheNearestPosting() {
        try (SpannVectorIndex idx = new SpannVectorIndex(twoBlobs(), cfg())) {
            assertThat(idx.stats().centroidsLive()).isEqualTo(2);
            // Move six of blob A's eight ids into blob B: A drops to 2 < postingMin 3.
            for (int i = 0; i < 6; i++) idx.addOrUpdate(i, new float[]{52f + i * 0.1f, 0f});
            SpannStats st = idx.stats();
            assertThat(st.merges()).isGreaterThanOrEqualTo(1);
            assertThat(st.entriesLive()).isEqualTo(16);
            assertThat(idx.search(new float[]{0.7f, 0f}, 20, Set.of()))
                    .hasSize(16).extracting(SearchResult::id).doesNotHaveDuplicates().contains(6, 7);
            // The two survivors are still retrievable with their own vectors.
            float[] q = {1f, 0f};
            assertThat(idx.search(q, 20, Set.of())).contains(new SearchResult(7, VectorMath.innerProduct(q, new float[]{0.7f, 0f})));
        }
    }

    @Test
    void mergeNeverRunsOnTheLastLivePosting() {
        Map<Integer, float[]> m = new LinkedHashMap<>();
        for (int i = 0; i < 4; i++) m.put(i, new float[]{i, 0f});
        try (SpannVectorIndex idx = new SpannVectorIndex(m, cfg().withPostingMax(20))) {
            assertThat(idx.stats().centroidsLive()).isEqualTo(1);
            idx.addOrUpdate(0, new float[]{100f, 0f});       // same posting; live count stays 4
            idx.addOrUpdate(1, new float[]{100f, 1f});
            assertThat(idx.stats().merges()).isZero();
            assertThat(idx.stats().centroidsLive()).isEqualTo(1);
            assertThat(idx.stats().entriesLive()).isEqualTo(4);
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=SpannVectorIndexMergeTest 2>&1 | grep -E "Tests run:|merges|Expecting" | head`
Expected: `shrinkingBelowPostingMin_mergesIntoTheNearestPosting` FAILS with `merges()` = 0; the second test passes already (it pins a non-event).

- [ ] **Step 3: Write minimal implementation**

Replace the Task 7 stub with:

```java
    /**
     * Merges an underfull posting into its nearest live neighbour. The destination centroid's
     * vector is left unchanged (as SPFresh does). If the destination overflows it is split once,
     * without cascading merges — a second overflow waits for the next update.
     */
    private CentroidTable mergeIfUnderfull(Snapshot s, CentroidTable t, int slot) {
        if (!t.alive(slot) || t.live(slot) >= cfg.postingMin() || t.aliveCount() <= 1) return t;
        List<int[]> ids = new ArrayList<>();
        List<float[]> vecs = new ArrayList<>();
        collectLive(new Snapshot(t, s.store()), slot, Integer.MIN_VALUE, ids, vecs);
        int[] nearest = t.nearestN(t.centroid(slot), 1, slot);
        if (nearest.length == 0) return t;
        int dest = nearest[0];
        distanceComputations.add(t.aliveCount());
        t = appendTo(s, t, dest, ids, vecs);
        publish(new Snapshot(t, s.store()));                            // slot still referenced
        for (int[] e : ids) idMap.put(e[0], dest);
        t = t.withKilled(slot);
        deadBytes += 8L + (long) ids.size() * entryBytes(dim);
        publish(new Snapshot(t, s.store()));
        merges.incrementAndGet();
        log.info("SPANN merge: slot {} ({} entries) -> {} (now {})", slot, ids.size(), dest, t.live(dest));
        if (t.live(dest) > cfg.postingMax()) {
            t = split(s, t, dest, false);
        }
        return t;
    }
```

And in `addOrUpdate`, after the `if (t.live(target) > cfg.postingMax()) { ... }` block, add:

```java
            if (old != null && old != target) {
                t = mergeIfUnderfull(snap, t, old);
            }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='SpannVectorIndexMergeTest,SpannVectorIndexSplitTest,SpannVectorIndexUpdateTest,SpannVectorIndexSearchTest' 2>&1 | grep -E "Tests run:|BUILD|FAIL"`
Expected: `Tests run: 19, Failures: 0, Errors: 0`, `BUILD SUCCESS`.

- [ ] **Step 5: Add to the PR gate and commit**

Add `<include>**/spann/SpannVectorIndexMergeTest.java</include>` after the `SpannVectorIndexSplitTest` include.

```bash
git add pom.xml src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexMergeTest.java
git commit -m "feat(spann): SPFresh merge of underfull postings with a single bounded split

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 9: Compaction and write-failure containment

**Files:**
- Modify: `src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexCompactionTest.java`
- Modify: `pom.xml` (add `**/spann/SpannVectorIndexCompactionTest.java`)

**Interfaces:**
- Produces: private `compactIfNeeded()`, called at the end of every successful `addOrUpdate`. Slot numbers are preserved across compaction (dead slots are re-added dead) so the id map needs no rewrite and readers' liveness tests stay true through the swap.

- [ ] **Step 1: Write the failing test**

```java
package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.ExactVectorIndex;
import com.recsys.infrastructure.vectordb.SearchResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannVectorIndexCompactionTest {

    @TempDir Path dir;

    private SpannConfig cfg() {
        return SpannConfig.defaults().withDir(dir).withPostingMax(16).withPostingMin(2)
                .withNprobe(1_000_000).withCompactRatio(0.3).withRegionBytes(1 << 20);
    }

    @Test
    void repeatedOverwrites_triggerCompactionAndPreserveEveryResult() throws Exception {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(60, 4, 3, 11L));
        try (SpannVectorIndex idx = new SpannVectorIndex(data, cfg())) {
            Path first = ((MappedPostingStore) storeOf(idx)).path();
            for (int round = 0; round < 20; round++) {
                float[] v = data.get(5).clone();
                v[0] += round * 1e-3f;
                idx.addOrUpdate(5, v);
                data.put(5, v);
            }
            SpannStats st = idx.stats();
            assertThat(st.compactions()).isGreaterThanOrEqualTo(1);
            assertThat(st.deadBytes()).isLessThan((long) (0.3 * st.fileBytes()) + 1);
            assertThat(Files.exists(first)).isFalse();                  // old file gone
            assertThat(Files.list(dir).count()).isEqualTo(1);           // exactly one live file
            ExactVectorIndex exact = new ExactVectorIndex(data);
            for (float[] q : SpannTestVectors.mixture(10, 4, 3, 12L)) {
                List<SearchResult> e = exact.search(q, 8, Set.of());
                assertThat(idx.search(q, 8, Set.of())).usingRecursiveFieldByFieldElementComparator().containsExactlyElementsOf(e);
            }
        }
    }

    @Test
    void writeFailure_leavesThePreviousSnapshotServing() {
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(40, 4, 2, 13L));
        AtomicBoolean fail = new AtomicBoolean(false);
        try (SpannVectorIndex idx = new SpannVectorIndex(data, cfg(), () -> new PostingStore() {
            private final MappedPostingStore real = new MappedPostingStore(dir, 1 << 20);
            @Override public long append(Block block) {
                if (fail.get()) throw new UncheckedIOException(new java.io.IOException("disk full"));
                return real.append(block);
            }
            @Override public Block read(long offset) { return real.read(offset); }
            @Override public long bytes() { return real.bytes(); }
            @Override public void close() { real.close(); }
        })) {
            float[] q = data.get(3);
            List<SearchResult> before = idx.search(q, 5, Set.of());
            SpannStats statsBefore = idx.stats();
            fail.set(true);
            assertThatThrownBy(() -> idx.addOrUpdate(99, new float[]{1f, 1f, 1f, 1f}))
                    .isInstanceOf(UncheckedIOException.class).hasMessageContaining("disk full");
            assertThat(idx.search(q, 5, Set.of())).usingRecursiveFieldByFieldElementComparator().containsExactlyElementsOf(before);
            assertThat(idx.stats().entriesLive()).isEqualTo(statsBefore.entriesLive());
            assertThat(idx.stats().fileBytes()).isEqualTo(statsBefore.fileBytes());
            fail.set(false);
            idx.addOrUpdate(99, new float[]{1f, 1f, 1f, 1f});          // recovers without a rebuild
            assertThat(idx.search(new float[]{1f, 1f, 1f, 1f}, 1, Set.of()).get(0).id()).isEqualTo(99);
        }
    }

    /** Test-only access to the live store, via the package-private snapshot accessor added in Step 3. */
    private static PostingStore storeOf(SpannVectorIndex idx) {
        return idx.snapshotForTests().store();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=SpannVectorIndexCompactionTest 2>&1 | grep -E "symbol:" | sort -u`
Expected: `symbol: method snapshotForTests()` (compile failure). After adding only that accessor, `repeatedOverwrites_*` must FAIL on `compactions() >= 1` before the compaction code exists — run it that way once to see the red for the right reason.

- [ ] **Step 3: Write minimal implementation**

Add the accessor and compaction to `SpannVectorIndex`:

```java
    /** Package-private, for tests that need the live store (e.g. to assert the file was swapped). */
    Snapshot snapshotForTests() {
        return snap;
    }

    // ---------------------------------------------------------------- compaction

    /** Rewrites live entries into a fresh store when dead bytes pass the ratio. Slot numbers survive. */
    private void compactIfNeeded() {
        Snapshot s = snap;
        long bytes = s.store().bytes();
        if (bytes == 0 || deadBytes <= cfg.compactRatio() * bytes) return;
        PostingStore fresh = storeFactory.get();
        CentroidTable t = s.table();
        CentroidTable nt = CentroidTable.empty();
        try {
            for (int slot = 0; slot < t.size(); slot++) {
                if (!t.alive(slot)) {
                    nt = nt.withAdded(t.centroid(slot), -1L, 0).withKilled(slot);
                    continue;
                }
                List<int[]> ids = new ArrayList<>();
                List<float[]> vecs = new ArrayList<>();
                collectLive(s, slot, Integer.MIN_VALUE, ids, vecs);
                long off = fresh.append(block(ids, vecs));
                nt = nt.withAdded(t.centroid(slot), off, ids.size());
            }
        } catch (RuntimeException e) {
            fresh.close();
            throw e;
        }
        long before = bytes, dead = deadBytes;
        publish(new Snapshot(nt, fresh));
        deadBytes = 0;
        s.store().close();                 // the old mapping stays valid for any reader still on it
        compactions.incrementAndGet();
        log.info("SPANN compaction: {} bytes ({} dead) -> {} bytes", before, dead, fresh.bytes());
    }
```

And at the end of `addOrUpdate`'s `try` block (after the merge call from Task 8), add `compactIfNeeded();`.

- [ ] **Step 4: Run test to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='com.recsys.infrastructure.vectordb.spann.*Test' 2>&1 | grep -E "Tests run:|BUILD|FAIL" | tail -3`
Expected: all spann tests pass (`Tests run: 40`-ish, `Failures: 0, Errors: 0`), `BUILD SUCCESS`.

- [ ] **Step 5: Add to the PR gate and commit**

Add `<include>**/spann/SpannVectorIndexCompactionTest.java</include>` after the `SpannVectorIndexMergeTest` include.

```bash
git add pom.xml src/main/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndex.java src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexCompactionTest.java
git commit -m "feat(spann): compaction into a fresh store; write failures leave the previous snapshot serving

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 10: Concurrent readers during updates

**Files:**
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexConcurrencyTest.java`
- Modify: `pom.xml` (add `**/spann/SpannVectorIndexConcurrencyTest.java`)

**Interfaces:**
- Consumes: everything from Tasks 5–9. Produces nothing new; this task pins the reader/writer contract. If it fails, the fix goes into `SpannVectorIndex` (the publish-then-flip ordering or the dedup), never into the test's tolerance.

- [ ] **Step 1: Write the test**

```java
package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.SearchResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Readers never block and never see a torn block, a duplicate id, an id that was never inserted,
 * or a score that matches neither the previous nor the current vector of that id.
 */
class SpannVectorIndexConcurrencyTest {

    @TempDir Path dir;

    @Test
    void readersSeeOnlyLiveConsistentEntriesWhileTheWriterSplitsMergesAndCompacts() throws Exception {
        SpannConfig cfg = SpannConfig.defaults().withDir(dir).withPostingMax(8).withPostingMin(2)
                .withNprobe(1_000_000).withCompactRatio(0.4).withRegionBytes(1 << 20);
        List<float[]> base = SpannTestVectors.mixture(200, 8, 4, 21L);
        Map<Integer, float[]> data = SpannTestVectors.asMap(base);
        // Every vector an id has ever had, so a reader's score can be checked against all of them.
        ConcurrentHashMap<Integer, List<float[]>> history = new ConcurrentHashMap<>();
        data.forEach((id, v) -> history.put(id, new java.util.concurrent.CopyOnWriteArrayList<>(List.of(v))));

        try (SpannVectorIndex idx = new SpannVectorIndex(data, cfg)) {
            List<float[]> queries = SpannTestVectors.mixture(16, 8, 4, 22L);
            AtomicBoolean stop = new AtomicBoolean(false);
            AtomicLong searches = new AtomicLong();
            List<String> violations = new java.util.concurrent.CopyOnWriteArrayList<>();
            ExecutorService pool = Executors.newFixedThreadPool(5);
            List<Future<?>> readers = new ArrayList<>();
            for (int r = 0; r < 4; r++) {
                readers.add(pool.submit(() -> {
                    int qi = 0;
                    while (!stop.get()) {
                        float[] q = queries.get(qi++ % queries.size());
                        List<SearchResult> hits = idx.search(q, 20, Set.of());
                        searches.incrementAndGet();
                        Set<Integer> seen = new HashSet<>();
                        double prev = Double.POSITIVE_INFINITY;
                        for (SearchResult h : hits) {
                            if (!seen.add(h.id())) violations.add("duplicate id " + h.id());
                            if (h.score() > prev) violations.add("unsorted at id " + h.id());
                            prev = h.score();
                            List<float[]> versions = history.get(h.id());
                            if (versions == null) { violations.add("unknown id " + h.id()); continue; }
                            boolean matches = false;
                            for (float[] v : versions) {
                                if (Math.abs(com.recsys.infrastructure.vectordb.VectorMath.innerProduct(q, v) - h.score()) < 1e-4) { matches = true; break; }
                            }
                            if (!matches) violations.add("score of id " + h.id() + " matches no version");
                        }
                    }
                }));
            }
            Future<?> writer = pool.submit(() -> {
                java.util.Random rng = new java.util.Random(23);
                int nextId = 1000;
                for (int i = 0; i < 600; i++) {
                    float[] v = base.get(rng.nextInt(base.size())).clone();
                    v[0] += rng.nextFloat();
                    int id;
                    if (rng.nextInt(3) == 0) {                       // overwrite an existing id
                        id = rng.nextInt(200);
                        history.get(id).add(v);
                    } else {                                          // insert a new id
                        id = nextId++;
                        history.put(id, new java.util.concurrent.CopyOnWriteArrayList<>(List.of(v)));
                    }
                    idx.addOrUpdate(id, v);
                }
            });
            writer.get(60, TimeUnit.SECONDS);
            stop.set(true);
            for (Future<?> f : readers) f.get(10, TimeUnit.SECONDS);
            pool.shutdownNow();

            assertThat(violations).isEmpty();
            assertThat(searches.get()).isGreaterThan(100);
            SpannStats st = idx.stats();
            assertThat(st.splits()).isGreaterThan(0);
            assertThat(st.compactions()).isGreaterThan(0);
            assertThat(st.entriesLive()).isEqualTo(history.size());
        }
    }
}
```

- [ ] **Step 2: Run the test and confirm it passes for the right reason**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=SpannVectorIndexConcurrencyTest 2>&1 | grep -E "Tests run:|BUILD|violations|Expecting"`
Expected: `Tests run: 1, Failures: 0`. Then prove the test can fail: temporarily swap the two lines `publish(new Snapshot(t, s.store()));` / `idMap.put(id, target);` in `addOrUpdate` (flip before publish), re-run, and expect `violations` to contain `unknown id` or `score ... matches no version` entries. Restore the order, re-run green. Do not commit the swapped order.

- [ ] **Step 3: Add to the PR gate and commit**

Add `<include>**/spann/SpannVectorIndexConcurrencyTest.java</include>` after the `SpannVectorIndexCompactionTest` include.

```bash
git add pom.xml src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexConcurrencyTest.java
git commit -m "test(spann): readers stay consistent through concurrent split, merge and compaction

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 11: Lifecycle and wiring — `VectorIndex.close()`, `CandidateGenerator`, `RecSysServer`

**Files:**
- Modify: `src/main/java/com/recsys/infrastructure/vectordb/VectorIndex.java`
- Modify: `src/main/java/com/recsys/infrastructure/vectordb/CandidateGenerator.java` (the `createEmbeddingIndex` switch at the bottom of the file, and a new `close()` method)
- Modify: `src/main/java/com/recsys/api/serving/RecSysServer.java` (the `recsys-shutdown` hook)
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexLifecycleTest.java`
- Test: `src/test/java/com/recsys/infrastructure/vectordb/CandidateGeneratorSpannWiringTest.java`
- Modify: `pom.xml` (add both)

**Interfaces:**
- Produces: `VectorIndex.close()` default no-op; `CandidateGenerator.close()`; backend name `spann` in `createEmbeddingIndex`.

- [ ] **Step 1: Write the failing tests**

`SpannVectorIndexLifecycleTest.java`:

```java
package com.recsys.infrastructure.vectordb.spann;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannVectorIndexLifecycleTest {

    @TempDir Path dir;

    @Test
    void fileExistsWhileOpen_andIsGoneAfterClose() throws Exception {
        SpannConfig cfg = SpannConfig.defaults().withDir(dir).withRegionBytes(1 << 20);
        SpannVectorIndex idx = new SpannVectorIndex(Map.of(1, new float[]{1f, 0f}, 2, new float[]{0f, 1f}), cfg);
        assertThat(Files.list(dir).count()).isEqualTo(1);
        idx.close();
        assertThat(Files.list(dir).count()).isZero();
        assertThat(idx.search(new float[]{1f, 0f}, 1, Set.of())).isEmpty();
        assertThatThrownBy(() -> idx.addOrUpdate(3, new float[]{1f, 1f})).isInstanceOf(IllegalStateException.class);
        idx.close(); // idempotent
    }

    @Test
    void closeViaTheVectorIndexInterface_isTheSameClose() throws Exception {
        SpannConfig cfg = SpannConfig.defaults().withDir(dir).withRegionBytes(1 << 20);
        com.recsys.infrastructure.vectordb.VectorIndex idx = new SpannVectorIndex(Map.of(1, new float[]{1f}), cfg);
        idx.close();
        assertThat(Files.list(dir).count()).isZero();
    }
}
```

`CandidateGeneratorSpannWiringTest.java` (package `com.recsys.infrastructure.vectordb`):

```java
package com.recsys.infrastructure.vectordb;

import com.recsys.infrastructure.dataloading.DataManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class CandidateGeneratorSpannWiringTest {

    @TempDir Path dir;

    @AfterEach
    void clearProperty() {
        System.clearProperty("recsys.vector.backend");
    }

    @Test
    void backendPropertySpann_buildsASpannIndexAndCloseRemovesItsFile() throws Exception {
        System.setProperty("recsys.vector.backend", "spann");
        CandidateGenerator gen = new CandidateGenerator(mock(DataManager.class), null, dir);
        assertThat(gen.embeddingBackendName()).isEqualTo("spann");
        assertThat(Files.list(dir).count()).isEqualTo(1);
        gen.close();
        assertThat(Files.list(dir).count()).isZero();
    }

    @Test
    void defaultBackend_isStillLsh_andCloseIsANoOp() {
        CandidateGenerator gen = new CandidateGenerator(mock(DataManager.class));
        assertThat(gen.embeddingBackendName()).isEqualTo("lsh");
        gen.close();
    }
}
```

This needs two small additions to `CandidateGenerator`: a package-private three-arg constructor `CandidateGenerator(DataManager, EmbeddingStore, Path spannDir)` that overrides only the SPANN directory (so the test never writes to the real temp dir), and `String embeddingBackendName()` returning `embeddingIndex.name()`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest='SpannVectorIndexLifecycleTest,CandidateGeneratorSpannWiringTest' 2>&1 | grep -E "symbol:" | sort -u`
Expected: `symbol: method close()` (on `VectorIndex`), `symbol: method embeddingBackendName()`, and a constructor mismatch for `CandidateGenerator(DataManager, null, Path)`.

- [ ] **Step 3: Write minimal implementation**

`VectorIndex.java` — add after `addOrUpdate`:

```java
    /** Releases any off-heap or file-system resources. Default: nothing to release. */
    default void close() {}
```

`CandidateGenerator.java` — three edits.

(a) Imports: add `import com.recsys.infrastructure.vectordb.spann.SpannConfig;`, `import com.recsys.infrastructure.vectordb.spann.SpannVectorIndex;`, `import java.nio.file.Path;`.

(b) Replace the two-arg constructor body's `this.embeddingIndex = createEmbeddingIndex(movieEmbeddings);` with `this.embeddingIndex = createEmbeddingIndex(movieEmbeddings, null);` and add, directly below the two-arg constructor:

```java
    /** Test seam: same as the two-arg constructor but pins the SPANN index directory. */
    CandidateGenerator(DataManager dataManager, EmbeddingStore userEmbeddingStore, Path spannDir) {
        this.dataManager = dataManager;
        this.movieEmbeddings = DataLoader.loadMovieEmbeddings();
        this.userEmbeddingStore = userEmbeddingStore;
        this.embeddingIndex = createEmbeddingIndex(movieEmbeddings, spannDir);
        this.embeddingDim = movieEmbeddings.isEmpty()
                ? 0
                : movieEmbeddings.values().iterator().next().length;
        log.info("Embedding backend={}, movies={}, userStore={}",
                embeddingIndex.name(), movieEmbeddings.size(),
                userEmbeddingStore != null ? "cache+redis" : "classpath");
    }

    /** Name of the vector backend in use ({@code exact}, {@code lsh}, {@code spann}). */
    public String embeddingBackendName() {
        return embeddingIndex.name();
    }

    /** Releases the vector index's resources (the SPANN index file); a no-op for heap backends. */
    public void close() {
        embeddingIndex.close();
    }
```

(c) Replace `createEmbeddingIndex`:

```java
    private static VectorIndex createEmbeddingIndex(Map<Integer, float[]> embeddings, Path spannDir) {
        if (embeddings.isEmpty()) return new ExactVectorIndex(Map.of());

        String backend = System.getProperty("recsys.vector.backend");
        if (backend == null || backend.isBlank()) {
            backend = System.getenv().getOrDefault("RECSYS_VECTOR_BACKEND", "lsh");
        }

        return switch (backend.trim().toLowerCase()) {
            case "exact", "flat" -> new ExactVectorIndex(embeddings);
            case "lsh", "ann" -> new LshVectorIndex(embeddings);
            case "spann" -> {
                SpannConfig cfg = SpannConfig.fromEnv(System::getenv);
                if (spannDir != null) cfg = cfg.withDir(spannDir);
                yield new SpannVectorIndex(embeddings, cfg);
            }
            case "faiss" -> {
                log.warn("FAISS backend requested but not enabled in the portable build; falling back to LSH.");
                yield new LshVectorIndex(embeddings);
            }
            default -> throw new IllegalArgumentException("Unknown vector backend: " + backend);
        };
    }
```

`RecSysServer.java` — in the `recsys-shutdown` hook, immediately after the line `GracefulExecutors.shutdownGracefully(executor);`, add:

```java
                try {
                    candidateGenerator.close();
                } catch (RuntimeException e) {
                    log.warn("vector index close failed during shutdown", e);
                }
```

(`candidateGenerator` is the local assigned once earlier in `main`, so the lambda can capture it.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='SpannVectorIndexLifecycleTest,CandidateGeneratorSpannWiringTest,CandidateGeneratorDimensionTest,com.recsys.infrastructure.vectordb.*Test' 2>&1 | grep -E "Tests run:|BUILD|FAIL" | tail -4`
Expected: all pass, `BUILD SUCCESS`. Then compile the server: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q compile` → no output.

- [ ] **Step 5: Add to the PR gate and commit**

Add `<include>**/spann/SpannVectorIndexLifecycleTest.java</include>` and `<include>**/vectordb/CandidateGeneratorSpannWiringTest.java</include>` after the `SpannVectorIndexConcurrencyTest` include.

```bash
git add pom.xml src/main/java/com/recsys/infrastructure/vectordb/VectorIndex.java src/main/java/com/recsys/infrastructure/vectordb/CandidateGenerator.java src/main/java/com/recsys/api/serving/RecSysServer.java src/test/java/com/recsys/infrastructure/vectordb/spann/SpannVectorIndexLifecycleTest.java src/test/java/com/recsys/infrastructure/vectordb/CandidateGeneratorSpannWiringTest.java
git commit -m "feat(spann): select with RECSYS_VECTOR_BACKEND=spann; close the index file on shutdown

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 12: Benchmark (`@Tag("load")`)

**Files:**
- Test: `src/test/java/com/recsys/infrastructure/vectordb/spann/SpannBenchmarkLoadTest.java`

**Interfaces:**
- Consumes: `SpannVectorIndex`, `ExactVectorIndex`, `LshVectorIndex`, `SpannTestVectors`. Not added to the resilience profile (it is excluded by the default `excludedGroups=load,docker`).

Retained-heap measurement: the source map is dropped before measuring, so the exact backend's number includes the `float[]`s it shares with the map (its real footprint), and SPANN's includes only centroids plus the id map. Each phase regenerates the data from the same seed.

- [ ] **Step 1: Write the benchmark**

```java
package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.ExactVectorIndex;
import com.recsys.infrastructure.vectordb.LshVectorIndex;
import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorIndex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Opt-in: {@code mvn test -DexcludedGroups="" -Dgroups=load -Dtest=SpannBenchmarkLoadTest}.
 * Asserts the acceptance bar from the spec on synthetic data; prints all three backends.
 * Wall-clock is printed but never asserted.
 */
@Tag("load")
class SpannBenchmarkLoadTest {

    private static final int N = 200_000, DIM = 64, COMPONENTS = 64, QUERIES = 1_000, K = 10;
    private static final long DATA_SEED = 101L, QUERY_SEED = 102L;

    @TempDir Path dir;

    record Row(String name, long retainedHeapBytes, double recallAt10, double distancesPerQuery, long buildMs, long queryMs) {}

    @Test
    void spannMeetsTheAcceptanceBar() throws Exception {
        List<float[]> queries = SpannTestVectors.mixture(QUERIES, DIM, COMPONENTS, QUERY_SEED);
        List<Set<Integer>> truth = groundTruth(queries);

        Row exact = measure("exact", ExactVectorIndex::new, queries, truth, null);
        Row lsh = measure("lsh", LshVectorIndex::new, queries, truth, null);
        SpannConfig cfg = SpannConfig.defaults().withDir(dir);
        Row spann = measure("spann", m -> new SpannVectorIndex(m, cfg), queries, truth, SpannVectorIndex.class);

        System.out.printf("%n%-6s %14s %10s %16s %9s %9s%n", "index", "retainedHeapMB", "recall@10", "distances/query", "buildMs", "queryMs");
        for (Row r : List.of(exact, lsh, spann)) {
            System.out.printf("%-6s %14.1f %10.3f %16.0f %9d %9d%n", r.name(), r.retainedHeapBytes() / 1048576.0,
                    r.recallAt10(), r.distancesPerQuery(), r.buildMs(), r.queryMs());
        }
        System.out.printf("machine: %s, JVM %s, heap max %d MB%n", System.getProperty("os.arch"),
                System.getProperty("java.version"), Runtime.getRuntime().maxMemory() / 1048576);

        assertThat(spann.retainedHeapBytes()).isLessThanOrEqualTo((long) (0.25 * exact.retainedHeapBytes()));
        assertThat(spann.recallAt10()).isGreaterThanOrEqualTo(0.90);
        assertThat(spann.distancesPerQuery()).isLessThan(N);
    }

    private List<Set<Integer>> groundTruth(List<float[]> queries) {
        ExactVectorIndex exact = new ExactVectorIndex(SpannTestVectors.asMap(SpannTestVectors.mixture(N, DIM, COMPONENTS, DATA_SEED)));
        List<Set<Integer>> truth = new ArrayList<>(queries.size());
        for (float[] q : queries) {
            Set<Integer> ids = new HashSet<>();
            for (SearchResult r : exact.search(q, K, Set.of())) ids.add(r.id());
            truth.add(ids);
        }
        return truth;
    }

    private Row measure(String name, Function<Map<Integer, float[]>, VectorIndex> factory, List<float[]> queries,
                        List<Set<Integer>> truth, Class<?> statsClass) throws Exception {
        long baseline = usedHeapAfterGc();
        Map<Integer, float[]> data = SpannTestVectors.asMap(SpannTestVectors.mixture(N, DIM, COMPONENTS, DATA_SEED));
        long t0 = System.nanoTime();
        VectorIndex idx = factory.apply(data);
        long buildMs = (System.nanoTime() - t0) / 1_000_000;
        data = null;                                                   // only the index stays reachable
        long retained = usedHeapAfterGc() - baseline;

        long before = idx instanceof SpannVectorIndex s ? s.stats().distanceComputations() : 0;
        int hits = 0;
        long q0 = System.nanoTime();
        for (int i = 0; i < queries.size(); i++) {
            for (SearchResult r : idx.search(queries.get(i), K, Set.of())) if (truth.get(i).contains(r.id())) hits++;
        }
        long queryMs = (System.nanoTime() - q0) / 1_000_000;
        double distances = idx instanceof SpannVectorIndex s
                ? (s.stats().distanceComputations() - before) / (double) queries.size()
                : (name.equals("exact") ? N : Double.NaN);
        double recall = hits / (double) (queries.size() * K);
        idx.close();
        return new Row(name, retained, recall, distances, buildMs, queryMs);
    }

    private static long usedHeapAfterGc() throws InterruptedException {
        MemoryMXBean mx = ManagementFactory.getMemoryMXBean();
        long last = Long.MAX_VALUE;
        for (int i = 0; i < 5; i++) {                                  // poll until the reading settles
            System.gc();
            Thread.sleep(100);
            long used = mx.getHeapMemoryUsage().getUsed();
            if (Math.abs(used - last) < 1_000_000) return used;
            last = used;
        }
        return last;
    }
}
```

- [ ] **Step 2: Run it**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -DexcludedGroups="" -Dgroups=load -Dtest=SpannBenchmarkLoadTest 2>&1 | grep -E "^(index|exact|lsh|spann|machine)|Tests run:|BUILD|Expecting"`
Expected: the table, then `Tests run: 1, Failures: 0`. Build of SPANN at this scale takes on the order of a minute (sample k-means with ~3k centroids, then assignment of 200k); the surefire `-Xshare:off` JVM args already apply. If the heap assertion fails, look at the id map first (a boxed `ConcurrentHashMap<Integer,Integer>` is the dominant term); if recall fails, raise nothing — check that centroids are ordered by inner product descending and the widening loop, then report the number.

Record the printed rows: they go into the doc in Task 13.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/recsys/infrastructure/vectordb/spann/SpannBenchmarkLoadTest.java
git commit -m "test(spann): opt-in benchmark asserting heap, recall and distance-computation bar

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

### Task 13: Documentation and spec amendment

**Files:**
- Modify: `docs/system_design/13_DB_Indexing.md` (§5, after the "Multi-vector scoring: Sum of MaxSim" subsection and before `## 6. Testing the indexes`)

- [ ] **Step 1: Confirm the spec matches the implementation**

`grep -c "keep their slot for the life of the index" docs/superpowers/specs/2026-09-13-spann-spfresh-vector-index-design.md` → `1` (the slot-numbering rule) and `grep -c PRUNE_EPSILON` on the spec → `0`. Both were amended during planning; nothing to edit here.

- [ ] **Step 2: Write the doc subsection**

Insert before `## 6. Testing the indexes` in `13_DB_Indexing.md`:

```markdown
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
then flips the map: a reader sees the old entry until the flip and the new one after, never
neither and — because search dedups by id — never both. That publish-then-flip order is the
whole concurrency story; readers never block and pin one snapshot (table + store) per search.

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
(16; must be below half of max or merge and split would oscillate), `RECSYS_SPANN_NPROBE` (8),
`RECSYS_SPANN_REASSIGN_PROBE` (4),
`RECSYS_SPANN_COMPACT_RATIO` (0.5), `RECSYS_SPANN_SEED` (42). In k8s an `emptyDir` is enough
for the directory; note that a mapped file's pages are page cache, which the cgroup charges but
can reclaim — a soft cost, unlike heap.

**Measured envelope (synthetic; the classpath corpus is 12 six-dimensional vectors and can
show nothing).** `SpannBenchmarkLoadTest` (`@Tag("load")`, run with
`mvn test -DexcludedGroups="" -Dgroups=load -Dtest=SpannBenchmarkLoadTest`) builds exact, LSH
and SPANN over 200 000 × 64-dim Gaussian-mixture vectors and asserts, for SPANN only: retained
heap ≤ 25 % of exact, recall@10 ≥ 0.90 against exact at the default `nprobe`, and fewer
distance computations per query than exact's 200 000. Numbers from <DATE>, <MACHINE>:

| index | retained heap MB | recall@10 | distances/query | build ms | query ms (1 000) |
|---|---|---|---|---|---|
| exact | <..> | 1.000 | 200 000 | <..> | <..> |
| lsh | <..> | <..> | — | <..> | <..> |
| spann | <..> | <..> | <..> | <..> | <..> |

**What deliberately does not exist.** Persistence across restarts (rebuild on boot, delete on
shutdown); a graph or tree over centroids — at ~3k centroids a brute-force scan is ~200k
multiply-adds and the scan becomes the bottleneck only around 10M+ vectors; multi-vector
postings; native or SIMD kernels; a delete API; Prometheus metrics (the `stats()` snapshot is
the seam). The default backend stays `lsh` and no manifest sets `spann`.
```

Then replace the `<DATE>`, `<MACHINE>` and every `<..>` with the rows Task 12 printed.

- [ ] **Step 3: Verify the doc index test and commit**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=DocumentationIndexTest 2>&1 | grep -E "Tests run:|BUILD"`
Expected: pass (no new top-level doc was added). Confirm no placeholder remains: `grep -c "<\.\.>\|<DATE>\|<MACHINE>" docs/system_design/13_DB_Indexing.md` → `0`.

```bash
git add docs/system_design/13_DB_Indexing.md
git commit -m "docs(indexing): SPANN/SPFresh backend — layout, liveness rule, bounded rebalancing, measured envelope

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
```

---

## Final verification before the PR

- [ ] `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Presilience 2>&1 | grep -E "Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+, Skipped: [0-9]+$|BUILD"` — the whole PR gate, green.
- [ ] `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='com.recsys.infrastructure.vectordb.**.*Test,com.recsys.api.serving.*Test,DocumentationIndexTest'` — green.
- [ ] Live: start 6010 with `RECSYS_VECTOR_BACKEND=spann` (plus `REDIS_ALLOW_NO_AUTH=true`, `RECOMMENDATION_CURSOR_SIGNING_KEY` ≥ 32 bytes, an ephemeral `redis-server --port 6399 --save "" --appendonly no`), confirm the startup log says `Embedding backend=spann`, `GET /getrecommendation?userId=123` returns the same ids as with the default backend for the 12-movie corpus, `ls $TMPDIR/spann-*.bin` shows one file while up and none after `kill <pid>`.
- [ ] Open the PR against `main` (never merge to main directly); the body carries the benchmark table and the live-check results.
