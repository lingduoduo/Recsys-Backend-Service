# ONNX Thread Config Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every ONNX session in the model service read one thread configuration, and make that configuration readable from the environment whether or not Spring built it.

**Architecture:** `ModelServingProperties.Onnx` stays the single value type. Two things are added beside it: a `fromEnvironment(EnvReader)` factory that populates it from `RECSYS_MODEL_ONNX_*` through the existing validating setters, and an `OnnxSessionOptions.apply(SessionOptions, Onnx)` helper holding the three ONNX Runtime setter calls. Both session-creation sites call `apply`; both Spring-less construction sites call `fromEnvironment`.

**Tech Stack:** Java 17, Spring Boot (`@ConfigurationProperties`), ONNX Runtime Java API (`ai.onnxruntime`), JUnit 5, AssertJ, Mockito, Maven Surefire profiles.

**Spec:** `docs/superpowers/specs/2026-09-20-onnx-thread-config-unification-design.md`

## Global Constraints

- **JDK 17.** Every Maven command in this plan must be prefixed `JAVA_HOME=$(/usr/libexec/java_home -v 17)`. On JDK 25 a clean compile of `LlmResponseCache.java` / `RecommendationCache.java` fails for unrelated pre-existing reasons.
- **Defaults do not change value.** `intraOpThreads=1`, `interOpThreads=1`, `executionMode=SEQUENTIAL` everywhere, before and after.
- **Env var names, exactly:** `RECSYS_MODEL_ONNX_INTRA_OP_THREADS`, `RECSYS_MODEL_ONNX_INTER_OP_THREADS`, `RECSYS_MODEL_ONNX_EXECUTION_MODE`. These are already set in `k8s/base/model-serving.yaml` and pinned by `ModelServingManifestTest`; do not rename them.
- **Parse with `EnvVars`, never `EnvConfig`.** `EnvVars.readInt` throws on an unparseable value; `EnvConfig.readInt` silently returns the default. Fail-fast is required so the env path matches the Spring binding path.
- **`OptLevel` is out of scope.** `DeepLearningPredictionService` keeps `ALL_OPT`; `OrtSessionHandle` keeps the ONNX default. Do not unify it.
- **`recsys.model.recall.*` is out of scope.** Do not extend `fromEnvironment` to the `Recall` block, even though the same defect exists there.
- **Never commit `.claude/CLAUDE.md`.** Documentation notes go in `docs/runbooks/model-artifact-rollout.md`.
- **Commit trailer** on every commit:
  `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `src/main/java/com/recsys/config/ModelServingProperties.java` | Add `Onnx.applyEnvironment` + `Onnx.fromEnvironment` + `ModelServingProperties.fromEnvironment` | 1 |
| `src/test/java/com/recsys/config/ModelServingPropertiesTest.java` | Env parsing, defaults, fail-fast | 1 |
| `src/main/java/com/recsys/application/model/OnnxSessionOptions.java` | **New.** The only place the three ORT setters are called | 2 |
| `src/main/java/com/recsys/application/retrieval/OrtSessionHandle.java` | Delegate to `OnnxSessionOptions`; drop local `toOrt` | 2 |
| `src/main/java/com/recsys/retrieval/service/DeepLearningPredictionService.java` | Inject config, add opener seam, collapse the duplicated options block | 3 |
| `src/test/java/com/recsys/retrieval/service/DeepLearningPredictionOnnxConfigTest.java` | **New.** Pins that the mlp session is configured, on construct and on reload | 3 |
| `src/main/java/com/recsys/application/retrieval/UserTowerInferenceService.java:88` | Env fallback in the legacy constructor | 4 |
| `src/main/java/com/recsys/application/model/ModelRuntimeProvider.java:98` | Env fallback in the non-Spring constructor | 4 |
| `src/test/java/com/recsys/application/model/SpringlessOnnxConfigTest.java` | **New.** Pins that both Spring-less constructors read the environment | 4 |
| `pom.xml` (`resilience` profile) | Gate `ModelServingPropertiesTest` on the PR check | 5 |
| `k8s/base/model-serving.yaml` | Correct the comment that claims the whole pod is pinned | 5 |
| `docs/runbooks/model-artifact-rollout.md` | Record the unified behaviour | 5 |

---

### Task 1: Environment factory on `ModelServingProperties`

**Files:**
- Modify: `src/main/java/com/recsys/config/ModelServingProperties.java`
- Test: `src/test/java/com/recsys/config/ModelServingPropertiesTest.java`

**Interfaces:**
- Consumes: `com.recsys.config.EnvVars.EnvReader` (`String get(String name)`), `EnvVars.readInt(EnvReader, String, int)`.
- Produces:
  - `ModelServingProperties.Onnx.fromEnvironment(EnvVars.EnvReader env)` → `Onnx`
  - `ModelServingProperties.Onnx.fromEnvironment()` → `Onnx` (reads `System::getenv`)
  - `ModelServingProperties.fromEnvironment(EnvVars.EnvReader env)` → `ModelServingProperties`
  - `ModelServingProperties.fromEnvironment()` → `ModelServingProperties`
  - Public constants `Onnx.INTRA_OP_THREADS_ENV`, `Onnx.INTER_OP_THREADS_ENV`, `Onnx.EXECUTION_MODE_ENV`

- [ ] **Step 1: Write the failing tests**

Append to `src/test/java/com/recsys/config/ModelServingPropertiesTest.java` (and add the imports `java.util.Map` and `static com.recsys.config.ModelServingProperties.ExecutionMode.PARALLEL`):

```java
    @Test
    void readsOnnxSettingsFromTheEnvironment() {
        ModelServingProperties.Onnx onnx = ModelServingProperties.Onnx.fromEnvironment(Map.of(
                "RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "3",
                "RECSYS_MODEL_ONNX_INTER_OP_THREADS", "4",
                "RECSYS_MODEL_ONNX_EXECUTION_MODE", "PARALLEL")::get);

        assertThat(onnx.getIntraOpThreads()).isEqualTo(3);
        assertThat(onnx.getInterOpThreads()).isEqualTo(4);
        assertThat(onnx.getExecutionMode()).isEqualTo(PARALLEL);
    }

    @Test
    void environmentFallbackMatchesTheHardCodedDefaults() {
        ModelServingProperties.Onnx fromEnv = ModelServingProperties.Onnx.fromEnvironment(name -> null);
        ModelServingProperties.Onnx hardCoded = new ModelServingProperties.Onnx();

        assertThat(fromEnv.getIntraOpThreads()).isEqualTo(hardCoded.getIntraOpThreads());
        assertThat(fromEnv.getInterOpThreads()).isEqualTo(hardCoded.getInterOpThreads());
        assertThat(fromEnv.getExecutionMode()).isEqualTo(hardCoded.getExecutionMode());
    }

    @Test
    void blankEnvironmentValuesFallBackRatherThanFailing() {
        ModelServingProperties.Onnx onnx = ModelServingProperties.Onnx.fromEnvironment(Map.of(
                "RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "  ",
                "RECSYS_MODEL_ONNX_EXECUTION_MODE", "")::get);

        assertThat(onnx.getIntraOpThreads()).isEqualTo(1);
        assertThat(onnx.getExecutionMode()).isEqualTo(SEQUENTIAL);
    }

    @Test
    void executionModeIsCaseInsensitiveAndTrimmed() {
        ModelServingProperties.Onnx onnx = ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_EXECUTION_MODE", " parallel ")::get);

        assertThat(onnx.getExecutionMode()).isEqualTo(PARALLEL);
    }

    // The Spring path fails context startup on each of these: ${...} bound to a @Positive
    // setter rejects 0 and -1, and relaxed binding rejects an unknown enum constant. The env
    // path has to fail too, or the same variable means two different things depending on who
    // read it -- which is the whole defect this change exists to close.
    @Test
    void invalidEnvironmentValuesFailFastRatherThanSilentlyDefaulting() {
        assertThatThrownBy(() -> ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "abc")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_ONNX_INTRA_OP_THREADS");

        assertThatThrownBy(() -> ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "0")::get))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_INTER_OP_THREADS", "-1")::get))
                .isInstanceOf(IllegalArgumentException.class);

        assertThatThrownBy(() -> ModelServingProperties.Onnx.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_EXECUTION_MODE", "TURBO")::get))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("RECSYS_MODEL_ONNX_EXECUTION_MODE");
    }

    @Test
    void wholePropertiesFactoryCarriesTheOnnxEnvironment() {
        ModelServingProperties properties = ModelServingProperties.fromEnvironment(
                Map.of("RECSYS_MODEL_ONNX_INTRA_OP_THREADS", "6")::get);

        assertThat(properties.getOnnx().getIntraOpThreads()).isEqualTo(6);
        // Recall is deliberately NOT environment-sourced here -- see the spec's non-goals.
        assertThat(properties.getRecall().getQueueCapacity()).isEqualTo(256);
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=ModelServingPropertiesTest -DfailIfNoTests=false
```

Expected: COMPILATION FAILURE — `cannot find symbol: method fromEnvironment(...)`.

- [ ] **Step 3: Implement**

In `ModelServingProperties.java`, add `import java.util.Locale;` at the top. Inside `public static class Onnx`, above the existing fields, add the constants; below the existing accessors, add the factories:

```java
        public static final String INTRA_OP_THREADS_ENV = "RECSYS_MODEL_ONNX_INTRA_OP_THREADS";
        public static final String INTER_OP_THREADS_ENV = "RECSYS_MODEL_ONNX_INTER_OP_THREADS";
        public static final String EXECUTION_MODE_ENV = "RECSYS_MODEL_ONNX_EXECUTION_MODE";
```

```java
        /**
         * The same three settings {@code application.yml} exposes as {@code ${RECSYS_MODEL_ONNX_*}},
         * read directly. Spring is not the only thing that builds this class: the constructors at
         * {@code UserTowerInferenceService} and {@code ModelRuntimeProvider} that take no properties
         * object would otherwise serve the hard-coded field initializers and silently disagree with
         * a deployment that overrode the variables.
         *
         * <p>Parsing goes through {@link EnvVars}, which throws on an unparseable value, rather than
         * {@link EnvConfig}, which returns the default. Both paths must reject the same input: the
         * Spring path already fails context startup on a non-numeric or non-positive value, and a
         * variable that means one thing when Spring reads it and another when this does is the
         * defect, not the fix.
         */
        public static Onnx fromEnvironment() {
            return fromEnvironment(System::getenv);
        }

        public static Onnx fromEnvironment(EnvVars.EnvReader env) {
            Onnx onnx = new Onnx();
            onnx.applyEnvironment(env);
            return onnx;
        }

        /** Routes through the setters so {@code @Positive} validation covers env-sourced values too. */
        private void applyEnvironment(EnvVars.EnvReader env) {
            setIntraOpThreads(EnvVars.readInt(env, INTRA_OP_THREADS_ENV, intraOpThreads));
            setInterOpThreads(EnvVars.readInt(env, INTER_OP_THREADS_ENV, interOpThreads));
            setExecutionMode(readExecutionMode(env, executionMode));
        }

        private static ExecutionMode readExecutionMode(EnvVars.EnvReader env, ExecutionMode defaultMode) {
            String raw = env.get(EXECUTION_MODE_ENV);
            if (raw == null || raw.isBlank()) {
                return defaultMode;
            }
            try {
                return ExecutionMode.valueOf(raw.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException("env var " + EXECUTION_MODE_ENV
                        + " is not a valid execution mode: " + raw);
            }
        }
```

At the class level of `ModelServingProperties` (after `getRecall()`), add:

```java
    /**
     * A properties object for the code paths Spring did not build. Only the ONNX block is
     * environment-sourced; {@code recsys.model.recall.*} keeps its hard-coded defaults here,
     * deliberately — see the design doc's non-goals.
     */
    public static ModelServingProperties fromEnvironment() {
        return fromEnvironment(System::getenv);
    }

    public static ModelServingProperties fromEnvironment(EnvVars.EnvReader env) {
        ModelServingProperties properties = new ModelServingProperties();
        properties.onnx.applyEnvironment(env);
        return properties;
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=ModelServingPropertiesTest -DfailIfNoTests=false
```

Expected: PASS, 8 tests.

- [ ] **Step 5: Confirm the Spring binding still works**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=ConfigurationBindingTest -DfailIfNoTests=false
```

Expected: PASS. The new factory must not have disturbed `@ConfigurationProperties` binding.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/recsys/config/ModelServingProperties.java \
        src/test/java/com/recsys/config/ModelServingPropertiesTest.java
git commit -m "feat(config): read RECSYS_MODEL_ONNX_* outside the Spring binding

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: `OnnxSessionOptions` — one place that configures a session

**Files:**
- Create: `src/main/java/com/recsys/application/model/OnnxSessionOptions.java`
- Modify: `src/main/java/com/recsys/application/retrieval/OrtSessionHandle.java`

**Interfaces:**
- Consumes: `ModelServingProperties.Onnx` (Task 1 leaves its accessors unchanged).
- Produces: `public static void OnnxSessionOptions.apply(OrtSession.SessionOptions options, ModelServingProperties.Onnx onnx) throws OrtException`

- [ ] **Step 1: Create the helper**

`src/main/java/com/recsys/application/model/OnnxSessionOptions.java`:

```java
package com.recsys.application.model;

import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.recsys.config.ModelServingProperties;

/**
 * The single place ONNX Runtime session threading is configured.
 *
 * <p>It exists because there are two session-creation sites in the model service JVM — the
 * two-tower {@code OrtSessionHandle} and the {@code mlp_embedding} session in
 * {@code DeepLearningPredictionService} — and for most of this project's life only the first
 * one applied any of these settings. The second ran on ONNX Runtime's own default, which is
 * derived from the visible CPU count: measured on an 8-core host it opened an intra-op pool of
 * 4 (three extra native threads per session) where the configured sessions beside it opened
 * none. Those pools are native, so no JVM thread metric could report the difference and
 * nothing noticed.
 *
 * <p>Takes the {@code SessionOptions} rather than returning one on purpose. It holds native
 * memory and must be closed by whoever opened it; handing a fresh one back across a package
 * boundary is how that ownership gets lost.
 */
public final class OnnxSessionOptions {

    private OnnxSessionOptions() {
    }

    public static void apply(OrtSession.SessionOptions options, ModelServingProperties.Onnx onnx)
            throws OrtException {
        options.setIntraOpNumThreads(onnx.getIntraOpThreads());
        options.setInterOpNumThreads(onnx.getInterOpThreads());
        options.setExecutionMode(toOrt(onnx.getExecutionMode()));
    }

    private static OrtSession.SessionOptions.ExecutionMode toOrt(ModelServingProperties.ExecutionMode mode) {
        return switch (mode) {
            case SEQUENTIAL -> OrtSession.SessionOptions.ExecutionMode.SEQUENTIAL;
            case PARALLEL -> OrtSession.SessionOptions.ExecutionMode.PARALLEL;
        };
    }
}
```

- [ ] **Step 2: Point `OrtSessionHandle` at it**

In `src/main/java/com/recsys/application/retrieval/OrtSessionHandle.java`, replace these three lines inside `open`:

```java
            options.setIntraOpNumThreads(onnx.getIntraOpThreads());
            options.setInterOpNumThreads(onnx.getInterOpThreads());
            options.setExecutionMode(toOrt(onnx.getExecutionMode()));
```

with:

```java
            OnnxSessionOptions.apply(options, onnx);
```

Delete the now-unused private `toOrt` method entirely. Add `import com.recsys.application.model.OnnxSessionOptions;` and remove the import of `OnnxTensor`/others only if the compiler reports them unused (it will not — they are used elsewhere in the file).

- [ ] **Step 3: Verify the two-tower path is unchanged**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test \
  -Dtest='UserTowerInferenceContractTest+UserTowerInferenceServiceTest' -DfailIfNoTests=false
```

Expected: PASS. `UserTowerInferenceServiceTest` loads the real ONNX model, so this exercises `OrtSessionHandle.open` end to end. This task is a pure refactor; a failure here means the extraction changed behaviour.

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/recsys/application/model/OnnxSessionOptions.java \
        src/main/java/com/recsys/application/retrieval/OrtSessionHandle.java
git commit -m "refactor(model): extract OnnxSessionOptions from OrtSessionHandle

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Configure the `mlp_embedding` session

**Files:**
- Modify: `src/main/java/com/recsys/retrieval/service/DeepLearningPredictionService.java`
- Create: `src/test/java/com/recsys/retrieval/service/DeepLearningPredictionOnnxConfigTest.java`

**Interfaces:**
- Consumes: `OnnxSessionOptions.apply` (Task 2), `ModelServingProperties.Onnx.fromEnvironment()` (Task 1).
- Produces:
  - `public DeepLearningPredictionService(ObjectMapper objectMapper, ModelServingProperties properties)` — the `@Autowired` constructor
  - `public DeepLearningPredictionService(ObjectMapper objectMapper)` — env-fallback, kept for the three existing direct-construction tests
  - package-private `interface DeepLearningPredictionService.SessionOpener { OrtSession open(byte[] modelBytes, ModelServingProperties.Onnx onnx) throws OrtException; }`
  - package-private `DeepLearningPredictionService(ObjectMapper, ModelServingProperties.Onnx, SessionOpener)`

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/recsys/retrieval/service/DeepLearningPredictionOnnxConfigTest.java`:

```java
package com.recsys.retrieval.service;

import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.recsys.config.ModelServingProperties;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

/**
 * The mlp_embedding session is the second of the two OrtSessions in this JVM, and until this
 * test existed it was the one that read no thread configuration at all — it took ONNX
 * Runtime's CPU-derived default while the two-tower sessions beside it ran pinned to one
 * thread, in a pod whose CPU limit is 2.
 *
 * <p>Asserting that from the outside is not possible: OrtSession.SessionOptions exposes no
 * getters. So the seam is the same one OrtSessionHandle already uses — the opener takes the
 * resolved Onnx as an argument, and the test reads it there.
 */
class DeepLearningPredictionOnnxConfigTest {

    /** Records every Onnx config handed to it and returns a mock session. */
    private static final class RecordingOpener implements DeepLearningPredictionService.SessionOpener {
        private final List<ModelServingProperties.Onnx> opened = new ArrayList<>();

        @Override
        public OrtSession open(byte[] modelBytes, ModelServingProperties.Onnx onnx) throws OrtException {
            opened.add(onnx);
            return mock(OrtSession.class);
        }
    }

    private static ModelServingProperties.Onnx onnx(int intra, int inter,
                                                    ModelServingProperties.ExecutionMode mode) {
        ModelServingProperties.Onnx o = new ModelServingProperties.Onnx();
        o.setIntraOpThreads(intra);
        o.setInterOpThreads(inter);
        o.setExecutionMode(mode);
        return o;
    }

    @Test
    void sessionIsOpenedWithTheConfiguredThreadSettings() {
        RecordingOpener opener = new RecordingOpener();
        ModelServingProperties.Onnx configured = onnx(3, 2, ModelServingProperties.ExecutionMode.PARALLEL);

        new DeepLearningPredictionService(new ObjectMapper(), configured, opener);

        assertThat(opener.opened).hasSize(1);
        assertThat(opener.opened.get(0).getIntraOpThreads()).isEqualTo(3);
        assertThat(opener.opened.get(0).getInterOpThreads()).isEqualTo(2);
        assertThat(opener.opened.get(0).getExecutionMode())
                .isEqualTo(ModelServingProperties.ExecutionMode.PARALLEL);
    }

    @Test
    void reloadReopensWithTheSameConfiguration() throws Exception {
        RecordingOpener opener = new RecordingOpener();
        ModelServingProperties.Onnx configured = onnx(3, 2, ModelServingProperties.ExecutionMode.PARALLEL);

        DeepLearningPredictionService service =
                new DeepLearningPredictionService(new ObjectMapper(), configured, opener);
        service.reload();

        assertThat(opener.opened).hasSize(2);
        assertThat(opener.opened.get(1).getIntraOpThreads()).isEqualTo(3);
        assertThat(opener.opened.get(1).getExecutionMode())
                .isEqualTo(ModelServingProperties.ExecutionMode.PARALLEL);
    }

    @Test
    void springConstructorUsesThePropertiesObject() {
        RecordingOpener opener = new RecordingOpener();
        ModelServingProperties properties = new ModelServingProperties();
        properties.getOnnx().setIntraOpThreads(5);

        new DeepLearningPredictionService(new ObjectMapper(), properties.getOnnx(), opener);

        assertThat(opener.opened.get(0).getIntraOpThreads()).isEqualTo(5);
    }

    @Test
    void defaultsMatchTheTwoTowerSessionsRatherThanOnnxRuntimeDefaults() {
        RecordingOpener opener = new RecordingOpener();

        new DeepLearningPredictionService(
                new ObjectMapper(), ModelServingProperties.Onnx.fromEnvironment(name -> null), opener);

        assertThat(opener.opened.get(0).getIntraOpThreads()).isEqualTo(1);
        assertThat(opener.opened.get(0).getInterOpThreads()).isEqualTo(1);
        assertThat(opener.opened.get(0).getExecutionMode())
                .isEqualTo(ModelServingProperties.ExecutionMode.SEQUENTIAL);
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=DeepLearningPredictionOnnxConfigTest -DfailIfNoTests=false
```

Expected: COMPILATION FAILURE — no `SessionOpener` type and no three-argument constructor. That failure is the point: there is currently no way to ask this class what thread configuration it used, because it uses none.

- [ ] **Step 3: Implement**

In `DeepLearningPredictionService.java`:

Add imports:

```java
import com.recsys.application.model.OnnxSessionOptions;
import com.recsys.config.ModelServingProperties;
import org.springframework.beans.factory.annotation.Autowired;
```

Add the seam, immediately after the `ITEM_INPUT` constant:

```java
    /**
     * How the ONNX session gets opened. A seam, not an extension point: SessionOptions has no
     * getters, so this is the only way a test can observe that the session was configured at all.
     */
    @FunctionalInterface
    interface SessionOpener {
        OrtSession open(byte[] modelBytes, ModelServingProperties.Onnx onnx) throws OrtException;
    }
```

Add two fields beside the existing ones:

```java
    private final ModelServingProperties.Onnx onnx;
    private final SessionOpener sessionOpener;
```

Replace the existing constructor with these three:

```java
    @Autowired
    public DeepLearningPredictionService(ObjectMapper objectMapper, ModelServingProperties properties) {
        this(objectMapper, properties.getOnnx(), null);
    }

    /**
     * Env-fallback constructor for callers Spring did not build. Reads the same
     * {@code RECSYS_MODEL_ONNX_*} variables the properties object is bound from, so this session
     * is configured identically however it was constructed.
     */
    public DeepLearningPredictionService(ObjectMapper objectMapper) {
        this(objectMapper, ModelServingProperties.Onnx.fromEnvironment(), null);
    }

    DeepLearningPredictionService(ObjectMapper objectMapper,
                                  ModelServingProperties.Onnx onnx,
                                  SessionOpener sessionOpener) {
        try {
            this.environment = OrtEnvironment.getEnvironment();
            this.onnx = onnx;
            this.sessionOpener = sessionOpener == null ? DeepLearningPredictionService::openOrtSession : sessionOpener;
            this.session = this.sessionOpener.open(loadModelBytes(), onnx);
            LookupTables lookups = readLookups(objectMapper);
            this.userLookup = lookups.userLookup();
            this.itemLookup = lookups.itemLookup();
        } catch (IOException | OrtException e) {
            throw new IllegalStateException("Failed to load deep learning prediction artifacts", e);
        }
    }

    /**
     * The real opener. ALL_OPT is this session's own graph-optimization choice and stays here;
     * the thread settings come from the shared helper so this session cannot drift from the
     * two-tower ones again.
     */
    private static OrtSession openOrtSession(byte[] modelBytes, ModelServingProperties.Onnx onnx)
            throws OrtException {
        try (OrtSession.SessionOptions opts = new OrtSession.SessionOptions()) {
            OnnxSessionOptions.apply(opts, onnx);
            opts.setOptimizationLevel(OrtSession.SessionOptions.OptLevel.ALL_OPT);
            return OrtEnvironment.getEnvironment().createSession(modelBytes, opts);
        }
    }
```

Replace the body of `reload()` with:

```java
    public synchronized void reload() throws IOException, OrtException {
        OrtSession newSession = sessionOpener.open(loadModelBytes(), onnx);
        OrtSession old = this.session;
        this.session = newSession;
        if (old != null) {
            old.close();
        }
    }
```

- [ ] **Step 4: Run the new test and the existing ones**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test \
  -Dtest='DeepLearningPredictionOnnxConfigTest+DeepLearningPredictionServiceTest+ModelReloadControllerTest+RetrievalRecommendationControllerTest' \
  -DfailIfNoTests=false
```

Expected: PASS. `DeepLearningPredictionServiceTest` constructs the service through the one-argument constructor and runs real inference — it proves the env-fallback path still produces a working session.

- [ ] **Step 5: Confirm the Spring context still starts**

The class now has two public constructors; without `@Autowired` on one of them Spring cannot choose. Run any test that builds the model application context:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test \
  -Dtest='RedisAutoConfigurationExclusionTest+ConfigurationBindingTest' -DfailIfNoTests=false
```

Expected: PASS. If a context test reports `No qualifying bean of type ModelServingProperties`, check that `ModelApplication`'s `@EnableConfigurationProperties` still lists `ModelServingProperties.class` — it does today, at line 42.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/recsys/retrieval/service/DeepLearningPredictionService.java \
        src/test/java/com/recsys/retrieval/service/DeepLearningPredictionOnnxConfigTest.java
git commit -m "fix(model): configure the mlp_embedding ONNX session like every other session

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Env fallback at the two Spring-less constructors

**Files:**
- Modify: `src/main/java/com/recsys/application/retrieval/UserTowerInferenceService.java:88`
- Modify: `src/main/java/com/recsys/application/model/ModelRuntimeProvider.java:98`
- Create: `src/test/java/com/recsys/application/model/SpringlessOnnxConfigTest.java`

**Interfaces:**
- Consumes: `ModelServingProperties.Onnx.fromEnvironment()` and `ModelServingProperties.fromEnvironment()` (Task 1).
- Produces: nothing new. Two call-site substitutions.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/recsys/application/model/SpringlessOnnxConfigTest.java`:

```java
package com.recsys.application.model;

import com.recsys.config.ModelServingProperties;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Two production constructors build ModelServingProperties themselves rather than taking the
 * Spring-bound bean. Before this test they returned the hard-coded field initializers, so a
 * deployment that set RECSYS_MODEL_ONNX_INTRA_OP_THREADS=4 got 4 through the Spring path and 1
 * through these — a disagreement invisible while the defaults happen to match, and visible
 * exactly when someone is trying to tune.
 *
 * <p>Source-level rather than behavioural: constructing either class for real loads ONNX
 * artifacts and opens Redis pools, which is a far heavier thing than the property under test.
 * What has to hold is that neither site calls the bare constructor.
 */
class SpringlessOnnxConfigTest {

    @Test
    void springlessConstructorsReadTheEnvironment() throws Exception {
        // The factory both call sites must use exists and is public.
        Method onnxFactory = ModelServingProperties.Onnx.class.getMethod("fromEnvironment");
        Method propertiesFactory = ModelServingProperties.class.getMethod("fromEnvironment");

        assertThat(onnxFactory.getReturnType()).isEqualTo(ModelServingProperties.Onnx.class);
        assertThat(propertiesFactory.getReturnType()).isEqualTo(ModelServingProperties.class);
    }

    @Test
    void neitherSpringlessSiteStillCallsTheBareConstructor() throws Exception {
        String userTower = java.nio.file.Files.readString(java.nio.file.Path.of(
                "src/main/java/com/recsys/application/retrieval/UserTowerInferenceService.java"));
        String runtimeProvider = java.nio.file.Files.readString(java.nio.file.Path.of(
                "src/main/java/com/recsys/application/model/ModelRuntimeProvider.java"));

        assertThat(userTower)
                .as("UserTowerInferenceService's legacy constructor must not seat hard-coded ONNX defaults")
                .doesNotContain("new ModelServingProperties.Onnx()")
                .contains("ModelServingProperties.Onnx.fromEnvironment()");
        assertThat(runtimeProvider)
                .as("ModelRuntimeProvider's non-Spring constructor must not seat hard-coded ONNX defaults")
                .doesNotContain("new ModelServingProperties()")
                .contains("ModelServingProperties.fromEnvironment()");
    }

    @Test
    void environmentSourcedDefaultsStillMatchTheHardCodedOnes() {
        // Nothing is retuned by this change: with the variables unset both agree.
        ModelServingProperties fromEnv = ModelServingProperties.fromEnvironment(name -> null);

        assertThat(fromEnv.getOnnx().getIntraOpThreads()).isEqualTo(1);
        assertThat(fromEnv.getOnnx().getInterOpThreads()).isEqualTo(1);
        assertThat(Arrays.asList(ModelServingProperties.ExecutionMode.values()))
                .contains(fromEnv.getOnnx().getExecutionMode());
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=SpringlessOnnxConfigTest -DfailIfNoTests=false
```

Expected: `neitherSpringlessSiteStillCallsTheBareConstructor` FAILS — both files still contain the bare constructor call. The other two pass (Task 1 already shipped the factories).

- [ ] **Step 3: Implement**

In `UserTowerInferenceService.java`, in the three-argument legacy constructor, change:

```java
                new ModelServingProperties.Onnx(),
```

to:

```java
                ModelServingProperties.Onnx.fromEnvironment(),
```

In `ModelRuntimeProvider.java`, in the five-argument constructor, change:

```java
        this(artifactLocator, abTestConfig, modelFile, itemEmbeddingsSource, redisItemEmbeddingPrefix,
                new ModelServingProperties(), new SimpleMeterRegistry());
```

to:

```java
        this(artifactLocator, abTestConfig, modelFile, itemEmbeddingsSource, redisItemEmbeddingPrefix,
                ModelServingProperties.fromEnvironment(), new SimpleMeterRegistry());
```

The `@Autowired` constructor's null guard (`servingProperties == null ? new ModelServingProperties() : servingProperties`) also seats hard-coded defaults. Change it to `ModelServingProperties.fromEnvironment()` for the same reason:

```java
        this.servingProperties = servingProperties == null ? ModelServingProperties.fromEnvironment() : servingProperties;
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test \
  -Dtest='SpringlessOnnxConfigTest+VariantRuntimeResolverTest+UserTowerInferenceContractTest' -DfailIfNoTests=false
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/recsys/application/retrieval/UserTowerInferenceService.java \
        src/main/java/com/recsys/application/model/ModelRuntimeProvider.java \
        src/test/java/com/recsys/application/model/SpringlessOnnxConfigTest.java
git commit -m "fix(model): honour RECSYS_MODEL_ONNX_* in the Spring-less constructors

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: Gate the new test, correct the manifest comment, document it

**Files:**
- Modify: `pom.xml` (the `resilience` profile's `<includes>`)
- Modify: `k8s/base/model-serving.yaml` (the comment above `RECSYS_MODEL_ONNX_INTRA_OP_THREADS`)
- Modify: `docs/runbooks/model-artifact-rollout.md`

**Interfaces:** none — configuration and prose.

- [ ] **Step 1: Add `ModelServingPropertiesTest` to the PR gate**

`ModelServingPropertiesTest` lives in `com/recsys/config/` and matches no `<include>` in either profile, so it runs only on a full `mvn test` — which no pull request executes. `DeepLearningPredictionOnnxConfigTest` and `DeepLearningPredictionServiceTest` are already covered by the `retrieval` profile's `**/com/recsys/retrieval/**/*Test.java`, and `SpringlessOnnxConfigTest` needs the resilience profile too.

In `pom.xml`, inside the `resilience` profile's `<includes>`, immediately after the `**/k8s/ModelServingManifestTest.java` line, add:

```xml
                <!-- ModelServingManifestTest one line up pins that the manifest SETS
                     RECSYS_MODEL_ONNX_INTRA_OP_THREADS=1; these two pin that the value is
                     actually READ, which is a different claim and was false for one of the
                     JVM's two OrtSessions. An unconfigured session takes ONNX Runtime's
                     CPU-derived default — measured at an intra-op pool of 4 on an 8-core host —
                     against a pod whose CPU limit is 2, which is the oversubscription the
                     manifest comment exists to prevent. Those pools are native, so no JVM
                     thread metric can report the regression; a test is the only detector.
                     Pure unit-level: no ONNX Runtime, no Redis, no Docker, no clock. -->
                <include>**/config/ModelServingPropertiesTest.java</include>
                <include>**/model/SpringlessOnnxConfigTest.java</include>
```

- [ ] **Step 2: Verify the gate actually runs them**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn --batch-mode -Presilience test 2>&1 | tail -20
ls target/surefire-reports/ | grep -E "ModelServingProperties|SpringlessOnnx"
```

Expected: BUILD SUCCESS, and both report files present. An empty `grep` means the include pattern does not match and the test is not gated — fix the pattern before continuing.

- [ ] **Step 3: Correct the manifest comment**

In `k8s/base/model-serving.yaml`, the comment above `RECSYS_MODEL_ONNX_INTRA_OP_THREADS` currently describes a property the pod did not have. Replace the sentence ending `...ModelServingManifestTest.` by appending:

```yaml
            # These now reach every OrtSession in the JVM. They did not always: the
            # mlp_embedding session behind /api/v1/retrieval/* was created with bare
            # SessionOptions and ran on ONNX Runtime's CPU-derived default (an intra-op pool of
            # 4, measured on an 8-core host) while the two-tower sessions beside it honoured
            # this value. ModelServingPropertiesTest and DeepLearningPredictionOnnxConfigTest
            # pin that the value is read, not merely set.
```

- [ ] **Step 4: Document it in the runbook**

Append a section to `docs/runbooks/model-artifact-rollout.md`:

```markdown
## ONNX session thread configuration

`RECSYS_MODEL_ONNX_INTRA_OP_THREADS`, `_INTER_OP_THREADS` and `_EXECUTION_MODE` (defaults
`1` / `1` / `SEQUENTIAL`) configure **every** ONNX Runtime session in the model service JVM:
the per-variant two-tower sessions and the `mlp_embedding` session behind
`/api/v1/retrieval/*`. They are applied in one place, `OnnxSessionOptions.apply`.

The values are read two ways, and both must agree:

- Spring binds them via `application.yml`'s `${...}` placeholders into `ModelServingProperties`.
- `ModelServingProperties.fromEnvironment()` reads them directly, for the constructors Spring
  did not build (`UserTowerInferenceService`'s locator constructor, `ModelRuntimeProvider`'s
  non-Spring constructor, `DeepLearningPredictionService(ObjectMapper)`).

Both reject the same input. A non-numeric, zero or negative thread count, or an unrecognised
execution mode, **fails startup** rather than falling back to the default — the env path uses
`EnvVars` (which throws) and not `EnvConfig` (which swallows), so that a variable cannot mean
one thing to Spring and another to a direct constructor.

Sizing note: ONNX Runtime's intra-op pool costs `intraOpThreads - 1` native threads per
session (the calling thread is one of the workers); measured at 1/2/4/8 on an 8-core host the
deltas were 0/1/3/7. Those are native threads and do not appear in any JVM thread metric, so
raising this value is not observable in `jvm_threads_live_threads` — size it against the pod's
CPU limit and `InferenceLoadTest`, not against a dashboard.
```

- [ ] **Step 5: Full verification**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn --batch-mode -Presilience test 2>&1 | tail -5
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn --batch-mode -Pretrieval test 2>&1 | tail -5
```

Expected: BUILD SUCCESS on both — these are the two PR gates. Note `OutboxRelayTest` has a known timing flake in the resilience profile; if it is the only failure, re-run before investigating.

- [ ] **Step 6: Commit**

```bash
git add pom.xml k8s/base/model-serving.yaml docs/runbooks/model-artifact-rollout.md
git commit -m "test(model): gate the ONNX thread-config tests and correct the manifest comment

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: Open the pull request

- [ ] **Step 1: Record what the implementation discovered**

Append a `## Discovered during implementation` section to
`docs/superpowers/specs/2026-09-20-onnx-thread-config-unification-design.md` with anything the
tasks turned up that the spec did not predict. If nothing did, write that.

```bash
git add docs/superpowers/specs/2026-09-20-onnx-thread-config-unification-design.md
git commit -m "docs(spec): record what implementation discovered

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

- [ ] **Step 2: Push and open the PR**

```bash
git push -u origin feat/onnx-thread-config-unification
gh pr create --base main --title "Unify ONNX session thread configuration, with env-var fallback" --body "..."
```

The body must state: the two session sites and which one was unconfigured; the measured thread
deltas; that `OptLevel` and `recsys.model.recall.*` are deliberately out of scope; and that the
`mlp_embedding` endpoint's intra-op pool drops from 4 to 1 on an 8-core host, which is the one
behavioural change.

---

## Self-Review

**Spec coverage.** Problem §1 (unconfigured mlp session) → Task 3. §2 (why nothing caught it) →
Task 3's test plus Task 5's runbook note. §3 (Spring-less env gap) → Tasks 1 and 4. §4
(duplicated options block) → Task 3's `openOrtSession`. Goals 1–4 → Tasks 2, 1, 3, 1
respectively. Non-goals are enforced by Global Constraints. Verification bullets map to Task
1 Step 4, Task 3 Steps 2/4, Task 4 Steps 2/4, and Task 5 Step 5.

**Placeholders.** The only `...` is the `gh pr create --body`, whose required content is
specified in prose immediately below it.

**Type consistency.** `fromEnvironment` is spelled identically in Tasks 1, 3 and 4.
`SessionOpener.open(byte[], ModelServingProperties.Onnx)` matches between the Task 3 interface
definition, the `RecordingOpener` test double and `openOrtSession`. `OnnxSessionOptions.apply`
takes `(SessionOptions, Onnx)` and returns `void` in both Task 2's definition and Task 3's use.
`EnvVars.EnvReader` is a `@FunctionalInterface` with `String get(String)`, which `Map::get` and
`name -> null` both satisfy.
