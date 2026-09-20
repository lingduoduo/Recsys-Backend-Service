# Unifying ONNX Session Thread Configuration

## Problem

The Spring model service (8080) creates ONNX Runtime sessions from two places. Only one of
them reads the thread configuration that the deployment sets.

| Site | Threads | Inter-op | Execution mode | Optimization |
|---|---|---|---|---|
| `OrtSessionHandle.open` (DSSM two-tower, one session per variant) | `recsys.model.onnx.intra-op-threads` | bound | bound | default |
| `DeepLearningPredictionService` (`mlp_embedding`, one session) | **unset** | **unset** | **unset** | `ALL_OPT` |

Both run in the same JVM: `ModelApplication` scans `com.recsys.application` and
`com.recsys.retrieval` alike. So a single pod hosts a set of sessions pinned to one thread
and, beside them, one session on ONNX Runtime's own defaults.

### What the unset session actually costs

Measured on an 8-core machine against `mlp_embedding_model.onnx`, counting **native** process
threads (`ps -M`) before and after session creation plus one inference:

| Session options | Extra native threads |
|---|---:|
| Bare + `ALL_OPT` — today's `DeepLearningPredictionService` | **3** |
| `intraOp=1`, `interOp=1`, `SEQUENTIAL` — today's `OrtSessionHandle` | **0** |
| `intraOp=2` | 1 |
| `intraOp=4` | 3 |
| `intraOp=8` | 7 |

The delta is `intraOpThreads - 1` — the calling thread serves as one of the pool's workers.
The unconfigured session lands on 3, i.e. it is running with an intra-op pool of 4 on an
8-core host: ONNX Runtime derives its default from the visible CPU count. The count is
therefore a property of the machine the pod lands on, not of anything the repo states.

`k8s/base/model-serving.yaml` sets `RECSYS_MODEL_ONNX_INTRA_OP_THREADS=1` and explains itself
at length:

> One intra-op thread, one inter-op thread, sequential graph execution: the pod's CPU limit is
> 2 and the request path already parallelises ACROSS requests […] so letting each
> `OrtSession.run` spin up its own thread pool oversubscribes the limit […]

That comment describes a property the pod does not have. The env var reaches the two-tower
sessions and stops there. The `mlp_embedding` session — the one behind
`/api/v1/retrieval/*` — is exactly the "spin up its own thread pool" case the comment warns
against, against a 2-CPU limit.

### Why nothing caught it

ONNX Runtime's intra-op and inter-op pools are native threads, not JVM threads. A probe
listing `Thread.getAllStackTraces()` around session creation observes **zero** new threads in
either configuration. `JvmMetricsBinder` publishes a JVM thread gauge; no value it can ever
report is a function of this setting. There was no signal to notice.

### The second gap: the env vars are invisible outside Spring

`ModelServingProperties` is bound by Spring from `application.yml`, where each key carries an
`${RECSYS_MODEL_ONNX_...}` placeholder. Two production constructors build the properties
object directly instead:

- `UserTowerInferenceService:88` — `new ModelServingProperties.Onnx()`
- `ModelRuntimeProvider:98` — `new ModelServingProperties()`

Both yield the **hard-coded** field initializers (1 / 1 / SEQUENTIAL), never the environment.
A deployment setting `RECSYS_MODEL_ONNX_INTRA_OP_THREADS=4` gets 4 through the Spring-bound
path and 1 through these. The two disagree silently, and the defaults being equal today is
what hides it: the bug is invisible until someone overrides the value, which is precisely
when they are trying to tune.

### A third, smaller thing

`DeepLearningPredictionService` builds its `SessionOptions` twice — once in the constructor
and once in `reload()` — as two copies of the same three lines. Any fix has to land in both,
and a future one will have the same trap.

## Goals

- One expression of ONNX session thread configuration, applied at every site that creates a
  session. Adding a fourth knob means editing one place.
- `RECSYS_MODEL_ONNX_INTRA_OP_THREADS` / `_INTER_OP_THREADS` / `_EXECUTION_MODE` are honoured
  whether or not Spring built the properties object.
- The `mlp_embedding` session is pinned like the others, making
  `k8s/base/model-serving.yaml`'s comment true of the whole pod.
- Invalid configuration fails the same way on both paths.

## Non-goals

- **`OptLevel`.** `DeepLearningPredictionService` sets `ALL_OPT`; `OrtSessionHandle` leaves
  the default. That asymmetry is real, but it is a graph-optimization knob, not thread
  configuration, and unifying it would change DSSM inference behaviour. It stays as it is,
  per-site, and is noted in the PR.
- **Retuning.** No default changes value. `1 / 1 / SEQUENTIAL` stays the default everywhere;
  the k8s manifest keeps setting it explicitly. Per the manifest's own instruction, a change
  to these numbers wants `InferenceLoadTest` figures from the same CPU shape, which is a
  different piece of work.
- **A separate knob for the mlp session.** One shared setting, deliberately.
- **The other three services.** 6010, 7010 and 8010 create no ONNX session — `createSession`
  appears at exactly two call sites, both above.
- **`recsys.model.recall.*`.** `ModelRuntimeProvider:250` reads `servingProperties.getRecall()`,
  so the Spring-less constructor at `:98` leaks the same defect into the recall executor's
  core-threads/queue-capacity/timeout: `RECSYS_MODEL_RECALL_*` is equally unread there. It is
  the identical bug one field over, and it is deliberately left alone — this change is about
  ONNX session threads, and widening it would make the diff argue two cases at once. The
  factory added here (`ModelServingProperties.fromEnvironment`) is the seam a follow-up would
  extend; the gap is called out in the PR so it is recorded rather than quietly inherited.

## Design

### `ModelServingProperties.Onnx.fromEnvironment(EnvReader)`

A static factory beside the existing class, reading the same three variables and falling back
to the same field initializers. `EnvReader` is injected (`EnvVars.EnvReader`) so tests need
not mutate the process environment; a `fromEnvironment()` overload defaults it to
`System::getenv`.

It parses with `EnvVars`, **not** `EnvConfig`. The two differ exactly here: `EnvConfig.readInt`
swallows an unparseable value and returns the default, `EnvVars.readInt` throws. Fail-fast is
what the Spring path already does — `${RECSYS_MODEL_ONNX_INTRA_OP_THREADS:1}` bound to a
`@Positive` setter fails context startup on `abc` or `0` — and the point of this change is
that the two paths agree. An unrecognised `_EXECUTION_MODE` throws likewise rather than
silently seating `SEQUENTIAL`.

Thread counts route through the existing setters, so `@Positive` validation applies to the
environment-sourced values too, without restating the rule.

### `OnnxSessionOptions.apply(SessionOptions, Onnx)`

The three setters and the `ExecutionMode` mapping, lifted out of `OrtSessionHandle` into
`com.recsys.application.model` — the layer the package map already assigns to the ONNX
pipeline, and reachable from both `com.recsys.application.retrieval` and
`com.recsys.retrieval.service`.

`apply` rather than a factory returning a `SessionOptions`: the object holds native memory and
must be closed by whoever opened it. Handing one back would split ownership across a package
boundary, which is how the native leak the existing comment in `OrtSessionHandle` warns about
gets reintroduced. Callers keep their `try (SessionOptions …)` and hand it in.

### Call sites

- `OrtSessionHandle.open` delegates to `apply`. No behaviour change.
- `DeepLearningPredictionService` gains an injected `ModelServingProperties`, and its two
  duplicated options blocks collapse into one private `openSession()` that calls `apply` and
  then sets `ALL_OPT`. The existing `(ObjectMapper)` constructor stays, delegating with
  `Onnx.fromEnvironment()` — three tests construct the service directly, and the fallback is
  the point of the exercise rather than a concession to them.
- `UserTowerInferenceService:88` and `ModelRuntimeProvider:98` swap
  `new ModelServingProperties.Onnx()` / `new ModelServingProperties()` for the
  `fromEnvironment` equivalent.

### Testability of the mlp session

`OrtSession.SessionOptions` exposes no getters, so "was this session configured?" cannot be
asserted against ONNX Runtime. `OrtSessionHandle` already solved this for the two-tower path
with the `OnnxSessionFactory` seam that takes the `Onnx` record as an argument.
`DeepLearningPredictionService` gets the equivalent: a package-private session-opener field,
defaulting to the real one, through which a test observes the `Onnx` the service resolved.

## Verification

Written test-first; each assertion watched failing against today's code before the fix lands,
because a conformance test that has never failed is not evidence that it constrains anything.

- `fromEnvironment` reads all three variables; unset yields 1 / 1 / SEQUENTIAL; non-numeric,
  zero, negative and unknown-mode inputs each throw.
- `DeepLearningPredictionService` passes the configured `Onnx` to its session opener — both
  from the constructor and across `reload()`. Fails today: nothing is passed at all.
- The Spring-less constructors of `UserTowerInferenceService` and `ModelRuntimeProvider`
  observe the environment. Fails today: they return the hard-coded defaults.
- Existing `ConfigurationBindingTest`, `ModelServingPropertiesTest`,
  `UserTowerInferenceContractTest`, `ModelServingManifestTest` and
  `DeepLearningPredictionServiceTest` keep passing unchanged.
- The full `-Presilience` profile, which is the merge gate.

## Risks

**The `mlp_embedding` endpoint gets slower on a multi-core host.** This is the intended
change, and it is the only behavioural one. Its intra-op pool goes from 4 threads to 1 on an
8-core machine. Per-request latency for `/api/v1/retrieval/*` rises for single large batches;
aggregate throughput under concurrency should not, since the service parallelises across
requests and the admission cap already bounds concurrency. The model is small — an
MLP over embedding lookups — so the intra-op pool has little to divide. Mitigation if it
bites: raise `RECSYS_MODEL_ONNX_INTRA_OP_THREADS`, which now actually reaches this session,
which is the fix working rather than failing.

**Fail-fast on a bad env var turns a silent default into a boot failure.** Deliberate, and it
matches the existing Spring behaviour for the same variable. A pod that would previously have
ignored `RECSYS_MODEL_ONNX_INTRA_OP_THREADS=all` now crash-loops on it. Nothing sets these
variables to anything but `1` today (`k8s/base/model-serving.yaml`, pinned by
`ModelServingManifestTest`), so there is no live value to break.

**The env-var read happens at construction, not refresh.** `fromEnvironment` is a snapshot;
changing the variable requires a restart. That already describes every other env var here.

## Discovered during implementation

**The `<!-- ... -- ... -->` in the pom comment made the POM unparseable.** An em-dash rendered
as `--` inside an XML comment is illegal, and Maven refuses to read the file at all. What is
worth recording is not the typo but how it was nearly missed: the first `-Presilience` run
after the edit *looked* like it passed, because the check was `ls target/surefire-reports/`
and those reports were left over from the previous run. The build had actually died before
running a single test. Deleting `target/surefire-reports` before a verification run is the
difference between evidence and a stale artifact — a report file proves a test ran *at some
point*, not that it ran now.

**`-Dtest='A+B'` silently matches nothing** in this Surefire version; the separator is a
comma. The plan used `+` throughout, copied from the syntax Surefire accepts for *method*
selection. `-DfailIfNoTests=false` then turns "I ran no tests" into a green build, so the
combination is quietly dangerous: `-Dtest='A+B' -DfailIfNoTests=false` reports success
without executing anything. The plan's commands have not been rewritten, since they are a
record of what was planned; the commands actually run used commas.

**A third Spring-less default was found while editing.** `ModelRuntimeProvider`'s `@Autowired`
constructor carries a null guard, `servingProperties == null ? new ModelServingProperties()`,
which seats the same hard-coded defaults as the two sites the spec named. It was folded into
Task 4 rather than left, since it is the identical expression one line away.

**The measured ORT default is 4 intra-op threads on an 8-core host, not 8.** The spec says
"derived from the visible CPU count", which the 1/2/4/8 sweep supports (delta is always
`intraOp - 1`), but the derivation is evidently not one-thread-per-core. No claim about the
exact formula is made anywhere in the change; what is stated is the measurement and that it
varies with the host.
