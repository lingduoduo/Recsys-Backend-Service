# Model artifact rollout

How ONNX model bundles reach the model-serving pods (8080), what the loader verifies before a
pod reports ready, how a bad bundle fails, and how to roll a generation forward and back.
Design: [2026-09-03 ONNX serving hardening](../superpowers/specs/2026-09-03-onnx-serving-hardening-design.md).
Fault-tolerance context: [18_Fault_Tolerance §8](../system_design/18_Fault_Tolerance.md#8-observability--knowing-that-any-of-this-happened).

## 1. What the repository ships, and what it does not

Tracked in git, so a clean checkout starts model serving:

```text
src/main/resources/dssm_model.onnx                                  # the two-tower demo model
src/main/resources/artifacts/model/training/feature_config.json     # control variant vocabularies
src/main/resources/artifacts/model/test/feature_config.json         # treatment variant vocabularies
```

These are **legacy bundles**: no `model_manifest.json`, so the loader cannot verify that the
model and the feature config came from the same training run. It loads them anyway and logs
one warning per variant:

```text
WARN  Model variant 'training' uses a legacy bundle without model_manifest.json; artifact consistency and checksums are not verified
```

Production bundles are produced by the training pipeline, not by this repository, and must be
manifest-backed. Every configured A/B variant (`recsys.ab-test.default-variant`,
`bucket-a-variant`, `bucket-b-variant`) needs its own bundle from the same pipeline.

## 2. Immutable generation layout

Publish under the directory `RECSYS_MODEL_ARTIFACTS_DIR` points at:

```text
<artifact-root>/releases/<generation>/<variant>/
  model_manifest.json
  feature_config.json
  dssm_model.onnx
  item_embeddings.json          # optional companion; checksummed when listed in the manifest
current -> releases/<generation>
```

Rules the loader enforces or assumes:

- **A generation is written completely, verified, and never edited again.** The loader reads a
  variant's files into memory once at startup, checks every checksum, and builds the session
  from those bytes. It does not watch the directory and does not hot-reload. Editing a file under
  the active generation does nothing until a pod restarts — and then that pod serves a different
  model from its neighbours.
- **The artifact root may be a symlink; the files may not.** `RECSYS_MODEL_ARTIFACTS_DIR` is
  resolved to its real path, so pointing it at `current` and switching that link between
  deployments is the intended mechanism. Inside a variant directory every manifest-referenced
  file must be a regular file: a symlink, a directory, or a path that resolves outside the variant
  directory rejects the variant.
- **Switch the link, then roll the pods.** Pods read the root at startup only, so a link switch
  with no rollout changes nothing running; a rollout with no link switch loads the old generation.

## 3. Manifest schema (version 1)

`model_manifest.json` sits beside `feature_config.json`. The filename is fixed.

```json
{
  "schema_version": 1,
  "model_version": "dssm-2026-09-03",
  "model_file": "dssm_model.onnx",
  "sha256": {
    "feature_config.json": "<64 lowercase hex characters>",
    "dssm_model.onnx": "<64 lowercase hex characters>",
    "item_embeddings.json": "<64 lowercase hex characters>"
  },
  "inputs": {
    "user_id": { "type": "INT64", "rank": 1 },
    "item_id": { "type": "INT64", "rank": 1 }
  },
  "output": {
    "name": "score",
    "type": "FLOAT",
    "rank": 1
  }
}
```

| Field | Rule |
|---|---|
| `schema_version` | Must be `1`. Anything else rejects the variant. |
| `model_version` | Non-blank, and must equal `model_version` in `feature_config.json`. This is the check that a model and a vocabulary came from the same run. |
| `model_file` | A simple filename in the variant directory. `/abs/path`, `../x.onnx`, and any separator are rejected. Authoritative for manifest bundles — `RECSYS_MODEL_FILE` applies to legacy bundles only. |
| `sha256` | Must cover `feature_config.json` and `model_file`. Any other listed file (`item_embeddings.json`) is read, verified, and used from the verified bytes; a listed file that is missing or mismatched rejects the variant. Lowercase hex, 64 characters. |
| `inputs` | Exactly `user_id` and `item_id`, both `INT64` rank 1. Version 1 requires these two names because the Java feature adapter feeds exactly those semantics; an extra required input rejects the variant, since serving could never populate it. |
| `output` | The scored output: `FLOAT`, rank 1. Its `name` is what the loader reads back from the session, so a model that emits `prob` instead of `score` declares that here. Extra model outputs are allowed and ignored. |
| *(anything else)* | Rejected. Version 1 has exactly these six top-level fields; an unknown field (a `"notes"` key, a typo) fails parsing with `Invalid model_manifest.json for variant '<v>'`. Keep publisher metadata outside the manifest. |

Generate checksums with whichever of these the publishing host has:

```bash
# Linux (coreutils)
cd <artifact-root>/releases/<generation>/<variant>
sha256sum feature_config.json dssm_model.onnx item_embeddings.json

# macOS
shasum -a 256 feature_config.json dssm_model.onnx item_embeddings.json
```

Both print `<hex>  <filename>`; the hex goes into `sha256` under that filename. Verify the
manifest you wrote before publishing:

```bash
# Linux — re-derive the checksums from the manifest and compare
jq -r '.sha256 | to_entries[] | "\(.value)  \(.key)"' model_manifest.json | sha256sum -c -
# macOS
jq -r '.sha256 | to_entries[] | "\(.value)  \(.key)"' model_manifest.json | shasum -a 256 -c -
```

## 4. What a pod verifies before it is ready

Per variant, in this order. The first failure stops the variant; the log line names the file
and the check.

| Step | Rejects with | Example |
|---|---|---|
| Parse `model_manifest.json` | `Invalid model_manifest.json for variant '<v>'` | malformed JSON, duplicate keys, `schema_version` ≠ 1 |
| Path safety | `must be a simple relative filename`, `must not be a symbolic link`, `escapes variant directory` | `model_file: "../shared.onnx"`, a symlinked `.onnx` |
| Checksums | `SHA-256 mismatch for <file>` / `missing SHA-256 checksum for <file>` | model replaced in place after the manifest was written |
| Feature version | `manifest model_version X does not match feature_config.json model_version Y` | vocab from one run, model from another |
| ONNX contract | `ONNX model has no input named 'item_id'`, `must be INT64 (INT64), got INT32`, `must have rank 1, got rank 2`, `declares unexpected input 'context'`, `has no output named 'score'` | exported model drifted from the serving contract |
| Smoke inference | `ONNX smoke inference returned N scores for a single-row batch`, `returned a non-finite score` | a session that opens but cannot score |

The production smoke batch uses indices from the bundle's feature configuration:
the user vocabulary's `__UNK__` index (or its smallest index when `__UNK__` is
absent) and the smallest item index. An empty vocabulary falls back to 0;
this does not make an empty or incompatible bundle valid. Legacy locator-based
constructors without a feature configuration still use user/item row 0. Bundles
with non-zero vocabulary indices no longer fail startup merely because the
smoke batch assumed row 0.

Only after the smoke inference does the variant's session report ready. Initialization is
transactional: a failure closes whatever native state was opened.

**A manifest that is present is strict.** A malformed manifest never falls back to legacy
loading and never falls through to a checksum-less root artifact — that would defeat the point
of shipping one. A variant directory with **no** manifest loads the legacy way with the warning
in §1.

## 5. Control versus treatment failure

| Variant that fails | What happens | Signal |
|---|---|---|
| The default/control variant | Startup fails. The pod never passes its startup probe, the rollout stalls at `maxUnavailable: 0`, and the previous ReplicaSet keeps serving. | Pod log (first lines); `ModelServingUnavailable` if the whole fleet is affected |
| A non-default (treatment) variant | Startup completes. Requests assigned to that bucket are served by control, attributed as control in the response, metrics, and exposure events (`fellBack=true`). After a 60 s cooldown one request retries the build; if the artifact was fixed in the meantime the treatment starts serving without a restart. | `recsys_model_runtime_load_failures_total{variant,phase="warmup"}`, `recsys_abtest_variant_fallback_total{variant}`, `ModelRuntimeLoadFailure` |

Spring registers the variant resolver's warm-up failure listener through
`@PostConstruct`, before provider warm-up. Code constructing a
`VariantRuntimeResolver` outside Spring must call `listenForWarmUpFailures()`
before warm-up, as the service's convenience constructors do. This lets a
failed treatment enter cooldown immediately without counting the provider's
warm-up failure twice.

Under overload, the degraded-cache path follows the same rule: a failed or unloaded treatment's
requests are answered from control's cache, and never trigger a model load.

## 6. Rollout and rollback

Roll forward:

1. Publish `releases/<new>/` completely, for every configured variant, and verify checksums (§3).
2. Deploy an image that understands manifests (any build from this change onward) before
   publishing the first manifest-backed generation, so the reader is in place first.
3. Canary the new model as the **experimental** variant. There is one artifact root, so a
   generation holds every variant: publish `releases/<new>/` with the **current control bundle
   unchanged** under the control variant's directory and the **new bundle** under
   `bucket-a-variant`'s directory, switch `current`, roll the pods. A bad new bundle degrades
   that bucket to control (§5) instead of taking the pod down.
4. Watch, for at least one cooldown window (60 s) plus a scrape interval:
   `recsys_model_runtime_load_failures_total` flat, `recsys_abtest_variant_fallback_total` flat,
   `recsys_model_onnx_runs_total{variant="<treatment>"}` rising, `ModelInferenceLatencyHigh` and
   `ModelServingShedding` quiet.
5. Promote: make the validated generation the control variant, switch, roll.

Roll back: point `current` at the previous generation, roll the pods (and the previous image if
the image changed too). Nothing in Redis or MySQL is written by a model rollout, so there is no
data migration in either direction.

## 7. Metrics and alerts

| Metric | Source | Meaning |
|---|---|---|
| `recsys_model_onnx_runs_total{variant}` | `UserTowerInferenceService` | One increment per `OrtSession.run`, including the startup smoke run. **The proof that inference happened**: a load test whose request rate rises while this stays flat is measuring caches or out-of-vocab fallback ranking, not the model. |
| `recsys_model_runtime_load_failures_total{variant,phase}` | `ModelRuntimeProvider` (`phase=warmup`), `VariantRuntimeResolver` (`phase=request`) | A variant failed to build. Control failures never reach this — they fail startup. |
| `recsys_abtest_variant_fallback_total{variant}` | `VariantRuntimeResolver` | A request assigned to `variant` was served by control. |
| `recsys_model_recall_tasks_total{result,channel}` | `RecallTaskMetrics` | Recall channel work that produced nothing: `rejected` at the bounded queue, or `timeout` (cancelled with interruption at the channel deadline). `channel` is the closed set of configured channel names. |
| `recsys_load_shedder_requests_total{result}` | `LoadShedder` | Admission decisions; `rejected` are served from the degraded cache when possible, else 503. |

| Alert | Fires when |
|---|---|
| `ModelServingUnavailable` | No ready replica for 5 m — `kube_deployment_status_replicas_ready == 0`, or no `up` series at all (unready pods leave the Service's Endpoints and therefore Prometheus' target list). |
| `ModelServingShedding` | More than 5% of admission decisions rejected over 10 m, with at least 100 decisions in the window. |
| `ModelInferenceLatencyHigh` | p95 of `/api/v1/recommend` and `/v2/recommend` above 500 ms for 10 m. |
| `ModelRuntimeLoadFailure` | Any increase in `recsys_model_runtime_load_failures_total` in a 15 m window. Self-clears when the window passes. |

## 8. Local checks

Start the model service against the bundled demo (legacy) bundle and confirm it is ready and
running inference:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)   # macOS; any JDK 17
export REDIS_ALLOW_NO_AUTH=true
export RECOMMENDATION_CURSOR_SIGNING_KEY=0123456789abcdef0123456789abcdef
mvn spring-boot:run

curl -fsS http://localhost:8080/health/ready
curl -fsS -H 'Content-Type: application/json' \
  -d '{"userId":"123","k":10}' http://localhost:8080/api/v1/recommend
curl -fsS http://localhost:8080/actuator/prometheus | grep recsys_model_onnx_runs_total
```

The counter must be at least 2 after that sequence (one smoke run at startup, one for the
request). Repeat the same request: the counter does **not** move — the response cache answered.
That distinction is what `InferenceLoadTest` (`mvn test -DexcludedGroups= -Dgroups=load`) and
`scripts/load-test/model-serving.js` assert on; both fail when the counter fails to rise.

To try a manifest-backed bundle locally, copy the demo files into
`<root>/training/`, write `model_manifest.json` per §3 with `model_version: "dssm-demo-v1"`
(the version in the bundled `feature_config.json`), and start with
`RECSYS_MODEL_ARTIFACTS_DIR=<root>`. Then corrupt one byte of the model and restart: startup must
fail naming `dssm_model.onnx` and `SHA-256`.

## 9. Configuration touched by this path

| Variable | Default | Role here |
|---|---|---|
| `RECSYS_MODEL_ARTIFACTS_DIR` | classpath | Artifact root (§2). May be a symlink. |
| `RECSYS_MODEL_FILE` | `dssm_model.onnx` | Model filename for **legacy** bundles only; a manifest's `model_file` wins. |
| `RECSYS_MODEL_ONNX_INTRA_OP_THREADS` / `_INTER_OP_THREADS` / `_EXECUTION_MODE` | `1` / `1` / `SEQUENTIAL` | Native parallelism for **every** session in the JVM — see §10. `k8s/base` sets these explicitly; they are safety defaults, not measured capacity. |
| `RECSYS_MODEL_RECALL_CORE_THREADS` / `_QUEUE_CAPACITY` / `_TIMEOUT_MS` | `2×CPUs` / `256` / `200` | The bounded recall executor and per-channel deadline behind `recsys_model_recall_tasks_total`. |
| `RECSYS_HEALTH_MAX_CONCURRENT_REQUESTS` | `64` (ConfigMap: `8`) | Admission cap behind `ModelServingShedding`. |
| `RECSYS_HEALTH_MAX_FAILURE_RATE` / `RECSYS_HEALTH_MAX_AVG_LATENCY_MS` | `0.5` / `2000` (manifest: `0.05` / `500`) | Readiness thresholds. Both are `${...}` placeholders in `application.yml` on purpose — Spring relaxed binding does not map these underscore names to the dashed properties on its own. |

## 10. ONNX session thread configuration

`RECSYS_MODEL_ONNX_INTRA_OP_THREADS`, `_INTER_OP_THREADS` and `_EXECUTION_MODE` (defaults
`1` / `1` / `SEQUENTIAL`) configure **every** ONNX Runtime session in the model service JVM:
the per-variant two-tower sessions and the `mlp_embedding` session behind
`/api/v1/retrieval/*`. They are applied in one place, `OnnxSessionOptions.apply`.

This was not always true. The `mlp_embedding` session was created with bare `SessionOptions`
and took ONNX Runtime's own default while the two-tower sessions beside it honoured the
variables. If you are reading an incident from before that fix, the pod's real thread count
was not what the manifest said.

The values are read two ways, and both must agree:

- Spring binds them via `application.yml`'s `${...}` placeholders into `ModelServingProperties`.
- `ModelServingProperties.fromEnvironment()` reads them directly, for the four construction
  sites Spring did not build: `UserTowerInferenceService`'s locator constructor,
  `ModelRuntimeProvider`'s non-Spring constructor, `ModelRuntimeProvider`'s `@Autowired` null
  guard, and `DeepLearningPredictionService(ObjectMapper)`.

  Today only the last of those is reachable in production: `grep` finds no
  `new ModelRuntimeProvider(` anywhere in `src/main`, and the one `new UserTowerInferenceService(`
  there already passes the Spring-bound config. The other three are test entry points and the
  seam a future non-Spring caller would use. They were fixed anyway, because a source-level
  disagreement about what a variable means does not become real only once someone trips on it.

The two paths are held to the same verdict on every input, which is the property this factory
exists for. A non-numeric, zero or negative thread count, an unrecognised execution mode, **or
a thread count that is set but blank**, all **fail startup** rather than falling back: the env
path uses `EnvVars` (which throws) and not `EnvConfig` (which swallows).

The blank case looks asymmetric and is not a mistake:

| Input | Spring | `fromEnvironment` |
|---|---|---|
| thread count set but blank (`value: ""`) | fails | fails |
| execution mode set but blank | falls back to `SEQUENTIAL` | falls back to `SEQUENTIAL` |

Spring's `${RECSYS_MODEL_ONNX_INTRA_OP_THREADS:1}` supplies its default only when the variable
is **unset**, so an empty value reaches a primitive `int` and fails context startup with "A null
value cannot be assigned to a primitive type"; Spring binds the enum as a nullable object and
falls back there instead. `fromEnvironment` reproduces both verdicts. Every env-path failure
names the variable in its message, because on that path there is no Spring `BindException` to
supply that context and the failure mode is a crash-looping pod.

`recsys.model.recall.*` is environment-sourced on the direct path too (`RECSYS_MODEL_RECALL_CORE_THREADS`,
`_QUEUE_CAPACITY`, `_TIMEOUT_MS`), which closes the gap issue #342 recorded. One floor differs
from the ONNX block and the difference is Spring's, measured:

| Input | Spring | `fromEnvironment` |
|---|---|---|
| `core-threads=0` | accepted, expands to `2 × availableProcessors` | same |
| `queue-capacity=0` / `timeout-ms=0` | rejected | rejected |
| any of the three, set but blank | rejected | rejected |

`0` is the documented "use the computed default" input for `core-threads` alone, so the
minimum is a per-property parameter rather than the single "at least 1" rule the ONNX thread
counts use. A shared floor would have rejected a valid input.

### Sizing

ONNX Runtime's intra-op pool costs `intraOpThreads - 1` native threads per session; the
calling thread serves as one of the workers. Measured on an 8-core host against
`mlp_embedding_model.onnx`, counting process threads with `ps -M` around session creation
plus one inference:

| `intraOpThreads` | Extra native threads |
|---:|---:|
| 1 | 0 |
| 2 | 1 |
| 4 | 3 |
| 8 | 7 |
| unset (ORT default) | 3 |

Two consequences for operating this. The unset row is not a constant — ONNX Runtime derives
it from the visible CPU count, so it is a property of the node the pod lands on. And these
are **native** threads: they do not appear in `jvm_threads_live_threads` or any other JVM
metric the services publish, so raising this value is invisible on the dashboards. Size it
against the pod's CPU limit and `InferenceLoadTest`, not against an observed thread gauge.
