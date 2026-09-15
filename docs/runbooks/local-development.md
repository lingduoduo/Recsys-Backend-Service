# Local development

Run all shell commands from the repository root.

## What runs locally

The clean-clone quick start uses the smallest useful runnable subset:

1. `docker-compose.streaming.yml` starts the Redis primary.
2. The catalog/recommendation service runs on the host and exposes health on
   port `6010`.

The full local topology additionally has ZooKeeper, Kafka, a Redis replica,
three Redis Sentinel processes, Flink, online prediction, ONNX model serving,
and the API gateway. The Java services run on the host so contributors can
rebuild, attach a debugger, or restart one process without rebuilding
containers.

MySQL-backed catalog routes are optional and disabled by default. The ordinary
quick start needs no MySQL server, database migration, cloud credentials, or
external LLM service. The repository includes demo ONNX model artifacts for
the four-service workflow; some Spark tests require a separate pipeline artifact. Full-stack
setup and gateway authentication options are documented under
[Common contributor workflows](#common-contributor-workflows).

## Prerequisites

Required:

- JDK 17. The Maven enforcer rejects other Java major versions.
- Maven available as `mvn`; this repository has no Maven wrapper.
- Docker Engine with the Docker Compose v2 plugin (`docker compose`).
- A POSIX-compatible shell for the repository scripts.
- `curl` for the health checks below.

On macOS, Colima (`colima start`) or Docker Desktop can provide the daemon.
Colima is not required on Linux. Start your Docker provider before Compose.

Run all commands from the repository root.

## Five-minute quick start for running the catalog/recommendation service

Start only the Redis service needed by catalog serving:

```bash
colima start
docker context use colima
docker info
docker compose -f docker-compose.streaming.yml up -d redis-primary
```

Build the application without running tests:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
mvn package -DskipTests
```

Generate a local recommendation cursor signing key, and accept the passwordless
local Redis (every serving process refuses an unauthenticated Redis connection
unless this is set; see the [Redis auth runbook](../../docs/runbooks/redis-auth.md)):

```bash
export RECOMMENDATION_CURSOR_SIGNING_KEY="$(openssl rand -hex 32)"
export REDIS_ALLOW_NO_AUTH=true
```

Start catalog/recommendation serving:

```bash
env PORT=6010 sh scripts/run-with-jvm-tuning.sh recsys-serving -- \
  mvn exec:java -Dexec.mainClass=com.recsys.api.serving.RecSysServer
```

Keep that terminal open. In another terminal, check the catalog health
endpoint:

```bash
curl --fail http://localhost:6010/health
```

This path uses only tracked classpath data and the Redis primary; it does not
need the ignored model/Spark artifact tree. To stop the Java service, press
`Ctrl-C` in its terminal.

Stop the supporting containers while retaining the Redis volumes:

```bash
docker compose -f docker-compose.streaming.yml down
```

> **Destructive local reset:** the following command stops the stack and
> deletes its named Redis and Sentinel volumes. Use it only when you intend to
> discard local state.

```bash
docker compose -f docker-compose.streaming.yml down --volumes
```

## Services and health checks

| Service | Main class / start command | Health endpoint | Purpose |
|---|---|---|---|
| Catalog and recommendation serving (`6010`) | `com.recsys.api.serving.RecSysServer` via `mvn exec:java` | `http://localhost:6010/health` | Catalog reads, embedding recall, multi-channel recommendation, and prediction-compatible routes |
| Online prediction (`7010`) | `com.recsys.api.online.OnlinePredictionServer` via `mvn exec:java` | `http://localhost:7010/health` | Redis-backed online features, recent behavior, trending recall, and online recommendation |
| Model serving (`8080`) | `com.recsys.api.rest.ModelApplication` via `mvn spring-boot:run` | `http://localhost:8080/health/ready` | Spring Boot ONNX inference, ranking, version management, caching, and load shedding |
| API gateway (`8010`) | `com.recsys.api.gateway.MicroserviceGatewayServer` via `mvn exec:java` | `http://localhost:8010/health` | Routing, upstream health aggregation, authentication, rate limiting, and circuit breaking |

Use liveness to answer “is the process alive?” and readiness to answer “should
this process receive new work?” The online and model services expose
`/health/live` and `/health/ready`; the catalog also exposes `/health/ready`.

The stable status semantics are:

| Endpoint | Status | Stable reason | Contributor action |
|---|---:|---|---|
| Catalog `/health` | `200` | The service handler is available. | Use `/health/ready` when checking admission readiness. |
| Catalog `/health/ready` | `503` | The catalog load shedder is draining. | Wait for in-flight work to fall or inspect the catalog log. |
| Online `/health/live` | `200` | The online-serving process is alive. | Use readiness before sending recommendation traffic. |
| Online `/health` or `/health/ready` | `503` | The instance is draining because of utilization or shutdown. | Stop new requests and inspect `online-serving.log`. |
| Model `/health/live` | `200` | The Spring Boot process is alive. | Use readiness before sending inference traffic. |
| Model `/health/ready` | `503` | `model not loaded`, `shutting down`, `overloaded`, `high failure rate`, or `high inference latency`. | Use the returned reason and `model-serving.log`; do not rely on sampled counters. |
| Gateway `/health` | `503` | At least one configured upstream health check is down, so aggregate status is degraded. | Probe ports `6010`, `7010`, and `8080`, then inspect the matching log. |

Health bodies contain live counters, latency, timestamps, and route details.
Those values are intentionally not copied into this README because they vary
on every run. The gateway health contract is described in the
[API gateway investigation](../../docs/system_design/09_API_Gateway.md).

## Common contributor workflows

### Start the full local stack

The checkout ships a runnable demo model bundle, all tracked in git:
`src/main/resources/dssm_model.onnx` (the two-tower ONNX model) and
`src/main/resources/artifacts/model/{training,test}/feature_config.json` (the
per-variant vocabularies). Model serving starts against them as *legacy*
bundles — there is no `model_manifest.json`, so it logs one warning per variant
that checksums are unverified. Production bundles come from the training
pipeline, not from this repository, and are published as immutable,
manifest-backed generations under `RECSYS_MODEL_ARTIFACTS_DIR`; the
[model artifact rollout runbook](../../docs/runbooks/model-artifact-rollout.md) has
the layout, the manifest schema, and how a bad bundle fails. Every configured
A/B variant needs a bundle from the same pipeline. A default-variant bundle that
fails validation makes model serving **fail during startup**, and the gateway
aggregate health endpoint stays `503`.

To use the checked-in demo artifacts, start the infrastructure and four services:

```bash

export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export RECOMMENDATION_CURSOR_SIGNING_KEY="$(openssl rand -hex 32)"

mvn clean package -DskipTests &&
docker compose -f docker-compose.streaming.yml up -d &&
GATEWAY_ALLOW_ANONYMOUS=true sh scripts/run-microservices-local.sh
```

Structured Splunk log shipping is optional. See the
[Splunk HEC logging runbook](../../docs/runbooks/splunk-hec-logging.md) to start the local
collector and web UI, configure stable credentials, verify HEC ingestion, troubleshoot
local startup, and clean up the stack.

> **Development only:** anonymous mode deliberately disables gateway
> authentication. Never use it in production. Without anonymous mode, API keys,
> or Cognito configuration, the gateway fails closed at startup. Configure a
> production mechanism using the [Configuration Guide](../../CONFIG_GUIDE.md).

The script waits ten seconds before the gateway and writes one log per service
under `logs/`. Check the aggregate only after all three backends are ready:

```bash
curl --fail http://localhost:8010/health
```

Press `Ctrl-C` in the script terminal to terminate its four child processes.

### Start one service

Run one command per terminal. Start Redis first for the catalog and online
services. Start all three backends before the gateway if you want the gateway
aggregate health check to return success.

Each command below carries its own environment, so it works in a fresh
terminal. Two settings are shared by design: use **one** signing key for every
catalog, online, and model instance in the local topology (a mismatch makes one
instance reject the cursors another issued), and opt in to the passwordless
local Redis, because the catalog, online, and model services all refuse an
unauthenticated Redis connection at startup (the gateway only opens Redis when
`SERVICE_REGISTRY_ENABLED=true`). `scripts/run-microservices-local.sh` sets the
Redis opt-in itself; these per-service commands do not, which is why it appears
in each of them.

Generate the signing key once, then paste the same value into every terminal:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export RECOMMENDATION_CURSOR_SIGNING_KEY="$(openssl rand -hex 32)"
echo "$RECOMMENDATION_CURSOR_SIGNING_KEY"   # reuse this value in the other terminals
```

If your local Redis has a password, replace `REDIS_ALLOW_NO_AUTH=true` with
`REDIS_PASSWORD=<password>` in each command.

Catalog and recommendation serving:

```bash
env PORT=6010 REDIS_ALLOW_NO_AUTH=true \
  RECOMMENDATION_CURSOR_SIGNING_KEY="$RECOMMENDATION_CURSOR_SIGNING_KEY" \
  sh scripts/run-with-jvm-tuning.sh recsys-serving -- \
  mvn exec:java -Dexec.mainClass=com.recsys.api.serving.RecSysServer
```

Check it with `curl --fail http://localhost:6010/health`.

Online prediction:

```bash
env ONLINE_DEMO_PORT=7010 REDIS_ALLOW_NO_AUTH=true \
  RECOMMENDATION_CURSOR_SIGNING_KEY="$RECOMMENDATION_CURSOR_SIGNING_KEY" \
  sh scripts/run-with-jvm-tuning.sh online-serving -- \
  mvn exec:java -Dexec.mainClass=com.recsys.api.online.OnlinePredictionServer
```

Check it with `curl --fail http://localhost:7010/health/ready`.

Model serving:

```bash
env SERVER_PORT=8080 REDIS_ALLOW_NO_AUTH=true \
  RECOMMENDATION_CURSOR_SIGNING_KEY="$RECOMMENDATION_CURSOR_SIGNING_KEY" \
  sh scripts/run-with-jvm-tuning.sh model-serving -- \
  mvn spring-boot:run
```

This command uses the checked-in demo model bundle by default. Custom bundles
must meet the requirements described in the full-stack workflow. Check it
with `curl --fail http://localhost:8080/health/ready`.

API gateway:

```bash
env GATEWAY_PORT=8010 GATEWAY_ALLOW_ANONYMOUS=true \
  sh scripts/run-with-jvm-tuning.sh api-gateway -- \
  mvn exec:java -Dexec.mainClass=com.recsys.api.gateway.MicroserviceGatewayServer
```

Check it with `curl --fail http://localhost:8010/health`; it returns `503`
until all three backends answer their health checks, or set
`GATEWAY_UPSTREAM_HEALTHCHECK_ENABLED=false` to run the gateway with some
backends absent.

The anonymous setting in the gateway command is development-only. The wrapper
loads the repository's checked-in JVM options. Use
`mvn package -DskipTests` for a fast rebuild and the all-service script when
you do not need process-level isolation.

### Inspect processes and logs

Show container state and recent infrastructure logs:

```bash
docker compose -f docker-compose.streaming.yml ps
docker compose -f docker-compose.streaming.yml logs --tail=100 \
  redis-primary kafka jobmanager
```

Follow all Java service logs:

```bash
tail -f logs/recsys-serving.log \
  logs/model-serving.log \
  logs/online-serving.log \
  logs/api-gateway.log
```

### Load sample online features

With Redis on `localhost:6379` and `redis-cli` installed:

```bash
sh streaming/online-serving/scripts/load_online_features.sh
```

It loads the checked-in sample; see the
[online-serving guide](../../streaming/online-serving/README.md) for Flink.

## Testing

The Maven default excludes JUnit tests tagged `load` or `docker`. This keeps
ordinary local and pull-request validation deterministic and independent of a
container daemon.

Validate the Java version and dependency convergence:

```bash
mvn --batch-mode validate
```

Run the focused deterministic resilience profile used by the pull-request
workflow:

```bash
mvn --batch-mode -Presilience test
```

Run the complete ordinary test suite:

```bash
mvn --batch-mode test
```

The resilience profile uses an explicit test allow-list in `pom.xml`; a new
test class is not automatically part of the PR gate. It includes model artifact
and ONNX contract checks, variant fallback, recall metrics, deployment manifests,
GET-only gateway health probes, and LLM streaming regressions. It does not opt
into the load or Docker tags. Real-model `UserTowerInferenceServiceTest` and
deadline-sensitive `MultiChannelRecallServiceTest` remain outside this profile.

For LLM proxy changes, run `mvn -Dtest='Llm*Test' test`. The
[SSE guide](../../docs/system_design/16_SSE_Streaming.md#maintaining-the-llm-proxy-tests)
explains the shared fixtures and the limits of the concurrency check.

### Known clean-checkout artifact limitation

The model-serving fixtures are tracked — `src/main/resources/dssm_model.onnx`
and both `src/main/resources/artifacts/model/<variant>/feature_config.json`
files — so the model service and its test classes run on a clean checkout. One
fixture is still not tracked:

```text
src/main/resources/artifacts/pyspark/als_model_metadata.json
```

Consequently, ordinary `mvn --batch-mode test` has two Spark-fixture errors in
`ModelArtifactLocatorTest` on a clean checkout
(`openSpark_classpath_readsContent` and
`resolveSparkPath_classpathExploded_returnsExistingFilesystemPath`). Restore
that file from a known-good PySpark pipeline output, or point
`RECSYS_SPARK_ARTIFACTS_DIR` at a directory holding it; this checkout has no
artifact-preparation script, and the `offline-embedding` profile generates only
Word2Vec item embeddings. `RECSYS_MODEL_ARTIFACTS_DIR` and
`RECSYS_SPARK_ARTIFACTS_DIR` are both bound in
[application.yml](../../src/main/resources/application.yml). This README does not
claim that the Spark-fixture-dependent tests pass.

### Opt-in load suite

The `load` group contains bounded characterization and load-evidence tests.
It is not an ordinary pre-commit suite:

```bash
mvn --batch-mode test -DexcludedGroups=docker -Dgroups=load
```

Expect this group to consume more CPU and time than the default suite. Keep
Docker-tagged tests excluded so the result represents the load boundary only.

### Opt-in Docker suite

The `docker` group contains Testcontainers and infrastructure integration
tests. It requires a working Docker daemon:

```bash
mvn --batch-mode test -DexcludedGroups=load -Dgroups=docker
```

Keep load-tagged tests excluded so the result represents the Docker integration
boundary only. Some tests carry both tags and are therefore reserved for an
explicit combined environment, not either isolated command above.

Scheduled load and Docker jobs add evidence-output settings and upload the
resulting reports. See the
[fault-tolerance evidence section](../../docs/system_design/18_Fault_Tolerance.md#7-proving-the-failure-paths),
the [pull-request resilience workflow](../../.github/workflows/resilience-pr.yml),
and the [scheduled resilience workflow](../../.github/workflows/resilience-scheduled.yml)
for the maintained commands and artifact paths.

## Configuration

The clean-clone quick start uses the catalog port and Redis defaults. The
full-stack command explicitly sets
`GATEWAY_ALLOW_ANONYMOUS=true` for local development; its script supplies the
standard service ports and local gateway upstreams. Catalog, Spring model, and
online recommendation serving also require
`RECOMMENDATION_CURSOR_SIGNING_KEY`; the quick-start and full-stack commands
generate a local key with OpenSSL. Production rotations follow the
[shared-key runbook](../../docs/runbooks/recommendation-cursor-key-rotation.md).

The local settings most often overridden are:

| Setting | Default | Local use |
|---|---:|---|
| `PORT` | `6010` | Catalog/recommendation service port |
| `ONLINE_DEMO_PORT` | `7010` | Online prediction service port |
| `SERVER_PORT` | `8080` | Spring Boot model-serving port |
| `GATEWAY_PORT` | `8010` | API gateway port |
| `REDIS_HOST` / `REDIS_PORT` | `localhost` / `6379` | Host Redis connection |
| `GATEWAY_ALLOW_ANONYMOUS` | `false` | Development-only opt-in used by the full-stack command |

Do not copy service, resilience, authentication, or deployment variables into
new README tables. The authoritative defaults, parsing behavior, Kubernetes
overrides, and secret-handling guidance are in the
[Configuration Guide](../../CONFIG_GUIDE.md).

The shared container image selects a service with `RECSYS_MAIN_CLASS`;
Kubernetes manifests set that value per workload. Local Maven commands invoke
their main class directly.

MySQL remains off in the standard workflow. If you enable it, follow the
requirements for `MYSQL_URL`, `MYSQL_USER`, `MYSQL_PASSWORD`, and
`MYSQL_CURSOR_SIGNING_KEY` in the Configuration Guide before starting the
catalog service. In particular `MYSQL_URL` must set `sslMode=VERIFY_IDENTITY`
unless the host is loopback, or the service refuses to start.

## Troubleshooting

### Docker or infrastructure is unavailable

Start your Docker provider, then inspect the daemon and containers:

```bash
docker version
docker compose -f docker-compose.streaming.yml ps
docker compose -f docker-compose.streaming.yml logs --tail=200 \
  zookeeper kafka redis-primary jobmanager
```

On macOS with Colima, use `colima status` and `colima start`.
If intentionally discarding corrupted or incompatible local Redis state, use
the destructive volume-reset command from the quick start.

### A Java service exits during startup

Read the service-specific log:

```bash
tail -n 200 logs/recsys-serving.log
tail -n 200 logs/model-serving.log
tail -n 200 logs/online-serving.log
tail -n 200 logs/api-gateway.log
```

Common causes are a missing Redis connection, a port already in use, an
unsupported Java version, invalid opt-in configuration, or missing model
artifacts. The catalog service's MySQL configuration is validated only when
MySQL is enabled.

### The gateway health check returns `503`

Probe the backend health endpoints directly:

```bash
curl --fail http://localhost:6010/health
curl --fail http://localhost:7010/health/ready
curl --fail http://localhost:8080/health/ready
```

The gateway intentionally returns `503` when any configured upstream route is
down. Start the missing backend or follow the stable readiness reason in its
own log and health response.

The gateway's data-path endpoint checks and `/health` aggregation both use
`GET`. Catalog and online health handlers reject `HEAD` with `405`, so use the
commands above rather than `curl -I`. If `/health` reports a backend UP but
requests return `503` with `no healthy endpoint`, check the endpoint-probe logs
and deployed gateway version; the old HEAD-based probe caused this mismatch.
See [gateway health aggregation](../../docs/system_design/09_API_Gateway.md#6-health-aggregation).

### Background state is stale while liveness remains healthy

For online serving, inspect `recsys_loop_seconds_since_success{loop}` and
`recsys_loop_failures_total{loop}` for `shard-topology-refresh`, `learner-flush`,
and `redis-feature-version-sampler`. An age of `-1` means no successful run yet;
a growing age means no recent success. `RecsysLoopStale` alerts on prolonged
staleness. These loops retain their schedules after recoverable body failures,
including JVM Errors, but a live process alone does not prove they are making
progress. See [fault-tolerance operations](../../docs/system_design/18_Fault_Tolerance.md)
for the alert thresholds and Error-boundary limits.

### An LLM stream closes while waiting for tokens

For optional LLM routes, check `LLM_SSE_KEEPALIVE_MS` in the
[Configuration Guide](../../CONFIG_GUIDE.md). Existing overrides above `10000` now
fail gateway startup; remove the override to use the default or reduce it.
Non-positive values disable heartbeats.

The gateway emits SSE comments only after a complete frame boundary. Waiting
for upstream headers or an unfinished frame can still hit an intermediary's
timeout. See the [CDN troubleshooting steps](../../docs/runbooks/cdn-operations.md#llm-streams-close-during-a-quiet-gap)
and [SSE timing details](../../docs/system_design/16_SSE_Streaming.md).

### A port is already in use

Identify the listening process, stop the previous local run, or use the
documented port override consistently:

```bash
lsof -nP -iTCP:6010 -sTCP:LISTEN
```

Changing a backend port also requires changing the corresponding gateway
upstream URL. See the Configuration Guide rather than changing only one side.


### Which serving rules are applied

The rule layer that decides whether a candidate may be shown at all. Detail, and the four
incompatible definitions of "already consumed", in
[17_Scalability §2](../../docs/system_design/17_Scalability.md#2-overload-protection--the-layers-that-let-it-scale-without-collapsing)
and [18_Fault_Tolerance §3](../../docs/system_design/18_Fault_Tolerance.md#3-graceful-degradation--a-degraded-answer-beats-no-answer).

| Rule | Status |
|---|---|
| Deduplication | ✅ applied on all three serving paths |
| Already-consumed removal | ⚠️ applied, but **four incompatible definitions** (3 recent on 7010, 20 on 8080, full history on 6010, own-ratings map in one channel) |
| Freshness | ⚠️ applied in recall, **reversed in ranking** — strict tiering puts every out-of-vocab item below every in-vocab one |
| Availability | ⚠️ **name only** — `addIfAvailable` checks model-vocabulary membership, not whether an item is published or licensed |
| Diversity | ❌ absent — channel quotas bound candidate *sources*, not content |
| Eligibility | ❌ absent — no region, age, tier or entitlement check |
| Safety / moderation | ❌ absent — no blocklist, maturity rating or suppression list |
| Sponsored content | ❌ absent — no promoted slots or organic/paid separation |
| Frequency caps | ❌ absent — nothing tracks how often an item was *shown* |

Most absences are data-model gaps first: `Movie` is `(id, title, year, genres)`, so four of
the five have no field to read — each is "acquire and plumb a signal, then filter", not "add a
filter". Diversity is the exception, needing only `genres`. The carrier exists and is inert:
`MovieCandidate.features` and `RankedMovie.features` are `Map<String, Object>` threaded from
recall through ranking and are `Map.of()` at every construction site but one. There is **no
`Rule`, `Filter` or `Policy` interface anywhere in `src/main/java`** — each rule lives wherever
it was first needed, which is the mechanical reason one rule acquired four definitions. This is
a demonstration system: "absent" means *not implemented*, not *broken*.

**"Already consumed" means four different things**, so the same user asking the same question
has 3, 20, or all of their history excluded depending on which port answers:

| Definition | Source of truth | Applied by |
|---|---|---|
| **Entire watch history** | `DataManager.getWatchedMovieIds` — classpath ratings, static | catalog 6010 `RecommendationService.V1`; `CandidateGenerator.byEmbedding` / `byUserHistory` |
| **3 most recent** | live `OnlineFeatureStore` | online 7010 `OnlineRecommendationService.RECENT_HISTORY_LIMIT = 3` |
| **20 most recent** | live `OnlineFeatureStore` | model 8080 `ModelRetrievalStage.RECENT_EXCLUDE_LIMIT = 20` |
| **The user's own rating map** | in-memory CF matrix | `Channels.UserSimilarity` (`currentRatings.containsKey(movieId)`) |

It is also inconsistent *within* a single 7010 request — the `Embedding` channel excludes the
full classpath watch history while the quota merge excludes only the 3 live recent items.
Enforcement, once a set exists, is correct (`legacyMerge`, `quotaMerge`, `ColdStartChannel`,
`OnnxInferencePipeline`); the defect is upstream in deciding the contents.

**On the canonical route, caller exclusions never reach recall.** `POST /api/recommend` routes
to 7010's `/v2/recommend`, and `OnlineRecommendationRequest` is `(userId, window, k)` — nowhere
to put them — so `OnlineBlendingPipeline` applies the caller's list to the *finished ranked
list*. Recall and ranking spend their budget on candidates then discarded.
`ModelRetrievalStage.withRecentExclusions` is the correct shape one service over: it merges
caller + recent history into the query *before* recall, and `catch (RuntimeException e)`
degrades instead of failing.

Three more qualifications behind the ⚠️ rows. **`addIfAvailable` checks
`ModelArtifactService.getAvailableItemIds()`**, which returns the model's item-vocab keys — so
"available" means *scoreable*, not published or licensed, and it runs only on 8080's Redis-down
fallback. **`RankingStage`'s strict tiering** appends every out-of-vocab candidate below every
in-vocab one, by design ("so the two score scales never need reconciling") — sound reason,
undocumented consequence: a new item cannot outrank anything the model knows until the next
artifact, so freshness is bounded by retraining cadence and the upstream recency boost is inert
whenever an in-vocab candidate is present. **Quotas cap candidate *sources*, not content**: the
2026-06-15 cold-start design named the gap ("No mechanism guarantees diversity or cold-start
coverage") and only the coverage half shipped, so nothing bounds genre spread — and since the
embedding channel retrieves neighbours of one user vector, a single-genre block is the expected
case. Detail on all of these lives with the mechanisms in
[17_Scalability §2](../../docs/system_design/17_Scalability.md#2-overload-protection--the-layers-that-let-it-scale-without-collapsing)
and [18_Fault_Tolerance §3](../../docs/system_design/18_Fault_Tolerance.md#3-graceful-degradation--a-degraded-answer-beats-no-answer).

These findings are a **code audit**, not a behavioural test: the 3-item claim comes from
`RECENT_HISTORY_LIMIT` and its call sites, not from a response, and the ordering consequence is
derived from `RankingStage`'s code and comment rather than measured against a real artifact.

See the [README documentation index](../../README.md#documentation) for
architecture investigations, operational runbooks, and contribution checks.
