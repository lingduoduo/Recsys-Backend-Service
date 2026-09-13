# Configuration Guide

This is the authoritative reference for contributor- and operator-facing
runtime controls explicitly read by the shipped services, shared container, and
local launcher. It covers direct Java environment reads, named Spring
placeholders, and the custom Spring property bindings used by model serving.
The [README](README.md) keeps only the settings needed for local workflows.

Defaults below are application defaults when no variable is supplied. Values in
[`k8s/base/configmap.yaml`](k8s/base/configmap.yaml) or an individual workload
manifest override those defaults for the base deployment. Put passwords, token
signing keys, API keys, and similar secrets in a Secret rather than a ConfigMap.
Generic Spring Boot switches, CI/test-only knobs, individual dynamic
`FEATURE_FLAG_*` names, and secret values generated or provisioned outside this
repository are outside this inventory.

## Local development essentials

The clean-clone quick start in the README starts only Redis and catalog serving.
The artifact-dependent four-service path runs
`GATEWAY_ALLOW_ANONYMOUS=true sh scripts/run-microservices-local.sh`; that is an
explicit development-only authentication choice. Redis connection and
per-service port overrides are in the next two sections.

Catalog and online recommendation serving require a shared cursor signing key.
For local development, generate one before starting either service or the
four-service launcher:

```bash
export RECOMMENDATION_CURSOR_SIGNING_KEY="$(openssl rand -hex 32)"
```

| Setting | Default | Use |
|---|---|---|
| `RECSYS_MAIN_CLASS` | `com.recsys.api.gateway.MicroserviceGatewayServer` in the image | Selects the Java main class for the shared container image. Local Maven commands invoke their main class directly. |
| `CORS_ALLOWED_ORIGIN` | unset | Optional catalog-serving CORS origin. |
| `SERVICE_REGISTRY_ENABLED` | `false` | Enables Redis service registration and gateway registry lookup. Registration occurs only when this is true and both settings below are nonblank. |
| `SERVICE_REGISTRY_SERVICE_NAME` / `SERVICE_REGISTRY_ADVERTISE_URL` | unset / unset | Registered service name and advertised URL; neither setting registers a service unless `SERVICE_REGISTRY_ENABLED=true`. |
| `SERVICE_REGISTRY_HEARTBEAT_MS` / `SERVICE_REGISTRY_TTL_MS` | `10000` / `30000` ms | Registrar heartbeat interval and Redis registry-record TTL. |
| `SERVICE_REGISTRY_REFRESH_MS` | `10000` ms | Gateway registry refresh interval when registry lookup is enabled; a non-positive value performs only the initial refresh. |
| `GATEWAY_START_DELAY_SECONDS` | `10` seconds | Local-script delay before launching the gateway. This is a shell `sleep` value, not an application setting; invalid values stop the launcher. |

## Service selection and ports

The image runs the class named by `RECSYS_MAIN_CLASS`; Kubernetes sets it per
workload. The image default is the gateway, while the base manifests select the
following classes: catalog `com.recsys.api.serving.RecSysServer`, online
`com.recsys.api.online.OnlinePredictionServer`, model
`com.recsys.api.rest.ModelApplication`, gateway
`com.recsys.api.gateway.MicroserviceGatewayServer`, outbox relay
`com.recsys.application.outbox.OutboxRelayCommand`, and reconciliation job
`com.recsys.application.reconciliation.ReconciliationCommand`.

| Variable | Default | Consumer and parsing |
|---|---:|---|
| `PORT` | `6010` | Catalog/recommendation server port. A nonblank non-integer stops startup. |
| `ONLINE_DEMO_PORT` | `7010` | Online-serving port. A non-integer falls back to `7010`. |
| `SERVER_PORT` | `8080` | Spring Boot model-serving port (standard Spring `server.port` environment binding). |
| `GATEWAY_PORT` | `8010` | Gateway port. A nonblank non-integer stops startup. |
| `OUTBOX_RELAY_PORT` | `7020` | Outbox relay HTTP port; the base deployment explicitly sets `7020`. |

`RECSYS_MAIN_CLASS` has no application-level fallback: the container entrypoint
executes its value. Do not change a service port without also changing the
corresponding Service, probe, and gateway upstream configuration.

## Redis and streaming infrastructure

| Variable | Default | Use |
|---|---:|---|
| `REDIS_MODE` | `standalone` | `sentinel` selects Sentinel discovery; any other value uses standalone Redis. The base ConfigMap sets `sentinel`; the EKS patch sets `standalone`. |
| `REDIS_HOST` | `localhost` | Standalone Redis host. The base ConfigMap sets `redis`; the EKS patch requires an ElastiCache primary endpoint. |
| `REDIS_PORT` | `6379` | Standalone Redis port. A non-integer falls back to `6379`. |
| `REDIS_PASSWORD` | empty | Redis password; provide through a Secret when used. |
| `REDIS_SENTINEL_MASTER` | `mymaster` | Sentinel master name. |
| `REDIS_SENTINEL_NODES` | `localhost:26379` | Comma-separated Sentinel `host:port` nodes. |
| `REDIS_REPLICA_NODES` | unset | Comma-separated read-replica `host:port@az` nodes; omitted means reads stay on the primary. |
| `AWS_AZ` / `AVAILABILITY_ZONE` | `unknown` | Local AZ used to prefer same-AZ replicas. |
| `REDIS_TIMEOUT_MS` | `2000` | Positive Redis connect/socket timeout; model serving caps it to its request budget. Base online-serving sets `200`. |
| `REDIS_POOL_MAX_TOTAL` | `50` | Positive per-process Redis connection-pool maximum. Base online-serving sets `64`. |
| `REDIS_POOL_MAX_IDLE` | `10` | Positive pool idle maximum. Base online-serving sets `16`. |
| `REDIS_POOL_MIN_IDLE` | `2` | Non-negative pool idle minimum. Base online-serving sets `4`. |
| `REDIS_POOL_MAX_WAIT_MS` | `250` | Positive pool-acquisition wait. Base online-serving sets `100`. |
| `REDIS_POOL_TEST_ON_BORROW` | `true` | Whether to validate borrowed pooled connections. |
| `REDIS_EMBEDDING_MGET_BATCH_SIZE` | `500` | Catalog/model embedding MGET batch size. Invalid integers use `500`; values below `1` clamp to `1`. |
| `REDIS_LOADALL_TIMEOUT_MS` | `30000` ms | Wall-clock budget for model Redis embedding `SCAN`/`MGET` startup loads. Invalid integers use `30000`; negative values clamp to `0`, and `0` disables the budget. |
| `ONLINE_FEATURE_REDIS_MGET_BATCH_SIZE` | `500` | Online feature-store Redis read batch size. |
| `REDIS_REPLICA_LAG_PROBE_SECONDS` | `10` seconds | Online-serving replica-lag sampling interval. Invalid integers use `10`; zero or negative values fail startup. |
| `REDIS_FEATURE_VERSION_SAMPLE_LIMIT` / `REDIS_FEATURE_VERSION_SAMPLE_SECONDS` | `1000` keys / `30` seconds | Online-serving feature-version sample bound and interval. Invalid integers use the defaults; either value below `1` fails startup. |
| `ONLINE_EVENTS_SQS_ENABLED` / `ONLINE_EVENTS_SQS_QUEUE_URL` | `false` / unset | Enable online-event SQS publishing only with a queue URL. |
| `ONLINE_EVENTS_KAFKA_ENABLED` / `ONLINE_EVENTS_KAFKA_BOOTSTRAP_SERVERS` / `ONLINE_EVENTS_KAFKA_TOPIC` | `false` / unset / `movie_events_v2` | Enable online-event Kafka publishing only with bootstrap servers; records are keyed by `userId`. |
| `ASYNC_EVENT_QUEUE_CAPACITY` / `ASYNC_EVENT_BATCH_SIZE` | `10000` / `100` | Bounded queue and drain batch shared by online-event and model A/B-exposure publishers. Invalid integers use the defaults; values below `1` clamp to `1`. |
| `FLINK_CHECKPOINT_DIR` | unset | Optional checkpoint location for the opt-in Flink job. |

## Recommendation and model serving

| Variable | Default | Use |
|---|---:|---|
| `RECSYS_VECTOR_BACKEND` / JVM `-Drecsys.vector.backend` | `lsh` | Catalog vector backend (`lsh`/`ann`, `exact`/`flat`, or portable `faiss` fallback to LSH). The JVM property wins; an unknown value stops catalog startup. |
| `LOCAL_EMBEDDING_CACHE_MAX_ENTRIES` | `100000` | JVM LRU capacity for catalog embeddings. |
| `RECALL_CHANNEL_TIMEOUT_MS` | `200` | Per-channel recall deadline shared by catalog and online serving. |
| `RECALL_BULKHEAD_QUEUE_CAPACITY` | four times the recall pool size | Bounded recall queue shared by catalog and online serving. |
| `RECOMMENDATION_CURSOR_SIGNING_KEY` | none (required) | Active HMAC key shared byte-for-byte by catalog, Spring model, and online recommendation serving in every region. Must contain at least 32 UTF-8 bytes. Provision it through a Secret; never place key material in a ConfigMap. |
| `RECOMMENDATION_CURSOR_PREVIOUS_KEY` | unset | Optional shared previous HMAC key accepted during rotation. When set, it must contain at least 32 UTF-8 bytes. Follow the [two-stage rotation runbook](docs/runbooks/recommendation-cursor-key-rotation.md) and remove it only after the maximum cursor age. |
| `RECOMMENDATION_CURSOR_MAX_AGE_SECONDS` | `900` | Signed-cursor lifetime in whole seconds. Required range: 1–86400. The base ConfigMap sets `900`. |
| `RECOMMENDATION_CURSOR_ACCEPT_LEGACY` | `true` | Temporarily accepts unsigned `v2` recommendation cursors. Only exact case-insensitive `true` or `false` is valid; other nonblank values stop startup. The base ConfigMap sets `true`. |
| `RECOMMENDATION_PAGINATION_MAX_CANDIDATES` | `500` | Per-request recall/ranking candidate ceiling shared by catalog, Spring model, and online recommendation paths. Required integer range: 101–10000. The base ConfigMap sets `500`. |
| `CATALOG_MAX_CONCURRENT_REQUESTS` | `64` | Catalog in-flight admission cap. Base ConfigMap sets `64`. |
| `CATALOG_DRAIN_UTILIZATION` | `0.90` | Catalog utilization threshold for drain readiness. Base ConfigMap sets `0.90`. |
| `ONLINE_REQUEST_TIMEOUT_MS` | `500` | Online-serving request deadline. Base ConfigMap sets `500`. |
| `ONLINE_USER_EMB_SOFT_TTL_SECONDS` | `30` | Online user-embedding logical-expiry soft TTL. |
| `ONLINE_TOPK_CACHE_TTL_MS` | `2000` | Local hot-key cache TTL for sharded trending data. |
| `ONLINE_FEATURE_CACHE_MAX_USERS` | `10000` | Maximum online feature keys retained in the JVM cache. |
| `ONLINE_FEATURE_STALE_TTL_MS` / `ONLINE_TOPK_STALE_TTL_MS` | `60000` / `60000` | Maximum stale feature/top-K age served during Redis errors. Base ConfigMap sets both to `60000`. |
| `SHARDED_RECORD_SHARD_COUNT` | `2` | Bootstrap shard count for online sharded records. |
| `SHARD_TOPOLOGY_REFRESH_SECONDS` | `30` | Per-instance shard-topology refresh interval. |
| `SHARDED_RECORD_MAX_TTL_SECONDS` | `86400` | Previous-generation dual-read period after a reshard. |
| `SHARD_ADMIN_TOKEN` | unset | Secret guarding online shard-topology and operations endpoints; unset fails those surfaces closed. |
| `MYSQL_ENABLED` | `false` | Enables the MySQL catalog path; see the durable-event requirements below. |
| `MYSQL_URL` / `MYSQL_USER` / `MYSQL_PASSWORD` | local JDBC URL / `recsys` / empty | Required and nonblank (including password) when MySQL is enabled. Supply the password from a Secret. **`MYSQL_URL` must also carry `sslMode=VERIFY_IDENTITY`** (spelled exactly that way — Connector/J property names are case-sensitive) and must not carry the deprecated `useSSL`; loopback hosts (`localhost`, `127.0.0.1`, `[::1]`) are exempt. The service **refuses to start** otherwise, because Connector/J's `PREFERRED` default falls back to plaintext without error. |
| `MYSQL_CURSOR_SIGNING_KEY` | empty | At least 32 UTF-8 bytes when MySQL is enabled; keep it in a Secret. |
| `MYSQL_QUERY_TIMEOUT_SECONDS` | `2` | Required range: 1–30. |
| `MYSQL_READ_MAX_ATTEMPTS` | `2` | Required range: 1–2. |
| `MYSQL_READ_RETRY_BACKOFF_MS` | `50` | Required range: 0–1000 ms. |
| `MYSQL_POOL_MAX_SIZE` / `MYSQL_POOL_MIN_IDLE` | `5` / `1` | Hikari maximum and minimum idle connections. |
| `MYSQL_POOL_CONNECTION_TIMEOUT_MS` / `MYSQL_POOL_IDLE_TIMEOUT_MS` / `MYSQL_POOL_MAX_LIFETIME_MS` | `10000` / `60000` / `1800000` | Hikari connection, idle, and lifetime timeouts. |
| `ONLINE_DURABLE_EVENTS_ENABLED` | `false` | Enables durable online feature-event acceptance; requires `MYSQL_ENABLED=true` and the consistency-token secret in the next row. |
| `ONLINE_CONSISTENCY_TOKEN_SECRET` | unset | At least 32 UTF-8 bytes for durable consistency tokens; use a Secret. |
| `RECSYS_MODEL_ARTIFACTS_DIR` | classpath artifacts | External model-variant artifact root; may be a symlink to an immutable generation directory. A `<variant>/model_manifest.json`, when present, is strict: checksums, `model_version` agreement with `feature_config.json`, and the ONNX input/output contract are verified before readiness, and a malformed manifest never falls back to legacy loading. See `docs/runbooks/model-artifact-rollout.md`. |
| `RECSYS_SPARK_ARTIFACTS_DIR` | classpath artifacts | External PySpark artifact root, including `als_model_metadata.json`; set it when a packaged JAR needs a filesystem path for Spark artifacts. |
| `RECSYS_MODEL_FILE` | `dssm_model.onnx` | Model artifact filename for **legacy** (manifest-less) bundles only; a manifest's `model_file` is authoritative. |
| `RECSYS_MODEL_ITEM_EMBEDDINGS_SOURCE` | `classpath` | `classpath` or `redis`. |
| `RECSYS_MODEL_REDIS_ITEM_EMBEDDING_PREFIX` | `i2vEmb` | Redis key prefix when model item embeddings use Redis. |
| `RECSYS_MODEL_ONNX_INTRA_OP_THREADS` / `RECSYS_MODEL_ONNX_INTER_OP_THREADS` | `1` / `1` | ONNX Runtime intra- and inter-operation thread counts. Both must be positive. |
| `RECSYS_MODEL_ONNX_EXECUTION_MODE` | `SEQUENTIAL` | ONNX Runtime execution mode: `SEQUENTIAL` (conservative default) or `PARALLEL`. |
| `RECSYS_MODEL_RECALL_CORE_THREADS` | computed | Recall executor core thread count. `0` selects `max(1, availableProcessors * 2)`; negative values fail startup. |
| `RECSYS_MODEL_RECALL_QUEUE_CAPACITY` / `RECSYS_MODEL_RECALL_TIMEOUT_MS` | `256` / `200` ms | Bounded recall queue capacity and task timeout. Both must be positive. |
| `RECSYS_RECOMMENDATION_CACHE_ENABLED` / `RECSYS_RECOMMENDATION_CACHE_TTL_SECONDS` / `RECSYS_RECOMMENDATION_CACHE_MAX_ENTRIES` | `true` / `300` / `10000` | Model-serving response cache switch, positive TTL, and positive capacity (maximum `100000`). Invalid bound values fail Spring startup. |
| `RECSYS_RECOMMENDATION_CACHE_COLD_START_ENABLED` / `RECSYS_RECOMMENDATION_CACHE_COLD_START_TTL_SECONDS` / `RECSYS_RECOMMENDATION_CACHE_COLD_START_MAX_K` | `true` / `3600` / `100` | Shared cold-start cache switch, positive TTL, and maximum result size in `1..100`. |
| `RECSYS_RECOMMENDATION_CACHE_COMPUTE_WAIT_TIMEOUT_MILLIS` | `2000` ms | Maximum wait for an in-flight duplicate cache computation; valid range `1..60000`. |
| `RECSYS_AB_BUCKET_A_PERCENT` / `RECSYS_AB_BUCKET_B_PERCENT` | `20` / `20` | Model A/B allocation percentages. Each is non-negative and their sum must be at most `100`, otherwise Spring startup fails. |
| Spring `recsys.ab-test.enabled`, `layer-name`, `bucket-a-variant`, `bucket-b-variant`, `default-variant` | `false` / `default` / `test` / `training` / `training` | Controls assignment and loaded variants. Layer and variant names must be nonblank. The named percentage environment aliases are in the preceding row. |
| `RECSYS_MODEL_RATE_LIMIT_RPS` / `RECSYS_MODEL_RATE_LIMIT_BURST` / `RECSYS_MODEL_RATE_LIMIT_MAX_USERS` | `0.0` / `0` / `10000` | Per-user, per-model-replica token bucket. Positive rate and burst are both required to enable it; tracked-user capacity clamps to at least `1`. Malformed Spring numeric binding fails startup. |
| `RECSYS_HEALTH_MAX_CONCURRENT_REQUESTS` | `64` | Model per-instance in-flight cap. Base ConfigMap sets `8` (one batched ONNX run per admitted request on one intra-op thread; 64 against a 2-CPU limit is oversubscription, not throughput). |
| `RECSYS_HEALTH_MAX_IN_FLIGHT_UTILIZATION` | `0.95` | Model readiness drain threshold. Base ConfigMap sets `0.95`. |
| `RECSYS_EVENTS_SQS_ENABLED` / `RECSYS_EVENTS_SQS_QUEUE_URL` / `RECSYS_EVENTS_SQS_REGION` | `false` / unset / `AWS_REGION` or `us-east-1` | Model A/B exposure SQS publishing; needs a queue URL. |
| `RECSYS_EVENTS_KAFKA_ENABLED` / `RECSYS_EVENTS_KAFKA_BOOTSTRAP_SERVERS` / `RECSYS_EVENTS_KAFKA_EXPOSURE_TOPIC` | `false` / unset / `ab_exposures` | Model A/B exposure Kafka publishing; needs bootstrap servers. |

## Gateway, authentication, and LLM integration

Gateway upstream defaults are `http://localhost:6010` for catalog,
`http://localhost:8080` for model, and `http://localhost:7010` for online.
The base ConfigMap overrides the compatibility, user, movie, and feature route
names. Other active route names below retain their localhost defaults unless a
deployment supplies them. Every configured value must be a URI with a scheme
and host; malformed values stop gateway startup.

| Variable | Default | Use |
|---|---:|---|
| `CATALOG_SERVICE_URL` / `MODEL_SERVICE_URL` / `ONLINE_SERVICE_URL` | ports `6010` / `8080` / `7010` on localhost | Backward-compatible gateway upstreams for catalog, model, and online routes. |
| `EMBED_RECALL_SERVICE_URL` / `MODEL_INFERENCE_SERVICE_URL` / `ONLINE_BLEND_SERVICE_URL` / `SEQUENTIAL_SERVICE_URL` | ports `6010` / `8080` / `7010` / `8080` on localhost | Gateway recommendation-route upstreams. |
| `USER_PROFILE_SERVICE_URL` / `MOVIE_METADATA_SERVICE_URL` / `FEATURE_SERVICE_URL` / `KNOWLEDGE_SERVICE_URL` | ports `6010` / `6010` / `7010` / `8080` on localhost | Gateway user, movie, feature, and knowledge-route upstreams. |
| `GATEWAY_TIMEOUT_MS` | `3000` | Gateway upstream deadline. Base ConfigMap sets `3000`. |
| `GATEWAY_API_KEYS` | unset | Comma-separated API keys; enable through a Secret. |
| `GATEWAY_COGNITO_ISSUER` | unset | Enables Cognito JWT validation. |
| `GATEWAY_COGNITO_AUDIENCE` | unset | Required when the issuer is set. |
| `GATEWAY_COGNITO_TOKEN_USE` | `access` | Comma-separated accepted token-use values. |
| `GATEWAY_ALLOW_ANONYMOUS` | `false` | Explicit local-development escape hatch when neither API keys nor Cognito is configured. Do not enable in production. The base ConfigMap sets `true`; the EKS patch changes it to `false`. |
| `GATEWAY_PUBLIC_PATHS` | `/health` | Comma-separated boundary-matched public paths. Base adds two non-sensitive catalog reads; do not use a broad catalog prefix. |
| `GATEWAY_ORIGIN_SECRET` | unset | Comma-separated accepted origin secrets; enables direct-origin rejection. Keep in a Secret. |
| `GATEWAY_DEPRECATION_SUNSET` | unset | ISO-8601 date published as the `Sunset` header on unversioned `/api` paths and the `/api/catalog`, `/api/model`, `/api/online` aliases. Unset or unparseable disables deprecation headers. Base ConfigMap sets `2027-07-27`. |
| `GATEWAY_UPSTREAM_HEALTHCHECK_ENABLED` / `GATEWAY_UPSTREAM_HEALTHCHECK_INTERVAL_MS` | `true` / `10000` | Enables and schedules GET-based gateway upstream health checks. Catalog and online health handlers reject HEAD with 405; unhealthy endpoints are excluded from request selection. |
| `RECSYS_LOGIN_API_KEYS` (Spring `recsys.login.api-keys`) | empty | Comma-separated API keys for model-serving `/api/v1/auth/login`; empty disables that login endpoint. Keep keys in a Secret. This is separate from gateway authentication. |
| `RECSYS_SUBMIT_TOKEN_ENABLED` / `RECSYS_SUBMIT_TOKEN_TTL_SECONDS` / `RECSYS_SUBMIT_TOKEN_KEY_PREFIX` | `false` / `300` / `submit_token:` | Optional one-use Redis token protection for model-serving recommendation submits. TTL must be `1..86400`; a blank prefix resets to `submit_token:`. |
| `LLM_SERVICE_URL` / `LLM_EXPLANATION_SERVICE_URL` | unset | Register the optional LLM / explanation routes only when explicitly configured. |
| `LLM_TIMEOUT_MS` / `LLM_CONNECT_TIMEOUT_MS` / `LLM_IDLE_TIMEOUT_MS` / `LLM_PING_INTERVAL_MS` | `120000` / `2000` / `60000` / `20000` ms | LLM proxy request, connect, idle, and ping timeouts. |
| `LLM_SSE_KEEPALIVE_MS` | `10000` ms | SSE comment idle threshold and scheduler period. Maximum `10000`; larger values fail construction when an LLM route is configured. Non-positive values disable heartbeats. |
| `LLM_CACHE_MAX_SIZE` / `LLM_CACHE_TTL_SECONDS` | `500` / `300` | LLM response-cache capacity and TTL. |
| `LLM_TOKEN_RATE_LIMIT_TPS` / `LLM_TOKEN_RATE_LIMIT_BURST` / `LLM_DEFAULT_TOKEN_ESTIMATE` / `LLM_MAX_RETRY_WAIT_MS` | `0` / `0` / `1000` / `30000` ms | LLM token admission, request token estimate, and retry-wait controls. Both token-limit values must be positive to enable the bucket. |
| `FEATURE_FLAG_ENVIRONMENT_PREFIX` | `FEATURE_FLAG_` | Prefix for environment-backed model feature flags. |
| `POSTHOG_FEATURE_FLAGS_ENABLED` / `POSTHOG_PROJECT_API_KEY` / `POSTHOG_HOST` / `POSTHOG_FEATURE_FLAGS_TIMEOUT` / `POSTHOG_FEATURE_FLAGS_CACHE_TTL` | `false` / empty / `https://us.i.posthog.com` / `2s` / `60s` | Optional PostHog feature-flag provider settings. Keep the API key in a Secret. |

For LLM streams, `LLM_SSE_KEEPALIVE_MS` leaves margin below the CDN script's
30-second origin read timeout. The idle check can defer a comment until nearly
twice the interval after upstream data. Remove or reduce existing overrides
above `10000` before rollout. Heartbeats require `text/event-stream` and a
complete frame boundary; they do not protect waiting for upstream headers,
partial frames, non-SSE responses, or event-loop stalls. `LLM_PING_INTERVAL_MS`
configures HTTP/2 PINGs on the gateway-to-upstream connection and does not
replace these client-facing comments. See the
[SSE guide](docs/system_design/16_SSE_Streaming.md) and
[CDN runbook](docs/runbooks/cdn-operations.md#llm-streams-close-during-a-quiet-gap).

## Resilience and overload controls

| Variable | Default | Use |
|---|---:|---|
| `ONLINE_MAX_CONCURRENT_REQUESTS` / `ONLINE_DRAIN_UTILIZATION` | `64` / `0.95` | Online per-replica admission cap and readiness-drain threshold. Parse failures use application defaults; capacity clamps to at least `1` and finite utilization values to `0..1`. The base ConfigMap overrides utilization to `0.90` (and capacity to `64`). |
| `ONLINE_REDIS_RATE_LIMIT_QPS` / `ONLINE_REDIS_RATE_LIMIT_WINDOW_SECONDS` | `0` / `1` | Redis-backed cross-instance online rate limit. A QPS of `0` disables this limiter and its emergency bucket. Base ConfigMap sets `200` and `1`. |
| `ONLINE_REDIS_EMERGENCY_LIMIT_ENABLED` | `true` | Exact case-insensitive `true` or `false`; any other nonblank value fails online-serving startup. |
| `ONLINE_REDIS_EMERGENCY_RATE_PER_SECOND` | `max(1, floor(QPS / 4))` | Finite non-negative decimal. Invalid, negative, or non-finite values fail startup. Base ConfigMap sets `50`. |
| `ONLINE_REDIS_EMERGENCY_BURST` | emergency rate rounded down, minimum `1` | Non-negative integer. Invalid or negative values fail startup. Base ConfigMap sets `50`. |
| `GATEWAY_RATE_LIMIT_RPS` / `GATEWAY_RATE_LIMIT_BURST` | `0` / `0` | Gateway token-bucket rate and burst per route and authenticated principal. Base ConfigMap sets `100` and `200`. |
| `GATEWAY_RATE_LIMIT_<ROUTE>_RPS` / `GATEWAY_RATE_LIMIT_<ROUTE>_BURST` | unset (inherit the global values) | Per-route gateway rate/burst override; base config sets the model route to `50` / `100`. |
| `GATEWAY_RL_MAX_PRINCIPALS` | `100000` | Maximum cached gateway principal buckets. |
| `GATEWAY_CB_FAILURE_THRESHOLD` / `GATEWAY_CB_COOLDOWN_MS` | `5` / `10000` | Gateway upstream circuit-breaker threshold and cooldown. Base ConfigMap sets both. |

The emergency bucket is constructed only when a Redis executor exists, online
Redis QPS is positive, the enable flag is true, and both emergency rate and
burst are positive. It is a local, per-online-serving-replica budget used only
when Redis limiting fails, returns a malformed decision, or its circuit cannot
admit the Redis call. Setting the flag to `false`, or either rate or burst to
`0`, disables that bucket as a rollback; while the Redis limiter itself remains
active, its Redis-error path is then unlimited fail-open. Exhaustion of an
active emergency bucket follows the normal rate-limit rejection (`429` with a
positive `Retry-After`); this setting is not a cluster-wide limit.

## Observability

| Variable | Default | Use |
|---|---:|---|
| `ONLINE_METRICS_WINDOW_SECONDS` | `60` | Rolling online-serving metrics window. |
| `ONLINE_TARGET_DAU` / `ONLINE_PEAK_QPS` / `ONLINE_PEAK_TPS` | `2000000` / `8000` / `20000` | Online capacity-planning inputs used by the operations surface. |
| Spring `recsys.health.window-seconds` / `recsys.health.min-sample-size` | `60` / `5` | Positive model-serving readiness metrics window and minimum sample count. |
| `RECSYS_HEALTH_MAX_FAILURE_RATE` / `RECSYS_HEALTH_MAX_AVG_LATENCY_MS` | `0.5` / `2000` ms | Model readiness thresholds; failure rate must be `0..1` and latency positive. Demo defaults — `k8s/base/model-serving.yaml` sets `0.05` / `500`. |
| `SPRING_APPLICATION_NAME` | `recsys-model-serving` | Spring application/metrics identity for model serving. |
| `RECSYS_SHUTDOWN_TIMEOUT` | `30s` | Spring graceful-shutdown phase timeout; must be a valid duration. |
| `MANAGEMENT_ENDPOINTS_EXPOSURE` | `health,info,prometheus` | Comma-separated Spring Actuator web exposure list. Do not expose sensitive endpoints without access controls. |
| `RECSYS_EXECUTOR_SHUTDOWN_TIMEOUT_MS` | `5000` ms | Graceful executor shutdown window; an invalid value falls back to the default. |

## Deployment-only settings

These settings belong in a Kustomize overlay or a Secret-backed deployment
environment rather than a contributor's shell.

| Variable | Base deployment value or default | Use |
|---|---:|---|
| `OUTBOX_KAFKA_BOOTSTRAP_SERVERS` / `OUTBOX_KAFKA_ONLINE_TOPIC` | `kafka:9092` / `online-events` | Kafka destination for the durable outbox relay. |
| `OUTBOX_DELIVERY_DEADLINE_MS` | `5000` | Per-send and relay-cycle deadline. |
| `OUTBOX_RELAY_BATCH_SIZE` / `OUTBOX_RELAY_MAX_CONCURRENT_SENDS` | `100` / `16` | Rows claimed per cycle and bounded concurrent sends. |
| `OUTBOX_RELAY_LEASE_SECONDS` / `OUTBOX_RELAY_POLL_MS` / `OUTBOX_RELAY_MAX_ATTEMPTS` | `30` / `500` / `8` | Relay claim lease, poll cadence, and delivery-attempt limit. |
| `OUTBOX_RELAY_READINESS_MAX_BACKLOG` | `100000` | Backlog ceiling for relay readiness. |
| `RECONCILIATION_WINDOW_HOURS` / `RECONCILIATION_MAX_BATCH` / `RECONCILIATION_LEASE_SECONDS` | `24` / `500` / `300` | Reconciliation window, batch size, and per-event lease. |
| `RECONCILIATION_REPAIR` | `false` | Report-only by default; turn on only after operational review. |
| `SAGA_EVENTS_SQS_ENABLED` / `SAGA_EVENTS_SQS_QUEUE_URL` / `SAGA_EVENTS_SQS_BEST_EFFORT` | `false` / empty / `false` | Optional saga SQS transition publishing; requires queue configuration and IAM permission. |
| `HOSTNAME` | generated `relay-<UUID>` / `reconciler-<UUID>` | Platform-provided worker identity used for outbox and reconciliation leases. |
| JVM `-DWATCHDOG_THREADS` | `2` | Shared Redis-lock renewal scheduler size; invalid values use `2` and values below `1` clamp to `1`. An environment variable of this name alone is not read. |
| `JAVA_OPTS` | workload-specific | Base manifests provide per-workload heap settings; tune with deployment resources, not a local default. |

For the exact base values and workload-specific overrides, inspect
[`k8s/base/configmap.yaml`](k8s/base/configmap.yaml) together with the matching
Deployment or Job manifest. The EKS overlay changes Redis topology and
authentication posture; review it before promoting a base configuration.
