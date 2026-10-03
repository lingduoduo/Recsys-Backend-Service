# Online Serving

This streaming path is intentionally separate from the main Armeria movie API and the Spring Boot model-artifact service.
It shows an online or near-real-time recommendation path where:

```text
LogCollector -> Kafka -> OnlineJoiner -> ExperienceCollector -> OnlineLearner -> serving parameters
                                        |
                                        +-> training streams / HDFS
                         |
                         +-> Flink -> Redis ─┐
                                             ├─> OnlineRecommendationService -> online prediction server
user embeddings (ANN) ───────────────────────┘
```

The streaming path uses:

- `LogCollector` as the app/API boundary that validates behavior logs and emits Kafka-ready JSON lines
- Kafka as the event ingress layer
- `OnlineJoiner` as the sample builder that joins logs with user, item, and context features
- `ExperienceCollector` as the list builder that groups joined samples into ranked recommendation experiences
- `OnlineLearner` as the online-training loop that updates lightweight serving parameters from experiences
- Flink (`OnlineFeatureStreamingJob`) to write per-user history and Top-K trending into Redis
- Redis as the online feature store
- `OnlineRecommendationEngine` for real-time behavioral scoring
- `CandidateGenerator.byEmbedding` for embedding-based ANN recall
- `OnlineRecommendationService` to blend both signals before serving
- `OnlinePredictionServer` (Armeria, port `7010`) as the HTTP layer

Today the repo includes both:

- a real Java Flink job in `com.recsys.streaming.flink`
- replay scripts for loading the same Redis shape without running Flink

That keeps the streaming path runnable without coupling it to the model-artifact service.

## Production Sizing Notes

Use these dimensions when translating the demo into a production online recommendation service:

| Dimension | Target / assumption | Notes |
|---|---:|---|
| DAU | `200w+` system daily active users | Keep online user state bounded and expire inactive keys with TTLs |
| Peak QPS | `8k` recommendation reads/s | Scale `OnlinePredictionServer` horizontally and keep Redis reads batched or cached |
| Peak TPS | Bursty behavior-log writes | Send logs to Kafka/MQ first so ingestion does not block recommendation reads |
| Data scale | Recent history, Top-K windows, engagement metrics, embeddings, learned serving parameters | Split durable offline artifacts from fast-changing Redis online features |
| Machine scale | API replicas, Kafka partitions, Flink task slots, Redis shards/replicas | Scale each layer independently based on QPS, TPS, memory footprint, and latency SLOs |

For the `200w+` DAU and `8k` peak-QPS case, Redis is the low-latency online feature store, not the primary shock absorber for raw events. MQ/Kafka handles traffic spikes first, Flink consumes at a controlled rate, and Redis receives compact aggregate updates. This Redis + MQ peak-shaving design protects the read path while preserving fresh behavioral features for serving.

Runtime services:

- `OnlineServingMetricsService` records rolling QPS, latency, failures, rejected requests, and per-strategy `failureRate` and `share` (traffic mix). The strategy map is capped at 50 entries.
- `OnlineLoadShedder` caps in-flight requests with `ONLINE_MAX_CONCURRENT_REQUESTS` and sheds excess traffic with HTTP `429`; `retryAfterSeconds()` returns `1` when draining so callers can set a `Retry-After` header.
- `OnlineCapacityService` exposes `ONLINE_TARGET_DAU`, `ONLINE_PEAK_QPS`, and `ONLINE_PEAK_TPS` alongside observed QPS, remaining `headroomQps`, and an `overloaded` flag.
- `/online/ops` returns the combined metrics/load/capacity snapshot with a `servedAt` ISO-8601 timestamp; also sets `Retry-After` on the HTTP response when the shedder is draining.
- `/health` returns `503` when the instance crosses its drain utilization threshold.

Related production concerns:

- **Latency:** measure p50/p95/p99 for HTTP serving, Redis lookups, candidate generation, and ranking separately.
- **Freshness:** alert on Kafka lag, Flink checkpoint failures, and stale Redis feature timestamps.
- **Availability:** keep the API stateless, use Redis replicas or cluster mode, and keep a cached fallback path for trending results.
- **Hot keys:** shard feature keys by user/item/window where needed, and cap list or sorted-set sizes to keep Redis operations bounded.
- **Load shedding:** reject or downgrade expensive requests when p99 latency or queue depth crosses the service budget.
- **Consistency:** prefer MQ + idempotent consumers + eventual consistency over distributed transactions across Kafka, Flink, and Redis.

## Files

- `streaming/online-serving/docker-compose.yml`
- `streaming/online-serving/data/movie_events.ndjson`
- `streaming/online-serving/scripts/load_online_features.sh`
- `streaming/online-serving/scripts/produce_movie_events.sh`
- `streaming/online-serving/scripts/key_movie_events.jq` — jq filter normalizing/validating each event's `userId` into the Kafka record key
- `streaming/online-serving/scripts/test_produce_movie_events.sh` — smoke test for the keyed producer script
- `src/main/java/com/recsys/streaming/LogCollector.java`
- `src/main/java/com/recsys/streaming/OnlineJoiner.java`
- `src/main/java/com/recsys/streaming/ExperienceCollector.java`
- `src/main/java/com/recsys/streaming/OnlineLearner.java`
- `src/main/java/com/recsys/streaming/*`
- `src/main/java/com/recsys/streaming/flink/*`

## Start Infra

```bash
docker compose -f streaming/online-serving/docker-compose.yml up -d
```

Services:

- Kafka on `localhost:9092`
- Redis on `localhost:6379`
- Flink UI on `http://localhost:8081`

## Produce Kafka Events

```bash
sh streaming/online-serving/scripts/produce_movie_events.sh
```

That publishes the bundled event stream from `movie_events.ndjson` into the 24-partition Kafka topic
`movie_events_v2`. Each record is keyed by its normalized positive user ID so one user's events remain
in partition order. `KAFKA_PARTITIONS` defaults to `24` and the script rejects any other value for this
topic generation. After topic creation it also describes the broker topic and aborts before producing
if an existing topic has a different partition count. Event values retain the original NDJSON line bytes.
In a real app, `LogCollector` is the counterpart to this replay script: product surfaces call it with
click, view, like, or order logs, then its JSON output is written to Kafka.
`OnlineJoiner` is the next step: it combines those behavior records with user, item, and request context
features and assigns labels for online and offline training streams.
`ExperienceCollector` then groups joined point samples by `event.requestId`, restores item order from
`event.rank`, and emits one list-shaped training record per recommendation request.
`OnlineLearner` consumes those experiences and updates bounded in-memory item-bias parameters used during
serving-time ranking. This is online learning over stream samples, not PyTorch/ONNX retraining.

Processing guarantees:

- event types and training labels are centralized in `EventSemantics`
- feature maps are trimmed, null-safe, and deterministic before they are joined
- joined sample features are namespaced as `user.*`, `item.*`, `context.*`, and `event.*`
- recommendation experiences are grouped by `userId + event.requestId` and duplicate movie feedback keeps the strongest label
- online learning updates small serving parameters continuously, while offline embedding/model artifacts remain on the batch training path
- Kafka/Flink delivery is treated as at-least-once; the streaming job deduplicates events by `eventId` before updating online features
- Redis feature writes are eventually consistent and guarded by `:updated_at` companion keys, so older retries or windows do not overwrite newer snapshots

## Run The Flink Job

The Flink job is implemented in:

- `com.recsys.online.flink.OnlineFeatureStreamingJob`

It supports:

- Kafka source via `--bootstrap.servers ... --topic movie_events_v2`
- local file replay via `--input-file streaming/online-serving/data/movie_events.ndjson`

Build the Flink profile:

```bash
mvn -Pstreaming-flink -DskipTests compile
```

Run from the bundled file:

```bash
mvn -Pstreaming-flink exec:java \
  -Dexec.mainClass="com.recsys.online.flink.OnlineFeatureStreamingJob" \
  -Dexec.args="--input-file streaming/online-serving/data/movie_events.ndjson --redis.host localhost --redis.port 6379 --window-seconds 10 --window-label last_hour --top-k 10 --idempotency-ttl-seconds 86400"
```

Run from Kafka:

```bash
mvn -Pstreaming-flink exec:java \
  -Dexec.mainClass="com.recsys.online.flink.OnlineFeatureStreamingJob" \
  -Dexec.args="--bootstrap.servers localhost:9092 --topic movie_events_v2 --group.id online-features-v2 --checkpoint-dir s3://recsys-flink/checkpoints --expected-topic-partitions 24 --source-parallelism 24 --operator-parallelism 24 --max-parallelism 128 --top-k-bucket-count 24 --final-top-k-parallelism 1 --top-k-allowed-lateness-ms 5000 --watermark-idle-timeout-ms 30000 --redis.host localhost --redis.port 6379 --window-seconds 10 --window-label last_hour --top-k 10 --idempotency-ttl-seconds 86400"
```

Flink consistency knobs:

| Argument | Default | Purpose |
|---|---:|---|
| `--idempotency-ttl-seconds` | `86400` | How long Flink state remembers processed `eventId` values for duplicate suppression |
| `--checkpoint-dir` | none | Approved shared durable URI; mandatory for normal Kafka and bridge execution |
| `--allow-local-checkpoint-storage` | `false` | Local/Docker test override; forbidden in production |
| `--bridge-mode` | `false` | Legacy replay mode with a distinct source UID/group; requires an explicit legacy topic |
| `--bridge-replay-cutoff-ms` | none | Required positive cutoff for bridge Kafka offsets and parsed event-time filtering |
| `--bridge-reference-time-ms` | none | Required cutover reference used for per-operator logical event-time expiry |
| `--expected-topic-partitions` | `24` | Startup fence: broker topic must have exactly 24 partitions |
| `--source-parallelism` | `24` | Kafka source parallelism; cannot exceed the expected partition count |
| `--operator-parallelism` | `24` | Parallelism for keyed state, partial Top-K, and Redis sinks |
| `--max-parallelism` | `128` | Stable keyed-state ceiling used when restoring/rescaling |
| `--top-k-bucket-count` | operator parallelism (`24`) | Number of movie-hash buckets feeding partial Top-K |
| `--final-top-k-parallelism` | `1` | Final event-time Top-K merge parallelism, capped at operator parallelism |
| `--top-k-allowed-lateness-ms` | `5000` | Time retained for late partial results after a window watermark |
| `--watermark-idle-timeout-ms` | `30000` | Marks silent source partitions idle so they cannot stall event-time windows |
| `--user-history-ttl-seconds` | `86400` | TTL for `user:<id>:recent_movies` and its `:updated_at` guard key |
| `--metric-ttl-seconds` | `3600` | TTL for movie metric, Top-K, and their `:updated_at` guard keys |
| `--mq.max-poll-records` | `500` | Kafka consumer batch size for MQ peak shaving |
| `--mq.fetch-min-bytes` | `1024` | Minimum bytes fetched per Kafka request; increase to favor batching over latency |
| `--mq.fetch-max-wait-ms` | `500` | Max broker wait for `fetch.min.bytes`, bounding async ingestion latency |

This path intentionally does not use a distributed transaction spanning MQ, Flink state, and Redis. Kafka absorbs retries and bursts; Flink checkpointing plus event-id deduplication handles duplicate delivery; Redis receives compact feature snapshots that converge to the newest `updatedAtMillis` value.

Kafka execution fails fast without approved shared durable checkpoint storage; only local file replay may omit it. Bridge replay is state-only: it attaches no Redis or other production side-effect sink, starts from earliest with source UID `kafka-movie-events-bridge-v1`, and retains the exact future-compatible downstream contracts. Graph UIDs are migration contracts. This v2 artifact's normal Kafka source UID is exactly `kafka-movie-events-v2`; stable downstream UIDs include `event-idempotency-v1`, `recent-movies-v1`, `user-embedding-feature-v1`, `session-feature-v1`, `movie-metrics-v1`, `topk-partial-v1`, and `topk-final-v1`, plus the sink UIDs. `-n` may omit only the unmatched bridge source state; it does not discard matched downstream state. Direct restore of legacy generated operator IDs is forbidden. A future v3 topic requires a new artifact with source UID `kafka-movie-events-v3`, not a `--topic` change on the v2 artifact. The event-time Top-K watermark allows five seconds of out-of-order events and uses 30-second partition idleness by default.

The opt-in Kafka/Flink load guard targets **50,000 events/s**, requires completed checkpoints with none failed and zero final consumer lag, and requires ordinary-user partition skew (declared hot users excluded) to stay at or below **twice the median** active-partition volume. Run it only with Docker available. Focused non-Docker coverage is `mvn -Pstreaming-flink test -Dtest=KafkaTopicPartitionValidatorTest,OnlineFeatureStreamingJobTest,MovieEventTest`; deduplication cases live in `OnlineFeatureStreamingJobTest` rather than a separate test class. For a production migration, follow the [Kafka partition cutover runbook](../../docs/runbooks/kafka-partition-cutover.md), including generation-specific source state, producer fencing, savepoint restore with `--allowNonRestoredState`, phase-dependent recovery, and delayed old-topic retirement.

What the job writes to Redis:

- `user:<id>:recent_movies`
- `movie:<id>:views_1h`
- `movie:<id>:clicks_1h`
- `movie:<id>:likes_1h`
- `movie:<id>:orders_1h`
- `topk:last_hour`

Each Redis feature key also has a companion freshness key:

- `user:<id>:recent_movies:updated_at`
- `movie:<id>:<metric>:updated_at`
- `topk:<window>:updated_at`

Recent history is reserved for stronger feedback such as click, like, order, or a view with meaningful
watch time, matching the Online Joiner split between raw logs and labeled samples.

## Load Online Features Into Redis

```bash
sh streaming/online-serving/scripts/load_online_features.sh
```

That script replays the sample Redis state from `src/main/java/com/recsys/data/online_features.txt`.
Use it as a fallback when you want the serving side without running the Flink job.

Redis keys written by the script:

- `user:<id>:recent_movies`
- `movie:<id>:views_1h`
- `movie:<id>:likes_1h`
- `topk:last_hour`

## Run The Separate Online Server

```bash
mvn clean compile
sh scripts/run-with-jvm-tuning.sh online-serving -- \
  mvn exec:java -Dexec.mainClass="com.recsys.streaming.OnlinePredictionServer"
```

Optional env vars:

```bash
ONLINE_DEMO_PORT=7010 REDIS_HOST=localhost REDIS_PORT=6379 \
  sh scripts/run-with-jvm-tuning.sh online-serving -- \
  mvn exec:java -Dexec.mainClass="com.recsys.streaming.OnlinePredictionServer"
```

Online-serving env vars:

| Env var | Default | Purpose |
|---|---:|---|
| `ONLINE_DEMO_PORT` | `7010` | Online Armeria server port |
| `REDIS_HOST` | `localhost` | Redis host for online features and Top-K |
| `REDIS_PORT` | `6379` | Redis port |
| `ONLINE_REQUEST_TIMEOUT_MS` | `500` | End-to-end request deadline |
| `ONLINE_MAX_CONCURRENT_REQUESTS` | `64` | Pre-queue in-flight request cap before the service returns HTTP `429` |
| `ONLINE_DRAIN_UTILIZATION` | `0.90` | In-flight utilization where `/health/ready` returns `503` so load balancers can drain the node |
| `ONLINE_REDIS_RATE_LIMIT_QPS` | `0` | Optional Redis-backed cross-instance request limit; `0` disables distributed rate limiting |
| `ONLINE_REDIS_RATE_LIMIT_WINDOW_SECONDS` | `1` | Redis rate-limit window size |
| `ONLINE_FEATURE_CACHE_MAX_USERS` | `10000` | Max users kept in the short-TTL recent-history JVM cache |
| `ONLINE_FEATURE_STALE_TTL_MS` | `60000` | Maximum stale recent-history age served during Redis errors |
| `ONLINE_TOPK_STALE_TTL_MS` | `60000` | Maximum stale Top-K age served during Redis errors |
| `ONLINE_METRICS_WINDOW_SECONDS` | `60` | Rolling metrics window for QPS, latency, failures, rejected requests, and strategy mix |
| `ONLINE_TARGET_DAU` | `2000000` | Capacity target shown by `/online/ops` |
| `ONLINE_PEAK_QPS` | `8000` | Peak recommendation read-QPS target shown by `/online/ops` |
| `ONLINE_PEAK_TPS` | `20000` | Peak behavior-event TPS target shown by `/online/ops` |
| `REDIS_POOL_MAX_TOTAL` | `50` | Maximum Redis connections per process |
| `REDIS_POOL_MAX_WAIT_MS` | `250` | Fail-fast wait when the Redis pool is exhausted |
| `REDIS_TIMEOUT_MS` | `2000` | Redis connect/socket timeout; set below the request deadline for online serving |

Health and metrics endpoints:

- `/health/live` reports process liveness and remains healthy while a pod drains.
- `/health/ready` reports whether the pod should receive new traffic.
- `/health` remains a readiness-compatible alias.
- `/metrics` exposes Armeria request metrics in Prometheus format.

## Recommendation Strategy

`OnlineRecommendationService` blends two recall sources on every request:

| Source | Signal | Weight |
|---|---|---|
| `OnlineRecommendationEngine` | Real-time: recent-history similarity + trending rank | 1.0 |
| `CandidateGenerator.byEmbedding` | Offline: ANN search on user-tower embeddings | 0.5 |
| `OnlineLearner` | Online: learned item-bias adjustment from recent experiences | dynamic |

Each recall source contributes a normalized rank score `weight × (n − rank) / n`; learned online item-bias parameters are then added as small score adjustments. Movies that appear in multiple paths accumulate scores and surface at the top. Recently-watched movies are excluded from the final output.

When no embedding exists for the user (cold-start), the service falls back to the online path only. The response includes a `strategy` field (`"online+model"` or `"online"`) so callers can observe which signals fired.

## Try It

Inspect the online feature snapshot:

```bash
curl "http://localhost:7010/online/features?userId=123&window=last_hour&k=5"
```

Request blended recommendations:

```bash
curl "http://localhost:7010/online/recommendation?userId=123&window=last_hour&k=5"
```

Check readiness and capacity telemetry:

```bash
curl "http://localhost:7010/health"
curl "http://localhost:7010/online/ops"
```

Example response shape:

```json
{
  "user": { "userId": 123, "name": "Alice" },
  "window": "last_hour",
  "strategy": "online+model",
  "recentMovies": [...],
  "trendingMovies": [...],
  "recommendations": [...]
}
```

Example `/online/ops` response shape:

```json
{
  "servedAt": "2026-05-17T10:00:00.123Z",
  "metrics": {
    "totalRequests": 12,
    "successCount": 11,
    "failureCount": 1,
    "rejectedCount": 0,
    "recentRequests": 12,
    "qps": 0.2,
    "recentAvgLatencyMs": 8.5,
    "recentFailureRate": 0.083,
    "recentRejectedRate": 0.0,
    "strategies": {
      "online+model": {
        "requests": 10,
        "failureCount": 1,
        "avgLatencyMs": 9.0,
        "failureRate": 0.1,
        "share": 0.833
      },
      "online": {
        "requests": 2,
        "failureCount": 0,
        "avgLatencyMs": 5.0,
        "failureRate": 0.0,
        "share": 0.167
      }
    }
  },
  "load": {
    "inFlightRequests": 0,
    "maxConcurrentRequests": 512,
    "utilization": 0.0,
    "drainUtilization": 0.9,
    "acceptedRequests": 12,
    "rejectedRequests": 0,
    "suggestedWeight": 100,
    "retryAfterSeconds": 0
  },
  "rateLimit": {
    "enabled": false,
    "limit": 0,
    "windowSeconds": 1
  },
  "capacity": {
    "targetDau": 2000000,
    "peakQps": 8000,
    "peakTps": 20000,
    "observedQps": 0.2,
    "qpsUtilization": 0.000025,
    "headroomQps": 7999.8,
    "overloaded": false
  }
}
```

This models the production split where:

- offline jobs build durable model assets such as PyTorch-exported compact ONNX, vocab/config metadata, and pretrained item embeddings
- pretrained item embeddings are stored in Redis as key-value records, which keeps the online model artifact smaller and easier to deploy
- Flink keeps short-lived behavioral features fresh in Redis
- online learning updates fast-changing function parameters from streaming samples, such as blending weights, recency decay, trending boosts, exploration rates, and thresholds
- `OnlineRecommendationService` fuses offline artifacts, online features, and learned serving parameters at request time

## Stop Infra

```bash
docker compose -f streaming/online-serving/docker-compose.yml down
```

### Redis failover in Kubernetes

For the in-cluster Redis tier, writers must use Sentinel discovery. The `redis-primary`
Service is bootstrap-only and can route to a read-only replica after an election; the
static `redis` alias is no longer deployed. Configure the driver with `REDIS_MODE=sentinel`,
`REDIS_SENTINEL_MASTER=mymaster`, and `REDIS_SENTINEL_NODES` from `recsys-config`, plus the
existing Redis credentials. Both jobs capture these values and serialize them to workers.

Flink supports `--redis.mode sentinel --redis.sentinel-master mymaster
--redis.sentinel-nodes sentinel-a:26379,sentinel-b:26379`. Spark ItemEmbeddingJob uses
`--redis-mode=sentinel --redis-sentinel-master=mymaster
--redis-sentinel-nodes=sentinel-a:26379,sentinel-b:26379`. Host/port arguments apply only in
standalone mode, which remains appropriate for local demo scripts and ElastiCache.
Explicit arguments override driver environment settings. Never use bootstrap Services as
application write hosts. See [the deployment upgrade steps](../../docs/system_design/04_Replication.md).
