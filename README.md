# RecSys Backend Service

A recommendation-system backend built with Java 17 and Maven. Four
independently runnable HTTP services provide catalog and recommendation APIs,
online prediction, ONNX model inference, and an API gateway. The local demo
includes Redis, Kafka, and Flink infrastructure for online features and
streaming workflows.

![RecSys backend architecture](recsys-architecture.png)

> Interactive version: [recsys-architecture.html](recsys-architecture.html)

## Features

- **Recommendation APIs:** catalog reads, embedding-based retrieval, and
  multi-channel recommendations.
- **Online prediction:** Redis-backed features, recent user behavior, and
  trending-item retrieval.
- **ONNX model serving:** inference, ranking, model version management, and
  A/B variants.
- **API gateway:** request routing, authentication, rate limiting, circuit
  breaking, and upstream health checks.
- **Serving resilience:** caching, timeouts, fallbacks, load shedding, and
  readiness checks.
- **Local streaming workflow:** sample events, replay scripts, and an opt-in
  Flink job for online features.

## Run locally

The four Java services run on your host; Redis runs in Docker. The checkout
includes demo model artifacts. MySQL and external LLM services are optional.

### 1. Prepare your environment

Install **JDK 17**, **Maven**, **Docker with Compose v2**, **curl**, and
**OpenSSL**. Run the commands below from the repository root.

Start Docker Desktop, or use Colima on macOS:

```bash
colima start
docker context use colima
```

Confirm Docker is reachable:

```bash
docker info
```

On macOS, select Java 17:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
```

On other platforms, set `JAVA_HOME` to your JDK 17 installation.

### 2. Build and start

Generate one cursor signing key for the local session, build the application,
and start Redis before launching the four services:

```bash
export RECOMMENDATION_CURSOR_SIGNING_KEY="$(openssl rand -hex 32)"

mvn clean package -DskipTests &&
docker compose -f docker-compose.streaming.yml up -d --wait redis-primary &&
GATEWAY_ALLOW_ANONYMOUS=true sh scripts/run-microservices-local.sh
```

Keep this terminal open. The script writes service logs under `logs/` and opts
in to passwordless local Redis. `GATEWAY_ALLOW_ANONYMOUS=true` disables gateway
authentication for local development; configure authentication before deployment.
Reuse the same signing key when restarting services in this session.

### 3. Check the gateway

In another terminal:

```bash
curl -i http://localhost:8010/health
```

Wait for `200` after the backends start. A `503` response lists unavailable
backends in its JSON body; the script's “starting” message does not confirm
readiness. Use the port table below to check the affected service directly.

### 4. Stop

Press `Ctrl-C` in the script terminal to stop the Java processes. Stop the
containers while keeping their data volumes:

```bash
docker compose -f docker-compose.streaming.yml down
```

## Local ports and health checks

### Application services — run on the host

| Service | Port | Port setting | Health check | Log under `logs/` |
|---|---:|---|---|---|
| API gateway | 8010 | `GATEWAY_PORT` | `http://localhost:8010/health` | `api-gateway.log` |
| Catalog and recommendations | 6010 | `PORT` | `http://localhost:6010/health/ready` | `recsys-serving.log` |
| Online prediction | 7010 | `ONLINE_DEMO_PORT` | `http://localhost:7010/health/ready` | `online-serving.log` |
| ONNX model serving | 8080 | `SERVER_PORT` | `http://localhost:8080/health/ready` | `model-serving.log` |

Changing a backend port also requires updating its gateway upstream URL. See
[configuration](CONFIG_GUIDE.md) for overrides and
[individual service commands](docs/runbooks/local-development.md#start-one-service).

### Infrastructure — run in Docker

| Component | Host port | Container port | Started by the quick start? |
|---|---:|---:|---|
| Redis primary | 6379 | 6379 | Yes |
| Kafka | 9092 | 9092 | No |
| Flink dashboard | 8081 | 8081 | No |
| ZooKeeper | Not published | 2181 | No |
| Redis replica | Not published | 6379 | No |
| Redis Sentinels (three containers) | Not published | 26379 each | No |

Kafka, Flink, and the Redis replication topology belong to the optional
streaming stack. To start it:

```bash
docker compose -f docker-compose.streaming.yml up -d
```

See the [streaming guide](streaming/online-serving/README.md) for loading sample
features and running the Flink job.

### Port conflicts or an unavailable backend

Check which containers publish host ports and inspect this stack:

```bash
docker ps --format 'table {{.Names}}\t{{.Ports}}'
docker compose -f docker-compose.streaming.yml ps -a
```

If another stack owns a required port, stop its conflicting container when it
is no longer needed. A healthy Redis container must also publish its host port
for the Java services to reach it. Check the mapping and Redis itself:

```bash
docker port redis-primary
docker exec redis-primary redis-cli ping
```

Read the affected Java service's log from the table above. A
`ClassNotFoundException` for a project class requires a successful rebuild
before restarting. More diagnostics are in the
[local troubleshooting guide](docs/runbooks/local-development.md#troubleshooting).

## Tests

```bash
mvn --batch-mode validate
mvn --batch-mode -Presilience test
```

The resilience profile runs a focused suite without Docker or load tests.
For the ordinary suite, use `mvn --batch-mode test`; two Spark fixture tests
require an artifact absent from a clean checkout. See
[test commands and fixture requirements](docs/runbooks/local-development.md#testing)
for details and the opt-in load and Docker suites.

## Repository layout

```text
.
├── src/main/java/com/recsys/
│   ├── api/                  HTTP entry points and service main classes
│   ├── application/          recommendation, gateway, model, and workflow logic
│   ├── domain/               domain records and value objects
│   ├── infrastructure/       Redis, MySQL, messaging, caches, and integrations
│   ├── online/flink/         opt-in Flink online-feature job
│   ├── health/, loadshed/    health, admission, drain, and shutdown controls
│   └── config/, metrics/     runtime parsing and serving metrics
├── src/main/resources/       Spring config, fallback ONNX, and DB migrations
├── src/test/java/com/recsys/ matching unit, contract, load, and Docker tests
├── streaming/online-serving/ local event data, replay scripts, and guide
├── scripts/                  local startup and operational helpers
├── config/jvm/               per-service local JVM option profiles
├── docker/, k8s/             local infrastructure and deployment manifests
├── docs/                     subsystem investigations and runbooks
├── .github/workflows/        deterministic and scheduled resilience CI
├── docker-compose.streaming.yml
├── CONFIG_GUIDE.md
└── pom.xml
```

## Documentation

- [Configuration guide](CONFIG_GUIDE.md) — settings, defaults, and deployment overrides.
- [Local development](docs/runbooks/local-development.md) — individual service commands, model artifacts, test profiles, and troubleshooting.
- [Streaming guide](streaming/online-serving/README.md) — events, Redis features, and Flink.

### Architecture

[System-design investigations](docs/system_design/) — the numbered investigation
directory. **Every investigation is listed here**; this section is the index, and
`DocumentationIndexTest` fails the build if a doc is added, renamed, or removed
without updating it.

| # | Investigation | Covers |
|---|---|---|
| 01 | [Load Balancing](docs/system_design/01_Load_Balancing.md) | ALB → kube-proxy/topology-aware routing → Armeria health-checked groups, and the capacity-weight feedback signal |
| 02 | [Caching](docs/system_design/02_Caching.md) | The cache classes and the per-object inventory: what each of user/item embeddings, popular-item lists, recommendation lists and candidate sets actually gets, its TTL and invalidation, the two meanings of cold start, and what a Redis outage does to each |
| 03 | [DB Scaling & Sharding](docs/system_design/03_DB_Scaling_Sharding.md) | The two Redis sharded stores, versioned topology and online reshard, which scaling lever buys what, and where sharding ends the single-transaction guarantee |
| 04 | [Replication](docs/system_design/04_Replication.md) | Single-primary Redis with AZ-aware read replicas, Sentinel failover, replica-lag probing, cross-region DR |
| 05 | [CAP](docs/system_design/05_CAP.md) | Where each store chooses consistency over availability during a partition, and the tunable dial |
| 06 | [Consistent Hashing](docs/system_design/06_Consistent_Hashing.md) | The shared FNV-1a primitive and the virtual-node ring that maps devices to shards |
| 07 | [Message Queue](docs/system_design/07_Message_Queue.md) | The fire-and-forget bounded queue carrying behavioral/experiment events off the serving path |
| 08 | [Rate Limits](docs/system_design/08_Rate_Limits.md) | One token-bucket primitive, three per-instance limiters, one global cluster limiter |
| 09 | [API Gateway](docs/system_design/09_API_Gateway.md) | Route ownership, authentication, health aggregation, circuit breakers, metrics |
| 10 | [Microservices](docs/system_design/10_MicroServices.md) | Service boundaries, one-image/four-mains deployment model, layering, and why every hop is HTTP/JSON |
| 11 | [Service Discovery](docs/system_design/11_Service_Discovery.md) | Static route table, the opt-in Redis registry, and Cloud Map resolution |
| 12 | [CDN](docs/system_design/12_CDNS.md) | What CloudFront caches, cache-key constraints, origin lockdown, and the local stand-in |
| 13 | [DB Indexing](docs/system_design/13_DB_Indexing.md) | Which secondary indexes exist, and how every query is pinned to its index by contract tests |
| 14 | [Partitioning](docs/system_design/14_Partitioning.md) | The five partition dimensions, and where the shards physically live |
| 15 | [Eventual Consistency](docs/system_design/15_Eventual_Consistency.md) | How eventual consistency manifests and is deliberately bounded per layer |
| 16 | [SSE Streaming](docs/system_design/16_SSE_Streaming.md) | The LLM-proxy SSE passthrough, its lifecycle, and why no WebSockets or gRPC |
| 17 | [Scalability](docs/system_design/17_Scalability.md) | Compute-tier HPA, data-tier levers, and the overload-protection layers that let it scale without collapsing |
| 18 | [Fault Tolerance](docs/system_design/18_Fault_Tolerance.md) | Resilience contracts, graceful drain, observability, failure-path evidence, what a JVM `Error` does at each boundary (§9), and status |
| 19 | [Pagination](docs/system_design/19_Pagination.md) | Keyset and offset implementations, and the shared signed live-keyset recommendation contract |
| 20 | [AuthN / AuthZ](docs/system_design/20_AuthN_AuthZ.md) | The six credentials, fail-closed startup, credential stripping, and the operator-token tier |

### Cross-cutting guides

- [What a Redis outage actually does](docs/system_design/02_Caching.md#9-what-happens-when-redis-goes-down)
  — per cached object, warm cache versus cold. Serve-stale is fail-open only when a last-good
  value exists; on a cold cache three of the four cache families rethrow, and the reads that
  do so sit outside every degradation layer in
  [18_Fault_Tolerance §3](docs/system_design/18_Fault_Tolerance.md#3-graceful-degradation--a-degraded-answer-beats-no-answer).
- [The serving time budget](docs/system_design/17_Scalability.md#the-request-time-budget-end-to-end)
  — every timeout on the request path in one table, read outside-in, plus the measurement
  showing `orTimeout` frees the caller but not the bulkhead worker. A budget that inverts
  between two layers is a property of neither and is invisible from inside either.
- [API versioning](docs/system_design/09_API_Gateway.md#api-versioning-and-deprecation)
  — the gateway-owned `/api/v{n}` path version, why an unversioned `/api` path is
  implicit v1, why `/v2` on the internal services means "different pipeline" rather
  than "next generation", and the `Deprecation`/`Sunset`/`Link` headers. Edge
  cache-key constraints are in
  [12_CDNS §1](docs/system_design/12_CDNS.md#1-what-is-cached-and-what-isnt).
- [API compatibility contract](docs/system_design/09_API_Gateway.md#the-compatibility-contract)
  — what counts as a breaking change, the two-version support window, twelve-month
  deprecation notice, the routes deprecated today, and the `Deprecation` / `Sunset`
  headers clients should watch.

### Operational runbooks

#### Disaster recovery

- [Regional failover](docs/runbooks/dr-regional-failover.md) — promote the standby region and capture evidence.
- [Regional failback](docs/runbooks/dr-failback.md) — return traffic to the recovered primary region.
- [Data-tier promotion](docs/runbooks/dr-data-tier-promotion.md) — restore the **write** path after DNS failover.
- [DR game day](docs/runbooks/dr-game-day.md) — rehearse and evaluate a regional recovery.
- [Zonal resilience](docs/runbooks/zonal-resilience.md) — surviving the loss of one AZ within a region.

#### Edge and CDN

- [CDN operations](docs/runbooks/cdn-operations.md) — running the CloudFront distribution in front of the gateway.
- [CDN rollback](docs/runbooks/cdn-rollback.md) — the rollout order reversed; skipping ahead strands traffic.
- [CDN local stand-in](docs/runbooks/cdn-local.md) — nginx mirror of the cache behaviors, no AWS account needed.
- [WAF WebACL](docs/runbooks/waf-webacl.md) — the out-of-band WAFv2 ACL Kustomize cannot create.
- [Local ElastiCache stand-in](docs/runbooks/elasticache-local.md) — run the eviction invariant EKS depends on, no AWS account needed.

#### Traffic and load

- [Overload protection](docs/runbooks/overload-protection.md) — overload symptoms, controls, validation, recovery.
- [Overload characterization](docs/runbooks/overload-characterization.md) — the opt-in `@Tag("load")` harnesses and the invariants they lock in.
- [Gateway auth](docs/runbooks/gateway-auth.md) — API keys, Cognito JWT, and the fail-closed startup rule.
- [Redis auth](docs/runbooks/redis-auth.md) — provisioning the shared credential, the missing-Secret failure mode, and rotation.
- [Splunk HEC logging](docs/runbooks/splunk-hec-logging.md) — shipping structured application logs to Splunk, and the at-most-once limits of what lands there.

#### Model serving

- [Model artifact rollout](docs/runbooks/model-artifact-rollout.md) — immutable manifest-backed bundles, what a pod verifies before it is ready, how a bad bundle fails, and the counter that proves the model actually ran.

#### Data and delivery

- [Serving data freshness](docs/runbooks/serving-data-freshness.md) — the online feature-view and outbox-delivery SLOs, and why a freshness gauge can read healthy while nothing is measuring it.
- [Durable eventual consistency](docs/runbooks/durable-eventual-consistency.md) — outbox, saga state, relay, consistency tokens, reconciliation.
- [Kafka partition cutover](docs/runbooks/kafka-partition-cutover.md) — moving to a new topic generation without breaking per-user ordering.
- [Cursor key rotation](docs/runbooks/recommendation-cursor-key-rotation.md) — rotating the HMAC signing key behind pagination cursors.
- [Deploy by image digest](docs/runbooks/deploy-image-digest.md) — why the EKS overlay pins an immutable digest.
- [Retire a backend](docs/runbooks/retire-backend.md) — **irreversible**; permanently shuts down all four services and their infrastructure.

**How this index stays honest.** It is complete by construction, not by discipline:
[`DocumentationIndexTest`](src/test/java/com/recsys/docs/DocumentationIndexTest.java)
asserts in both directions — every doc under `docs/system_design/` and
`docs/runbooks/` is linked here, and every `docs/…` link here resolves to a file that
exists. Adding a doc without indexing it fails the build, and so does renaming one
without updating the link. Historical design records under `docs/superpowers/` are
deliberately out of scope; they are point-in-time artifacts, not topics.

## Before opening a pull request

From a clean working tree, run at least:

```bash
mvn --batch-mode validate
mvn --batch-mode test
git diff --check
```

If the change touches circuit breakers, bulkheads, load shedding, rate
limiting, graceful shutdown, degraded recall, outbox, or saga behavior, also
run:

```bash
mvn --batch-mode -Presilience test
```

Run the opt-in `load` or `docker` group only when the change requires that
environment; report those results separately from the ordinary deterministic
suite.

Before requesting review:

- verify new or changed commands from the repository root;
- add or update tests in the matching package;
- keep configuration changes synchronized with `CONFIG_GUIDE.md`;
- update the owning system-design page or runbook for behavioral changes;
- avoid committing generated logs, Maven `target/` output, credentials, or
  local service data;
- confirm linked files and anchors exist;
- summarize which deterministic and opt-in suites you ran.
