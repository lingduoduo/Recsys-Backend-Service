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
- [Streaming guide](streaming/online-serving/README.md) — events, Redis features, and Flink.

<details>
<summary>Runbooks and system design</summary>

- [Local CDN](docs/runbooks/cdn-local.md)
- [CDN Operations](docs/runbooks/cdn-operations.md)
- [CDN Rollback](docs/runbooks/cdn-rollback.md)
- [Deploy: pinning the EKS image digest](docs/runbooks/deploy-image-digest.md)
- [Runbook: DR Data-Tier Promotion (us-west-2)](docs/runbooks/dr-data-tier-promotion.md)
- [Runbook: DR Failback (us-west-2 → us-east-1)](docs/runbooks/dr-failback.md)
- [Runbook: DR Game Day](docs/runbooks/dr-game-day.md)
- [Runbook: Regional DR Failover (us-east-1 → us-west-2)](docs/runbooks/dr-regional-failover.md)
- [Durable Eventual Consistency — Migration & Operations Runbook](docs/runbooks/durable-eventual-consistency.md)
- [Local ElastiCache stand-in](docs/runbooks/elasticache-local.md)
- [Gateway authentication (fail-closed)](docs/runbooks/gateway-auth.md)
- [Kafka partition cutover runbook](docs/runbooks/kafka-partition-cutover.md)
- [Local development](docs/runbooks/local-development.md)
- [Model artifact rollout](docs/runbooks/model-artifact-rollout.md)
- [Runbook: Overload-Protection Characterization](docs/runbooks/overload-characterization.md)
- [Runbook: Overload Protection & Rate Limits](docs/runbooks/overload-protection.md)
- [Recommendation cursor key rotation](docs/runbooks/recommendation-cursor-key-rotation.md)
- [Redis authentication](docs/runbooks/redis-auth.md)
- [Runbook: Retire (Decommission) the Recsys Backend](docs/runbooks/retire-backend.md)
- [Runbook: Serving Data Freshness SLOs](docs/runbooks/serving-data-freshness.md)
- [Splunk HEC logging](docs/runbooks/splunk-hec-logging.md)
- [Runbook: WAFv2 WebACL for the API Gateway ALB](docs/runbooks/waf-webacl.md)
- [Runbook: Single-AZ Failure Resilience (us-east-1)](docs/runbooks/zonal-resilience.md)
- [Load Balancing in Recsys-Backend-Service](docs/system_design/01_Load_Balancing.md)
- [Caching in Recsys-Backend-Service](docs/system_design/02_Caching.md)
- [Database Scaling & Sharding in Recsys-Backend-Service](docs/system_design/03_DB_Scaling_Sharding.md)
- [Replication in Recsys-Backend-Service](docs/system_design/04_Replication.md)
- [CAP in Recsys-Backend-Service](docs/system_design/05_CAP.md)
- [Consistent Hashing in Recsys-Backend-Service](docs/system_design/06_Consistent_Hashing.md)
- [Message Queues in Recsys-Backend-Service](docs/system_design/07_Message_Queue.md)
- [Rate Limiting in Recsys-Backend-Service](docs/system_design/08_Rate_Limits.md)
- [API Gateway in Recsys-Backend-Service](docs/system_design/09_API_Gateway.md)
- [Microservices in Recsys-Backend-Service](docs/system_design/10_MicroServices.md)
- [Service Discovery in Recsys-Backend-Service](docs/system_design/11_Service_Discovery.md)
- [CDN Edge in Recsys-Backend-Service](docs/system_design/12_CDNS.md)
- [Database Indexing in Recsys-Backend-Service](docs/system_design/13_DB_Indexing.md)
- [Partitioning in Recsys-Backend-Service](docs/system_design/14_Partitioning.md)
- [Eventual Consistency in Recsys-Backend-Service](docs/system_design/15_Eventual_Consistency.md)
- [SSE Streaming in Recsys-Backend-Service](docs/system_design/16_SSE_Streaming.md)
- [Scalability in Recsys-Backend-Service](docs/system_design/17_Scalability.md)
- [Fault Tolerance in Recsys-Backend-Service](docs/system_design/18_Fault_Tolerance.md)
- [Pagination in Recsys-Backend-Service](docs/system_design/19_Pagination.md)
- [AuthN / AuthZ in Recsys-Backend-Service](docs/system_design/20_AuthN_AuthZ.md)

</details>
