# Microservices in Recsys-Backend-Service

An investigation of how the system is structured as **four independently-runnable
services that share one codebase and one container image**: what each service does,
how a single JAR becomes any of the four at runtime, the clean-architecture layering
that keeps the shared code honest, and what is genuinely shared versus
service-specific.

## The big picture — four services, one codebase, one image

The system is a microservice deployment with an unusual economy: **one Maven
artifact (`com.recsys:recsys-api`), one Docker image, four `main` classes.** Which
service a container becomes is a runtime choice (`RECSYS_MAIN_CLASS`), not a separate
build. The payoff is that shared logic — the recall pipeline, the Redis adapters, the
domain types — lives once and is compiled once, while each service still deploys,
scales, and fails independently behind the gateway.

Three principles hold it together:

- **Package by role, not by service.** A class's package says what it *is*
  (transport, use-case, domain, adapter), never which service uses it — so shared
  code has an obvious home and nothing is duplicated per service.
- **One image, selected at runtime.** Every service is the same image with a
  different `RECSYS_MAIN_CLASS`; there is no per-service build or launcher.
- **Compose at the edge.** The four services know nothing about each other's
  addresses except through config; the gateway is the only thing that fans out to
  all of them.

## 1. The four services

All four entry points live under `com.recsys.api.` — three on Armeria, one on Spring
Boot:

| Service | Main class | Framework | Port | Responsibility |
|---|---|---|---:|---|
| **Catalog / Recommendation Serving** | [`RecSysServer`](../../src/main/java/com/recsys/api/serving/RecSysServer.java) | Armeria | 6010 | Movie/user reads, similar-items, `/v2/recommend`, embeddings, opt-in MySQL catalog |
| **Online Prediction** | [`OnlinePredictionServer`](../../src/main/java/com/recsys/api/online/OnlinePredictionServer.java) | Armeria | 7010 | Real-time feature-store reads, online recommendation, `OnlineLearner`, sharded record store, `/online/ops` |
| **Model Serving** | [`ModelApplication`](../../src/main/java/com/recsys/api/rest/ModelApplication.java) | Spring Boot | 8080 | ONNX two-tower model, REST controllers (retrieval/ranking/knowledge/sequential) |
| **API Gateway** | [`MicroserviceGatewayServer`](../../src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java) | Armeria | 8010 | Front-door reverse proxy fanning `/api/*` to the three backends |

The three Armeria servers build their routes and register health/metrics
programmatically in `main` (each exposes a slightly different health surface — see
§5). `ModelApplication` is declarative — a single `SpringApplication.run` whose
behavior comes from `scanBasePackages = com.recsys.*` and `@EnableConfigurationProperties`.
The gateway builds its whole route table from `MicroserviceRoute.defaults()`; its
internals are the [API Gateway investigation](09_API_Gateway.md).

## 2. One codebase, many entry points

There is **one Maven artifact** (`com.recsys:recsys-api`, no sub-modules) and the
service is chosen at runtime:

- **`RECSYS_MAIN_CLASS`** — the [`Dockerfile`](../../Dockerfile) ENTRYPOINT execs
  `java … $RECSYS_MAIN_CLASS`; the comment says outright *"this single image runs all
  four services, selected at runtime via `RECSYS_MAIN_CLASS`"* and `EXPOSE 6010 7010
  8010 8080`. There is **no launcher/dispatch class** — the JVM invokes whichever
  `main` the env var names.
- **k8s per-service override** — every Deployment in [k8s/base/](../../k8s/base/) sets
  `RECSYS_MAIN_CLASS` to its class (`catalog-serving.yaml`, `online-serving.yaml`,
  `model-serving.yaml`, `api-gateway.yaml`), and the same trick runs the outbox
  relay and reconciliation jobs.
- **Local run** — [`scripts/run-microservices-local.sh`](../../scripts/run-microservices-local.sh)
  starts all four on one machine (three via `exec:java -Dexec.mainClass=…`, the model
  service via `spring-boot:run`) and wires the inter-service URLs to localhost.
- **JVM tuning by profile** —
  [`scripts/run-with-jvm-tuning.sh`](../../scripts/run-with-jvm-tuning.sh) maps a
  service-name profile (`recsys-serving` / `online-serving` / `model-serving` /
  `api-gateway`, plus `-zgc` variants) to a `config/jvm/<profile>.jvmopts` file.
- **Shared AppCDS** — the Dockerfile dumps a class-data-sharing archive (`app.jsa`)
  once using the gateway main and reuses it across all four services at runtime
  (`-XX:SharedArchiveFile`), so start-up cost is shared. Design:
  [docker image optimization](../superpowers/specs/2026-06-30-docker-image-optimization-design.md).

## 3. Clean-architecture layering

The package tree under `com.recsys` is layered by **role, not service** (the
authoritative map is the repository's CLAUDE.md Package Map):

| Layer | Responsibility |
|---|---|
| `api/` | Transport / entry points — `serving`, `online`, `gateway` (Armeria), `rest` (Spring Boot), plus `request`/`response`/`converter`/`envelope` |
| `application/` | Use-case orchestration — `recommendation`, `retrieval`, `ranking`, `feature`, `experiment`, `model`, `online`, `gateway`, `pagination`, `saga`, … |
| `domain/` | Value types — `item`, `user`, `rating`, `recommendation`, `prediction`, `online`, `saga`, … |
| `infrastructure/` | Adapters implementing ports — `redis`(+`sharding`), `cache`, `vectordb`, `store`, `messaging`, `persistence`, `registry`, `resilience`, … |
| `metrics/` `jvm/` `tracing/` `ratelimit/` `loadshed/` `resilience/` `health/` `config/` `exception/` | Cross-cutting technical concerns |

**Dependency direction is `api → application → domain`, with `infrastructure`
implementing ports.** It is visible in the imports: `RecSysServer` (api) pulls in
application services and infrastructure adapters;
[`RecommendationOrchestrator`](../../src/main/java/com/recsys/application/recommendation/RecommendationOrchestrator.java)
(application) depends only on `MultiChannelRecallService` and domain types, never on
`api`; the port it implements
([`RecommendationPipeline`](../../src/main/java/com/recsys/application/recommendation/RecommendationPipeline.java))
returns a domain type. Note that this direction is **enforced by convention** (the
Package Map + the Maven compile-excludes), *not* by an ArchUnit test — there is no
automated layering guard.

## 4. Shared vs service-specific

The economy of one codebase shows up as genuinely shared building blocks:

- **The recall pipeline is shared by 6010 and 7010.** Both build
  [`MultiChannelRecallService`](../../src/main/java/com/recsys/application/retrieval/multichannel/MultiChannelRecallService.java)
  and drive it through the same recall → rank → hydrate → paginate
  `RecommendationOrchestrator`; they differ only in quota policy and feature source
  (see [17_Scalability](17_Scalability.md) for how this shared core scales). Design:
  [shared recall core](../superpowers/specs/2026-06-18-shared-recall-core-design.md).
- **Domain value objects are service-agnostic** — `Movie`, `MovieCandidate`, `User`,
  `RecommendationQuery` under `com.recsys.domain`, imported by any layer.
- **Infrastructure adapters are shared** — `RedisEmbeddingStore`,
  `LettuceClientFactory`, `ShardedTopKStore`, the caches — reused across servers,
  with only the concrete cache class differing (serving vs online).
- **Excluded from the compile** — `online/flink/**` and `training/embedding/**` need
  Spark/Flink classpaths, so [`pom.xml`](../../pom.xml) excludes them from the default
  `maven-compiler-plugin` run (re-included only via the `streaming-flink` / Spark
  profiles). They live outside the layer scheme deliberately.

## 5. How they compose

- **All four sit behind the gateway.** `MicroserviceRoute.defaults()` maps every
  `/api/*` prefix to a backend (`/api/users`+`/api/movies`→6010, `/api/features`→7010,
  `/api/model`+`/api/knowledge`→8080, the `/api/recommend/*` strategy routes, opt-in
  `/api/llm`). Details in the [API Gateway investigation](09_API_Gateway.md).
- **Inter-service addresses come from config** — [k8s/base/configmap.yaml](../../k8s/base/configmap.yaml)
  sets `CATALOG_SERVICE_URL` / `MODEL_SERVICE_URL` / `ONLINE_SERVICE_URL` (the same
  env vars `MicroserviceRoute` reads); the opt-in
  [Service Registry](11_Service_Discovery.md) can resolve them dynamically instead.
- **Each is independently deployable** — one `Deployment` + `Service` per service in
  `k8s/base/`, all the same image differing only by `RECSYS_MAIN_CLASS`, each with its
  own HPA and PDB.
- **Health surfaces differ by service** — gateway `/health` (aggregated), catalog
  `/health` + `/health/ready`, online `/health/live` + `/health/ready` + `/online/ops`
  + `/metrics`, model `/health/live` + `/health/ready` + `/actuator/prometheus`. The
  probes in each Deployment match its service's surface.
- **Every hop is HTTP/JSON — there is no gRPC anywhere in the system.** Not in the
  request path, not between services, not as a dependency: the build has no
  `grpc`/`protobuf` artifact and Armeria is pulled in as core `armeria` only, never
  `armeria-grpc`. The single occurrence of the string "gRPC" in the repo is a negative
  test fixture in the ALB reference model, and it was
  [changed to "TCP"](../../src/test/java/com/recsys/infrastructure/alb/ApplicationLoadBalancerTest.java)
  because it misrepresented real ALB behaviour. Treat an apparent gRPC reference as a
  mistake, not as a surface you have not found yet. The reasoning is below.
- **URL versioning is per-service, not shared** — 6010 and 7010 use root `/v1/…` and
  `/v2/…` next to unversioned legacy routes, while 8080 (Spring) uses both `/api/v1/…`
  and root `/v2/…`. The gateway's own prefixes carry no version at all, so the edge
  contract and the backend contracts version independently — or, in practice, don't.
  See [09_API_Gateway §1](09_API_Gateway.md#1-routing-and-prefix-strip).

### Why not gRPC (and why not bidirectional streaming)

Recorded because the absence is easy to mistake for an oversight, and because "use gRPC
for real-time bidirectional streaming" is the obvious next suggestion for a system with
four services and a streaming surface. It extends the same reasoning
[16_SSE_Streaming](16_SSE_Streaming.md) already applies to WebSockets: SSE is one-way,
"which is exactly what an LLM token stream needs; nothing here requires a bidirectional
socket."

1. **gRPC cannot do bidirectional streaming to a browser at all.** Full bidi needs
   HTTP/2 trailers and true full-duplex framing, which browser `fetch`/XHR cannot
   express. gRPC-Web — the only browser-viable variant — supports unary and
   *server*-streaming only; client-streaming and bidirectional streaming are
   unsupported, including behind an Envoy translation proxy. So the trade-off is not
   "SSE vs. gRPC bidi"; it is SSE vs. server-streaming gRPC-Web, which is the same
   capability plus a protobuf toolchain. The premise mostly dissolves before the other
   reasons are needed.
2. **Nothing in the system is shaped like a bidirectional stream.** The only
   client-facing stream is LLM tokens (server → client). Client → server traffic is
   discrete and asynchronous — events into Kafka — which buys durability and replay
   that a socket does not. Internal streaming (`sr:stream:<shard>`, Kafka → Flink →
   Redis) never reaches a client. Bidi would be a capability with no consumer.
3. **The edge is built on HTTP semantics, and much of the system's value lives there.**
   CloudFront's cache behaviours are path-keyed (`GET /api/catalog/item`, `/similar`);
   the WebACL and `x-origin-secret` origin lockdown are header-shaped; and the gateway
   strips `/api/v1` *before* routing, authorization, and rate-limit keying, on top of
   per-route circuit breakers and exact-path `GATEWAY_PUBLIC_PATHS` matching. gRPC does
   not slot into that machinery — it would bypass or require rebuilding most of it.
4. **Nonzero cost, zero current benefit** — protobuf toolchain, a codegen step in a
   deliberately flat single-module build, and a second serialization format alongside
   the existing JSON DTOs.

**Where the question is genuinely open.** The honest case for gRPC is *not*
client-facing: it is the internal hops (gateway → 8080/7010/6010), where the browser
constraint in point 1 does not apply. There it would buy binary encoding and schema
enforcement across four services that currently share DTO shapes by convention alone —
weighed against reworking the path-based routing and circuit-breaking in point 3. And
if a feature ever needs continuous client → server signal (live dwell/scroll telemetry
driving in-session re-ranking), that is where bidi would earn its keep — trading Kafka's
durability and replay for latency, which is a real trade rather than a free upgrade.

## 6. Testing and module structure

The project is a **single flat Maven module** (jar packaging; the Spring Boot plugin's
`<mainClass>` only affects `spring-boot:run`, not the shared runtime image). The test
tree mirrors the layers (`src/test/java/com/recsys/{api,application,domain,infrastructure,…}`
with per-service `api/*` sub-dirs), and JUnit tags scope the suite (`load` and
`docker` groups excluded by default). There is **no ArchUnit / layering-enforcement
test** — the architecture is kept honest by the Package Map, code review, and the
compile-excludes rather than an automated rule.

## Sharp edges — notes

1. **One image is a coupling as well as an economy.** All four services ship from the
   same JAR, so a dependency bump or a shared-code change redeploys everyone; the win
   is no drift between services, the cost is no independent dependency versioning.
   API versioning is uncoupled in the opposite direction: each service picked its own
   URL convention and nothing reconciles them at the edge.
2. **Layering is conventional, not enforced.** Without an ArchUnit guard, an
   `api → infrastructure` shortcut or a `domain → application` back-edge would compile;
   the Package Map and review are the only backstop.
3. **`RECSYS_MAIN_CLASS` is load-bearing and unchecked.** A typo in the env var fails
   at JVM start with a class-not-found, not a friendly error — the k8s manifests are
   the source of truth for the correct value per service.
4. **Two source trees are outside the build.** `online/flink/**` and
   `training/embedding/**` exist in the repo but are excluded from the default compile;
   edits there are only validated under the Spark/Flink Maven profiles.
5. **The gateway is a shared fate.** Independent backends still funnel through one
   edge, so gateway availability is covered separately —
   [API Gateway](09_API_Gateway.md) and [Fault Tolerance](18_Fault_Tolerance.md).
