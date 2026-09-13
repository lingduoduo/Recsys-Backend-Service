# Service Discovery in Recsys-Backend-Service

An investigation of how the API gateway finds a backend: the static route table it
uses by default, the opt-in Redis registry that lets backends advertise their own
address without a redeploy, the Cloud Map DNS layer underneath, and the
health-checked endpoint groups that decide which resolved address is actually
selectable. The theme is **degrade, never fail** — every dynamic layer falls back
to the static default rather than breaking the hop.

## The big picture — three layers, three questions

Resolving "where is `recsys-model-serving`?" is answered in three composable layers,
each answering a different question:

| Layer | Question it answers | Default behavior |
|---|---|---|
| Route table / registry | **Which address?** (`host:port`) | Static route address; optionally overridden by the registry |
| Cloud Map DNS | **Which IP?** for that host | Armeria resolves per-connection under a 30 s DNS TTL |
| Health-checked endpoint group | **Is it selectable?** | A down upstream is dropped → request fast-fails `503` |

The registry decides the *address*, DNS decides the *IP*, and the health check
decides *selectability* — and each is independently optional/fail-safe. Addressing
is **static by default**; the Redis registry is an opt-in dynamic overlay
(`SERVICE_REGISTRY_ENABLED=false` out of the box, so the gateway opens no Redis
connection at all).

## 1. Static addressing — the default

The gateway's authority is the static route table
([`MicroserviceRoute`](../../src/main/java/com/recsys/application/gateway/MicroserviceRoute.java)):
each route is `(name, prefix, envVar, defaultBaseUri, healthPath, serviceName)`, and
the three backend authorities are `recsys-catalog-serving` (6010, `/health`),
`recsys-model-serving` (8080, `/health/ready`), and `recsys-online-serving` (7010,
`/health`). Each route's address comes from its env var / configmap (or the coded
default), and `serviceName` is the key the registry overlay resolves against
(`null` for LLM/unmapped routes, which never use the registry). With the registry
off, this table *is* the resolution; the gateway's request pipeline is documented in
[API Gateway](09_API_Gateway.md).

## 2. The opt-in Redis registry

When `SERVICE_REGISTRY_ENABLED=true`, backends advertise their own address and the
gateway follows them without a redeploy — trading a Redis dependency for dynamic
membership, and **degrading to the static addresses (never failing)** if the
registry is unavailable.

### Backends self-register

[`ServiceRegistrar`](../../src/main/java/com/recsys/infrastructure/registry/ServiceRegistrar.java)
writes a service's advertised address to Redis and renews it on a daemon heartbeat.
`fromEnvironment` returns `null` (a no-op) unless the registry is enabled *and* both
`SERVICE_REGISTRY_SERVICE_NAME` and `SERVICE_REGISTRY_ADVERTISE_URL` are set. On
`start()` it heartbeats immediately, then every `SERVICE_REGISTRY_HEARTBEAT_MS`
(default 10000) on the `svc-registry-heartbeat` thread; all store I/O is best-effort
(logged, never breaks serving).

[`ServiceRegistryStore`](../../src/main/java/com/recsys/infrastructure/registry/ServiceRegistryStore.java)
does the Redis I/O over `RedisExecutor`: `register` is a `SET svc:registry:<name>
<address> PX <ttlMs>` (`SERVICE_REGISTRY_TTL_MS`, default 30000) so a crashed
instance's key **expires on its own**; `lookup(names)` is an `MGET` of the prefixed
keys that keeps only present values. Liveness is simply "key present."

All four services register:

- The three Armeria services register directly —
  [`RecSysServer`](../../src/main/java/com/recsys/api/serving/RecSysServer.java) (6010) and
  [`OnlinePredictionServer`](../../src/main/java/com/recsys/api/online/OnlinePredictionServer.java)
  (7010) build a `ServiceRegistrar.fromEnvironment(...)` and `start()` it (closed on
  shutdown).
- The Spring model service (8080) registers via
  [`ServiceRegistryConfig`](../../src/main/java/com/recsys/config/ServiceRegistryConfig.java) —
  an `@Bean(initMethod="start", destroyMethod="close")` that wraps the registrar in
  a null-safe `ServiceRegistrarLifecycle`, so a non-Armeria service joins the same
  registry and stays an inert no-op when the registry is off.
- The gateway is the *consumer*, not a producer (§below).

### The gateway resolves

[`ServiceRegistryProvider`](../../src/main/java/com/recsys/infrastructure/registry/ServiceRegistryProvider.java)
holds a **lock-free, periodically-refreshed** view: a single volatile immutable map
swapped atomically per refresh. It refreshes immediately, then every
`SERVICE_REGISTRY_REFRESH_MS` (default 10000) on the `svc-registry-refresh` thread,
`MGET`-ing the known service names. It is **fail-static** — a refresh error keeps
the last-good snapshot and bumps a failure counter rather than dropping resolutions.
`resolve(name)` returns the advertised address if the key is present.

[`RegistryBackedUpstreams`](../../src/main/java/com/recsys/application/gateway/RegistryBackedUpstreams.java)
overlays those resolved addresses onto the static route table: `resolveAddresses()`
is `provider.resolve(route.serviceName()).orElse(route.baseUri())` — **static-route
fallback per route** when a service is unregistered or Redis is down. When the
provider's refresh callback fires, `rebuildIfChanged()` recomputes the resolved map
and, only if it changed, builds a fresh set of upstream groups and atomically swaps
them in (the same swap pattern the shard-topology provider uses). The
[`GatewayRequestForwarder`](../../src/main/java/com/recsys/application/gateway/GatewayRequestForwarder.java)
holds mutually exclusive `staticUpstreams` vs `registryUpstreams` and picks per
request — so the disabled path never touches registry code.

```bash
export SERVICE_REGISTRY_ENABLED=true
export SERVICE_REGISTRY_SERVICE_NAME=recsys-catalog-serving
export SERVICE_REGISTRY_ADVERTISE_URL=http://10.0.1.5:6010
```

| Env var | Default | Purpose |
|---|---:|---|
| `SERVICE_REGISTRY_ENABLED` | `false` | Master switch (gateway + backends). Off → no Redis connection, static routing |
| `SERVICE_REGISTRY_SERVICE_NAME` | _(per service)_ | Name a backend registers under (`svc:registry:<name>`) |
| `SERVICE_REGISTRY_ADVERTISE_URL` | _(per service)_ | Address a backend advertises to callers |
| `SERVICE_REGISTRY_HEARTBEAT_MS` | `10000` | Backend re-registration interval |
| `SERVICE_REGISTRY_TTL_MS` | `30000` | Registration key TTL (expires a dead instance) |
| `SERVICE_REGISTRY_REFRESH_MS` | `10000` | Gateway registry poll interval |

## 3. Cloud Map DNS + health-checked endpoint groups

Whatever host the route table or registry resolves to still has to become an IP and
prove it's alive:

- **Cloud Map DNS (30 s TTL)** —
  [`MicroserviceGatewayServer`](../../src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java)
  sets the JVM `networkaddress.cache.ttl=30` (only if unset), so EKS blue/green
  Cloud Map endpoint changes actually propagate — otherwise the JVM caches DNS
  lookups indefinitely and would pin traffic to retired pods.
- **Health-checked endpoint groups** —
  [`UpstreamEndpointGroups`](../../src/main/java/com/recsys/application/gateway/UpstreamEndpointGroups.java)
  builds one Armeria `EndpointGroup` per unique `(protocol, host, port, healthPath)`
  (deduped, so pollers scale with backends not routes). With
  `GATEWAY_UPSTREAM_HEALTHCHECK_ENABLED` (default true,
  `GATEWAY_UPSTREAM_HEALTHCHECK_INTERVAL_MS` default 10000) each backend is probed and
  a down one is dropped from selection with `allowEmptyEndpoints(false)`, so a
  request to a dead upstream **fast-fails `503`** instead of hanging. This is the
  same mechanism covered from the resilience angle in the
  [Fault Tolerance investigation](18_Fault_Tolerance.md#4-dependency-resilience--surviving-a-sick-downstream).

So the layers compose cleanly: the registry (or static table) picks the address,
Armeria's per-connection resolver turns it into an IP under the 30 s Cloud Map
cache, and the health check decides whether that endpoint is eligible.

Health probes use **GET** explicitly. Catalog and online health handlers are
GET-only and reject HEAD with 405; relying on Armeria's default HEAD probe
would remove healthy backends from selection while the gateway's separate
GET-based `/health` aggregation reported them UP. The GET-only upstream fixture
in `GatewayUpstreamHealthCheckIntegrationTest` guards this distinction.

## 4. Observability

When the registry is enabled, the gateway `/health` gains a `registry` section
([`GatewayHealthService.registrySection`](../../src/main/java/com/recsys/application/gateway/GatewayHealthService.java),
null when no provider) reporting each service's resolution `source` (`registry` vs
`static` fallback), its resolved `address`, and the snapshot age:

```json
"registry": {
  "enabled": true,
  "snapshotAgeMs": 1200,
  "services": {
    "recsys-catalog-serving": {"source": "registry", "address": "http://10.0.1.5:6010"},
    "recsys-model-serving":   {"source": "static",   "address": null}
  }
}
```

The Prometheus `/metrics` endpoint publishes five pull-based meters
([`GatewayRegistryMetrics`](../../src/main/java/com/recsys/metrics/GatewayRegistryMetrics.java),
sampled at scrape time — no background thread, no Redis on scrape):

| Meter | Meaning |
|---|---|
| `gateway_registry_services_total` | Services the gateway tracks |
| `gateway_registry_services_resolved` | Services currently resolved from the registry (not static fallback) |
| `gateway_registry_snapshot_age_seconds` | Age of the last successful snapshot (`-1` if never) |
| `gateway_registry_refresh_total` | Registry refresh attempts |
| `gateway_registry_refresh_failures_total` | Refresh attempts that fell back to the last-good snapshot |

`services_total` vs `services_resolved` is the key operational signal: a gap means
some service is on static fallback, and a rising `snapshot_age_seconds` alongside
`refresh_failures_total` means the registry poll is failing while serving stays up on
the last-good view.

## 5. Testing

- **Registrar / store** — `ServiceRegistrarTest` (heartbeat registers with TTL,
  swallows store errors, close best-effort deregisters), `ServiceRegistryStoreTest`
  (SET-with-TTL, MGET omits absent, DEL — mocked Lettuce),
  `ServiceRegistrarLifecycleTest` (null registrar is a no-op),
  `ServiceRegistryConfigTest` (the Spring bean is a safe no-op when disabled).
- **Provider / resolution** — `ServiceRegistryProviderTest` (refresh swaps snapshot;
  **fail-static keeps last-good**; snapshot-age accessor),
  `RegistryBackedUpstreamsTest` (registered address wins over static; falls back to
  static when unregistered).
- **Endpoint groups / health** — `UpstreamEndpointGroupsTest` (dedupe by
  host/port/health-path; a client per route even with health-check off),
  `GatewayUpstreamHealthCheckIntegrationTest` (healthy upstream forwarded; unhealthy
  dropped and fast-fails `503`).
- **Observability** — `GatewayHealthServiceRegistryTest` (registry section present
  only with a provider), `GatewayRegistryMetricsTest` (all five meters; age is `-1`
  before the first refresh).

Note: the registry store test uses Mockito mocks of `RedisExecutor`, not
Testcontainers — there is no `@Tag("docker")` registry test.

## Sharp edges — notes

1. **Everything degrades to static.** A registry outage, an unregistered service, or
   a stale snapshot all fall back to the static route address per route — the hop
   never fails *because of* discovery. The cost is that a moved backend keeps getting
   traffic at its old static address until it re-registers.
2. **Off means truly off.** With `SERVICE_REGISTRY_ENABLED=false` the gateway opens
   no Redis connection and the registrars are no-ops, so the registry adds zero
   dependency surface unless explicitly turned on.
3. **Three independent resolution layers.** Address (registry/static), IP (DNS), and
   selectability (health check) are separate — a registered-but-unhealthy backend is
   still dropped by the health check, and a healthy backend at a stale DNS record is
   still limited by the 30 s TTL.
4. **Liveness is coarse.** Registry liveness is just "key present within TTL"
   (heartbeat 10 s, TTL 30 s), so a wedged-but-not-crashed instance can stay
   advertised for up to the TTL; the endpoint-group health check is the finer-grained
   liveness signal.
5. **Built in stacked PRs, feature-flagged throughout.** Core registry, Spring
   registration, observability, and metrics landed as separate designs
   ([registry](../superpowers/specs/2026-07-10-redis-service-registry-design.md),
   [PR2](../superpowers/specs/2026-07-10-redis-service-registry-pr2-design.md),
   [metrics](../superpowers/specs/2026-07-10-registry-prometheus-metrics-design.md),
   [upstream discovery](../superpowers/specs/2026-07-10-gateway-upstream-endpoint-discovery-design.md)),
   all off by default.
