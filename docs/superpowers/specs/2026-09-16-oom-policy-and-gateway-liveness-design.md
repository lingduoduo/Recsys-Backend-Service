# Process Restart Policy: OOM Exit and Gateway Liveness

## Problem

Two defects decide, between them, when Kubernetes restarts a pod in this system. Both were
found by the investigation recorded in `docs/system_design/18_Fault_Tolerance.md` §9; neither
was fixed there.

**Nothing turns an `OutOfMemoryError` into a restart.** No container sets
`-XX:+ExitOnOutOfMemoryError`, so a JVM that has thrown OOM keeps running. §9.2 measured what
that looks like: request threads keep answering — 500s while the heap is short, 200s once GC
recovers something — while §9.4 showed a background loop can die silently at the same moment
and stay dead after the heap recovers. Liveness for catalog serving, online serving, model
serving and the outbox relay is a constant `200 OK` that inspects nothing, so the platform
never intervenes. The operator sees `JvmHeapPressureHigh`, `JvmGcTimeFractionHigh` and a
climbing 5xx rate, and must restart the pod by hand.

**The gateway's liveness probe is coupled to its dependencies.** §9.5 asserts every liveness
probe in the system is a constant 200; that is wrong for the gateway.
`k8s/base/api-gateway.yaml` points `livenessProbe`, `readinessProbe` *and* `startupProbe` at
`/health`, which is `GatewayHealthService` — it returns `503` when any upstream is down. So an
upstream outage lasting past `periodSeconds: 20 × failureThreshold` (defaulting to 3) makes
kubelet kill and restart every gateway pod: the one component still able to serve the routes
whose upstreams are healthy. The startup probe carries the same coupling with a 120 s budget
(`failureThreshold: 24`, `periodSeconds: 5`), so on a cold start — a DR region coming up, a
full-namespace restart — a gateway whose upstreams are slow to boot is killed before it ever
starts, and restarts into the same condition.

The two are the same question asked at opposite extremes: the JVM that should be restarted and
is not, and the JVM that should not be restarted and is.

## Goals

- A JVM that has thrown `OutOfMemoryError` exits, and the platform restarts it.
- Gateway liveness and startup reflect whether the gateway process is running, not whether its
  upstreams are.
- Both properties are enforced by a test that fails in CI, not by a convention that drifts.
- Correct §9.5, whose description of gateway liveness does not match the manifest.

## Non-goals

- **Heap dumps.** Deliberately not added. The root filesystem is read-only with only an
  `emptyDir` `/tmp` writable, so a dump from model serving's 2 GiB heap lands on ephemeral
  storage, disappears with the pod, and can pressure the node into a disk-eviction while the
  workload is already degraded. If dumps are wanted later they need a persistent volume and a
  retention story, which is its own change.
- **Gateway readiness.** Stays on the upstream-aware `/health`. See "Accepted residuals".
- **Making any liveness probe inspect JVM state.** `-XX:+ExitOnOutOfMemoryError` makes an
  OOM-aware liveness endpoint unnecessary for the OOM case, and a probe that inspects heap
  headroom would restart pods during ordinary GC pressure.
- **Catalog serving's probe path.** Its `livenessProbe` is `/health`, not `/health/live`, but
  `RecommendationService.Health` is a constant `{"ok": true}` — correct behaviour under an
  inconsistent name. Renaming it would churn a manifest for no behavioural gain.

## Design

### Part A — OOM exit policy

Add `-XX:+ExitOnOutOfMemoryError` to the `JAVA_OPTS` value of every `k8s/base` workload that
sets one:

| Workload | Current `JAVA_OPTS` |
|---|---|
| `recsys-catalog-serving` | `-Xms512m -Xmx1g` |
| `recsys-api-gateway` | `-Xms256m -Xmx512m` |
| `recsys-model-serving` | `-Xms1g -Xmx2g -XX:MaxDirectMemorySize=512m` |
| `recsys-online-serving` | `-Xms512m -Xmx1g` |
| `recsys-outbox-relay` | `-Xms256m -Xmx512m` |
| `recsys-outbox-reconciliation` (CronJob) | `-Xms256m -Xmx512m` |

The flag reaches the JVM: `Dockerfile`'s entrypoint is
`exec java $JAVA_OPTS …`, an unquoted expansion. Both EKS overlays compose `../base` and patch
region-specific values only, so they inherit the flag without edits.

For the two Deployments that are not request-serving, the semantics are worth stating. The
outbox relay exits and is restarted by its Deployment, resuming from the outbox table — the
relay is already designed for at-least-once redelivery. The reconciliation CronJob exits
non-zero, which fails that Job run and leaves the next scheduled run to retry; a reconciliation
pass is idempotent, so a lost run costs latency, not correctness.

### Part B — gateway liveness decoupling

Add a constant-200 `/health/live` to `MicroserviceGatewayServer`, mirroring
`OnlineServices.Live`, and point `livenessProbe` and `startupProbe` at it. `readinessProbe`
keeps `/health`.

**No auth or config change is needed.** *(Corrected during review — the first version of this
section named the wrong mechanism, and the wrong one is the more intuitive one.)*

The worry is that a kubelet probe on a brand-new path gets `401` in the EKS overlays, where
`GATEWAY_ALLOW_ANONYMOUS=false` and the public-path set is the literal
`/health,/api/catalog/item,/api/catalog/similar`. It does not, but **not** because the
authenticator's prefix matching covers it. The authenticator is never consulted at all:
`GatewayAuthenticator.check` is called from exactly three request-handling services
(`GatewayProxyService`, `RecommendationGatewayService`, `LlmProxyService`) and is not a
server-wide decorator, so an exact route on the `ServerBuilder` bypasses it entirely.

The gate that *does* see every request is `GatewayOriginSecret`, one of four server-wide
decorators and the only one that can reject. It is enabled wherever the CDN is, and the kubelet
sends no `x-origin-secret`. `/health/live` passes because `isExempt` matches by
prefix-with-boundary — `path.equals(p) || path.startsWith(p + "/")` — so the existing `/health`
exemption covers it. **That** is the load-bearing, invisible property: tightening `isExempt` to
exact equality reads as hardening, leaves every existing `GatewayOriginSecretTest` case green,
and 403s every gateway pod's liveness probe. Pinned by
`GatewayLivenessRouteTest.livenessPathIsExemptFromTheOriginSecret`, verified by mutating
`isExempt` and confirming that test is the only one that fails.

The authenticator's prefix matching is still worth a test, but as a *fallback*: because
`GatewayProxyService.serve` calls `check` before `routeTable.match`, it would apply only if the
exact route were removed and the request fell through to the catch-all.

`BackendRoutePolicy` classifies `/health` as `NO_PROXY`; `/health/live` is served locally off
the server builder and never forwarded, so it needs no new entry. Were the exact route ever
removed, the catch-all would answer `404 "no route found"` rather than proxying the probe —
`routeTable.match("/health/live")` returns null, since every route prefix is `/api/...`.

### Enforcement

Two tests in `src/test/java/com/recsys/infrastructure/k8s/`, built on the existing
`ManifestDocuments` helper:

**`OomPolicyManifestTest`** — walks every container in every `k8s/base` Deployment, StatefulSet
and CronJob, collects those that set `JAVA_OPTS`, and asserts each value contains
`-XX:+ExitOnOutOfMemoryError`. The workload set is **derived from the manifests, not
hardcoded**, so a service added later without the flag fails the build. A hardcoded list would
pass forever while the property silently stopped being universal — the failure mode the Splunk
egress conformance work hit and fixed the same way.

**`GatewayLivenessManifestTest`** — asserts the gateway's `livenessProbe` and `startupProbe`
target `/health/live` while its `readinessProbe` still targets `/health`, and, generally, that
no `livenessProbe` anywhere in `k8s/base` points at a dependency-aware endpoint: every HTTP
liveness path must be one of the known constant-200 endpoints. That general assertion is the
part with a future: the next service wired to liveness-on-a-real-health-check fails CI.

Plus unit coverage on the Java side, in `GatewayLivenessRouteTest`: `/health/live` returns 200
with no credentials while `GATEWAY_ALLOW_ANONYMOUS` is false — the prefix-matching property
above — and returns 200 when an upstream is unreachable, which is what distinguishes it from
`/health`.

Every test is run red against the current tree before being committed, per the standing rule
that a conformance test which has never failed has not been shown to test anything. All are
added to the `-Presilience` profile, or they do not gate the PR.

## Accepted residuals

**An OOM exit skips the drain.** `-XX:+ExitOnOutOfMemoryError` halts the JVM without running
shutdown hooks, so the §5 graceful-drain sequence and any in-flight requests are lost — a
`terminationGracePeriodSeconds: 60` and a `preStop: sleep 5` buy nothing on this path. Accepted:
the alternative is a JVM in an unknown state, possibly with dead background threads, reporting
itself live and serving indefinitely. Dropping in-flight requests on a replica that is already
failing is the better trade, and every serving deployment runs multiple replicas behind a
readiness-gated Service.

**Gateway readiness stays coupled to upstreams.** One upstream down still pulls the gateway
out of its target group, so routes to *healthy* upstreams stop being served at the edge. This
is redundant with a more precise mechanism — `GATEWAY_UPSTREAM_HEALTHCHECK_ENABLED` drops a
down backend from endpoint selection and fast-fails that route with 503, and the per-route
circuit breakers do the same under failure — but it is left as-is by decision, not oversight.
Recorded here so the next reader does not take it for a bug.

**`-XX:+ExitOnOutOfMemoryError` does not fire on every OOM-adjacent failure.** It triggers when
the JVM throws `OutOfMemoryError`, which covers heap exhaustion but not native allocation
failures inside `onnxruntime` — those are a SIGSEGV that kills the process anyway (§9.5) — and
not a JVM that is merely thrashing GC without throwing. The heap and GC alerts remain the
signal for that case.

## Documentation

- `18_Fault_Tolerance.md` §9.5: rewrite. Correct the claim that every liveness probe is a
  constant 200, state the OOM policy now in force, and record the drain interaction.
- §9.6 "Process" bullet: change from "decide the OOM policy explicitly" to the decision made.
- The "No OOM policy" entry in the sharp-edges list: resolved, with the residuals above.
- `09_API_Gateway.md`: document `/health/live` and why liveness and readiness differ.
