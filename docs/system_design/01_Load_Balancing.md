# Load Balancing in Recsys-Backend-Service

An investigation of how traffic is spread across healthy, non-overloaded instances:
the real production stack (AWS ALB → kube-proxy / topology-aware routing → Armeria
client-side health-checked groups), the **capacity-weight feedback signal** that lets
an instance tell the balancer how loaded it is, and an in-memory L7
`ApplicationLoadBalancer` model used as a routing reference. The theme is
**capacity-aware balancing** — not just "is it up?" but "how much more can it take?"

## The big picture

Load balancing here is three cooperating layers, only two of which are on the
production data path:

| Layer | What it is | Prod or reference |
|---|---|---|
| Edge + cluster LB | AWS ALB Ingress → kube-proxy ClusterIP → topology-aware routing | **production** |
| Gateway upstream clients | Armeria `UpstreamEndpointGroups` (one Service address per backend; connection recycling + startup health gate) | **production** |
| Capacity-weight feedback | `X-Capacity-Weight` / `suggestedWeight` from the load shedders | **production** (signal) |
| `ApplicationLoadBalancer` | in-memory L7 listener→rule→target-group→round-robin model | **reference / tested** |

The distinctive piece is the feedback: most balancers only know liveness, but here
each instance also emits a **0–100 capacity weight** so the balancer can shift traffic
away from a saturated node *before* it starts failing health checks.

## 1. The production load-balancing stack

Real traffic is balanced by infrastructure, in three nested tiers:

- **AWS ALB Ingress (edge).** A WAF-protected ALB is the sole public entry to the
  gateway (the EKS overlay drops the NLB and patches the gateway Service to
  `ClusterIP`); [Scalability](17_Scalability.md#1-compute-tier-scaling--hpa-is-the-real-autoscaler)
  owns the EKS topology and [CDN Edge](12_CDNS.md) owns the CloudFront→ALB edge.
- **kube-proxy + topology-aware routing (in-cluster).** Service-to-service calls resolve
  through ClusterIP names, and `trafficDistribution: PreferClose` prefers same-AZ
  endpoints to cut cross-AZ cost — detailed in
  [17_Scalability](17_Scalability.md#1-compute-tier-scaling--hpa-is-the-real-autoscaler).
- **Armeria upstream clients (gateway → backends).** The gateway wraps each upstream in a
  `HealthCheckedEndpointGroup` (`UpstreamEndpointGroups`) — but each group holds **one**
  endpoint, the backend's ClusterIP Service address, so Armeria does no balancing of its
  own: the pod is kube-proxy's choice, made once per TCP connection. A shared client
  factory recycles connections every `GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS` (default
  30 s) so that choice keeps being re-made — see
  [Connection pinning and recycling](#connection-pinning-and-recycling). The health check
  gates initial readiness only. Covered in
  [09_API_Gateway](09_API_Gateway.md#5-resilience-the-gateway-applies-cross-links) and
  [11_Service_Discovery](11_Service_Discovery.md#3-cloud-map-dns--health-checked-endpoint-groups).

At the data layer, AZ-aware Redis read routing is a form of read load-balancing across
replicas — [04_Replication](04_Replication.md#1-redis-read-replicas--az-aware-read-routing).

### Connection pinning and recycling

kube-proxy balances **connections**, not requests. Armeria speaks h2c to the Armeria
backends (catalog 6010, online 7010) and multiplexes every request onto one long-lived
HTTP/2 connection, so before 2026-10-06 each gateway pod was pinned to one backend pod
for as long as that connection lived — and pods added by the HPA got no gateway traffic
at all. Measured with a per-connection round-robin TCP proxy standing in for kube-proxy:

| Setup | Result |
|---|---|
| 2 pods, 2048 requests, 64 in flight | 1 TCP connection; 100% of requests on one pod |
| scale out to 4 pods, 2048 more | still 100% on the original pod; new pods got 0 |
| `maxConnectionAgeMillis(1000)` | 8 connections; 4352 / 6080 / 7232 / 6336 across 4 pods, 0 errors |
| one pod, healthy → unhealthy | never dropped from selection (`allowEmptyEndpoints(false)`) |

The fix keeps the kube-proxy path — so `trafficDistribution: PreferClose` still holds —
and gives the upstream `WebClient`s and health checkers one shared `ClientFactory` with
`maxConnectionAgeMillis` set from `GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS` (default
`30000`; `0` disables; `1..999` and negatives fail startup, Armeria's floor being 1 s).
An aged connection takes no new requests, finishes its in-flight ones, and closes; the
next request opens a fresh connection and kube-proxy picks again. The factory belongs to
`GatewayRequestForwarder` (static upstreams) or `RegistryBackedUpstreams` (registry
path), and outlives endpoint-group rebuilds so a swap never closes connections that
in-flight requests are still using.

The health check is deliberately left gating **initial readiness only**. Making it drop
an endpoint after startup (`allowEmptyEndpoints(true)`) would turn harmful once
connections rotate: each probe lands on a random pod, so one bad pod out of N would
periodically empty the whole backend and `503` every request. Readiness probes already
remove a bad pod from the Service's endpoints.

## 2. Capacity-weight feedback — the app → LB signal

The one piece of load balancing the *application* owns is the capacity signal. Both
load shedders compute a weight that falls as the instance fills up:

```
suggestedWeight = shuttingDown ? 0 : max(0, round((1 - utilization) * 100))
```

([`LoadShedder`](../../src/main/java/com/recsys/loadshed/LoadShedder.java) on model-serving,
[`OnlineLoadShedder`](../../src/main/java/com/recsys/loadshed/OnlineLoadShedder.java) on 7010).
That weight is surfaced two ways:

- **As a response header** — every model-serving response carries
  `X-Capacity-Weight: <0–100>`
  ([`RecommendationController`](../../src/main/java/com/recsys/api/rest/RecommendationController.java)
  sets it from `loadShedder.snapshot().suggestedWeight()`), so an inline balancer can
  weight-shift per response.
- **On the health surface** — `GET /health/load` and `GET /online/ops` expose the same
  `suggestedWeight`, and `GET /health/ready` returns `503` past the drain utilization so
  the LB removes the node from rotation entirely.

```bash
curl -s -D - -o /dev/null -X POST http://localhost:8080/api/v1/recommend \
  -H "Content-Type: application/json" -d '{"userId":"123","k":5}' | grep -i x-capacity-weight
# X-Capacity-Weight: 89
```

The result is a graceful gradient rather than a binary in/out: at 11% utilization the
weight is ~89 and the node takes a full share; as it approaches the concurrency cap the
weight decays toward 0 and traffic drifts elsewhere; past the drain threshold it fails
readiness and drains. The shedders themselves are the [Fault Tolerance
investigation](18_Fault_Tolerance.md#2-overload-protection--shed-fast-never-queue-unbounded).

## 3. The `ApplicationLoadBalancer` L7 model

[`ApplicationLoadBalancer`](../../src/main/java/com/recsys/infrastructure/alb/ApplicationLoadBalancer.java)
and the `infrastructure/alb/` package model an ALB-style Layer-7 balancer in memory:
a `route(port, path, host, method)` call resolves the `AlbListener` for the port,
evaluates its `ListenerRule`s (path-pattern conditions) in priority order to pick a
target-group name, and then asks the
[`AlbTargetGroup`](../../src/main/java/com/recsys/infrastructure/alb/AlbTargetGroup.java) for
the next target. Target selection is **round-robin over healthy targets only**:
`nextTarget()` filters to routable targets and advances an `AtomicInteger` counter
(`counter.getAndIncrement() % healthy.size()`), so an unhealthy target is skipped and
each healthy one gets an even share.

It is **a reference/tested model, not the production data path** — real routing is done
by the AWS ALB and Armeria (§1). Its value is as an executable spec of the listener →
rule → target-group → health-aware-round-robin shape the EKS ALB ingress implements, and
as the unit under test for that routing logic.

## 4. How the layers relate

- **Load-bearing in production:** AWS ALB (edge), kube-proxy + topology-aware routing
  (in-cluster), Armeria upstream clients with connection recycling (gateway → backends), AZ-aware
  Redis read routing (data). These actually move packets.
- **The feedback loop:** the capacity-weight signal is what makes any of those balancers
  *capacity-aware* rather than merely *liveness-aware* — the app publishes `X-Capacity-
  Weight` / `suggestedWeight`, and an ALB/Envoy/mesh weights traffic accordingly.
- **Reference:** `ApplicationLoadBalancer` documents and tests the L7 routing algorithm
  without being in the request path.

## 5. Testing

- **The L7 model** — `ApplicationLoadBalancerTest` covers the whole shape:
  listener-rule routing by priority, path-pattern matching, and health-aware round-robin
  over the target group (including skipping unhealthy targets).
- **Capacity weight** — the load-shedder tests (`LoadShedderTest`,
  `OnlineLoadShedderTest`) cover `suggestedWeight = (1 − util) × 100` and the
  shutting-down → 0 case.
- **Gateway upstream clients** — `UpstreamSelectionTest` drives a per-connection
  round-robin proxy standing in for kube-proxy: with recycling every pod receives requests,
  without it one pod takes everything, and a pod that turns unhealthy after startup stays
  selectable (in the `-Presilience` PR gate). `GatewayUpstreamHealthCheckIntegrationTest`
  covers a never-healthy upstream being answered `503` within the selection timeout.

## Design specs & plans

Each production load-balancing layer has an explicit design spec (with a paired
implementation plan) under `docs/superpowers/`; the in-memory `ApplicationLoadBalancer`
model has none (it is a reference/test artifact, not a shipped feature).

- **Client-side health-aware LB** — [Health-Aware Upstream Discovery (Option A1)](../superpowers/specs/2026-07-10-gateway-upstream-endpoint-discovery-design.md)
  ([plan](../superpowers/plans/2026-07-10-gateway-upstream-endpoint-discovery.md)): the
  Armeria `HealthCheckedEndpointGroup` per backend that drops unhealthy replicas (§1).
- **Same-AZ routing** — [Cross-AZ Traffic Reduction](../superpowers/specs/2026-07-02-cross-az-traffic-reduction-design.md)
  ([reduction plan](../superpowers/plans/2026-07-02-cross-az-traffic-reduction.md),
  [AZ-aware reads plan](../superpowers/plans/2026-07-08-az-aware-redis-reads.md)):
  `trafficDistribution: PreferClose` and AZ-aware Redis reads (§1).
- **The edge ALB** — [Gateway WAF Ingress](../superpowers/specs/2026-07-02-gateway-waf-ingress-design.md)
  ([plan](../superpowers/plans/2026-07-02-gateway-waf-ingress.md)): the WAF-protected
  ALB Ingress that replaces the NLB as the sole public entry (§1).
- **Gateway connection recycling** — [Gateway Upstream Connection Recycling](../superpowers/specs/2026-10-06-gateway-upstream-connection-recycling-design.md)
  ([plan](../superpowers/plans/2026-10-06-gateway-upstream-connection-recycling.md)): the
  shared `ClientFactory` with a max connection age, and the initial-readiness health gate
  ([Connection pinning and recycling](#connection-pinning-and-recycling)).
- **Capacity-weight feedback** — [Overload Protection Hardening](../superpowers/specs/2026-07-08-overload-protection-design.md)
  ([plan](../superpowers/plans/2026-07-08-overload-protection.md)): the load shedders
  that compute `suggestedWeight` and emit `X-Capacity-Weight` (§2).

## Sharp edges — notes

1. **`ApplicationLoadBalancer` isn't in the request path.** It's a tested model of the
   ALB's L7 routing, not the production balancer — don't mistake it for where real
   traffic is routed. It also models only the target-group `Protocol` field
   (HTTP/HTTPS); `ProtocolVersion` — where a real ALB's gRPC support lives — is not
   represented, so its protocol rejection is a limit of the model, not of ALB.
2. **Capacity-weight needs an external consumer.** The app *emits* `X-Capacity-Weight`,
   but nothing balances on it unless an ALB target-group / Envoy / mesh is configured to
   read it; on its own it's just an observable signal.
3. **Two independent "is it up?" signals.** Armeria health checks (~10 s, but only until a
   backend first becomes ready — see edge 7) and the registry TTL (~20–40 s) detect a down
   backend on different timescales — see
   [11_Service_Discovery](11_Service_Discovery.md); capacity weight is a third, finer
   signal that acts before either trips.
4. **Readiness drain is binary; weight is a gradient.** `/health/ready` → `503` removes a
   node entirely past the drain threshold, whereas the weight decays smoothly below it —
   they're two stages of the same back-pressure, not alternatives.
5. **Round-robin is even, not capacity-aware, inside the model.** `AlbTargetGroup`
   round-robins healthy targets equally; capacity-aware shifting happens at the *real* LB
   via the weight header, not inside the in-memory model.
6. **Gateway → backend balancing is time-averaged, not per request.** Connection
   recycling spreads load across pods *over time*; at any instant, at most
   gateway-replica-count backend pods carry gateway traffic for a given backend. True
   per-request, per-pod balancing (headless Services + Armeria DNS endpoint groups,
   optionally weighted by `X-Capacity-Weight`) would bypass kube-proxy and therefore
   `PreferClose`, and would need same-AZ preference rebuilt in the gateway.
7. **The gateway health check never drops a backend after startup.** It keeps a
   never-ready backend out of selection (`503` after the selection timeout, which equals
   `GATEWAY_TIMEOUT_MS`), but `allowEmptyEndpoints(false)`
   makes Armeria ignore an update that would empty a one-endpoint group. Per-pod health
   is the readiness probe's job — deliberately; see
   [Connection pinning and recycling](#connection-pinning-and-recycling).
