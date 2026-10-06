# Gateway Upstream Connection Recycling — Design

**Date:** 2026-10-06
**Status:** Approved (in chat, 2026-10-06)
**Investigation:** [01_Load_Balancing](../../system_design/01_Load_Balancing.md)

## Problem

The gateway reaches each backend through `UpstreamEndpointGroups`, which wraps a single
static `Endpoint.of(host, port)` — the backend's ClusterIP Service name — in a
`HealthCheckedEndpointGroup`. Armeria therefore sees one endpoint per backend and does no
balancing; kube-proxy picks a pod once per TCP connection. Armeria's default client speaks
h2c to the Armeria backends (6010, 7010) and multiplexes every request onto one long-lived
connection, so each gateway pod is pinned to one backend pod indefinitely.

Measured on 2026-10-06 with a throwaway probe (a per-connection round-robin TCP proxy standing
in for kube-proxy, the client built exactly as `UpstreamEndpointGroups` builds it):

| Probe | Result |
|---|---|
| 2 backends, 2048 requests, 64 in flight | 1 TCP connection (h2c); 100% of requests on one backend |
| scale out to 4 backends, 2048 more | still 100% on the original backend; new backends got 0 |
| same, `ClientFactory.maxConnectionAgeMillis(1000)` | 8 connections; 4352 / 6080 / 7232 / 6336 across 4 backends, 0 errors |
| one backend, healthy → unhealthy, `allowEmptyEndpoints(false)` | endpoint **never dropped**; requests still sent |
| same, `allowEmptyEndpoints(true)` | endpoint dropped; requests fail fast |

Consequences in production: catalog and online serving carry gateway traffic on at most
*gateway-replica-count* pods at a time, and pods added by the HPA receive no gateway traffic
until an existing connection happens to close. Separately, the gateway health check only
gates *initial* readiness: `allowEmptyEndpoints(false)` makes Armeria ignore an update to an
empty list, so a one-endpoint group can never empty after it first becomes ready. The code
comment and `01_Load_Balancing.md` both claim otherwise, and
`GatewayUpstreamHealthCheckIntegrationTest` only covers a never-healthy port.

## Goals

- Gateway traffic to a backend spreads across all of its ready pods over time, including
  pods added by scale-out.
- Topology-aware routing (`trafficDistribution: PreferClose`) keeps working — the fix must
  stay on the kube-proxy path.
- The health-check semantics are stated truthfully in code, tests, and docs.

## Non-goals

- Per-request, per-pod client-side balancing (headless Services + Armeria DNS endpoint
  groups, weighting on `X-Capacity-Weight`). That bypasses kube-proxy and would need
  same-AZ preference rebuilt in the gateway; it is a separate, architectural change.
- The LLM proxy's dedicated `ClientFactory` (Ollama is a single upstream, HTTP/1.1-only).

## Design

### A. Recycle upstream connections

Give the gateway's upstream clients a shared Armeria `ClientFactory` with
`maxConnectionAgeMillis` set. When a connection reaches that age Armeria stops sending new
requests on it, lets in-flight ones finish, and closes it; the next request opens a fresh
connection, and kube-proxy picks a pod again. Over time every ready pod is chosen.

- **Config:** `GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS`, default `30000`; `0` disables.
  Armeria rejects a non-zero value below 1000 ms, so the config record rejects negative
  values and `1..999` at construction — a misconfiguration fails startup, not first request.
  `k8s/base/configmap.yaml` sets the default explicitly.
- **Cost:** one TCP + HTTP/2 handshake per max-age per gateway pod per backend authority —
  in-cluster, negligible.
- **Limit (stated in the docs):** this balances *over time*, not *instantaneously*. At any
  moment, at most gateway-replica-count backend pods carry gateway traffic per backend.
- **Config record:** `UpstreamEndpointGroups.HealthCheckConfig` becomes
  `UpstreamEndpointGroups.UpstreamClientConfig(healthCheckEnabled, healthCheckIntervalMs,
  maxConnectionAgeMs)`, read by `fromEnvironment()`. The record now configures the client as a
  whole, so the old name would mislead. A two-argument convenience constructor keeps test
  call sites short, defaulting the age to the production default.
- **Factory ownership:** `UpstreamEndpointGroups.create(...)` takes a `ClientFactory` and
  uses it for both the `WebClient`s and the health checker (so probes rotate too). It does
  **not** own the factory. Owners close it once:
  - static path — `GatewayRequestForwarder` builds it in the canonical constructor, closes it
    in `close()` after the groups;
  - registry path — `RegistryBackedUpstreams` builds it once and reuses it across rebuilds,
    so swapping endpoint groups never kills in-flight requests on the shared connections;
    closed in its `close()`.

### B. Keep `allowEmptyEndpoints(false)` and say what it does

Flipping to `allowEmptyEndpoints(true)` would make post-startup health checking work —
but once connections rotate, each probe lands on a random pod, so a single bad pod out of N
would periodically empty the *whole* backend and 503 every request for up to one probe
interval. Per-pod health already belongs to Kubernetes readiness probes, which remove a bad
pod from the Service's endpoints. So:

- Keep `allowEmptyEndpoints(false)`.
- Correct the comment in `UpstreamEndpointGroups.buildGroup` and the class Javadoc: the
  gateway health check gates initial readiness (a never-ready backend fast-fails with 503);
  after that, pod health is kube-proxy's job via readiness.
- Add a test pinning that behaviour (healthy → unhealthy stays selectable), so a future
  change to it is a deliberate one.

## Testing

- **`UpstreamConnectionRecyclingTest`** (new, non-docker, added to `-Presilience` with a
  comment): a per-connection round-robin TCP proxy in front of four Armeria backends.
  - with max age 1000 ms, every backend receives requests within a bounded poll window;
  - with age 0 (disabled), every request lands on one backend — this is the bug, asserted
    so the test demonstrably distinguishes the two;
  - a healthy → unhealthy single backend stays selectable (B).
  Watch the recycling assertion fail with the factory option removed before trusting it.
- **Config:** `UpstreamClientConfig` rejects negative and `1..999`, accepts `0` and `>= 1000`,
  and reads the env default of 30000.
- Existing gateway suites (`UpstreamEndpointGroupsTest`, `RegistryBackedUpstreamsTest`,
  `GatewayUpstreamHealthCheckIntegrationTest`, …) stay green after the rename.

## Docs

- `01_Load_Balancing.md`: correct §1's "load-balances across the healthy replicas" claim,
  add the measured pinning behaviour and the recycling fix, add a sharp edge for the
  time-averaged limit and the health-check semantics, and link this spec under "Design specs
  & plans".
- `09_API_Gateway.md`: document the env var next to `GATEWAY_UPSTREAM_HEALTHCHECK_*`, and
  correct any claim there that the health check drops a backend after startup.
- `.claude/CLAUDE.md` is not committed (project convention); the PR description notes the
  new env var for it.
