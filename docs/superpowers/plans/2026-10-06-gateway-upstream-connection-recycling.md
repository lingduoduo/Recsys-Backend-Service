# Gateway Upstream Connection Recycling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stop each gateway pod pinning all traffic for a backend onto one backend pod, by recycling upstream connections so kube-proxy re-picks pods over time, and make the gateway health-check semantics truthful.

**Architecture:** The gateway's upstream `WebClient`s and health checkers share one Armeria `ClientFactory` with `maxConnectionAgeMillis` set (env `GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS`, default 30000, 0 disables). The factory is owned by whoever owns the upstream set (`GatewayRequestForwarder` on the static path, `RegistryBackedUpstreams` on the registry path) and outlives endpoint-group rebuilds. `allowEmptyEndpoints(false)` is kept and documented as "gates initial readiness only".

**Tech Stack:** Java 17, Armeria 1.28.4, JUnit 5 + AssertJ, Maven (`JAVA_HOME=$(/usr/libexec/java_home -v 17)`).

**Spec:** [docs/superpowers/specs/2026-10-06-gateway-upstream-connection-recycling-design.md](../specs/2026-10-06-gateway-upstream-connection-recycling-design.md)

## Global Constraints

- Env var: `GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS`, default `30000`, `0` disables; negative and `1..999` are rejected at construction (Armeria's floor is 1000 ms).
- Upstream hosts stay ClusterIP Service names on the kube-proxy path — no headless Services, no DNS endpoint groups (preserves `trafficDistribution: PreferClose`).
- `allowEmptyEndpoints(false)` is kept.
- The LLM proxy's `ClientFactory` is out of scope.
- Do not commit `.claude/CLAUDE.md`.
- New merge-blocking tests must be non-docker and listed in the `-Presilience` profile with a comment saying what breaks without them.

## Review Focus

1. **Registry rebuild while requests are in flight** — swapping endpoint groups must not close the shared factory; only `RegistryBackedUpstreams.close()` does. Pinned by Task 2's `RegistryBackedUpstreams` reuse of one factory (verified by code review; existing `RegistryBackedUpstreamsTest` exercises rebuild + close).
2. **Misconfigured age (e.g. `500`)** — must fail gateway startup with a message naming the env var, not first request. Pinned by Task 1's validation tests.
3. **Age disabled (`0`)** — must behave exactly as today (one pinned connection), not throw. Pinned by Task 2's `withoutRecyclingOnePodTakesEverything`.
4. **Pinned pod reports unhealthy after startup** — must stay selectable (deliberate; readiness owns per-pod health). Pinned by Task 3's test.
5. **Shutdown** — closing the forwarder closes groups then the factory, once; double close must not throw. Covered by existing `closeIsIdempotent` plus Task 2's owner close.

---

### Task 1: `UpstreamClientConfig` (rename + max connection age + validation)

**Files:**
- Modify: `src/main/java/com/recsys/application/gateway/UpstreamEndpointGroups.java` (the `HealthCheckConfig` record)
- Modify (mechanical rename `HealthCheckConfig` → `UpstreamClientConfig`): `GatewayRequestForwarder.java`, `RegistryBackedUpstreams.java`, and tests `GatewayStreamingBenchmarkTest`, `RegistryBackedUpstreamsTest`, `GatewayResponseStreamingTest`, `GatewayPathCanonicalizationTest`, `ProxyRoutePolicyEnforcementTest`, `UserScopeAuthorizationTest`, `UpstreamEndpointGroupsTest`, `GatewayCircuitMetricsTest`, `GatewayUpstreamHealthCheckIntegrationTest`
- Test: `src/test/java/com/recsys/application/gateway/UpstreamEndpointGroupsTest.java`

**Interfaces:**
- Produces:
  ```java
  record UpstreamClientConfig(boolean healthCheckEnabled, long healthCheckIntervalMs, long maxConnectionAgeMs) {
      static final long DEFAULT_MAX_CONNECTION_AGE_MS = 30_000L;
      UpstreamClientConfig(boolean healthCheckEnabled, long healthCheckIntervalMs); // age = default
      static UpstreamClientConfig fromEnvironment();
      static UpstreamClientConfig fromEnvironment(EnvVars.EnvReader env);
      ClientFactory newClientFactory(); // caller owns and must close it
  }
  ```

- [ ] **Step 1: Rename mechanically**

```bash
grep -rl 'HealthCheckConfig' src/main/java src/test/java | xargs sed -i '' 's/HealthCheckConfig/UpstreamClientConfig/g'
```

- [ ] **Step 2: Write the failing tests** (append to `UpstreamEndpointGroupsTest`; add imports `java.util.Map`, `static org.assertj.core.api.Assertions.assertThatThrownBy`)

```java
    @Test
    void maxConnectionAgeDefaultsTo30sWhenUnset() {
        assertThat(UpstreamEndpointGroups.UpstreamClientConfig.fromEnvironment(name -> null).maxConnectionAgeMs())
                .isEqualTo(30_000L);
        assertThat(new UpstreamEndpointGroups.UpstreamClientConfig(true, 10_000L).maxConnectionAgeMs())
                .isEqualTo(UpstreamEndpointGroups.UpstreamClientConfig.DEFAULT_MAX_CONNECTION_AGE_MS);
    }

    @Test
    void maxConnectionAgeIsReadFromTheEnvironment() {
        Map<String, String> env = Map.of("GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS", "0");
        assertThat(UpstreamEndpointGroups.UpstreamClientConfig.fromEnvironment(env::get).maxConnectionAgeMs())
                .isZero();
    }

    @Test
    void maxConnectionAgeAcceptsZeroAndAtLeastOneSecond() {
        assertThat(new UpstreamEndpointGroups.UpstreamClientConfig(true, 1000, 0).maxConnectionAgeMs()).isZero();
        assertThat(new UpstreamEndpointGroups.UpstreamClientConfig(true, 1000, 1000).maxConnectionAgeMs())
                .isEqualTo(1000);
    }

    @Test
    void maxConnectionAgeRejectsNegativeAndSubSecondValues() {
        for (long bad : new long[]{-1, 1, 999}) {
            assertThatThrownBy(() -> new UpstreamEndpointGroups.UpstreamClientConfig(true, 1000, bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS");
        }
    }
```

- [ ] **Step 3: Run to verify they fail**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=UpstreamEndpointGroupsTest`
Expected: compilation failure (`fromEnvironment(EnvReader)` / 3-arg constructor / `DEFAULT_MAX_CONNECTION_AGE_MS` missing).

- [ ] **Step 4: Implement** — replace the record in `UpstreamEndpointGroups`:

```java
    /**
     * How the gateway's upstream clients behave. {@code maxConnectionAgeMs} recycles each upstream
     * connection after that age so kube-proxy re-picks a backend pod: without it, Armeria multiplexes
     * every request to a backend onto one long-lived HTTP/2 connection, pinning a gateway pod to one
     * backend pod indefinitely. {@code 0} disables recycling.
     */
    record UpstreamClientConfig(boolean healthCheckEnabled, long healthCheckIntervalMs, long maxConnectionAgeMs) {

        static final long DEFAULT_MAX_CONNECTION_AGE_MS = 30_000L;
        // Armeria rejects a non-zero max connection age below this.
        private static final long MIN_MAX_CONNECTION_AGE_MS = 1_000L;

        UpstreamClientConfig {
            if (maxConnectionAgeMs < 0
                    || (maxConnectionAgeMs > 0 && maxConnectionAgeMs < MIN_MAX_CONNECTION_AGE_MS)) {
                throw new IllegalArgumentException("GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS must be 0 (disabled) or >= "
                        + MIN_MAX_CONNECTION_AGE_MS + ", was " + maxConnectionAgeMs);
            }
        }

        UpstreamClientConfig(boolean healthCheckEnabled, long healthCheckIntervalMs) {
            this(healthCheckEnabled, healthCheckIntervalMs, DEFAULT_MAX_CONNECTION_AGE_MS);
        }

        static UpstreamClientConfig fromEnvironment() {
            return fromEnvironment(System::getenv);
        }

        static UpstreamClientConfig fromEnvironment(EnvVars.EnvReader env) {
            boolean enabled = EnvVars.readBool(env, "GATEWAY_UPSTREAM_HEALTHCHECK_ENABLED", true);
            long intervalMs = EnvVars.readLong(env, "GATEWAY_UPSTREAM_HEALTHCHECK_INTERVAL_MS", 10_000L);
            long maxAgeMs = EnvVars.readLong(env, "GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS",
                    DEFAULT_MAX_CONNECTION_AGE_MS);
            return new UpstreamClientConfig(enabled, intervalMs, maxAgeMs);
        }

        /** A new factory for the upstream clients and health checkers; the caller owns and closes it. */
        ClientFactory newClientFactory() {
            return ClientFactory.builder().maxConnectionAgeMillis(maxConnectionAgeMs).build();
        }
    }
```

(Import `com.linecorp.armeria.client.ClientFactory`. Confirm `EnvVars.readBool(EnvReader, String, boolean)` exists — it does, `EnvVars.java:49`.)

- [ ] **Step 5: Run to verify they pass**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=UpstreamEndpointGroupsTest`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A src/main/java/com/recsys/application/gateway src/test/java/com/recsys/application/gateway
git commit -m "refactor(gateway): UpstreamClientConfig with validated max connection age"
```

---

### Task 2: Shared recycling `ClientFactory` for upstream clients

**Files:**
- Modify: `UpstreamEndpointGroups.java` (`create`, `buildGroup`)
- Modify: `GatewayRequestForwarder.java` (canonical static constructor, `close()`)
- Modify: `RegistryBackedUpstreams.java` (constructor, `build`, `close()`)
- Modify: `UpstreamEndpointGroupsTest.java` (pass a factory to `create`)
- Create: `src/test/java/com/recsys/application/gateway/UpstreamSelectionTest.java`
- Modify: `pom.xml` (`resilience` profile include)
- Modify: `k8s/base/configmap.yaml`

**Interfaces:**
- Consumes: `UpstreamClientConfig.newClientFactory()` (Task 1).
- Produces: `static UpstreamEndpointGroups create(List<MicroserviceRoute> routes, Duration responseTimeout, Function<? super HttpClient, ? extends HttpClient> decorator, UpstreamClientConfig config, ClientFactory factory)` — does not own `factory`.

- [ ] **Step 1: Write the failing test** — `UpstreamSelectionTest.java`:

```java
package com.recsys.application.gateway;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How the gateway's upstream clients spread load across the pods behind one Service address. A
 * per-connection round-robin TCP proxy stands in for kube-proxy, which picks a backend pod once per
 * connection: without connection recycling Armeria multiplexes every request onto one HTTP/2
 * connection, so a single pod takes all of it — including after scale-out.
 */
class UpstreamSelectionTest {

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        for (int i = resources.size() - 1; i >= 0; i--) {
            resources.get(i).close();
        }
    }

    /** A backend pod: counts /work requests, serves a togglable GET /health. */
    private static final class Pod implements AutoCloseable {
        final AtomicInteger requests = new AtomicInteger();
        final AtomicInteger probes = new AtomicInteger();
        final AtomicBoolean healthy = new AtomicBoolean(true);
        final Server server = Server.builder().http(0)
                .service("/health", (ctx, req) -> {
                    probes.incrementAndGet();
                    return HttpResponse.of(healthy.get() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);
                })
                .service("/work", (ctx, req) -> {
                    requests.incrementAndGet();
                    return HttpResponse.of("ok");
                })
                .build();

        Pod() {
            server.start().join();
        }

        int port() {
            return server.activeLocalPort();
        }

        @Override
        public void close() {
            server.stop().join();
        }
    }

    /** kube-proxy stand-in: each accepted TCP connection goes to the next pod, round-robin. */
    private static final class PerConnectionProxy implements AutoCloseable {
        final ServerSocket socket;
        final List<Pod> pods;
        final AtomicInteger next = new AtomicInteger();

        PerConnectionProxy(List<Pod> pods) throws IOException {
            this.pods = pods;
            this.socket = new ServerSocket(0);
            Thread acceptor = new Thread(this::acceptLoop, "per-connection-proxy");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        private void acceptLoop() {
            while (!socket.isClosed()) {
                try {
                    Socket client = socket.accept();
                    Pod pod = pods.get(Math.floorMod(next.getAndIncrement(), pods.size()));
                    Socket upstream = new Socket();
                    upstream.connect(new InetSocketAddress("127.0.0.1", pod.port()));
                    pipe(client, upstream);
                    pipe(upstream, client);
                } catch (IOException ignored) {
                    // socket closed at test end, or a pod already stopped
                }
            }
        }

        private static void pipe(Socket from, Socket to) {
            Thread t = new Thread(() -> {
                try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                    in.transferTo(out);
                } catch (IOException ignored) {
                    // peer closed
                } finally {
                    closeQuietly(from);
                    closeQuietly(to);
                }
            }, "per-connection-proxy-pipe");
            t.setDaemon(true);
            t.start();
        }

        private static void closeQuietly(Socket s) {
            try {
                s.close();
            } catch (IOException ignored) {
                // already closed
            }
        }

        int port() {
            return socket.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private List<Pod> pods(int n) {
        List<Pod> pods = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Pod pod = new Pod();
            resources.add(pod);
            pods.add(pod);
        }
        return pods;
    }

    private WebClient client(int port, UpstreamEndpointGroups.UpstreamClientConfig config) {
        ClientFactory factory = config.newClientFactory();
        resources.add(factory);
        MicroserviceRoute route = new MicroserviceRoute("catalog", "/api/catalog", "CATALOG_SERVICE_URL",
                URI.create("http://127.0.0.1:" + port), "/health");
        UpstreamEndpointGroups groups = UpstreamEndpointGroups.create(
                List.of(route), Duration.ofSeconds(2), null, config, factory);
        resources.add(groups);
        return groups.clientFor("catalog");
    }

    /** Sends {@code concurrency} requests at once and waits for all of them; returns the non-200 count. */
    private static int burst(WebClient client, int concurrency) {
        AtomicInteger failures = new AtomicInteger();
        CompletableFuture<?>[] inFlight = new CompletableFuture<?>[concurrency];
        for (int i = 0; i < concurrency; i++) {
            inFlight[i] = client.get("/work").aggregate().handle((AggregatedHttpResponse res, Throwable err) -> {
                if (err != null || res.status() != HttpStatus.OK) {
                    failures.incrementAndGet();
                }
                return null;
            });
        }
        CompletableFuture.allOf(inFlight).join();
        return failures.get();
    }

    private static long podsServing(List<Pod> pods) {
        return pods.stream().filter(p -> p.requests.get() > 0).count();
    }

    @Test
    void recyclingSpreadsRequestsAcrossEveryPod() throws Exception {
        List<Pod> pods = pods(4);
        PerConnectionProxy proxy = new PerConnectionProxy(pods);
        resources.add(proxy);
        WebClient client = client(proxy.port(), new UpstreamEndpointGroups.UpstreamClientConfig(true, 200, 1000));

        int failures = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (podsServing(pods) < pods.size() && System.nanoTime() < deadline) {
            failures += burst(client, 32);
            Thread.sleep(20);
        }

        assertThat(podsServing(pods)).as("pods that received requests").isEqualTo(pods.size());
        assertThat(failures).as("recycling must not fail requests").isZero();
    }

    @Test
    void withoutRecyclingOnePodTakesEverything() throws Exception {
        List<Pod> pods = pods(4);
        PerConnectionProxy proxy = new PerConnectionProxy(pods);
        resources.add(proxy);
        WebClient client = client(proxy.port(), new UpstreamEndpointGroups.UpstreamClientConfig(true, 200, 0));

        // Run well past the 1 s age the recycling test uses, so recycling would have spread these.
        long until = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < until) {
            assertThat(burst(client, 32)).isZero();
            Thread.sleep(20);
        }

        assertThat(podsServing(pods)).as("pods that received requests").isEqualTo(1);
    }
}
```

Note: `UpstreamEndpointGroups` must be `AutoCloseable` for `resources.add(groups)` — it implements `java.io.Closeable`, which extends `AutoCloseable`. ✔

- [ ] **Step 2: Run to verify it fails**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=UpstreamSelectionTest`
Expected: compilation failure — `create(..., ClientFactory)` does not exist.

- [ ] **Step 3: Implement `create`/`buildGroup` with the factory** in `UpstreamEndpointGroups`:

```java
    static UpstreamEndpointGroups create(List<MicroserviceRoute> routes,
                                         Duration responseTimeout,
                                         Function<? super HttpClient, ? extends HttpClient> decorator,
                                         UpstreamClientConfig config,
                                         ClientFactory factory) {
        // ... unchanged loop, except:
        //   buildGroup(protocol, host, port, healthPath, responseTimeout, config, factory)
        //   WebClient.builder(protocol, group).factory(factory).responseTimeoutMillis(...)
    }
```

and in `buildGroup` add the parameter and `.clientFactory(factory)` to the `HealthCheckedEndpointGroup.builder(...)` chain, so health probes rotate with the data connections.

- [ ] **Step 4: Owners create and close the factory**

`GatewayRequestForwarder` — add `private final ClientFactory upstreamClientFactory;` (null on the registry path). Canonical static constructor:

```java
        this.upstreamClientFactory = healthConfig.newClientFactory();
        this.staticUpstreams = UpstreamEndpointGroups.create(routes, timeout, retryDecorator(), healthConfig,
                upstreamClientFactory);
```

registry constructor: `this.upstreamClientFactory = null;`. `close()`:

```java
    @Override
    public void close() {
        if (registryUpstreams != null) {
            registryUpstreams.close();
        } else {
            staticUpstreams.close();
            upstreamClientFactory.close();
        }
    }
```

`RegistryBackedUpstreams` — add `private final ClientFactory clientFactory;`, set `this.clientFactory = healthConfig.newClientFactory();` before `this.current = build(...)`; `build` passes it to `create`; `close()` closes `current` then `clientFactory`. One factory for the object's lifetime, so a rebuild never closes connections that in-flight requests on the old groups still use.

`UpstreamEndpointGroupsTest` — the three `create(...)` calls pass a factory; use one per test from `ClientFactory.builder().build()` closed in the test's `finally`, e.g.:

```java
        ClientFactory factory = ClientFactory.builder().build();
        UpstreamEndpointGroups groups = UpstreamEndpointGroups.create(
                routes, Duration.ofSeconds(3), null, cfg(false), factory);
        try {
            ...
        } finally {
            groups.close();
            factory.close();
        }
```

- [ ] **Step 5: Run to verify it passes**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest='UpstreamSelectionTest,UpstreamEndpointGroupsTest,RegistryBackedUpstreamsTest'`
Expected: PASS.

- [ ] **Step 6: Watch the test fail against the bug** — temporarily change `newClientFactory()` to `ClientFactory.builder().build()` (ignore the age), rerun `-Dtest=UpstreamSelectionTest`, confirm `recyclingSpreadsRequestsAcrossEveryPod` FAILS with 1 pod serving, then restore.

- [ ] **Step 7: Wire config and the PR gate**

`k8s/base/configmap.yaml`, next to the other gateway keys:

```yaml
  # Recycle each gateway→backend connection after this age so kube-proxy re-picks a pod; without it
  # one HTTP/2 connection pins a gateway pod to one backend pod. 0 disables. See 01_Load_Balancing.md.
  GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS: "30000"
```

`pom.xml`, `resilience` profile, after the `UpstreamEndpointGroupsTest` include:

```xml
                <!-- Load spread behind a Service address. kube-proxy picks a pod per connection and
                     Armeria multiplexes a backend onto one HTTP/2 connection, so without
                     GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS every gateway pod is pinned to one backend
                     pod and scale-out adds pods that get no gateway traffic. Measured: 4096/4096
                     requests on one of four pods. Local sockets only; polls with a generous deadline. -->
                <include>**/gateway/UpstreamSelectionTest.java</include>
```

- [ ] **Step 8: Run the full gateway package and the gate profile**

Run: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest='com.recsys.application.gateway.**'` then `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Presilience`
Expected: PASS (note OutboxRelayTest is a known timing flake in `-Presilience`).

- [ ] **Step 9: Commit**

```bash
git add src pom.xml k8s/base/configmap.yaml
git commit -m "fix(gateway): recycle upstream connections so kube-proxy spreads load across pods"
```

---

### Task 3: Health-check semantics — documented and pinned

**Files:**
- Modify: `UpstreamEndpointGroups.java` (class Javadoc + `buildGroup` comment)
- Modify: `src/test/java/com/recsys/application/gateway/GatewayUpstreamHealthCheckIntegrationTest.java` (class Javadoc only)
- Test: `UpstreamSelectionTest.java`

**Interfaces:**
- Consumes: `Pod`, `client(...)`, `burst(...)` helpers in `UpstreamSelectionTest` (Task 2).

- [ ] **Step 1: Add the test** to `UpstreamSelectionTest`:

```java
    @Test
    void healthCheckGatesOnlyInitialReadiness() throws Exception {
        // Deliberate: after first readiness the one-endpoint group never empties, because with
        // connections rotating each probe lands on a random pod and one bad pod would 503 the whole
        // backend. Per-pod health is the readiness probe's job. Changing this must be a decision.
        Pod pod = pods(1).get(0);
        WebClient client = client(pod.port(), new UpstreamEndpointGroups.UpstreamClientConfig(true, 200, 0));
        assertThat(burst(client, 4)).isZero();

        pod.healthy.set(false);
        int probesBefore = pod.probes.get();
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (pod.probes.get() < probesBefore + 3 && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }

        assertThat(pod.probes.get()).as("the checker kept probing and saw 503s").isGreaterThanOrEqualTo(probesBefore + 3);
        assertThat(burst(client, 4)).as("still selectable after failing health checks").isZero();
    }
```

- [ ] **Step 2: Run it** — `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=UpstreamSelectionTest` — expected PASS (this pins existing behaviour; the spike measured it). Then confirm it distinguishes: temporarily set `.allowEmptyEndpoints(true)` in `buildGroup`, rerun, see it FAIL, restore.

- [ ] **Step 3: Correct the comments.** In `buildGroup`, replace the `allowEmptyEndpoints(false)` paragraph with:

```java
        // allowEmptyEndpoints(false): Armeria then ignores an update that would leave the group empty.
        // With one endpoint per group that means the health check gates INITIAL readiness only — a
        // backend that never answers healthy stays out of the group and selection fails fast with 503
        // (bounded by the selection timeout) — but once ready, the endpoint is never dropped. That is
        // deliberate: the endpoint is a Service address, and with connections recycling each probe
        // lands on a random pod, so dropping on a failed probe would let one bad pod 503 the whole
        // backend. Per-pod health belongs to the readiness probe, which removes the pod from the
        // Service. Pinned by UpstreamSelectionTest.healthCheckGatesOnlyInitialReadiness.
```

Update the class Javadoc sentence "so a down upstream is dropped from selection and requests fast-fail instead of hanging" to "so an upstream that is not yet healthy is kept out of selection and requests fast-fail instead of hanging (initial readiness only — see {@code buildGroup})", and add a sentence that the shared `ClientFactory` recycles connections. In `GatewayUpstreamHealthCheckIntegrationTest`'s class Javadoc, change "an upstream whose health check never passes is dropped from selection" to "an upstream whose health check never passes is never added to selection".

- [ ] **Step 4: Commit**

```bash
git add src/main/java/com/recsys/application/gateway/UpstreamEndpointGroups.java src/test/java/com/recsys/application/gateway
git commit -m "docs(gateway): state that upstream health checks gate initial readiness only"
```

---

### Task 4: Documentation

**Files:**
- Modify: `docs/system_design/01_Load_Balancing.md` (§1 Armeria bullet, §4, §5, Design specs & plans, Sharp edges)
- Modify: `docs/system_design/09_API_Gateway.md` (line ~335 bullet)

- [ ] **Step 1: `01_Load_Balancing.md`**
  - §1 Armeria bullet: replace "drops a down backend from selection and load-balances across the healthy replicas" with: each group holds one endpoint — the Service address — so the pod choice is kube-proxy's, made once per connection; the shared client factory recycles connections every `GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS` (default 30 s) so that choice is re-made; the health check gates initial readiness only.
  - Add a subsection "### Connection pinning and recycling" with the measured table from the spec.
  - §4: "Armeria health-checked endpoint groups" → "Armeria upstream clients (connection recycling + startup health gate)".
  - §5 Testing: add `UpstreamSelectionTest`.
  - Design specs & plans: add this spec and plan.
  - Sharp edges: add (6) balancing is time-averaged — at any moment at most gateway-replica-count backend pods carry gateway traffic per backend; per-request per-pod balancing needs headless Services and would bypass `PreferClose`. (7) the gateway health check never drops a backend after startup; readiness probes own per-pod health. Fix sharp edge 3's "Armeria health checks (~10 s, drops from LB)" accordingly.
- [ ] **Step 2: `09_API_Gateway.md`** — in the "Health-checked upstreams + registry resolution" bullet, state the initial-readiness-only semantics and the new env var.
- [ ] **Step 3: Verify the docs index test still passes** — `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test -Dtest=DocumentationIndexTest` → PASS.
- [ ] **Step 4: Commit** — `git add docs && git commit -m "docs(lb): document gateway connection pinning and recycling"`

---

### Task 5: Verify and open the PR

- [ ] `rm -rf target/surefire-reports && JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test` — record pass/fail counts; compare failures against `main` (2 pre-existing Spark-fixture errors are known).
- [ ] Push the branch and open a PR to `main`; body notes the new env var for `.claude/CLAUDE.md` (not committed) and the time-averaged limit.
