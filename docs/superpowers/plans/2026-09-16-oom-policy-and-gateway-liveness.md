# OOM Exit Policy and Gateway Liveness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a JVM that has thrown `OutOfMemoryError` exit and restart, and stop the gateway from restarting itself when an upstream is down.

**Architecture:** Two independent changes to the pod restart contract. Part A adds `-XX:+ExitOnOutOfMemoryError` to every `k8s/base` workload's `JAVA_OPTS`. Part B adds a constant-200 `/health/live` route to the gateway and repoints its liveness and startup probes at it, leaving readiness on the upstream-aware `/health`. Each is pinned by a manifest conformance test that derives its subject set from the manifests rather than a hardcoded list.

**Tech Stack:** Java 17, Armeria 1.28.4, JUnit 5, AssertJ, SnakeYAML, Kustomize, Maven.

**Spec:** `docs/superpowers/specs/2026-09-16-oom-policy-and-gateway-liveness-design.md`

## Global Constraints

- Build with JDK 17: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn …`. Newer JDKs fail a clean compile of two pre-existing files.
- Add no dependencies. SnakeYAML and AssertJ are already test-scope.
- Every new test must be run **red** against the pre-change tree before the implementation lands. A conformance test that has never failed has not been shown to test anything.
- Every new test must be added to the `-Presilience` profile in `pom.xml`, each with a comment saying why it gates, matching the surrounding entries. Tests outside that profile do not run in the PR gate.
- Do not edit `.claude/CLAUDE.md`. Notes belong in the owning `docs/` file.
- Manifest tests live in `com.recsys.infrastructure.k8s` and use the package-private `ManifestDocuments` helper. Do not duplicate its accessors.
- `k8s/eks/` and `k8s/eks-us-west-2/` patch neither `JAVA_OPTS` nor any probe. Do not add overlay patches.

---

## File Structure

| File | Responsibility |
|---|---|
| `k8s/base/{catalog-serving,api-gateway,model-serving,online-serving,outbox-relay-deployment,outbox-reconciliation-cronjob}.yaml` | Add the OOM flag to `JAVA_OPTS` (Task 1) |
| `src/test/java/com/recsys/infrastructure/k8s/OomPolicyManifestTest.java` | Derives every `JAVA_OPTS`-setting container and requires the flag (Task 1) |
| `src/main/java/com/recsys/application/gateway/GatewayLivenessService.java` | Constant-200 `/health/live` handler (Task 2) |
| `src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java` | Register the route (Task 2) |
| `src/test/java/com/recsys/application/gateway/GatewayLivenessRouteTest.java` | Handler returns 200; the path is anonymously reachable (Task 2) |
| `k8s/base/api-gateway.yaml` | Repoint `livenessProbe` and `startupProbe` (Task 3) |
| `src/test/java/com/recsys/infrastructure/k8s/GatewayLivenessManifestTest.java` | Probe wiring, plus the general no-dependency-aware-liveness rule (Task 3) |
| `pom.xml` | `-Presilience` includes for all three new tests (Tasks 1–3) |
| `docs/system_design/18_Fault_Tolerance.md`, `docs/system_design/09_API_Gateway.md` | Record the decision and correct §9.5 (Task 4) |

---

### Task 1: OOM exit policy

**Files:**
- Create: `src/test/java/com/recsys/infrastructure/k8s/OomPolicyManifestTest.java`
- Modify: `k8s/base/catalog-serving.yaml:76`, `k8s/base/api-gateway.yaml:49`, `k8s/base/model-serving.yaml:61`, `k8s/base/online-serving.yaml:98`, `k8s/base/outbox-relay-deployment.yaml:62`, `k8s/base/outbox-reconciliation-cronjob.yaml:50`
- Modify: `pom.xml` (`-Presilience` include)

**Interfaces:**
- Consumes: `ManifestDocuments.allIn(Path)`, `.ofKind(List, String)`, `.mapAt(Map, String…)`, `.listOf(Map, String)`, `.nameOf(Map)` — all package-private statics in `com.recsys.infrastructure.k8s`.
- Produces: nothing other tasks depend on.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/recsys/infrastructure/k8s/OomPolicyManifestTest.java`:

```java
package com.recsys.infrastructure.k8s;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.recsys.infrastructure.k8s.ManifestDocuments.listOf;
import static com.recsys.infrastructure.k8s.ManifestDocuments.mapAt;
import static com.recsys.infrastructure.k8s.ManifestDocuments.nameOf;
import static com.recsys.infrastructure.k8s.ManifestDocuments.ofKind;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A JVM that has thrown OutOfMemoryError keeps running: request threads answer 500s while the
 * heap is short and 200s once GC recovers something, while a background loop killed by the same
 * Error stays dead. Liveness on every service is a constant 200 that inspects nothing, so the
 * platform never restarts the pod — an operator does, by hand, after reading a heap alert.
 *
 * <p>{@code -XX:+ExitOnOutOfMemoryError} is what turns that into a restart the platform already
 * knows how to perform. This test is the drift catcher: the flag lives in a string inside a YAML
 * env value, so nothing else would notice it going missing — not a compiler, not a schema, and
 * not a running cluster, which would simply go back to never restarting an OOM'd pod.
 *
 * <p>The workload set is <b>derived from the manifests</b>, never hardcoded. A hardcoded list
 * passes forever while the property quietly stops being universal: the next service added with a
 * JAVA_OPTS of its own would be exempt by omission, which is the one failure mode a conformance
 * test exists to prevent.
 */
class OomPolicyManifestTest {

    private static final Path BASE = Path.of("k8s", "base");

    private static final String OOM_EXIT_FLAG = "-XX:+ExitOnOutOfMemoryError";

    /** Workload kinds that carry a pod template. StatefulSet is included for future workloads. */
    private static final List<String> WORKLOAD_KINDS = List.of("Deployment", "StatefulSet", "CronJob");

    /** The pod spec, whether the workload nests it directly or under a jobTemplate. */
    private static Map<String, Object> podSpecOf(Map<String, Object> workload) {
        Map<String, Object> direct = mapAt(workload, "spec", "template", "spec");
        return direct != null ? direct : mapAt(workload, "spec", "jobTemplate", "spec", "template", "spec");
    }

    /** "<workload>/<container>" -> JAVA_OPTS value, for every container that sets one. */
    private static Map<String, String> javaOptsByContainer() throws IOException {
        List<Map<String, Object>> docs = ManifestDocuments.allIn(BASE);
        Map<String, String> found = new LinkedHashMap<>();
        for (String kind : WORKLOAD_KINDS) {
            for (Map<String, Object> workload : ofKind(docs, kind)) {
                for (Map<String, Object> container : listOf(podSpecOf(workload), "containers")) {
                    for (Map<String, Object> env : listOf(container, "env")) {
                        if ("JAVA_OPTS".equals(env.get("name")) && env.get("value") != null) {
                            found.put(nameOf(workload) + "/" + container.get("name"),
                                    String.valueOf(env.get("value")));
                        }
                    }
                }
            }
        }
        return found;
    }

    @Test
    void everyJvmWorkloadExitsOnOutOfMemoryError() throws IOException {
        Map<String, String> javaOpts = javaOptsByContainer();

        assertThat(javaOpts)
                .as("no container in k8s/base sets JAVA_OPTS — the derivation is broken, "
                        + "and a green result here would prove nothing")
                .isNotEmpty();

        List<String> missing = new ArrayList<>();
        javaOpts.forEach((container, opts) -> {
            if (!opts.contains(OOM_EXIT_FLAG)) {
                missing.add(container + " -> \"" + opts + "\"");
            }
        });

        assertThat(missing)
                .as("every JVM workload must exit on OutOfMemoryError so the platform restarts it; "
                        + "these set JAVA_OPTS without " + OOM_EXIT_FLAG)
                .isEmpty();
    }

    /**
     * The flag is worthless if the entrypoint does not pass JAVA_OPTS to the JVM. Pinned here
     * because the two halves live in different files and neither one fails on its own: a quoted
     * "$JAVA_OPTS" would hand java a single unparseable argument, and dropping the variable
     * entirely would silently ignore every flag in every manifest.
     */
    @Test
    void entrypointExpandsJavaOpts() throws IOException {
        String dockerfile = java.nio.file.Files.readString(Path.of("Dockerfile"));
        assertThat(dockerfile)
                .as("Dockerfile ENTRYPOINT must expand $JAVA_OPTS unquoted into the java command")
                .contains("exec java $JAVA_OPTS");
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Dtest=OomPolicyManifestTest test
```

Expected: FAIL on `everyJvmWorkloadExitsOnOutOfMemoryError`, listing all six containers.
`entrypointExpandsJavaOpts` should already PASS — it pins existing behaviour.
Record the six names from the failure output; they are the checklist for Step 3.

- [ ] **Step 3: Add the flag to all six manifests**

```bash
for f in k8s/base/catalog-serving.yaml k8s/base/api-gateway.yaml k8s/base/model-serving.yaml \
         k8s/base/online-serving.yaml k8s/base/outbox-relay-deployment.yaml \
         k8s/base/outbox-reconciliation-cronjob.yaml; do
  perl -0pi -e 's/(name: JAVA_OPTS\n(\s*)value: ")([^"]*)(")/$1$3 -XX:+ExitOnOutOfMemoryError$4/g' "$f"
done
git diff --stat
```

Verify by eye that each value now reads e.g. `-Xms512m -Xmx1g -XX:+ExitOnOutOfMemoryError`,
that exactly six values changed, and that no other `value:` line was touched.

Then add this comment immediately above the `- name: JAVA_OPTS` line in each of the six files,
indented to match the surrounding env comments:

```yaml
            # -XX:+ExitOnOutOfMemoryError: an OOM'd JVM keeps serving (500s, then 200s once GC
            # recovers) with no liveness probe that notices, so nothing restarts it. Exiting
            # skips the graceful drain deliberately — see 18_Fault_Tolerance.md §9.5.
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Dtest=OomPolicyManifestTest test
```

Expected: PASS, both tests.

- [ ] **Step 5: Verify the manifests still render**

```bash
kubectl kustomize k8s/base > /dev/null && \
kubectl kustomize k8s/eks > /dev/null && \
kubectl kustomize k8s/eks-us-west-2 > /dev/null && echo "all three render"
```

Expected: `all three render`. If `kubectl` is unavailable, `kustomize build` on the same three
paths is equivalent; if neither is installed, say so in the PR rather than skipping silently.

- [ ] **Step 6: Register the test in the resilience profile**

In `pom.xml`, inside the `resilience` profile's Surefire `<includes>`, next to the other
`**/k8s/*ManifestTest.java` entries, add:

```xml
                <!-- The process-boundary counterpart to the k8s auth manifest tests: those prove
                     a workload can reach its dependencies, this proves the platform can recover
                     it. The flag is a substring in a YAML env value, so nothing but this test
                     notices it disappearing. -->
                <include>**/k8s/OomPolicyManifestTest.java</include>
```

- [ ] **Step 7: Confirm it runs in the gate**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Presilience test 2>&1 | tail -30
```

Expected: the run includes `OomPolicyManifestTest` and the suite passes.
`OutboxRelayTest` has a known timing flake; if it is the only failure, re-run it alone to confirm.

- [ ] **Step 8: Commit**

```bash
git add k8s/base pom.xml src/test/java/com/recsys/infrastructure/k8s/OomPolicyManifestTest.java
git commit -m "feat(k8s): exit the JVM on OutOfMemoryError so the platform restarts it

An OOM'd JVM kept running: request threads answered 500s while the heap
was short and 200s once GC recovered something, while a background loop
killed by the same Error stayed dead, and every liveness probe was a
constant 200. Nothing restarted the pod.

The test derives the workload set from the manifests rather than listing
it, so a service added later without the flag fails the build.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Gateway `/health/live` route

**Files:**
- Create: `src/main/java/com/recsys/application/gateway/GatewayLivenessService.java`
- Create: `src/test/java/com/recsys/application/gateway/GatewayLivenessRouteTest.java`
- Modify: `src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java:213`
- Modify: `pom.xml` (`-Presilience` include)

**Interfaces:**
- Consumes: `com.recsys.api.serving.BaseApiService` (`writeJson(HttpStatus, Object)`, `doGet(ServiceRequestContext, HttpRequest)`); `GatewayAuthenticator.forTesting(Set<String> apiKeys, Set<String> publicPaths, CognitoJwtVerifier verifier)` and `check(RequestHeaders, String)` returning `GatewayAuthResult` with `isAllowed()`.
- Produces: `com.recsys.application.gateway.GatewayLivenessService`, a public final class with a no-arg constructor, serving `GET /health/live` → 200. Task 3 depends on the path string `/health/live` only.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/recsys/application/gateway/GatewayLivenessRouteTest.java`:

```java
package com.recsys.application.gateway;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.ServiceRequestContext;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Liveness answers "is this process running", and nothing else. The gateway used to answer it
 * with GatewayHealthService, which returns 503 when any upstream is down — so an upstream outage
 * lasting past 60s made kubelet restart every gateway pod, removing the one component still able
 * to serve the routes whose upstreams were healthy.
 *
 * <p>These two tests pin the properties that make the replacement route usable as a probe target.
 */
class GatewayLivenessRouteTest {

    private static AggregatedHttpResponse get(String path) throws Exception {
        GatewayLivenessService svc = new GatewayLivenessService();
        ServiceRequestContext ctx = ServiceRequestContext.builder(
                HttpRequest.of(HttpMethod.GET, path)).build();
        return svc.serve(ctx, ctx.request()).aggregate().join();
    }

    /**
     * No dependency is consulted, so there is nothing to stub: the handler cannot fail for a
     * reason outside this process. That is the whole point of the route.
     */
    @Test
    void livenessIsConstant200() throws Exception {
        AggregatedHttpResponse resp = get("/health/live");

        assertThat(resp.status()).isEqualTo(HttpStatus.OK);
        assertThat(resp.contentUtf8()).contains("\"live\":true");
    }

    /**
     * The probe reaches the pod with no credentials. In the EKS overlays GATEWAY_ALLOW_ANONYMOUS
     * is false, so a path the authenticator does not treat as public gets 401 — and a liveness
     * probe that 401s restarts the pod on a loop.
     *
     * <p>/health/live is public today only because GatewayAuthenticator matches public paths by
     * prefix-with-boundary ({@code path.equals(p) || path.startsWith(p + "/")}), so the existing
     * "/health" entry covers it and no config change was needed. That is load-bearing and
     * invisible: tightening matchesPrefix to exact equality would look like a hardening change
     * and would take down every gateway pod's liveness probe. This is the test that catches it.
     */
    @Test
    void livenessPathIsAnonymouslyReachableUnderTheDeployedPublicPaths() {
        // Exactly the GATEWAY_PUBLIC_PATHS value from k8s/base/configmap.yaml.
        GatewayAuthenticator auth = GatewayAuthenticator.forTesting(
                Set.of("a-real-api-key"),
                Set.of("/health", "/api/catalog/item", "/api/catalog/similar"),
                null);

        RequestHeaders noCredentials = RequestHeaders.of(HttpMethod.GET, "/health/live");

        assertThat(auth.check(noCredentials, "/health/live").isAllowed())
                .as("the kubelet liveness probe sends no credentials")
                .isTrue();
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Dtest=GatewayLivenessRouteTest test
```

Expected: FAIL to compile — `GatewayLivenessService` does not exist.

If `GatewayAuthResult` has no `isAllowed()` accessor, read
`src/main/java/com/recsys/application/gateway/GatewayAuthResult.java` and use its real accessor
instead; do not add one. If `forTesting` is package-private, the test is already in the same
package, so it is reachable.

- [ ] **Step 3: Write the handler**

Create `src/main/java/com/recsys/application/gateway/GatewayLivenessService.java`:

```java
package com.recsys.application.gateway;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.recsys.api.serving.BaseApiService;

import java.util.Map;

/**
 * GET /health/live — liveness probe. Reports process viability and nothing else.
 *
 * <p>Deliberately distinct from {@link GatewayHealthService}, which reports upstream
 * reachability and circuit state and returns 503 when an upstream is down. That answer is right
 * for readiness — take this pod out of the load balancer — and wrong for liveness, where 503
 * means "restart this container". An upstream outage would otherwise restart every gateway pod,
 * discarding warm connection pools and circuit state, and removing the component still capable
 * of serving the routes whose upstreams are healthy.
 *
 * <p>Mirrors {@code OnlineServices.Live} on 7010.
 */
public final class GatewayLivenessService extends BaseApiService {
    @Override
    protected HttpResponse doGet(ServiceRequestContext ctx, HttpRequest req) {
        return writeJson(HttpStatus.OK, Map.of(
                "ok", true,
                "live", true,
                "service", "api-gateway"
        ));
    }
}
```

- [ ] **Step 4: Register the route**

In `src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java`, directly below the
existing `/health` registration at line 213:

```java
        // Health endpoint — exposes per-route circuit state and upstream reachability.
        sb.service("/health", new GatewayHealthService(allRoutes, timeout, circuitBreakers, port, registryProvider));

        // Liveness — process viability only, never upstream state. /health answers 503 when an
        // upstream is down, which is correct for readiness and wrong for liveness: it would make
        // kubelet restart every gateway pod during an upstream outage. See 09_API_Gateway.md.
        sb.service("/health/live", new GatewayLivenessService());
```

Add the import `com.recsys.application.gateway.GatewayLivenessService` alongside the existing
`GatewayHealthService` import.

- [ ] **Step 5: Run the test to verify it passes**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Dtest=GatewayLivenessRouteTest test
```

Expected: PASS, both tests.

- [ ] **Step 6: Verify the route does not collide with the catch-all proxy**

`/health/live` must be served locally, never forwarded to an upstream. Armeria prefers the more
specific match, but confirm rather than assume:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q \
  -Dtest=MicroserviceRouteTest,BackendRouteCoverageTest,ProxyRoutePolicyEnforcementTest,GatewayPathCanonicalizationTest test
```

Expected: PASS. If `BackendRouteCoverageTest` fails because it requires every registered path to
carry a `BackendRoutePolicy` classification, add `/health/live` as `NO_PROXY` next to the
existing `/health` entries in `BackendRoutePolicy` and re-run.

- [ ] **Step 7: Register the test in the resilience profile**

In `pom.xml`, next to the other gateway entries:

```xml
                <!-- Liveness must not depend on upstreams, or an upstream outage restarts every
                     gateway pod. Also pins that /health/live stays anonymously reachable: it is
                     public only via prefix matching on the "/health" entry, so tightening that
                     match would break the probe in production and nothing else would notice. -->
                <include>**/gateway/GatewayLivenessRouteTest.java</include>
```

- [ ] **Step 8: Commit**

```bash
git add src/main/java/com/recsys/application/gateway/GatewayLivenessService.java \
        src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java \
        src/test/java/com/recsys/application/gateway/GatewayLivenessRouteTest.java \
        pom.xml
git commit -m "feat(gateway): add a constant-200 /health/live probe target

/health reports upstream reachability and returns 503 when an upstream is
down. That is the right answer for readiness and the wrong one for
liveness, where 503 means restart the container.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Repoint the gateway probes

**Files:**
- Create: `src/test/java/com/recsys/infrastructure/k8s/GatewayLivenessManifestTest.java`
- Modify: `k8s/base/api-gateway.yaml:108-123`
- Modify: `pom.xml` (`-Presilience` include)

**Interfaces:**
- Consumes: the `/health/live` path registered in Task 2, and the same `ManifestDocuments` statics as Task 1.
- Produces: nothing other tasks depend on.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/recsys/infrastructure/k8s/GatewayLivenessManifestTest.java`:

```java
package com.recsys.infrastructure.k8s;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.recsys.infrastructure.k8s.ManifestDocuments.listOf;
import static com.recsys.infrastructure.k8s.ManifestDocuments.mapAt;
import static com.recsys.infrastructure.k8s.ManifestDocuments.nameOf;
import static com.recsys.infrastructure.k8s.ManifestDocuments.ofKind;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Liveness and readiness answer different questions and must not share an endpoint. Readiness
 * asks "should traffic come here", so depending on upstreams is defensible. Liveness asks
 * "should this container be killed", so depending on upstreams means an upstream outage restarts
 * every replica of the service that proxies to it — the one component still able to serve the
 * routes whose upstreams are healthy, and the component whose warm pools and circuit state are
 * most expensive to discard.
 *
 * <p>The gateway had exactly that wiring on all three probes.
 */
class GatewayLivenessManifestTest {

    private static final Path BASE = Path.of("k8s", "base");

    private static final String GATEWAY = "recsys-api-gateway";

    /**
     * HTTP paths whose handler is a constant 200 that inspects no dependency. Anything else is
     * unfit for liveness. Kept as an allow-list rather than a deny-list of known-bad paths: a new
     * service wiring liveness to a new dependency-aware endpoint should fail here by default
     * rather than pass because nobody thought to add it.
     */
    private static final Set<String> CONSTANT_200_PATHS = Set.of(
            "/health/live",  // gateway, online serving, model serving, outbox relay
            "/health");      // catalog serving: RecommendationService.Health is {"ok": true}

    private static Map<String, Object> podSpecOf(Map<String, Object> workload) {
        Map<String, Object> direct = mapAt(workload, "spec", "template", "spec");
        return direct != null ? direct : mapAt(workload, "spec", "jobTemplate", "spec", "template", "spec");
    }

    private static String probePath(Map<String, Object> container, String probe) {
        Map<String, Object> httpGet = mapAt(container, probe, "httpGet");
        return httpGet == null ? null : String.valueOf(httpGet.get("path"));
    }

    private static Map<String, Object> gatewayContainer() throws IOException {
        List<Map<String, Object>> docs = ManifestDocuments.allIn(BASE);
        for (Map<String, Object> workload : ofKind(docs, "Deployment")) {
            if (GATEWAY.equals(nameOf(workload))) {
                List<Map<String, Object>> containers = listOf(podSpecOf(workload), "containers");
                assertThat(containers).as(GATEWAY + " has no containers").isNotEmpty();
                return containers.get(0);
            }
        }
        throw new AssertionError("no Deployment named " + GATEWAY + " in " + BASE);
    }

    @Test
    void gatewayLivenessAndStartupDoNotDependOnUpstreams() throws IOException {
        Map<String, Object> container = gatewayContainer();

        assertThat(probePath(container, "livenessProbe"))
                .as("liveness must not be /health: it 503s when any upstream is down, so an "
                        + "upstream outage restarts every gateway pod")
                .isEqualTo("/health/live");

        assertThat(probePath(container, "startupProbe"))
                .as("startup must not be /health either: with failureThreshold 24 x 5s, a cold "
                        + "start whose upstreams take over 120s to boot kills the gateway before "
                        + "it ever starts, and it restarts into the same condition")
                .isEqualTo("/health/live");
    }

    /**
     * Readiness keeps /health deliberately. Asserted rather than left unstated so that
     * "decouple the probes" is not later over-applied to the one probe where the coupling is a
     * decision — see the accepted residual in the design doc.
     */
    @Test
    void gatewayReadinessStillReflectsUpstreams() throws IOException {
        assertThat(probePath(gatewayContainer(), "readinessProbe")).isEqualTo("/health");
    }

    @Test
    void noLivenessProbeAnywhereDependsOnADependency() throws IOException {
        List<Map<String, Object>> docs = ManifestDocuments.allIn(BASE);
        List<String> offenders = new ArrayList<>();

        for (String kind : List.of("Deployment", "StatefulSet", "CronJob")) {
            for (Map<String, Object> workload : ofKind(docs, kind)) {
                for (Map<String, Object> container : listOf(podSpecOf(workload), "containers")) {
                    String path = probePath(container, "livenessProbe");
                    if (path != null && !CONSTANT_200_PATHS.contains(path)) {
                        offenders.add(nameOf(workload) + "/" + container.get("name") + " -> " + path);
                    }
                }
            }
        }

        assertThat(offenders)
                .as("a liveness probe must answer \"is this process running\". These point at a "
                        + "path that is not a known constant-200 handler; if the handler really is "
                        + "constant, add the path to CONSTANT_200_PATHS with the handler named")
                .isEmpty();
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Dtest=GatewayLivenessManifestTest test
```

Expected: FAIL on `gatewayLivenessAndStartupDoNotDependOnUpstreams` — both probes report
`/health`. `gatewayReadinessStillReflectsUpstreams` should PASS already.
`noLivenessProbeAnywhereDependsOnADependency` should also PASS already, since `/health` is in
the allow-list for catalog serving's sake — note this in the PR: that third test does not go red
here, so demonstrate it works by temporarily setting a probe path to `/health/ready`, watching it
fail, and reverting.

- [ ] **Step 3: Repoint the probes**

In `k8s/base/api-gateway.yaml`, change the `startupProbe` and `livenessProbe` paths from
`/health` to `/health/live`, leaving `readinessProbe` alone, and add the explanatory comment:

```yaml
          # Startup and liveness ask whether this process is running; /health/live answers only
          # that. Readiness keeps /health, which reports upstream reachability and 503s when an
          # upstream is down — the right answer for "should traffic come here" and the wrong one
          # for "should this container be killed". See 18_Fault_Tolerance.md §9.5.
          startupProbe:
            httpGet:
              path: /health/live
              port: http
            failureThreshold: 24
            periodSeconds: 5
          readinessProbe:
            httpGet:
              path: /health
              port: http
            periodSeconds: 5
          livenessProbe:
            httpGet:
              path: /health/live
              port: http
            periodSeconds: 20
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Dtest=GatewayLivenessManifestTest test
kubectl kustomize k8s/base > /dev/null && echo "renders"
```

Expected: PASS, three tests; `renders`.

- [ ] **Step 5: Demonstrate the third test can fail**

```bash
perl -0pi -e 's{(livenessProbe:\n\s*httpGet:\n\s*path: )/health/live}{$1/health/ready}' k8s/base/api-gateway.yaml
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Dtest=GatewayLivenessManifestTest test
git checkout k8s/base/api-gateway.yaml.orig 2>/dev/null || git diff k8s/base/api-gateway.yaml
```

Expected: `noLivenessProbeAnywhereDependsOnADependency` FAILS naming
`recsys-api-gateway/... -> /health/ready`. Then restore the `/health/live` value by hand and
re-run Step 4 to confirm green before continuing. Do not commit the mutated file.

- [ ] **Step 6: Register the test in the resilience profile**

```xml
                <!-- The manifest half of GatewayLivenessRouteTest: the route can exist and be
                     correct while the probe still points at /health. Also enforces, across every
                     workload, that a liveness path is a constant-200 handler. -->
                <include>**/k8s/GatewayLivenessManifestTest.java</include>
```

- [ ] **Step 7: Commit**

```bash
git add k8s/base/api-gateway.yaml pom.xml \
        src/test/java/com/recsys/infrastructure/k8s/GatewayLivenessManifestTest.java
git commit -m "fix(k8s): stop restarting the gateway when an upstream is down

livenessProbe and startupProbe pointed at /health, which returns 503 when
any upstream is unreachable, so an outage lasting past 60s restarted every
gateway pod. The startup probe had the same coupling on a 120s budget, so a
cold start with slow upstreams never completed. Readiness keeps /health.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Documentation and delivery

**Files:**
- Modify: `docs/system_design/18_Fault_Tolerance.md` (§9.5, §9.6 "Process" bullet, the "No OOM policy" sharp-edge entry)
- Modify: `docs/system_design/09_API_Gateway.md`

**Interfaces:**
- Consumes: the behaviour delivered by Tasks 1–3.
- Produces: the merged PR.

- [ ] **Step 1: Correct §9.5**

In `docs/system_design/18_Fault_Tolerance.md`, rewrite the §9.5 paragraph beginning "With no
`-XX:+ExitOnOutOfMemoryError` in any container `JAVA_OPTS`". It must now say:

- Every `k8s/base` workload sets `-XX:+ExitOnOutOfMemoryError`; an OOM exits and the platform restarts the pod.
- The claim that *every* liveness probe is a constant 200 was **wrong for the gateway**, which probed `/health` on liveness, readiness and startup. Say so explicitly — a silent correction loses the finding.
- The exit skips the §5 drain: no shutdown hooks run, so `terminationGracePeriodSeconds: 60` and `preStop: sleep 5` buy nothing on this path, and in-flight requests are dropped. Accepted, with the reasoning from the spec's "Accepted residuals".
- The flag does not fire on native OOM inside `onnxruntime` (a SIGSEGV that kills the process anyway) nor on GC thrashing without a thrown Error; the heap and GC alerts remain the signal for the latter.

Do not renumber any `##` heading.

- [ ] **Step 2: Update §9.6 and the sharp-edges entry**

Change the §9.6 "Process" bullet from "decide the OOM policy explicitly … Not done in this
change either" to the decision now in force, cross-referencing §9.5. Change the sharp-edges
entry "**No OOM policy: the JVM survives `OutOfMemoryError` and liveness stays `200`**" to
resolved, keeping the two accepted residuals (skipped drain, coupled gateway readiness) visible
rather than dropping them.

- [ ] **Step 3: Document the gateway probes**

In `docs/system_design/09_API_Gateway.md`, add `/health/live` wherever the gateway's endpoints
are listed, and state the liveness/readiness split: liveness and startup on `/health/live`
(constant 200), readiness on `/health` (503 when an upstream is down). Record the accepted
residual that readiness coupling pulls the gateway from its target group on any single upstream
outage, and that this is redundant with `GATEWAY_UPSTREAM_HEALTHCHECK_ENABLED` and the per-route
circuit breakers — left by decision, not oversight.

Also record the non-obvious reachability fact: `/health/live` needs no `GATEWAY_PUBLIC_PATHS`
entry because `GatewayAuthenticator` and `GatewayOriginSecret` both match with
`path.equals(p) || path.startsWith(p + "/")`.

- [ ] **Step 4: Verify the docs index still passes**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Dtest=DocumentationIndexTest test
```

Expected: PASS. It covers `docs/system_design/` and `docs/runbooks/`; `docs/superpowers/` is out
of scope, so the spec and this plan need no index entry.

- [ ] **Step 5: Run the full gate, then the full suite**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Presilience test 2>&1 | tail -30
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test 2>&1 | tail -40
```

Expected: `-Presilience` green. The full suite has known pre-existing failures — two Spark
fixture errors, and `QueueMetricsGcObservationTest` flakes on `System.gc()`. Record exactly which
failures appear and confirm each is pre-existing by running it on `main`; do not report the suite
as green if it is not.

- [ ] **Step 6: Independent code review**

Dispatch a code review of the full branch diff. Resolve every finding, or record why not.

- [ ] **Step 7: Commit the docs and open the PR**

```bash
git diff --check
git add docs/system_design
git commit -m "docs: record the OOM exit policy and the gateway liveness split

Corrects 18_Fault_Tolerance.md §9.5, which claimed every liveness probe in
the system was a constant 200. The gateway's was not.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
git push -u origin feat/oom-policy-and-gateway-liveness
gh pr create --base main --title "feat: exit on OOM, and stop restarting the gateway when an upstream is down"
```

The PR body must state: what each half fixes, that the OOM exit skips the graceful drain and why
that was accepted, that gateway readiness stays coupled by decision, that
`noLivenessProbeAnywhereDependsOnADependency` was demonstrated red by mutation rather than by the
change itself, and which pre-existing suite failures were observed. End it with:

```
🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

---

## Self-Review

- **Spec coverage.** Part A → Task 1. Part B route and reachability → Task 2. Part B probe repoint → Task 3. Both enforcement tests → Tasks 1 and 3; unit coverage → Task 2. `-Presilience` registration → Steps 6/7/6 of Tasks 1–3. Non-goals are honoured: no heap dump flag, no readiness change, no catalog probe rename. Documentation → Task 4. Accepted residuals all appear in Task 4's doc steps.
- **Placeholders.** None: every test and handler is written out in full, and the two doc tasks enumerate the specific claims to change rather than saying "update the docs".
- **Type consistency.** `GatewayLivenessService` is named identically in Tasks 2 and 3. `podSpecOf` and `probePath` are duplicated in the two manifest tests deliberately — `ManifestDocuments` is the shared helper and the existing tests each carry their own small traversal helpers, so extracting these would break the established pattern for two four-line methods.
- **Known risk.** Task 2 Step 6 and Task 3 Step 5 both anticipate a test that may not behave as predicted (`BackendRouteCoverageTest` requiring a policy entry; the third manifest test passing before the change). Both steps say what to do in either case rather than assuming.
