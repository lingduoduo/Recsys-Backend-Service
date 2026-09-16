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
 * <p>The gateway had exactly that wiring, on all three probes.
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
            // gateway (GatewayLivenessService), online serving (OnlineServices.Live),
            // model serving (HealthController.liveness), outbox relay
            "/health/live",
            // catalog serving: RecommendationService.Health is a constant {"ok": true}. Correct
            // behaviour under an inconsistent name; renaming churns a manifest for no gain.
            "/health");

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
