package com.recsys.infrastructure.k8s;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
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
 * <p>The workload set is <b>derived from the manifests</b>, never hardcoded, and derived from
 * {@code RECSYS_MAIN_CLASS} rather than from {@code JAVA_OPTS}. That distinction is the whole
 * point. Deriving "containers that set JAVA_OPTS" only checks workloads that already opted in:
 * a new JVM workload added with no JAVA_OPTS entry at all inherits the image default
 * ({@code Dockerfile}: {@code ENV JAVA_OPTS=""}), carries no flag, and passes — the omission
 * case, which is exactly the one a conformance test exists to catch. {@code RECSYS_MAIN_CLASS}
 * is what makes a container a JVM workload in this repo: all six set it and the redis and
 * sentinel containers do not, so "sets RECSYS_MAIN_CLASS ⇒ must set JAVA_OPTS ⇒ must carry the
 * flag" is a closed derivation rather than a self-selecting one.
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

    /** The value of {@code name} in this container's env, or null when it sets no such entry. */
    private static String envValue(Map<String, Object> container, String name) {
        for (Map<String, Object> env : listOf(container, "env")) {
            if (name.equals(env.get("name")) && env.get("value") != null) {
                return String.valueOf(env.get("value"));
            }
        }
        return null;
    }

    /**
     * "&lt;workload&gt;/&lt;container&gt;" -&gt; JAVA_OPTS (null when unset), for every container
     * that is a JVM workload — i.e. every container that names a main class for the image
     * entrypoint to run.
     */
    private static Map<String, String> javaOptsByJvmContainer() throws IOException {
        List<Map<String, Object>> docs = ManifestDocuments.allIn(BASE);
        Map<String, String> found = new LinkedHashMap<>();
        for (String kind : WORKLOAD_KINDS) {
            for (Map<String, Object> workload : ofKind(docs, kind)) {
                for (Map<String, Object> container : listOf(podSpecOf(workload), "containers")) {
                    if (envValue(container, "RECSYS_MAIN_CLASS") == null) {
                        continue;
                    }
                    String key = nameOf(workload) + "/" + container.get("name");
                    found.put(key, envValue(container, "JAVA_OPTS"));
                }
            }
        }
        return found;
    }

    @Test
    void everyJvmWorkloadExitsOnOutOfMemoryError() throws IOException {
        Map<String, String> javaOpts = javaOptsByJvmContainer();

        assertThat(javaOpts)
                .as("no container in k8s/base sets RECSYS_MAIN_CLASS — the derivation is broken, "
                        + "and a green result here would prove nothing")
                .isNotEmpty();

        List<String> missing = new ArrayList<>();
        javaOpts.forEach((container, opts) -> {
            if (opts == null) {
                missing.add(container + " -> sets no JAVA_OPTS at all (inherits the image's "
                        + "empty default, so it carries no flags)");
            } else if (!opts.contains(OOM_EXIT_FLAG)) {
                missing.add(container + " -> \"" + opts + "\"");
            }
        });

        assertThat(missing)
                .as("every JVM workload must exit on OutOfMemoryError so the platform restarts it; "
                        + "these do not carry " + OOM_EXIT_FLAG)
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
        String dockerfile = Files.readString(Path.of("Dockerfile"));
        assertThat(dockerfile)
                .as("Dockerfile ENTRYPOINT must expand $JAVA_OPTS unquoted into the java command")
                .contains("exec java $JAVA_OPTS");
    }
}
