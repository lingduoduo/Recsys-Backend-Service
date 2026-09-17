package com.recsys.infrastructure.k8s;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.recsys.infrastructure.k8s.ManifestDocuments.allIn;
import static com.recsys.infrastructure.k8s.ManifestDocuments.listOf;
import static com.recsys.infrastructure.k8s.ManifestDocuments.mapAt;
import static com.recsys.infrastructure.k8s.ManifestDocuments.nameOf;
import static com.recsys.infrastructure.k8s.ManifestDocuments.ofKind;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A workload that enforces the operator tier must be given the credential the tier runs on.
 *
 * <p>{@code SHARD_ADMIN_TOKEN} gates the control-plane routes — {@code /setembedding}, the model
 * version activate/rollback/preload endpoints, {@code /online/ops}, {@code POST /shards/topology}
 * and {@code GET /shards/shard}. Unset means the tier authorizes nobody, which is the right
 * fail-closed behaviour and a completely silent one: a service that starts reading the variable
 * without a matching manifest change comes up, logs one warning, and rejects every operator request
 * until somebody notices a rollback not working. Nothing else in the build would catch it.
 *
 * <p>The requirement is therefore <em>derived</em> from the source that reads the variable rather
 * than restated as a list of workloads — the same principle {@link NetworkPolicyEgressManifestTest}
 * applies to egress destinations. A test asserting "the gateway and online-serving inject the
 * token" would pin today's two instances and stay silent on the third, which is the case that
 * matters. That derivation only holds while the scan actually finds every reader, which is why
 * {@link #everyReaderOfTheOperatorTokenIsClassified} checks both directions: an unmapped reader
 * fails it, and so does a mapped file the scan no longer finds — the latter means the map has gone
 * stale (renamed, refactored, or the read removed) and must be re-derived from source rather than
 * trusted as-is.
 *
 * <p><strong>How a reader is found, and why the marker is broad.</strong> The scan matches the bare
 * name {@code SHARD_ADMIN_TOKEN} anywhere in a source file's text — not the {@code getenv} call
 * syntax, not a quoted-literal shape. A marker tied to one call shape misses real reads: an injected
 * {@code System::getenv} method reference ({@code MicroserviceGatewayServer} already threads one
 * through {@code GatewayAuthenticator.fromEnvironment()}), the {@code System.getenv()} map form
 * ({@code OnlinePredictionServer} already uses it for another variable), helper indirection via
 * {@code EnvConfig}/{@code EnvVars} (both already used in these files), or Spring
 * {@code @Value("${SHARD_ADMIN_TOKEN:}")} in a future Spring-based reader — plausible given the model
 * version endpoints live in {@code VersionController} on 8080, where {@code k8s/base/model-serving.yaml}
 * has no token block today. The trade this makes is deliberate and asymmetric: a false positive here
 * is a loud build failure a human resolves in one line — add the file to {@link #MENTIONS_ONLY} with
 * a reason, or to {@link #READER_WORKLOADS} with a Deployment. A false negative is a workload
 * enforcing a tier it has no credential for, discovered in production instead of in review. That
 * asymmetry is why the broad marker is correct even though it costs {@link #MENTIONS_ONLY} carrying
 * files that mention the variable without reading it — and why
 * {@link #everyMentionsOnlyExemptionIsStillMentioned} exists: a stale exemption that no longer
 * matches anything is how a real reader would get silently excused later.
 *
 * <p>Only one direction is enforced between readers and manifests: reader ⇒ manifest. A Deployment
 * injecting a token nobody reads is a stale line, not a broken control.
 *
 * <p><strong>Scope:</strong> this reads {@code k8s/base} only. Tests cannot run {@code kubectl
 * kustomize}, so an overlay that patched the env block away would not be caught here. Unlike
 * {@link NetworkPolicyEgressManifestTest}, which also reads {@code k8s/eks-shared} to check the
 * ElastiCache egress patch, this test has no overlay-side assertion at all — and unlike that test's
 * base-only gap, which {@code 20_AuthN_AuthZ.md} records as sharp edge 7, this one is documented
 * nowhere but here.
 *
 * <p><strong>What is not cross-checked.</strong> {@link #READER_WORKLOADS} is a hand-made
 * attribution from file to Deployment; nothing verifies the attribution itself, so a wrong entry
 * would hide a genuinely missing env block behind a passing test. The manifest check
 * ({@link #describeTokenProblems}) accepts the token on any container in the Deployment's pod spec,
 * so a sidecar carrying it would satisfy a check the application container fails — every Deployment
 * here is single-container today, but nothing enforces that it stays that way. And only
 * {@code kind: Deployment} is searched, so a reader packaged as a Job or CronJob fails with "no
 * Deployment of that name in k8s/base" — a correct refusal, but a misleading message for that case.
 */
class OperatorTokenManifestTest {

    private static final Path SOURCE_ROOT = Path.of("src", "main", "java");
    private static final Path BASE = Path.of("k8s", "base");

    private static final String ENV_VAR = "SHARD_ADMIN_TOKEN";
    private static final String SECRET_NAME = "recsys-online-admin";
    private static final String SECRET_KEY = "admin-token";

    /**
     * The bare variable name, matched anywhere in a file's text — deliberately not restricted to
     * the {@code getenv(...)} call shape. See the class javadoc for why the broader marker is
     * correct here despite the false positives it invites.
     */
    private static final String MENTION_MARKER = ENV_VAR;

    /** Which Deployment supplies the token to each file that reads it. */
    private static final Map<String, String> READER_WORKLOADS = Map.of(
            "OnlinePredictionServer.java", "recsys-online-serving",
            "MicroserviceGatewayServer.java", "recsys-api-gateway",
            // Second layer on POST /api/v1/retrieval/model/reload: the gateway's OPERATOR
            // classification binds only callers who come through the gateway, so the controller
            // checks the same token in-JVM, as 7010's operator surfaces have always done.
            "ModelReloadController.java", "recsys-model-serving");

    /**
     * Files the scan matches that do not actually read the variable, mapped to why. Each entry is
     * asserted still-matched by {@link #everyMentionsOnlyExemptionIsStillMentioned} — an exemption
     * for a file that has stopped mentioning the variable at all is a latent hole for the next real
     * reader to hide behind.
     */
    private static final Map<String, String> MENTIONS_ONLY = Map.of(
            "AdminTokenGuard.java", "its javadoc names the variable while the token arrives as a "
                    + "constructor argument");

    @Test
    void everyReaderOfTheOperatorTokenIsClassified() throws IOException {
        Set<String> found = filesReadingTheToken().stream()
                .map(p -> p.getFileName().toString())
                .collect(Collectors.toCollection(TreeSet::new));
        Set<String> mapped = new TreeSet<>(READER_WORKLOADS.keySet());

        Set<String> unmapped = new TreeSet<>(found);
        unmapped.removeAll(mapped);
        assertThat(unmapped)
                .describedAs("These files mention %s but READER_WORKLOADS does not say which "
                        + "Deployment supplies it. Add the mapping and the manifest env block "
                        + "together — a service that enforces the operator tier without the "
                        + "credential rejects every operator request and only says so in one "
                        + "startup warning.", ENV_VAR)
                .isEmpty();

        Set<String> stale = new TreeSet<>(mapped);
        stale.removeAll(found);
        assertThat(stale)
                .describedAs("READER_WORKLOADS maps these files to a Deployment, but the scan no "
                        + "longer finds %s in them. The map has gone stale — re-derive it from "
                        + "source: either the read moved/was removed and the mapping (and its "
                        + "manifest env block, if now unused) should go with it, or the file was "
                        + "renamed and the map's key needs to follow.", ENV_VAR)
                .isEmpty();
    }

    /**
     * Guards {@link #MENTIONS_ONLY} itself: an entry that no longer matches anything is not a
     * documented false positive anymore, it is dead configuration masking whatever the file does
     * now — including, potentially, a real read that would otherwise need a
     * {@link #READER_WORKLOADS} entry.
     */
    @Test
    void everyMentionsOnlyExemptionIsStillMentioned() throws IOException {
        Set<String> mentioned = allFilesMentioningTheToken().stream()
                .map(p -> p.getFileName().toString())
                .collect(Collectors.toSet());

        List<String> stale = new ArrayList<>();
        for (String file : MENTIONS_ONLY.keySet()) {
            if (!mentioned.contains(file)) {
                stale.add(file + ": no longer mentions " + ENV_VAR + " anywhere. Remove it from "
                        + "MENTIONS_ONLY, then check whether it now reads the variable and needs a "
                        + "READER_WORKLOADS entry instead.");
            }
        }

        assertThat(stale)
                .describedAs("A stale MENTIONS_ONLY entry is how a real reader would get silently "
                        + "excused: once the file stops mentioning %s at all, there is nothing left "
                        + "for the exemption to legitimately cover.", ENV_VAR)
                .isEmpty();
    }

    @Test
    void everyClassifiedWorkloadInjectsTheOperatorToken() throws IOException {
        List<Map<String, Object>> deployments = ofKind(allIn(BASE), "Deployment");
        Set<String> required = new LinkedHashSet<>(READER_WORKLOADS.values());

        List<String> problems = new ArrayList<>();
        for (String workload : required) {
            Map<String, Object> deployment = deployments.stream()
                    .filter(d -> workload.equals(nameOf(d)))
                    .findFirst()
                    .orElse(null);
            if (deployment == null) {
                problems.add(workload + ": no Deployment of that name in " + BASE);
                continue;
            }
            problems.addAll(describeTokenProblems(workload, deployment));
        }

        assertThat(problems)
                .describedAs("Workloads whose code reads %s must be given it from Secret %s/%s.",
                        ENV_VAR, SECRET_NAME, SECRET_KEY)
                .isEmpty();
    }

    /** @return one message per defect in this Deployment's injection of the token; empty when correct. */
    private static List<String> describeTokenProblems(String workload, Map<String, Object> deployment) {
        Map<String, Object> podSpec = mapAt(deployment, "spec", "template", "spec");
        for (Map<String, Object> container : listOf(podSpec, "containers")) {
            for (Map<String, Object> env : listOf(container, "env")) {
                if (!ENV_VAR.equals(env.get("name"))) {
                    continue;
                }
                Map<String, Object> ref = mapAt(env, "valueFrom", "secretKeyRef");
                if (ref == null) {
                    return List.of(workload + ": " + ENV_VAR + " is set, but not from a secretKeyRef");
                }
                List<String> defects = new ArrayList<>();
                if (!SECRET_NAME.equals(ref.get("name"))) {
                    defects.add(workload + ": secret is " + ref.get("name") + ", expected " + SECRET_NAME);
                }
                if (!SECRET_KEY.equals(ref.get("key"))) {
                    defects.add(workload + ": secret key is " + ref.get("key") + ", expected " + SECRET_KEY);
                }
                // Load-bearing: without optional, a cluster that has not been given the Secret
                // cannot schedule the pod at all, which turns a fail-closed tier into an outage.
                if (!Boolean.TRUE.equals(ref.get("optional"))) {
                    defects.add(workload + ": secretKeyRef must set optional: true, so a cluster "
                            + "without the Secret degrades to 403 on operator routes instead of "
                            + "failing to start the pod");
                }
                return defects;
            }
        }
        return List.of(workload + ": no " + ENV_VAR + " entry in any container's env");
    }

    /** Every source file whose text contains {@link #MENTION_MARKER}, exemptions included. */
    private static List<Path> allFilesMentioningTheToken() throws IOException {
        try (Stream<Path> files = Files.walk(SOURCE_ROOT)) {
            List<Path> matches = new ArrayList<>();
            for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                if (Files.readString(file).contains(MENTION_MARKER)) {
                    matches.add(file);
                }
            }
            return matches;
        }
    }

    /** {@link #allFilesMentioningTheToken()} minus the files {@link #MENTIONS_ONLY} excuses. */
    private static List<Path> filesReadingTheToken() throws IOException {
        List<Path> readers = new ArrayList<>();
        for (Path file : allFilesMentioningTheToken()) {
            if (!MENTIONS_ONLY.containsKey(file.getFileName().toString())) {
                readers.add(file);
            }
        }
        return readers;
    }
}
