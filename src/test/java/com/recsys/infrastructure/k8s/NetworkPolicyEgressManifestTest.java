package com.recsys.infrastructure.k8s;

import org.junit.jupiter.api.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static com.recsys.infrastructure.k8s.ManifestDocuments.listOf;
import static com.recsys.infrastructure.k8s.ManifestDocuments.mapAt;
import static com.recsys.infrastructure.k8s.ManifestDocuments.nameOf;
import static com.recsys.infrastructure.k8s.ManifestDocuments.ofKind;
import static com.recsys.infrastructure.k8s.ManifestDocuments.stringListOf;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * k8s/base/network-policy.yaml is this repo's only L3/L4 access-control list, and
 * docs/system_design/20_AuthN_AuthZ.md rests the whole backend trust model on it: 6010, 7010
 * and 8080 authenticate nobody *because* the policy proves the gateway is their only reachable
 * caller. That argument only holds while the policy's destination set matches the set the
 * services actually dial — and before this test it did not, in six places at once.
 *
 * <p>Egress drift is uniquely nasty because its failure modes are quiet. A blocked service
 * registry silently falls back to static routes and looks like a registry that resolved
 * nothing; a blocked sentinel connection fails at startup in a way that reads as a Redis
 * outage. So the addresses are <em>derived</em> from the manifests rather than restated here:
 * change an upstream's address and the requirement follows it. The source is the union of
 * k8s/base/configmap.yaml and every base Deployment's inline env — SPLUNK_HEC_URL is declared
 * only in the latter, and a ConfigMap-only derivation could not see that all four serving
 * workloads dial splunk:8088.
 *
 * <p>Ownership cannot be derived the same way — recsys-config is a single ConfigMap
 * envFrom'd into all five workloads, so every service receives LLM_SERVICE_URL and MYSQL_URL
 * in its environment whether or not it dials them. {@link #OWNED_KEYS} is that missing half,
 * and {@link #everyConfigMapUpstreamKeyIsClaimed} is what stops it going stale.
 * A Deployment env var is the opposite case — it names its own dialer, so ownership there is
 * derived and needs no OWNED_KEYS entry.
 */
class NetworkPolicyEgressManifestTest {

    private static final Path BASE = Path.of("k8s", "base");

    private static final Path EKS_SHARED = Path.of("k8s", "eks-shared");
    private static final String ELASTICACHE_PATCH = "network-policy-elasticache-patch.yaml";

    /** Which ConfigMap keys each workload actually dials. See the class comment for why. */
    static final Map<String, Set<String>> OWNED_KEYS = Map.of(
            // The gateway proxies to all three backends and the LLM, and opens its own Redis
            // connection (LettuceClientFactory.routingFromEnv) when SERVICE_REGISTRY_ENABLED
            // is true — which in base means the sentinel path, since REDIS_MODE is "sentinel".
            "recsys-api-gateway", Set.of(
                    "CATALOG_SERVICE_URL", "MODEL_SERVICE_URL", "ONLINE_SERVICE_URL",
                    "USER_PROFILE_SERVICE_URL", "MOVIE_METADATA_SERVICE_URL",
                    "FEATURE_SERVICE_URL", "RECOMMENDATION_RETRIEVAL_SERVICE_URL",
                    "RANKING_SERVICE_URL", "AGENT_WORKFLOW_SERVICE_URL",
                    "OBSERVABILITY_SERVICE_URL", "LLM_SERVICE_URL",
                    "LLM_EXPLANATION_SERVICE_URL", "REDIS_HOST", "REDIS_SENTINEL_NODES"),
            "recsys-catalog-serving", Set.of("REDIS_HOST", "REDIS_SENTINEL_NODES"),
            "recsys-model-serving", Set.of("REDIS_HOST", "REDIS_SENTINEL_NODES"),
            // Online serving writes the outbox rows when MYSQL_ENABLED=true.
            "recsys-online-serving", Set.of("REDIS_HOST", "REDIS_SENTINEL_NODES", "MYSQL_URL"),
            // NOTE: the relay declares policyTypes: [Ingress], so both egress assertions skip it
            // and anything claimed here requires no rule. That makes it the cheapest place to
            // silence a failure — claim a key here only when the relay genuinely dials it.
            "recsys-outbox-relay", Set.of(
                    "MYSQL_URL", "OUTBOX_KAFKA_BOOTSTRAP_SERVERS", "SAGA_EVENTS_SQS_QUEUE_URL"));

    /**
     * Hosts k8s/base names but does not deploy, so no Service can resolve their pod labels.
     * Resolution is otherwise strict — see {@link #destinationLabels}.
     */
    static final Set<String> EXTERNALLY_DEPLOYED = Set.of("ollama", "mysql", "kafka", "splunk");

    /** Key shapes that name a network destination. Anything matching these needs an owner. */
    private static final List<String> UPSTREAM_KEY_SUFFIXES =
            List.of("_SERVICE_URL", "_HOST", "_URL", "_NODES", "_BOOTSTRAP_SERVERS");

    private static List<Map<String, Object>> baseDocuments() throws IOException {
        return ManifestDocuments.allIn(BASE);
    }

    @SuppressWarnings("unchecked")
    static Map<String, String> configMap(List<Map<String, Object>> docs) {
        Map<String, Object> cm = ofKind(docs, "ConfigMap").stream()
                .filter(d -> "recsys-config".equals(nameOf(d)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no ConfigMap named recsys-config in k8s/base"));
        Map<String, String> data = new LinkedHashMap<>();
        ((Map<String, Object>) cm.get("data")).forEach((k, v) -> data.put(k, String.valueOf(v)));
        return data;
    }

    /**
     * Inline `env:` entries carrying a literal `value:`, keyed by Deployment name. This is the
     * other half of the source of truth. SPLUNK_HEC_URL is declared here rather than in
     * recsys-config, so a ConfigMap-only derivation could not see that all four serving workloads
     * dial splunk:8088 — and the Splunk appender is bounded, drop-on-full and at-most-once, so a
     * blocked connection loses events with no signal at all.
     *
     * <p>valueFrom entries (Secret refs, field refs) are skipped: they name no address.
     */
    static Map<String, Map<String, String>> deploymentEnv(List<Map<String, Object>> docs) {
        Map<String, Map<String, String>> byWorkload = new LinkedHashMap<>();
        for (Map<String, Object> deployment : ofKind(docs, "Deployment")) {
            Map<String, String> env = new LinkedHashMap<>();
            Map<String, Object> podSpec = mapAt(deployment, "spec", "template", "spec");
            for (Map<String, Object> container : listOf(podSpec, "containers")) {
                for (Map<String, Object> entry : listOf(container, "env")) {
                    Object name = entry.get("name");
                    Object value = entry.get("value");
                    if (name != null && value != null) {
                        env.put(String.valueOf(name), String.valueOf(value));
                    }
                }
            }
            byWorkload.put(nameOf(deployment), env);
        }
        return byWorkload;
    }

    /** Key shapes that name a network destination. */
    static boolean isUpstreamKey(String key) {
        return UPSTREAM_KEY_SUFFIXES.stream().anyMatch(key::endsWith);
    }

    /** The upstream keys a workload dials, and the values to resolve them against. */
    record Dialed(Set<String> keys, Map<String, String> values) {}

    /**
     * ConfigMap keys the workload claims in {@link #OWNED_KEYS}, unioned with every inline
     * Deployment env key shaped like an upstream.
     *
     * <p>The asymmetry is deliberate. recsys-config is one ConfigMap envFrom'd into five
     * workloads, so a ConfigMap key proves nothing about who dials it and ownership has to be
     * declared. A Deployment env var names its own dialer, so ownership is derived there and
     * cannot drift — which is why no OWNED_KEYS entry is required, or accepted, for one.
     *
     * <p>Blank values are skipped: an empty string names no destination. That is what keeps
     * ONLINE_EVENTS_SQS_QUEUE_URL: "" from demanding an egress rule to a queue nobody dials.
     *
     * <p>values starts as the ConfigMap so Upstream.parse can resolve REDIS_HOST against
     * REDIS_PORT, then Deployment env overrides it — the same precedence Kubernetes applies
     * between inline env and envFrom.
     */
    static Dialed dialedBy(String workload, List<Map<String, Object>> docs) {
        Map<String, String> values = new LinkedHashMap<>(configMap(docs));
        Set<String> keys = new TreeSet<>(OWNED_KEYS.getOrDefault(workload, Set.of()));
        deploymentEnv(docs).getOrDefault(workload, Map.of()).forEach((key, value) -> {
            if (isUpstreamKey(key) && !value.isBlank()) {
                keys.add(key);
                values.put(key, value);
            }
        });
        return new Dialed(keys, values);
    }

    static Map<String, Object> policyFor(String workload, List<Map<String, Object>> docs) {
        return ofKind(docs, "NetworkPolicy").stream()
                .filter(p -> workload.equals(nameOf(p)))
                .findFirst()
                .orElse(null);
    }

    /** A policy that does not list Egress in policyTypes leaves egress unrestricted. */
    static boolean restrictsEgress(Map<String, Object> policy) {
        return stringListOf(mapAt(policy, "spec"), "policyTypes").contains("Egress");
    }

    /**
     * Resolve a host to the labels of the pods behind it. Strict on purpose: an unresolvable
     * host is dead configuration, and a permissive `app: <host>` fallback would hide exactly
     * that. REDIS_HOST "redis" named no Service before this change.
     */
    static Map<String, Object> destinationLabels(String host, List<Map<String, Object>> docs) {
        Map<String, Map<String, Object>> selectors = new LinkedHashMap<>();
        for (Map<String, Object> svc : ofKind(docs, "Service")) {
            Map<String, Object> selector = mapAt(svc, "spec", "selector");
            if (selector != null) selectors.put(nameOf(svc), selector);
        }
        String[] labels = host.split("\\.");
        // StatefulSet pod FQDN: <pod>.<headless-service>.<ns>.svc.cluster.local
        if (labels.length >= 2 && selectors.containsKey(labels[1])) return selectors.get(labels[1]);
        if (selectors.containsKey(labels[0])) return selectors.get(labels[0]);
        // Explicit witness: Map.of("app", x) infers Map<String,String>, which is not a
        // Map<String,Object> and will not compile as this method's return value.
        if (EXTERNALLY_DEPLOYED.contains(labels[0])) return Map.<String, Object>of("app", labels[0]);
        throw new AssertionError("host '" + host + "' resolves to no Service in k8s/base and is not "
                + "listed in EXTERNALLY_DEPLOYED — it is dead configuration, or a Service is missing");
    }

    /** A selector matches a pod when every one of its label pairs is present on that pod. */
    private static boolean selects(Map<String, Object> selector, Map<String, Object> podLabels) {
        return selector != null && !selector.isEmpty()
                && podLabels.entrySet().containsAll(selector.entrySet());
    }

    /**
     * Selector AND port must sit in the SAME rule. Checking "some rule names the destination"
     * and "some rule names the port" independently is the exact false pass
     * ScrapeTargetManifestTest documents having slipped past its own earlier version.
     */
    static boolean permitsEgress(Map<String, Object> policy, Map<String, Object> destLabels, int port) {
        if (!restrictsEgress(policy)) return true;
        return listOf(mapAt(policy, "spec"), "egress").stream().anyMatch(rule -> {
            boolean toDestination = listOf(rule, "to").stream()
                    .anyMatch(to -> selects(mapAt(to, "podSelector", "matchLabels"), destLabels));
            boolean onPort = listOf(rule, "ports").stream()
                    .anyMatch(p -> Integer.valueOf(port).equals(p.get("port")));
            return toDestination && onPort;
        });
    }

    /** Mirror of {@link #permitsEgress}: selector and port must sit in the same ingress rule. */
    static boolean admitsIngress(Map<String, Object> policy, Map<String, Object> sourceLabels, int port) {
        return listOf(mapAt(policy, "spec"), "ingress").stream().anyMatch(rule -> {
            boolean fromSource = listOf(rule, "from").stream()
                    .anyMatch(from -> selects(mapAt(from, "podSelector", "matchLabels"), sourceLabels));
            boolean onPort = listOf(rule, "ports").stream()
                    .anyMatch(p -> Integer.valueOf(port).equals(p.get("port")));
            return fromSource && onPort;
        });
    }

    /**
     * The sentinel pods matched no NetworkPolicy at all, which in Kubernetes means every source
     * is admitted by default — the one failure mode indistinguishable from working correctly.
     * Every other data-tier pod in this namespace is ingress-restricted; these were missed
     * because the redis policy selects app: redis and sentinels carry app: redis-sentinel.
     */
    @Test
    void sentinelPodsAreIngressRestricted() throws IOException {
        List<Map<String, Object>> docs = baseDocuments();

        Map<String, Object> policy = ofKind(docs, "NetworkPolicy").stream()
                .filter(p -> Map.of("app", "redis-sentinel").equals(mapAt(p, "spec", "podSelector", "matchLabels")))
                .findFirst()
                .orElse(null);

        assertThat(policy)
                .as("no NetworkPolicy selects app: redis-sentinel, so Kubernetes admits ingress "
                        + "from every pod in the cluster to port 26379 — sentinels can rewrite a "
                        + "client's view of which node is primary")
                .isNotNull();

        Set<String> notAdmitted = new TreeSet<>();
        List<String> expectedSources = List.of(
                "recsys-api-gateway", "recsys-catalog-serving",
                "recsys-model-serving", "recsys-online-serving");
        for (String source : expectedSources) {
            // Map.<String, Object>of — the inferred Map<String,String> does not match the
            // Map<String,Object> parameter.
            if (!admitsIngress(policy, Map.<String, Object>of("app", source), 26379)) {
                notAdmitted.add(source);
            }
        }
        // Sentinels gossip among themselves to agree on a primary; without this they cannot
        // reach quorum and never fail over.
        if (!admitsIngress(policy, Map.<String, Object>of("app", "redis-sentinel"), 26379)) {
            notAdmitted.add("redis-sentinel (sentinel-to-sentinel quorum)");
        }

        assertThat(notAdmitted)
                .as("the sentinel policy must admit 26379 from every workload granted egress to it "
                        + "and from the sentinels themselves; a policy that blocks one of these is "
                        + "worse than no policy, because it fails at startup instead of at review")
                .isEmpty();
    }

    @Test
    void sentinelClientsCanReachDiscoveredRedisPods() throws IOException {
        var docs = baseDocuments();
        for (String workload : List.of("recsys-api-gateway", "recsys-catalog-serving",
                "recsys-model-serving", "recsys-online-serving")) {
            assertThat(permitsEgress(policyFor(workload, docs), Map.of("app", "redis"), 6379))
                    .as("%s must reach discovered pod IPs even with REDIS_HOST excluded", workload).isTrue();
            assertThat(admitsIngress(policyFor("redis", docs), Map.of("app", workload), 6379))
                    .as("Redis must admit %s on the discovered data connection", workload).isTrue();
        }
    }

    @Test
    void everyDeclaredUpstreamIsPermittedByEgress() throws IOException {
        List<Map<String, Object>> docs = baseDocuments();

        Set<String> unreachable = new TreeSet<>();
        for (String workload : OWNED_KEYS.keySet()) {
            Map<String, Object> policy = policyFor(workload, docs);
            assertThat(policy).as("no NetworkPolicy named %s in k8s/base", workload).isNotNull();
            if (!restrictsEgress(policy)) continue;

            Dialed dialed = dialedBy(workload, docs);
            for (String key : dialed.keys()) {
                for (Upstream upstream : Upstream.parse(key, dialed.values())) {
                    Map<String, Object> destLabels = destinationLabels(upstream.host(), docs);
                    if (!permitsEgress(policy, destLabels, upstream.port())) {
                        unreachable.add(workload + " -> " + key + " (" + upstream.host()
                                + ":" + upstream.port() + ", pods " + destLabels + ")");
                    }
                }
            }
        }

        assertThat(unreachable)
                .as("each of these is an endpoint a workload dials with no egress rule permitting "
                        + "it, so under an enforcing CNI the connection is dropped. The failures are "
                        + "quiet in different ways and need different fixes: a blocked service "
                        + "registry falls back to static routes and logs nothing unusual, a blocked "
                        + "sentinel connection fails at startup looking like a Redis outage, a "
                        + "blocked MySQL connection surfaces only on the first outbox append, "
                        + "and a blocked Splunk connection loses log events entirely, because the "
                        + "appender is bounded, drop-on-full and at-most-once by design — nothing "
                        + "retries and nothing errors. Add a "
                        + "matching egress rule to k8s/base/network-policy.yaml")
                .isEmpty();
    }

    /**
     * Both ends enforce independently, so a granted egress rule is worthless if the destination's
     * own policy does not admit the source. This is the half that is easy to forget, because the
     * egress side is the one you edit when you notice the connection failing.
     */
    @Test
    void everyPermittedEgressIsAdmittedByItsDestination() throws IOException {
        List<Map<String, Object>> docs = baseDocuments();
        List<Map<String, Object>> policies = ofKind(docs, "NetworkPolicy");

        Set<String> blocked = new TreeSet<>();
        for (String workload : OWNED_KEYS.keySet()) {
            Map<String, Object> sourcePolicy = policyFor(workload, docs);
            if (sourcePolicy == null || !restrictsEgress(sourcePolicy)) continue;
            Map<String, Object> sourceLabels = mapAt(sourcePolicy, "spec", "podSelector", "matchLabels");

            Dialed dialed = dialedBy(workload, docs);
            for (String key : dialed.keys()) {
                for (Upstream upstream : Upstream.parse(key, dialed.values())) {
                    Map<String, Object> destLabels = destinationLabels(upstream.host(), docs);

                    // Only destinations governed by a policy restrict ingress; ollama and mysql
                    // are deployed outside k8s/base and have none.
                    Map<String, Object> destPolicy = policies.stream()
                            .filter(p -> {
                                Map<String, Object> sel = mapAt(p, "spec", "podSelector", "matchLabels");
                                return sel != null && !sel.isEmpty()
                                        && destLabels.entrySet().containsAll(sel.entrySet())
                                        && stringListOf(mapAt(p, "spec"), "policyTypes").contains("Ingress");
                            })
                            .findFirst()
                            .orElse(null);
                    if (destPolicy == null) continue;

                    if (!admitsIngress(destPolicy, sourceLabels, upstream.port())) {
                        blocked.add(workload + " -> " + upstream.host() + ":" + upstream.port()
                                + " (egress granted, but NetworkPolicy " + nameOf(destPolicy)
                                + " does not admit " + sourceLabels + ")");
                    }
                }
            }
        }

        assertThat(blocked)
                .as("these connections are permitted on the way out and dropped on the way in. "
                        + "NetworkPolicy is enforced at both ends, so granting egress alone changes "
                        + "nothing observable — the connection still fails, and it fails identically "
                        + "to having no egress rule at all. Add the matching ingress rule")
                .isEmpty();
    }

    /**
     * The drift catcher. Adding an upstream to the ConfigMap without claiming it here is exactly
     * how the policy fell six rules behind reality — every one of those gaps was a config change
     * that nobody paired with a policy change. Claiming a key is cheap; the claim is what makes
     * {@link #everyDeclaredUpstreamIsPermittedByEgress} require a rule for it.
     */
    @Test
    void everyConfigMapUpstreamKeyIsClaimed() throws IOException {
        Map<String, String> cfg = configMap(baseDocuments());

        Set<String> claimed = OWNED_KEYS.values().stream()
                .flatMap(Set::stream).collect(java.util.stream.Collectors.toSet());

        Set<String> unclaimed = new TreeSet<>();
        for (String key : cfg.keySet()) {
            if (isUpstreamKey(key) && !claimed.contains(key)) unclaimed.add(key);
        }

        assertThat(unclaimed)
                .as("these ConfigMap keys name a network destination that no workload claims in "
                        + "OWNED_KEYS. Either add the key to the workload that dials it — which "
                        + "will then require a matching egress rule — or, if nothing dials it, "
                        + "delete it from the ConfigMap rather than leaving dead configuration "
                        + "that reads like a live dependency")
                .isEmpty();

        // The reverse direction. Upstream.parse returns an empty list for a key the ConfigMap
        // does not define, so a misspelled claim silently requires no egress rule at all — and
        // when the correctly-spelled key is claimed by another workload, the check above stays
        // green too. Renaming one claim to "REDIS_HOSTT" was enough to un-enforce an entire gap
        // this test exists to close.
        assertThat(cfg.keySet())
                .as("OWNED_KEYS claims a key that recsys-config does not define — a typo here "
                        + "drops that workload's egress requirement without failing anything")
                .containsAll(claimed);

        // The Deployment half. everyDeclaredUpstreamIsPermittedByEgress iterates OWNED_KEYS, so a
        // base Deployment missing from that map has its inline env upstreams checked by nothing —
        // which is precisely how SPLUNK_HEC_URL went unnoticed, one level up. An entry with an
        // empty key set is a valid claim: it says "this workload dials nothing from the ConfigMap".
        Set<String> unmapped = new TreeSet<>(deploymentEnv(baseDocuments()).keySet());
        unmapped.removeAll(OWNED_KEYS.keySet());
        assertThat(unmapped)
                .as("these k8s/base Deployments have no OWNED_KEYS entry, so no egress assertion "
                        + "iterates them and any upstream in their inline env is unchecked. Add an "
                        + "entry — Set.of() if the workload dials nothing from recsys-config")
                .isEmpty();
    }

    /**
     * The relay declares policyTypes: [Ingress] deliberately: its whole job is reaching MySQL,
     * Kafka and (in EKS) ElastiCache, so an egress allow-list there would black-hole delivery the
     * moment it drifted. That makes it satisfy the egress assertion trivially, which is precisely
     * why it needs asserting — a later edit "completing" the policy with an Egress type would
     * pass every other test in this class while stopping outbox delivery in production.
     */
    @Test
    void theOutboxRelayEgressIsDeliberatelyUnrestricted() throws IOException {
        Map<String, Object> policy = policyFor("recsys-outbox-relay", baseDocuments());

        assertThat(policy).as("no NetworkPolicy named recsys-outbox-relay in k8s/base").isNotNull();
        assertThat(restrictsEgress(policy))
                .as("recsys-outbox-relay must NOT declare Egress in policyTypes. Its destinations "
                        + "(MySQL, Kafka, ElastiCache) are partly external and partly per-overlay, "
                        + "so an allow-list here silently stops outbox delivery. If you are adding "
                        + "Egress deliberately, extend OWNED_KEYS to cover every destination it "
                        + "dials and delete this assertion in the same commit")
                .isFalse();
    }

    /**
     * The only peer a DNS egress rule may name today: the kube-system namespace. See the
     * operational note beside the port-53 rule in k8s/base/network-policy.yaml for why a
     * namespaceSelector rather than a podSelector, and for the NodeLocal DNSCache ipBlock a
     * cluster running that would need here — deliberately not added, since none of this repo's
     * overlays run it.
     */
    private static final Map<String, Object> KUBE_SYSTEM_NAMESPACE_PEER = Map.of(
            "namespaceSelector",
            Map.of("matchLabels", Map.of("kubernetes.io/metadata.name", "kube-system")));

    /**
     * A NetworkPolicy egress rule's `to[]` must name the peer set exactly, not merely be
     * non-empty: `to: [{ipBlock: {cidr: 0.0.0.0/0}}]` is as permissive as no `to[]` at all and
     * would pass a check that only asks "is `to[]` empty?". Four policies carried a bare
     * `ports: [53]` with no `to[]` at all — the one channel every workload needs was also the one
     * channel that reached anywhere — a tunnel out of an allow-list built to prevent exactly that.
     *
     * <p>This assertion exists because over-permissiveness is invisible to every other test here:
     * they all ask whether a destination is *reachable*, and a rule that reaches everything passes
     * all of them. It also requires the port-53 rule to exist at all: a policy with no DNS egress
     * rule would otherwise pass this check vacuously and then fail every DNS lookup at runtime
     * with unknown-host under an enforcing CNI.
     */
    @Test
    void dnsEgressIsScopedToKubeSystem() throws IOException {
        List<Map<String, Object>> docs = baseDocuments();

        Set<String> violations = new TreeSet<>();
        for (Map<String, Object> policy : ofKind(docs, "NetworkPolicy")) {
            if (!restrictsEgress(policy)) continue;

            List<Map<String, Object>> port53Rules = listOf(mapAt(policy, "spec"), "egress").stream()
                    .filter(rule -> listOf(rule, "ports").stream()
                            .anyMatch(p -> Integer.valueOf(53).equals(p.get("port"))))
                    .collect(java.util.stream.Collectors.toList());

            if (port53Rules.isEmpty()) {
                violations.add(nameOf(policy) + ": no port-53 egress rule at all");
                continue;
            }

            for (Map<String, Object> rule : port53Rules) {
                List<Map<String, Object>> peers = listOf(rule, "to");
                boolean hasKubeSystemPeer = peers.contains(KUBE_SYSTEM_NAMESPACE_PEER);
                boolean hasExtraPeer = peers.stream().anyMatch(p -> !KUBE_SYSTEM_NAMESPACE_PEER.equals(p));
                if (!hasKubeSystemPeer || hasExtraPeer) {
                    violations.add(nameOf(policy) + ": port-53 to[] = " + peers);
                }
            }
        }

        assertThat(violations)
                .as("every Egress-restricted policy must carry a port-53 rule whose `to[]` "
                        + "consists solely of a namespaceSelector matching "
                        + "kubernetes.io/metadata.name=kube-system. A rule with no `to[]`, or one "
                        + "with an extra peer such as an ipBlock, permits port 53 to more than "
                        + "kube-system — so a workload whose other egress is confined to a "
                        + "hand-checked allow-list can still send arbitrary UDP or TCP elsewhere on "
                        + "53. A policy with no port-53 rule at all is just as wrong the other way: "
                        + "every DNS lookup fails with unknown-host under an enforcing CNI")
                .isEmpty();
    }

    /**
     * Both EKS overlays scale in-cluster Redis to zero and point REDIS_HOST at an ElastiCache
     * endpoint outside the cluster. A podSelector cannot match an external address, and nothing
     * patched the policy, so every Redis call in EKS was unpermitted.
     *
     * <p>Asserts the patch APPENDS. NetworkPolicy's spec.egress has no patchMergeKey, so a
     * strategic-merge patch replaces the whole list — the first version of this patch did, and
     * the rendered overlay silently lost gateway→backends, sentinel, ollama and DNS. That failure
     * is invisible in the patch file itself and only shows up in a render, so the shape of the op
     * is what this test pins.
     *
     * <p>The CIDR is an operator-supplied REPLACE_ME value, so its correctness is deliberately
     * out of scope: this checks that the rule exists on both Redis ports and appends rather than
     * replaces, and stops there.
     */
    @Test
    @SuppressWarnings("unchecked")
    void theEksOverlayPatchesEgressForExternalRedis() throws IOException {
        Map<String, Object> kustomization;
        try (InputStream in = Files.newInputStream(EKS_SHARED.resolve("kustomization.yaml"))) {
            kustomization = (Map<String, Object>) new Yaml().load(in);
        }

        Set<String> patched = new TreeSet<>();
        for (Map<String, Object> entry : listOf(kustomization, "patches")) {
            if (!ELASTICACHE_PATCH.equals(entry.get("path"))) continue;
            Map<String, Object> target = mapAt(entry, "target");
            if (target != null && "NetworkPolicy".equals(target.get("kind"))) {
                patched.add(String.valueOf(target.get("name")));
            }
        }

        assertThat(patched)
                .as("k8s/eks-shared/kustomization.yaml must apply %s to each serving NetworkPolicy. "
                        + "Both region overlays replace in-cluster Redis with ElastiCache, which no "
                        + "podSelector in k8s/base can match, so a workload missing from this list "
                        + "has no permitted route to Redis at all in EKS", ELASTICACHE_PATCH)
                .containsExactlyInAnyOrder("recsys-api-gateway", "recsys-catalog-serving",
                        "recsys-model-serving", "recsys-online-serving");

        Object loaded;
        try (InputStream in = Files.newInputStream(EKS_SHARED.resolve(ELASTICACHE_PATCH))) {
            loaded = new Yaml().load(in);
        }
        assertThat(loaded)
                .as("%s must be a JSON-patch list (`op`/`path`/`value` entries). A document "
                        + "shaped like `kind: NetworkPolicy` here means the patch REPLACES "
                        + "spec.egress via strategic merge instead of appending to it, which "
                        + "silently strips every rule k8s/base declared", ELASTICACHE_PATCH)
                .isInstanceOf(List.class);
        List<Map<String, Object>> ops = (List<Map<String, Object>>) loaded;

        Set<Integer> ports = new TreeSet<>();
        boolean appendsIpBlock = false;
        for (Map<String, Object> op : ops) {
            if (!"add".equals(op.get("op")) || !"/spec/egress/-".equals(op.get("path"))) continue;
            Map<String, Object> value = mapAt(op, "value");
            if (listOf(value, "to").stream().noneMatch(to -> mapAt(to, "ipBlock") != null)) continue;
            appendsIpBlock = true;
            for (Map<String, Object> p : listOf(value, "ports")) ports.add((Integer) p.get("port"));
        }

        assertThat(appendsIpBlock)
                .as("%s must APPEND via `op: add` on `/spec/egress/-`. An op targeting "
                        + "`/spec/egress` itself, or a strategic-merge NetworkPolicy document, "
                        + "replaces the list and silently strips every rule k8s/base declared",
                        ELASTICACHE_PATCH)
                .isTrue();
        assertThat(ports)
                .as("the ElastiCache rule must cover 6379 and the sentinel port 26379")
                .contains(6379, 26379);
    }
}
