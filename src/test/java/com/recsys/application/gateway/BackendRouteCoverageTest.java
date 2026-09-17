package com.recsys.application.gateway;

import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.RequestHeaders;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every route a gateway caller can reach on a backend must be classified in
 * {@link BackendRoutePolicy}.
 *
 * <p>Adding a backend route therefore fails this test until someone decides which of NO_PROXY,
 * OPERATOR, USER_SCOPED or AUTHENTICATED it is. That is the point — the gap this closes was never
 * one missing check, it was that nothing forced the question to be asked.
 */
class BackendRouteCoverageTest {

    /**
     * Floors, not exact counts: a regex that silently stops matching would otherwise make this
     * whole test vacuous. Raise them when a service genuinely grows.
     */
    private static final Map<String, Integer> MINIMUM_ROUTES = Map.of(
            "recsys-catalog-serving", 16,
            "recsys-online-serving", 9,
            "recsys-model-serving", 20);

    @Test
    void everyBackendRouteIsClassified() throws IOException {
        Map<String, Set<String>> routes = scanAllServices();

        List<String> unclassified = new ArrayList<>();
        routes.forEach((service, paths) -> {
            int floor = MINIMUM_ROUTES.get(service);
            assertTrue(paths.size() >= floor,
                    "Route scan for " + service + " found only " + paths.size() + " routes (expected at "
                            + "least " + floor + "). The scanner has probably stopped matching — fix it "
                            + "rather than lowering the floor, or this test silently passes forever. "
                            + "Found: " + paths);
            for (String path : paths) {
                if (BackendRoutePolicy.lookup(service, path) == null) {
                    unclassified.add(service + path);
                }
            }
        });

        assertTrue(unclassified.isEmpty(),
                "Unclassified backend routes: " + unclassified + ". Every gateway-reachable route must "
                        + "be classified in BackendRoutePolicy as NO_PROXY, OPERATOR, USER_SCOPED or "
                        + "AUTHENTICATED. An unclassified route is denied at the gateway, so shipping "
                        + "one silently breaks it. See "
                        + "docs/superpowers/specs/2026-08-05-gateway-proxy-route-policy-design.md.");
    }

    /**
     * The reverse direction: a declared exact path that no scan finds is dead weight that would
     * pre-classify a future route of the same name. Prefixes are exempt — /actuator is
     * config-driven and cannot be scanned at all.
     */
    @Test
    void noDeclaredExactPathIsAnOrphan() throws IOException {
        Map<String, Set<String>> scanned = scanAllServices();
        List<String> orphans = new ArrayList<>();
        for (Map.Entry<String, Set<String>> entry : scanned.entrySet()) {
            for (String declared : BackendRoutePolicy.exactPaths(entry.getKey())) {
                if (!entry.getValue().contains(declared)) {
                    orphans.add(entry.getKey() + declared);
                }
            }
        }
        assertTrue(orphans.isEmpty(),
                "BackendRoutePolicy declares exact paths that no backend registers: " + orphans
                        + ". Remove them — a stale entry silently pre-classifies a future route.");
    }

    /**
     * Exact matching is tried first, so a declared exact path that sits <em>under</em> a prefix is
     * not shadowed by it — the exact entry wins, and the prefix still governs every sibling path
     * the exact table does not name. {@code "/shards/topology" -> OPERATOR} declared alongside the
     * {@code /shards -> AUTHENTICATED} prefix is exactly this: legal, and exercised by
     * {@code lookup}'s exact-first order, not dead code.
     *
     * <p>The one genuinely dead case is a prefix entry whose own path is <em>also</em> declared
     * exactly: {@code exact.equals(prefix)}. There, the exact entry always wins the lookup and the
     * prefix's own branch — matching {@code prefix} itself — can never fire, even though the prefix
     * still legitimately governs its siblings.
     */
    @Test
    void noPrefixEntryShadowsADeclaredExactPath() {
        // Every service the table declares, not just the ones MINIMUM_ROUTES names: a prefix
        // declared for a service absent from the floors map would otherwise go unchecked.
        for (String service : BackendRoutePolicy.declaredServices()) {
            for (String prefix : BackendRoutePolicy.prefixPaths(service)) {
                for (String exact : BackendRoutePolicy.exactPaths(service)) {
                    assertFalse(exact.equals(prefix),
                            "Prefix " + service + prefix + " is also declared as an exact path, so "
                                    + "its own prefix branch (matching " + prefix + " itself) is dead "
                                    + "code — the exact entry always wins the lookup.");
                }
            }
        }
    }

    /**
     * {@code PREFIX} is a {@code Map.of}, whose iteration order is randomized per JVM, and
     * {@code lookup} returns the FIRST matching prefix. If one declared prefix were a proper
     * prefix of another — say {@code /api/v1/retrieval} alongside
     * {@code /api/v1/retrieval/profile-audit} — which policy applied would depend on the JVM's
     * hash seed: OPERATOR on some pods, AUTHENTICATED on others, with the suite green either way.
     *
     * <p>No two of today's prefixes overlap, so this pins that rather than fixing anything. The
     * cheap fix for a future overlap is to keep the entries disjoint; the alternative is an
     * ordered map and a longest-match rule, which is a bigger change than the table needs today.
     */
    @Test
    void noDeclaredPrefixIsAProperPrefixOfAnother() {
        for (String service : BackendRoutePolicy.declaredServices()) {
            for (String outer : BackendRoutePolicy.prefixPaths(service)) {
                for (String inner : BackendRoutePolicy.prefixPaths(service)) {
                    if (outer.equals(inner)) {
                        continue;
                    }
                    assertFalse(inner.startsWith(outer + "/"),
                            "Prefix " + service + inner + " sits under " + outer + ", and PREFIX is "
                                    + "an unordered Map.of — which of the two governs a request is "
                                    + "then decided by the JVM's hash seed, not by this table.");
                }
            }
        }
    }

    /** Scans all three backend mains, keyed by their registry service name. */
    private static Map<String, Set<String>> scanAllServices() throws IOException {
        Map<String, Set<String>> routes = new LinkedHashMap<>();
        routes.put("recsys-catalog-serving",
                armeriaRoutes(Path.of("src/main/java/com/recsys/api/serving/RecSysServer.java")));
        routes.put("recsys-online-serving",
                armeriaRoutes(Path.of("src/main/java/com/recsys/api/online/OnlinePredictionServer.java")));
        routes.put("recsys-model-serving",
                springRoutes(Path.of("src/main/java/com/recsys/api/rest")));
        return routes;
    }

    // ---- the gateway route table is the other half of the coverage claim -------------------

    /** The three backends every user-scoped route lives behind. */
    private static final Set<Integer> BACKEND_PORTS = Set.of(6010, 7010, 8080);

    /** Route names served by {@link LlmProxyService}, which is not one of the three backends. */
    private static final Set<String> LLM_ROUTE_NAMES = Set.of("llm", "llm-explanation");

    /**
     * {@code BackendRoutePolicy.lookup} keys on {@code MicroserviceRoute.serviceName}, and returns
     * null for a null service. So a route added with the 5-arg convenience constructor — which
     * defaults {@code serviceName} to null — is permanently exempt from the user-scope check even
     * when it points straight at 6010, 7010 or 8080.
     *
     * <p>{@link #everyBackendRouteIsClassified} cannot see that: it scans the backend mains, never
     * the gateway's own route table. This is the missing half — the gateway side of the same claim.
     */
    @Test
    void everyRouteReachingABackendDeclaresItsRegistryServiceName() {
        List<String> unnamed = new ArrayList<>();
        for (MicroserviceRoute route : MicroserviceRoute.defaults()) {
            if (route.serviceName() != null) {
                continue;
            }
            boolean llm = LLM_ROUTE_NAMES.contains(route.name());
            // Both conditions, because baseUri is env-overridable: a route pointing at a backend
            // port is caught even if it is named oddly, and a non-LLM route is caught even if an
            // env var moved it off the default port.
            if (BACKEND_PORTS.contains(route.baseUri().getPort()) || !llm) {
                unnamed.add(route.name() + " (" + route.prefix() + " -> " + route.baseUri() + ")");
            }
        }
        assertTrue(unnamed.isEmpty(),
                "Gateway routes reaching a backend with no serviceName: " + unnamed + ". "
                        + "BackendRoutePolicy.lookup returns null for a null service, so such a route "
                        + "is silently exempt from the user-scope check forever. Use the 6-arg "
                        + "MicroserviceRoute constructor and give it its registry service name.");
    }

    /**
     * The never-public guard must cover every user-scoped route, or {@code GATEWAY_PUBLIC_PATHS}
     * becomes an off switch for §10: a public path yields an anonymous principal, anonymous is
     * SERVICE tier, and service tier is exempt from the check.
     *
     * <p>{@code PROTECTED_PREFIXES} is derived from {@code BackendRoutePolicy} rather than
     * restated, so this test pins that the derivation is actually wired into {@code isPublic} — it
     * configures every user-scoped path as public and asserts the gateway still demands a
     * credential.
     */
    @Test
    void noUserScopedRouteCanBeMadePublic() {
        Set<String> userScoped = BackendRoutePolicy.userScopedGatewayPaths(MicroserviceRoute.defaults());
        assertTrue(userScoped.size() >= 20,
                "expected the derivation to produce a path per (prefix, handler) pair, got: " + userScoped);
        // The three the finding named explicitly; none was covered by the old hand-written list.
        assertTrue(userScoped.containsAll(Set.of(
                        "/api/catalog/getrecommendation", "/api/movies/getuser", "/api/online/online/features")),
                "derived set is missing a known user-scoped gateway path: " + userScoped);

        // Misconfigure the gateway as badly as possible: every user-scoped path listed as public.
        GatewayAuthenticator auth =
                GatewayAuthenticator.forTesting(Set.of("key-1"), userScoped, null);
        List<String> reachable = new ArrayList<>();
        for (String path : userScoped) {
            if (!auth.check(RequestHeaders.of(HttpMethod.GET, path), path).rejected()) {
                reachable.add(path);
            }
        }
        assertTrue(reachable.isEmpty(),
                "GATEWAY_PUBLIC_PATHS made these user-scoped routes anonymously reachable: "
                        + reachable + ". Anonymous is SERVICE tier, which is exempt from the "
                        + "user-scope check, so this is an off switch for 20_AuthN_AuthZ §10.");
    }

    /**
     * {@code userScopedGatewayPaths} used to walk only {@code EXACT}, so the three retrieval
     * paths declared {@code userScoped(UserIdSource.PATH)} in {@code PREFIX} — a path template is
     * not a path, so they cannot be declared exactly — were invisible to the guard this test
     * exercises. This pins that the derivation now sees them too.
     */
    @Test
    void userScopedGatewayPathsIncludesPrefixDeclaredRoutes() {
        Set<String> userScoped = BackendRoutePolicy.userScopedGatewayPaths(MicroserviceRoute.defaults());
        assertTrue(userScoped.containsAll(Set.of(
                        "/api/retrieval/api/v1/retrieval/recommend",
                        "/api/retrieval/api/v1/retrieval/predict",
                        "/api/retrieval/api/v1/retrieval/users")),
                "derived set is missing a PREFIX-declared user-scoped retrieval path: " + userScoped);
    }

    // ---- the scanners only guarantee anything if nothing registers routes elsewhere ---------

    private static final String SPRING_SCAN_ROOT = "src/main/java/com/recsys/api/rest";

    private static final Set<String> SCANNED_ARMERIA_MAINS = Set.of(
            "src/main/java/com/recsys/api/serving/RecSysServer.java",
            "src/main/java/com/recsys/api/online/OnlinePredictionServer.java");

    /** Armeria route hosts that are deliberately not scanned, and why each is not a backend. */
    private static final Map<String, String> ROUTE_HOSTS_NOT_SCANNED = Map.of(
            "src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java",
            "the gateway itself — the caller side of these routes, not a backend behind it",
            "src/main/java/com/recsys/application/outbox/OutboxRelayCommand.java",
            "standalone relay daemon on its own port; no MicroserviceRoute points at it, and it "
                    + "serves only /health/live, /health/ready and /metrics");

    /**
     * Negative lookahead so {@code @RestControllerAdvice} — which contains {@code @RestController}
     * as a substring — does not read as a controller. {@code GlobalExceptionHandler} is the one
     * file that would otherwise fail this falsely.
     */
    private static final Pattern CONTROLLER_ANNOTATION =
            Pattern.compile("@(?:Rest)?Controller(?![A-Za-z])");
    private static final Pattern ROUTE_REGISTRATION =
            Pattern.compile("\\.(?:service|annotatedService)\\(");

    /**
     * The scanners above read three hardcoded locations, so they prove "no route ships
     * unclassified" only while every route registration lives in one of them. This sweep is what
     * makes that premise true rather than assumed: a controller in a new sub-package, or an
     * Armeria route registered from a new class, fails here instead of passing invisibly.
     */
    @Test
    void everyRouteRegistrationLivesWhereAScannerLooks() throws IOException {
        List<String> stray = new ArrayList<>();
        try (Stream<Path> files = Files.walk(Path.of("src/main/java"))) {
            for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                String path = file.toString().replace('\\', '/');
                String source = Files.readString(file);
                if (CONTROLLER_ANNOTATION.matcher(source).find()
                        && !path.startsWith(SPRING_SCAN_ROOT + "/")) {
                    stray.add(path + " — a Spring controller outside " + SPRING_SCAN_ROOT);
                }
                if (ROUTE_REGISTRATION.matcher(source).find()
                        && !SCANNED_ARMERIA_MAINS.contains(path)
                        && !ROUTE_HOSTS_NOT_SCANNED.containsKey(path)) {
                    stray.add(path + " — registers Armeria routes that no scanner reads");
                }
            }
        }
        assertTrue(stray.isEmpty(),
                "Routes are registered where no scanner looks: " + stray + ". everyBackendRouteIs"
                        + "Classified() only guarantees anything for the locations it scans, so "
                        + "either move this back under a scanned location, teach the scanner about "
                        + "it, or — if it is not a gateway-reachable backend — add it to "
                        + "ROUTE_HOSTS_NOT_SCANNED with the reason.");
    }

    // ---- scanners -------------------------------------------------------------------------

    private static final Pattern ROUTE_CONSTANT =
            Pattern.compile("String\\s+(ROUTE_[A-Z0-9_]+)\\s*=\\s*\"([^\"]+)\"");
    private static final Pattern SERVICE_CALL =
            Pattern.compile("\\.service\\(\\s*(?:(ROUTE_[A-Z0-9_]+)|\"([^\"]+)\")");
    private static final Pattern PATH_PREFIX = Pattern.compile("pathPrefix\\(\"([^\"]+)\"\\)");
    private static final Pattern REGEX_ROUTE =
            Pattern.compile("\\.regex\\(\\s*\"\\^\"\\s*\\+\\s*(ROUTE_[A-Z0-9_]+)");

    /** Armeria: `.service(ROUTE_X, ...)`, `.service("/literal", ...)`, pathPrefix and regex routes. */
    private static Set<String> armeriaRoutes(Path file) throws IOException {
        String source = Files.readString(file);
        Map<String, String> constants = new LinkedHashMap<>();
        Matcher constant = ROUTE_CONSTANT.matcher(source);
        while (constant.find()) {
            constants.put(constant.group(1), constant.group(2));
        }
        Set<String> paths = new LinkedHashSet<>();
        Matcher call = SERVICE_CALL.matcher(source);
        while (call.find()) {
            String value = call.group(1) != null ? constants.get(call.group(1)) : call.group(2);
            if (value != null && value.startsWith("/")) {
                paths.add(value);
            }
        }
        Matcher prefix = PATH_PREFIX.matcher(source);
        while (prefix.find()) {
            paths.add(prefix.group(1));
        }
        Matcher regex = REGEX_ROUTE.matcher(source);
        while (regex.find()) {
            String value = constants.get(regex.group(1));
            if (value != null) {
                paths.add(value);
            }
        }
        return paths;
    }

    private static final Pattern CLASS_MAPPING =
            Pattern.compile("@RequestMapping\\(\\s*\"([^\"]+)\"\\s*\\)");
    private static final Pattern METHOD_MAPPING = Pattern.compile(
            "@(?:Get|Post|Put|Delete|Patch)Mapping\\(\\s*(?:value\\s*=\\s*)?(?:\"([^\"]*)\")?");

    /**
     * Spring: class-level @RequestMapping joined with each method mapping's path (possibly empty).
     * Walks rather than lists: a controller in a sub-package would otherwise ship unclassified,
     * which is the exact failure this test exists to prevent.
     */
    private static Set<String> springRoutes(Path directory) throws IOException {
        Set<String> paths = new LinkedHashSet<>();
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path file : files.filter(p -> p.toString().endsWith(".java")).toList()) {
                String source = Files.readString(file);
                Matcher classMatcher = CLASS_MAPPING.matcher(source);
                String base = classMatcher.find() ? classMatcher.group(1) : "";
                Matcher methodMatcher = METHOD_MAPPING.matcher(source);
                while (methodMatcher.find()) {
                    String suffix = methodMatcher.group(1) == null ? "" : methodMatcher.group(1);
                    String path = base + suffix;
                    if (path.startsWith("/")) {
                        paths.add(path);
                    }
                }
            }
        }
        return paths;
    }
}
