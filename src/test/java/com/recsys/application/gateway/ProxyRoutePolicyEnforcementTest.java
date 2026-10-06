package com.recsys.application.gateway;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.recsys.application.auth.AdminTokenGuard;
import com.recsys.ratelimit.GatewayRateLimiter;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The gateway proxies only what it has been told to proxy.
 *
 * <p>{@code MicroserviceRoute.rewrite} forwards the suffix under a matched prefix verbatim, so
 * before this every backend route was reachable through every prefix targeting its service —
 * telemetry included. Denial is an allow-list default, not a denylist: a diagnostic route added
 * tomorrow is unreachable rather than exposed.
 */
class ProxyRoutePolicyEnforcementTest {

    private static final MicroserviceRoute CATALOG = new MicroserviceRoute(
            "catalog", "/api/catalog", "CATALOG_SERVICE_URL",
            URI.create("http://localhost:6010"), "/health", "recsys-catalog-serving");

    private static final MicroserviceRoute MODEL = new MicroserviceRoute(
            "model", "/api/model", "MODEL_SERVICE_URL",
            URI.create("http://localhost:8080"), "/health/ready", "recsys-model-serving");

    private static final MicroserviceRoute LLM = new MicroserviceRoute(
            "llm", "/api/llm", "LLM_SERVICE_URL", URI.create("http://localhost:11434"), "/api/tags");

    @Test
    void telemetryIsNotProxied() {
        assertDenied404(forwarder().enforceRoutePolicy(CATALOG, "/metrics", get(), apiKey()));
        assertDenied404(forwarder().enforceRoutePolicy(MODEL, "/actuator/prometheus", get(), apiKey()));
        assertDenied404(forwarder().enforceRoutePolicy(MODEL, "/health/ab-tests", get(), apiKey()));
    }

    @Test
    void anUnclassifiedPathIsNotProxied() {
        assertDenied404(forwarder().enforceRoutePolicy(CATALOG, "/brand-new-endpoint", get(), apiKey()));
    }

    /**
     * A withheld path must be indistinguishable from one that was never routed, or the allow-list
     * becomes an enumeration oracle: probe a path, read the status, learn whether it exists.
     */
    @Test
    void aDenialIsIndistinguishableFromAnUnroutedPath() {
        AggregatedHttpResponse denied = forwarder()
                .enforceRoutePolicy(CATALOG, "/metrics", get(), apiKey()).aggregate().join();
        AggregatedHttpResponse unrouted = GatewayProxyService
                .gatewayError(HttpStatus.NOT_FOUND, "no route found").aggregate().join();

        assertEquals(unrouted.status(), denied.status());
        assertEquals(unrouted.contentUtf8(), denied.contentUtf8());
    }

    @Test
    void ordinaryDataPathsStillProxy() {
        assertNull(forwarder().enforceRoutePolicy(CATALOG, "/item?id=7", get(), apiKey()));
        assertNull(forwarder().enforceRoutePolicy(MODEL, "/api/v1/token", get(), apiKey()));
        assertNull(forwarder().enforceRoutePolicy(CATALOG, "/similar?id=7", get(), apiKey()));
    }

    /**
     * A concrete id, not the template. Two of the four knowledge-base handlers are declared with a
     * Spring path template, and the policy table first spelled them out as the literal string
     * {@code /api/v1/knowledge-bases/{knowledgeBaseId}} — which only the route scanner ever emits.
     * Every real request 404'd, and both coverage tests were blind to it because the scanner and
     * the table agreed on the same unreachable literal. Asserting a real id is what catches that.
     */
    @Test
    void templatedBackendRoutesProxyForAConcreteId() {
        assertNull(forwarder().enforceRoutePolicy(MODEL, "/api/v1/knowledge-bases", get(), apiKey()));
        assertNull(forwarder().enforceRoutePolicy(
                MODEL, "/api/v1/knowledge-bases/kb-123", get(), apiKey()));
        // The boundary rule still holds: a sibling that merely starts with the same characters is
        // not under the prefix.
        assertDenied404(forwarder().enforceRoutePolicy(
                MODEL, "/api/v1/knowledge-bases-admin", get(), apiKey()));
    }

    /** The allow-list governs our backends. An LLM upstream is not ours to classify. */
    @Test
    void aRouteReachingNoKnownBackendIsUnaffected() {
        assertNull(forwarder().enforceRoutePolicy(LLM, "/api/generate", get(), apiKey()));
    }

    /** User-scope enforcement is unchanged: still applied, still only to user-tier callers. */
    @Test
    void userScopedRoutesKeepTheirExistingCheck() {
        assertNull(forwarder().enforceRoutePolicy(CATALOG, "/getuser?userId=99", get(), apiKey()));
        assertNotNull(forwarder().enforceRoutePolicy(CATALOG, "/getuser?userId=99", get(), user("42")));
        assertNull(forwarder().enforceRoutePolicy(CATALOG, "/getuser?userId=42", get(), user("42")));
    }

    @Test
    void operatorPathsRequireTheOperatorToken() {
        GatewayRequestForwarder forwarder = forwarder(new AdminTokenGuard("s3cret"));

        assertDenied403(forwarder.enforceRoutePolicy(CATALOG, "/setembedding", get(), apiKey()));
        assertDenied403(forwarder.enforceRoutePolicy(
                MODEL, "/api/v1/model/versions/activate", get(), apiKey()));
        assertDenied403(forwarder.enforceRoutePolicy(
                CATALOG, "/setembedding", withToken("wrong"), apiKey()));
    }

    @Test
    void operatorPathsProxyWithTheCorrectToken() {
        GatewayRequestForwarder forwarder = forwarder(new AdminTokenGuard("s3cret"));
        assertNull(forwarder.enforceRoutePolicy(CATALOG, "/setembedding", withToken("s3cret"), apiKey()));
        assertNull(forwarder.enforceRoutePolicy(
                MODEL, "/api/v1/model/versions/rollback", withToken("s3cret"), apiKey()));
    }

    /**
     * Unlike the user-scope check, this one binds service-tier callers too. An API key is the
     * credential every real caller holds today; if it were sufficient here, the class would mean
     * nothing — swapping the serving model would sit in the same tier as reading a movie.
     */
    @Test
    void theOperatorCheckBindsServiceTierCallersToo() {
        GatewayRequestForwarder forwarder = forwarder(new AdminTokenGuard("s3cret"));
        assertDenied403(forwarder.enforceRoutePolicy(CATALOG, "/setembedding", get(), apiKey()));
        assertDenied403(forwarder.enforceRoutePolicy(CATALOG, "/setembedding", get(),
                GatewayPrincipal.anonymous()));
    }

    /** No token configured authorizes nobody — the rule AdminTokenGuard already applies on 7010. */
    @Test
    void operatorPathsFailClosedWhenNoTokenIsConfigured() {
        assertDenied403(forwarder(new AdminTokenGuard("")).enforceRoutePolicy(
                CATALOG, "/setembedding", withToken("anything"), apiKey()));
        assertDenied403(forwarder(null).enforceRoutePolicy(
                CATALOG, "/setembedding", withToken("anything"), apiKey()));
    }

    private static void assertDenied403(HttpResponse response) {
        assertNotNull(response, "expected a denial");
        AggregatedHttpResponse aggregated = response.aggregate().join();
        assertEquals(HttpStatus.FORBIDDEN, aggregated.status());
        assertEquals("{\"error\":\"operator token required\"}", aggregated.contentUtf8());
    }

    private static AggregatedHttpRequest withToken(String token) {
        return AggregatedHttpRequest.of(
                RequestHeaders.builder(HttpMethod.POST, "/api/catalog/setembedding")
                        .add(AdminTokenGuard.HEADER, token)
                        .build(),
                HttpData.empty());
    }

    private static GatewayRequestForwarder forwarder(AdminTokenGuard guard) {
        return new GatewayRequestForwarder(
                List.of(CATALOG, MODEL, LLM), Duration.ofSeconds(1), Map.of(),
                GatewayRateLimiter.disabled(),
                new UpstreamEndpointGroups.UpstreamClientConfig(false, 0L), null, guard);
    }

    private static void assertDenied404(HttpResponse response) {
        assertNotNull(response, "expected a denial");
        AggregatedHttpResponse aggregated = response.aggregate().join();
        assertEquals(HttpStatus.NOT_FOUND, aggregated.status());
        assertEquals("{\"error\":\"no route found\"}", aggregated.contentUtf8());
    }

    private static GatewayPrincipal apiKey() {
        return GatewayPrincipal.ofApiKey("key-1");
    }

    private static GatewayPrincipal user(String appUserId) {
        return GatewayPrincipal.ofJwt(
                new CognitoJwtVerifier.VerifiedClaims("sub-1", "app-client", "access", appUserId));
    }

    private static AggregatedHttpRequest get() {
        return AggregatedHttpRequest.of(
                RequestHeaders.of(HttpMethod.GET, "/api/catalog/metrics"), HttpData.empty());
    }

    private static GatewayRequestForwarder forwarder() {
        // Health checking off: this test never intends a network call.
        return new GatewayRequestForwarder(
                List.of(CATALOG, MODEL, LLM), Duration.ofSeconds(1), Map.of(),
                GatewayRateLimiter.disabled(),
                new UpstreamEndpointGroups.UpstreamClientConfig(false, 0L), null);
    }
}
