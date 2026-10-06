package com.recsys.application.gateway;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.recsys.ratelimit.GatewayRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class UserScopeAuthorizationTest {

    private static final MicroserviceRoute CATALOG = new MicroserviceRoute(
            "catalog", "/api/catalog", "CATALOG_SERVICE_URL",
            URI.create("http://localhost:6010"), "/health", "recsys-catalog-serving");

    private static final MicroserviceRoute MODEL = new MicroserviceRoute(
            "model", "/api/model", "MODEL_SERVICE_URL",
            URI.create("http://localhost:8080"), "/health/ready", "recsys-model-serving");

    private static final MicroserviceRoute LLM = new MicroserviceRoute(
            "llm", "/api/llm", "LLM_SERVICE_URL", URI.create("http://localhost:11434"), "/api/tags");

    @Test
    void serviceTierIsUnaffected() {
        // The regression that proves "no behavior change today": every real caller is an API key.
        assertNull(forwarder().authorizeUserScope(
                CATALOG, "/getuser?userId=999", get(), GatewayPrincipal.ofApiKey("key-1")));
        assertNull(forwarder().authorizeUserScope(
                CATALOG, "/getuser?userId=999", get(), GatewayPrincipal.anonymous()));
    }

    @Test
    void userTierMatchingItsOwnIdIsAllowed() {
        assertNull(forwarder().authorizeUserScope(
                CATALOG, "/getuser?userId=42", get(), user("42")));
    }

    @Test
    void userTierNamingAnotherUserIsForbidden() {
        HttpResponse denied = forwarder().authorizeUserScope(
                CATALOG, "/getuser?userId=43", get(), user("42"));
        assertNotNull(denied);
        AggregatedHttpResponseAssert.assertForbidden(denied);
    }

    @Test
    void userTierWithNoResolvedClaimIsForbidden() {
        // Fails closed: a claim-name misconfiguration must not read as service-tier freedom.
        assertNotNull(forwarder().authorizeUserScope(
                CATALOG, "/getuser?userId=42", get(), user("")));
    }

    @Test
    void userTierWithNoUserIdInTheRequestIsForbidden() {
        // Authorize before validate: an unidentifiable subject is denied, not forwarded for the
        // backend to 400.
        assertNotNull(forwarder().authorizeUserScope(CATALOG, "/getuser", get(), user("42")));
    }

    @Test
    void bodyRoutesAreCheckedToo() {
        assertNull(forwarder().authorizeUserScope(
                CATALOG, "/v2/recommend", post("{\"userId\":\"42\"}"), user("42")));
        assertNotNull(forwarder().authorizeUserScope(
                CATALOG, "/v2/recommend", post("{\"userId\":\"43\"}"), user("42")));
    }

    @Test
    void idsAreComparedExactlyWithNoNumericNormalization() {
        // Same discipline as cacheKeyIntParam on the CDN-cached routes: one spelling, one identity.
        assertNotNull(forwarder().authorizeUserScope(
                CATALOG, "/getuser?userId=042", get(), user("42")));
    }

    @Test
    void undeclaredRoutesAreUntouched() {
        assertNull(forwarder().authorizeUserScope(
                CATALOG, "/item?id=7", get(), user("42")));
        // A route with no registry service name can never match the table.
        assertNull(forwarder().authorizeUserScope(LLM, "/api/tags", get(), user("42")));
    }

    /**
     * The lookup is exact and never canonicalizes, so a {@code /./} segment misses the table and
     * the check does not fire. Armeria preserves single-dot segments (it rejects only {@code ..});
     * Tomcat, on 8080, collapses them — the two parses diverge, and the divergence is a bypass of
     * every 8080 user-scoped route.
     *
     * <p>This test pins that the <em>lookup</em> is not where the fix lives: it is deliberately
     * still permissive here. What closes the hole is
     * {@link GatewayProxyService#rejectNonCanonicalPath}, which 400s the request at the edge
     * before routing, authorization, rate-limit keying, or the CDN cache key ever sees the
     * non-canonical spelling — see {@link GatewayPathCanonicalizationTest}. Canonicalizing inside
     * the lookup instead would leave all four of those on the uncanonicalized string.
     */
    @Test
    void aDotSegmentDefeatsTheLookup_whichIsWhyTheEdgeRejectsItInstead() {
        // Canonical spelling: denied.
        assertNotNull(forwarder().authorizeUserScope(
                MODEL, "/api/v1/recommend", post("{\"userId\":\"999\"}"), user("42")));
        // Dot-segment spelling: the table misses, so nothing here denies it.
        assertNull(forwarder().authorizeUserScope(
                MODEL, "/api/v1/./recommend", post("{\"userId\":\"999\"}"), user("42")));
    }

    @Test
    void denialsAreCounted() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        GatewayRequestForwarder forwarder = forwarder(registry);

        forwarder.authorizeUserScope(CATALOG, "/getuser?userId=43", get(), user("42"));
        forwarder.authorizeUserScope(CATALOG, "/getuser?userId=44", get(), user("42"));

        assertEquals(2.0, registry.get("gateway_user_scope_rejected_total").counter().count());
    }

    /**
     * Pins the check's <em>position</em> inside {@code forward}: it must run before the
     * circuit-breaker permit is acquired.
     *
     * <p>A HALF_OPEN permit is not merely a token — {@code CircuitBreaker.tryAcquirePermit} claims
     * the single probe slot by setting {@code probeGeneration = generation}, and only
     * recordSuccess/recordFailure release it by bumping the generation. {@code forward} settles the
     * permit exclusively on the upstream-response callbacks, so a 403 returned while holding a probe
     * permit would wedge the route: every later acquisition returns null and the route 503s until
     * the process restarts. Asserting the probe slot is still free after a denial is what makes a
     * refactor that moves the check below the acquisition fail here.
     */
    @Test
    void aDenialNeverConsumesTheCircuitBreakerProbeSlot() {
        // threshold 1, cooldown 0: one failure opens the breaker, and elapsed >= 0 makes it
        // immediately HALF_OPEN — deterministic, no clock injection needed.
        RouteCircuitBreaker cb = new RouteCircuitBreaker(1, 0L);
        cb.recordFailure(cb.tryAcquirePermit());
        assertEquals(RouteCircuitBreaker.State.HALF_OPEN, cb.state());

        GatewayRequestForwarder forwarder = forwarder(null, Map.of(CATALOG.name(), cb));
        AggregatedHttpRequest request = get();
        AggregatedHttpResponseAssert.assertForbidden(forwarder.forward(
                ServiceRequestContext.of(request.toHttpRequest()),
                request, CATALOG, "/getuser?userId=43", user("42")));

        assertNotNull(cb.tryAcquirePermit(),
                "the 403 must be returned before the probe permit is acquired — otherwise the "
                        + "unsettled permit wedges the route open forever");
    }

    /**
     * A route with no registry {@code serviceName} is not an exemption when it points at a backend
     * that declares user-scoped routes. {@code MicroserviceRoute}'s 5-arg constructor defaults
     * {@code serviceName} to null, so without this the check is skipped entirely for any route
     * added that way — silently, and invisibly to {@code BackendRouteCoverageTest}, which scans
     * the backend mains and never the gateway route table.
     */
    @Test
    void anUnnamedRouteTargetingABackendAuthorityIsStillChecked() {
        MicroserviceRoute unnamed = new MicroserviceRoute(
                "shadow", "/api/shadow", "SHADOW_SERVICE_URL",
                URI.create("http://localhost:6010"), "/health");   // serviceName is null

        assertNotNull(forwarder().authorizeUserScope(
                unnamed, "/getuser?userId=43", get(), user("42")));
        assertNull(forwarder().authorizeUserScope(
                unnamed, "/getuser?userId=42", get(), user("42")));
    }

    private static GatewayPrincipal user(String appUserId) {
        return GatewayPrincipal.ofJwt(
                new CognitoJwtVerifier.VerifiedClaims("sub-1", "app-client", "access", appUserId));
    }

    private static AggregatedHttpRequest get() {
        return AggregatedHttpRequest.of(
                RequestHeaders.of(HttpMethod.GET, "/api/catalog/getuser"), HttpData.empty());
    }

    private static AggregatedHttpRequest post(String json) {
        return AggregatedHttpRequest.of(
                RequestHeaders.of(HttpMethod.POST, "/api/recommend"), HttpData.ofUtf8(json));
    }

    private static GatewayRequestForwarder forwarder() {
        return forwarder(null);
    }

    private static GatewayRequestForwarder forwarder(io.micrometer.core.instrument.MeterRegistry registry) {
        return forwarder(registry, Map.of());
    }

    /**
     * Health checking is off: this test never intends a network call, and probing the two dead
     * localhost upstreams costs ~12 s of connection-refused retries per run. Reaching the
     * package-private constructor that accepts a {@link UpstreamEndpointGroups.UpstreamClientConfig}
     * is possible only because this test lives in {@code com.recsys.application.gateway}.
     */
    private static GatewayRequestForwarder forwarder(io.micrometer.core.instrument.MeterRegistry registry,
                                                     Map<String, RouteCircuitBreaker> circuitBreakers) {
        return new GatewayRequestForwarder(
                List.of(CATALOG, MODEL, LLM), Duration.ofSeconds(1), circuitBreakers,
                GatewayRateLimiter.disabled(),
                new UpstreamEndpointGroups.UpstreamClientConfig(false, 0L), registry);
    }

    /** Keeps the status assertion in one place; the body must never echo the requested id. */
    private static final class AggregatedHttpResponseAssert {
        static void assertForbidden(HttpResponse response) {
            var aggregated = response.aggregate().join();
            assertEquals(HttpStatus.FORBIDDEN, aggregated.status());
            assertEquals("{\"error\":\"forbidden: request is not scoped to the authenticated user\"}",
                    aggregated.contentUtf8());
        }
    }
}
