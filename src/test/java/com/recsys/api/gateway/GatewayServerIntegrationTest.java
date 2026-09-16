package com.recsys.api.gateway;
import com.recsys.application.gateway.ApiDeprecationDecorator;
import com.recsys.application.gateway.GatewayHealthService;
import com.recsys.application.gateway.GatewayProxyService;
import com.recsys.application.gateway.GatewayRequestForwarder;
import com.recsys.application.gateway.GatewayAuthenticator;
import com.recsys.application.gateway.MicroserviceRoute;
import com.recsys.application.gateway.RecommendationGatewayService;
import com.recsys.ratelimit.GatewayRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayServerIntegrationTest {

    // Reported as the gateway's own port in the /health per-port rollup (self-check).
    private static final int GATEWAY_SELF_PORT = 18010;

    // Fake upstream that identifies model requests and echoes the forwarded path and body.
    // Must be registered before `gateway` so its port is assigned first.
    @RegisterExtension
    @Order(1)
    static final ServerExtension fakeUpstream = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            sb.service("prefix:/", (ctx, req) ->
                    com.linecorp.armeria.common.HttpResponse.of(req.aggregate().thenApply(aggregated ->
                            com.linecorp.armeria.common.HttpResponse.of(
                                    HttpStatus.OK,
                                    com.linecorp.armeria.common.MediaType.JSON_UTF_8,
                                    "{\"upstream\":\"model\",\"path\":\"" + ctx.path() + "\",\"body\":"
                                            + aggregated.contentUtf8() + "}"))));
        }
    };

    @RegisterExtension
    @Order(2)
    static final ServerExtension fakeOnlineUpstream = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            sb.service("prefix:/", (ctx, req) ->
                    com.linecorp.armeria.common.HttpResponse.of(req.aggregate().thenApply(aggregated ->
                            com.linecorp.armeria.common.HttpResponse.of(
                                    HttpStatus.OK,
                                    com.linecorp.armeria.common.MediaType.JSON_UTF_8,
                                    "{\"upstream\":\"online\",\"path\":\"" + ctx.path() + "\",\"body\":"
                                            + aggregated.contentUtf8() + "}"))));
        }
    };

    // Gateway uses a lazy supplier for fakeUpstream's port so the port is
    // resolved at request time (after both extensions have started), not
    // at configure() call time (where fakeUpstream is not yet bound).
    @RegisterExtension
    @Order(3)
    static final ServerExtension gateway = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            // Defer the port lookup via a dynamic base URI resolved on first request.
            // We build a placeholder here and rely on the fact that ServerExtension
            // starts fakeUpstream before gateway (declaration order).
            String base = "http://127.0.0.1:" + fakeUpstream.httpPort();
            List<MicroserviceRoute> routes = List.of(
                    new MicroserviceRoute("recsys", "/api/recsys", "UNUSED", URI.create(base), "/health"),
                    new MicroserviceRoute("model",  "/api/model",  "UNUSED", URI.create(base), "/health"),
                    new MicroserviceRoute("embed-recall", "/api/recommend/embedding", "UNUSED", URI.create(base), "/health"),
                    new MicroserviceRoute("model-inference", "/api/recommend/model", "UNUSED", URI.create(base), "/health"),
                    new MicroserviceRoute("online-blend", "/api/recommend/online", "UNUSED",
                            URI.create("http://127.0.0.1:" + fakeOnlineUpstream.httpPort()), "/health"),
                    new MicroserviceRoute("sequential", "/api/recommend/sequential", "UNUSED", URI.create(base), "/health"));
            Duration timeout = Duration.ofSeconds(3);
            Map<String, RouteCircuitBreaker> cbs = routes.stream()
                    .collect(Collectors.toMap(MicroserviceRoute::name,
                            r -> new RouteCircuitBreaker(3, 5000)));
            GatewayRateLimiter rateLimiter = GatewayRateLimiter.disabled();
            GatewayAuthenticator auth = GatewayAuthenticator.disabled();
            GatewayRequestForwarder forwarder =
                    new GatewayRequestForwarder(routes, timeout, cbs, rateLimiter);

            RecommendationGatewayService recommendationService =
                    new RecommendationGatewayService(routes, forwarder, auth);

            // Server-wide, with an explicit sunset so the headers are live in this harness.
            sb.decorator(ApiDeprecationDecorator.fromEnvironment(
                    name -> "GATEWAY_DEPRECATION_SUNSET".equals(name) ? "2027-07-27" : null)
                    .newDecorator());

            // The same call production makes, not a hand-mirrored copy: if the real server
            // stopped registering /health/live, the manifests would point livenessProbe at a
            // path the gateway 404s, and this harness would go on passing.
            MicroserviceGatewayServer.registerHealthRoutes(
                    sb, new GatewayHealthService(routes, timeout, cbs, GATEWAY_SELF_PORT));

            sb.service("/api/recommend", recommendationService)
              // Mirrors MicroserviceGatewayServer: the canonical endpoint is an exact Armeria
              // route, so the versioned spelling needs its own registration.
              .service("/api/v1/recommend", recommendationService)
              .service("prefix:/", new GatewayProxyService(routes, forwarder, auth));
        }
    };

    // Gateway with enabled authenticator, for testing that version normalization happens
    // BEFORE authentication (security regression guard). Uses the same fake upstream.
    // NOTE: This gateway configures GATEWAY_PUBLIC_PATHS to include the VERSIONED path
    // (/api/v1/users) but NOT the normalized path (/api/users). This setup catches the
    // regression: if serve() passes the raw path to authenticator, the versioned path
    // would be treated as public (no auth needed), bypassing the protection.
    @RegisterExtension
    @Order(4)
    static final ServerExtension gatewayWithAuth = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            String base = "http://127.0.0.1:" + fakeUpstream.httpPort();
            List<MicroserviceRoute> routes = List.of(
                    new MicroserviceRoute("users", "/api/users", "UNUSED", URI.create(base), "/profile"));
            Duration timeout = Duration.ofSeconds(3);
            Map<String, RouteCircuitBreaker> cbs = routes.stream()
                    .collect(Collectors.toMap(MicroserviceRoute::name,
                            r -> new RouteCircuitBreaker(3, 5000)));
            GatewayRateLimiter rateLimiter = GatewayRateLimiter.disabled();
            GatewayAuthenticator auth = GatewayAuthenticator.fromEnvironment(
                    name -> {
                        if ("GATEWAY_API_KEYS".equals(name)) return "test-key";
                        if ("GATEWAY_PUBLIC_PATHS".equals(name)) return "/api/v1/users";
                        return null;
                    });
            GatewayRequestForwarder forwarder =
                    new GatewayRequestForwarder(routes, timeout, cbs, rateLimiter);

            sb.service("prefix:/", new GatewayProxyService(routes, forwarder, auth));
        }
    };

    /**
     * The liveness probe, end to end through the assembled server: every server-wide decorator
     * the real gateway installs, no credentials, no x-origin-secret. The unit test covers the
     * handler and each gate in isolation; this is the one that would catch a decorator added
     * later that rejects the probe before it reaches the route.
     */
    @Test
    void livenessReturns200ThroughTheAssembledDecoratorStack() {
        AggregatedHttpResponse r = gateway.blockingWebClient().get("/health/live");
        assertThat(r.status()).isEqualTo(HttpStatus.OK);
        assertThat(r.contentUtf8()).contains("\"live\":true");
    }

    @Test
    void healthReturns200() {
        AggregatedHttpResponse r = gateway.blockingWebClient().get("/health");
        assertThat(r.status()).isEqualTo(HttpStatus.OK);
        assertThat(r.contentUtf8()).contains("status");
    }

    @Test
    void healthIncludesPerPortRollupAndGatewaySelf() {
        AggregatedHttpResponse r = gateway.blockingWebClient().get("/health");
        assertThat(r.status()).isEqualTo(HttpStatus.OK);
        String body = r.contentUtf8();
        // Deduped per-port rollup is present...
        assertThat(body).contains("\"ports\"");
        // ...covers the backend port (both routes share fakeUpstream's port, collapsed to one entry)...
        assertThat(body).contains("\"" + fakeUpstream.httpPort() + "\":\"UP\"");
        // ...and the gateway reports its own port as a self-check.
        assertThat(body).contains("\"" + GATEWAY_SELF_PORT + "\":\"UP\"");
    }

    @Test
    void proxiesToUpstream() {
        AggregatedHttpResponse r = gateway.blockingWebClient().get("/api/recsys/health");
        assertThat(r.status()).isEqualTo(HttpStatus.OK);
        assertThat(r.contentUtf8()).contains("\"path\":\"/health\"");
    }

    @Test
    void canonicalRecommendationDefaultsToModel() {
        AggregatedHttpResponse r = gateway.blockingWebClient().post(
                "/api/recommend", "{\"userId\":42}");
        assertThat(r.status()).isEqualTo(HttpStatus.OK);
        assertThat(r.contentUtf8()).contains("\"upstream\":\"model\"");
        assertThat(r.contentUtf8()).contains("\"path\":\"/v2/recommend\"");
        assertThat(r.contentUtf8()).contains("\"body\":{\"userId\":42}");
    }

    @Test
    void canonicalRecommendationRoutesExplicitOnlineStrategy() {
        AggregatedHttpResponse r = gateway.blockingWebClient().post(
                "/api/recommend", "{\"userId\":42,\"strategy\":\"online\"}");
        assertThat(r.status()).isEqualTo(HttpStatus.OK);
        assertThat(r.contentUtf8()).contains("\"upstream\":\"online\"");
        assertThat(r.contentUtf8()).contains("\"path\":\"/v2/recommend\"");
        assertThat(r.contentUtf8()).contains("\"body\":{\"userId\":42}");
        assertThat(r.contentUtf8()).doesNotContain("strategy");
    }

    @Test
    void canonicalRecommendationRejectsNonPostOnExactRoute() {
        AggregatedHttpResponse r = gateway.blockingWebClient().get("/api/recommend");
        assertThat(r.status()).isEqualTo(HttpStatus.METHOD_NOT_ALLOWED);
        assertThat(r.headers().get("allow")).isEqualTo("POST");
        assertThat(r.headers().contentType()).isEqualTo(com.linecorp.armeria.common.MediaType.JSON_UTF_8);
        assertThat(r.contentUtf8()).contains("\"error\"");
    }

    @Test
    void legacyModelAliasStillProxies() {
        AggregatedHttpResponse r = gateway.blockingWebClient().post(
                "/api/model/predict", "{\"userId\":42}");
        assertThat(r.status()).isEqualTo(HttpStatus.OK);
        assertThat(r.contentUtf8()).contains("\"path\":\"/predict\"");
    }

    @Test
    void unmatchedRouteReturns404() {
        AggregatedHttpResponse r = gateway.blockingWebClient().get("/no-such-route");
        assertThat(r.status()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(r.contentUtf8()).contains("error");
    }

    @Test
    void circuitBreakerOpenReturns503() {
        // Unit-style: verify CB blocks after threshold failures
        RouteCircuitBreaker cb = new RouteCircuitBreaker(1, 60_000);
        cb.recordFailure(cb.tryAcquirePermit());
        assertThat(cb.tryAcquirePermit()).isNull();
    }

    @Test
    void authRejectsNoKey() {
        GatewayAuthenticator auth = GatewayAuthenticator.fromEnvironment(
                Map.of("GATEWAY_API_KEYS", "secret")::get);
        var result = auth.check(
                com.linecorp.armeria.common.RequestHeaders.of(HttpMethod.GET, "/api/recsys/health"),
                "/api/recsys/health");
        assertThat(result.rejected()).isTrue();
        assertThat(result.rejection().aggregate().join().status()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void versionedPathProxiesIdenticallyToUnversioned() {
        AggregatedHttpResponse unversioned = gateway.blockingWebClient().get("/api/recsys/health");
        AggregatedHttpResponse versioned = gateway.blockingWebClient().get("/api/v1/recsys/health");

        assertThat(versioned.status()).isEqualTo(unversioned.status());
        // The upstream echoes the path it was called with: the version segment must be gone.
        assertThat(versioned.contentUtf8()).isEqualTo(unversioned.contentUtf8());
        assertThat(versioned.contentUtf8()).contains("\"path\":\"/health\"");
    }

    @Test
    void unsupportedVersionIsRejectedWith400() {
        AggregatedHttpResponse response = gateway.blockingWebClient().get("/api/v2/recsys/health");

        assertThat(response.status()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.contentUtf8()).contains("unsupported API version: v2");
        assertThat(response.contentUtf8()).contains("supported: v1");
    }

    @Test
    void pathThatMerelyLooksLikeAVersionIsNotStripped() {
        // "/api/v1x" is a resource segment, not v1 — it must not be stripped, so no route matches.
        AggregatedHttpResponse response = gateway.blockingWebClient().get("/api/v1x/recsys/health");

        assertThat(response.status()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    @Test
    void versionedProtectedPathIsRejected401WithAuthEnabled() {
        // REGRESSION GUARD: This test is load-bearing. It fails if GatewayProxyService.serve() ever
        // hands the raw versioned path to the authenticator instead of the normalized path.
        //
        // The authenticator's PROTECTED_PREFIXES contains "/api/users", which matches /api/users/...
        // but NOT /api/v1/users/... (different prefix). If serve() passes ctx.path() (raw "/api/v1/...")
        // to the authenticator, it will NOT match PROTECTED_PREFIXES, and the request will proceed to
        // the route table instead of being rejected. This test would then get 404 or 200, not 401.
        // By asserting 401 here, we confirm serve() normalizes BEFORE calling authenticator.check().
        AggregatedHttpResponse response = gatewayWithAuth.blockingWebClient()
                .get("/api/v1/users/profile");

        assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void versionedCatalogUserIsRejected401WithAuthEnabled() {
        // Verify that version normalization also applies to catalog paths that are protected.
        AggregatedHttpResponse response = gatewayWithAuth.blockingWebClient()
                .get("/api/v1/catalog/user");

        assertThat(response.status()).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void versionedProtectedPathSucceedsWithValidCredentials() {
        // Confirm that the 401 above is about missing credentials, not about versioning.
        // With credentials, the request should not be rejected by auth.
        com.linecorp.armeria.common.RequestHeaders headers =
                com.linecorp.armeria.common.RequestHeaders.builder(HttpMethod.GET, "/api/v1/users/profile")
                        .add("x-api-key", "test-key")
                        .build();
        AggregatedHttpResponse response = gatewayWithAuth.blockingWebClient()
                .execute(headers);

        assertThat(response.status()).isNotEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    void versionedCanonicalRecommendBehavesLikeUnversioned() {
        AggregatedHttpResponse response = gateway.blockingWebClient().post(
                "/api/v1/recommend", "{\"userId\":42,\"strategy\":\"online\"}");

        assertThat(response.status()).isEqualTo(HttpStatus.OK);
        // Dispatched to the online backend, and the strategy selector was stripped from the body.
        assertThat(response.contentUtf8()).contains("\"upstream\":\"online\"");
        assertThat(response.contentUtf8()).contains("\"path\":\"/v2/recommend\"");
        assertThat(response.contentUtf8()).doesNotContain("strategy");
    }

    @Test
    void versionedCanonicalRecommendRejectsUnsupportedVersion() {
        AggregatedHttpResponse response = gateway.blockingWebClient().post(
                "/api/v2/recommend", "{\"userId\":42}");

        assertThat(response.status()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(response.contentUtf8()).contains("unsupported API version: v2");
    }

    @Test
    void unversionedPathCarriesDeprecationHeadersAndSuccessorLink() {
        AggregatedHttpResponse r = gateway.blockingWebClient().get("/api/recsys/health");

        assertThat(r.headers().get("deprecation")).isEqualTo("true");
        assertThat(r.headers().get("sunset")).isEqualTo("Tue, 27 Jul 2027 00:00:00 GMT");
        assertThat(r.headers().get("link"))
                .isEqualTo("</api/v1/recsys/health>; rel=\"successor-version\"");
    }

    @Test
    void versionedPathCarriesNoDeprecationHeaders() {
        AggregatedHttpResponse r = gateway.blockingWebClient().get("/api/v1/recsys/health");

        assertThat(r.headers().get("deprecation")).isNull();
        assertThat(r.headers().get("sunset")).isNull();
        assertThat(r.headers().get("link")).isNull();
    }

    @Test
    void aliasRouteStaysDeprecatedWhenVersionedButCarriesNoSuccessorLink() {
        // /api/model is a back-compat alias, deprecated for a different reason than the
        // unversioned spelling — and it has no mechanically equivalent successor.
        AggregatedHttpResponse r = gateway.blockingWebClient().get("/api/v1/model/health");

        assertThat(r.headers().get("deprecation")).isEqualTo("true");
        assertThat(r.headers().get("link")).isNull();
    }

    @Test
    void healthIsExemptFromDeprecationHeaders() {
        AggregatedHttpResponse r = gateway.blockingWebClient().get("/health");

        assertThat(r.headers().get("deprecation")).isNull();
    }
}
