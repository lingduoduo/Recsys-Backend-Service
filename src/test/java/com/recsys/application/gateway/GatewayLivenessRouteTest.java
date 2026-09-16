package com.recsys.application.gateway;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServiceRequestContext;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Liveness answers "is this process running", and nothing else. The gateway used to answer it
 * with {@link GatewayHealthService}, which returns 503 when any upstream is down — so an upstream
 * outage lasting past 60s made kubelet restart every gateway pod, removing the one component
 * still able to serve the routes whose upstreams were healthy.
 *
 * <p>These tests pin the properties that make the replacement route usable as a probe target.
 */
class GatewayLivenessRouteTest {

    /** A status no handler in this test returns by accident, so "who answered" is unambiguous. */
    private static final HttpStatus CATCH_ALL_MARKER = HttpStatus.valueOf(418);

    private static AggregatedHttpResponse get(String path) {
        GatewayLivenessService svc = new GatewayLivenessService();
        ServiceRequestContext ctx = ServiceRequestContext.builder(
                HttpRequest.of(HttpMethod.GET, path)).build();
        try {
            return svc.serve(ctx, ctx.request()).aggregate().join();
        } catch (Exception e) {
            throw new AssertionError("serving " + path + " threw", e);
        }
    }

    /**
     * No dependency is consulted, so there is nothing to stub: the handler cannot fail for a
     * reason outside this process. That is the whole point of the route.
     */
    @Test
    void livenessIsConstant200() {
        AggregatedHttpResponse resp = get("/health/live");

        assertThat(resp.status()).isEqualTo(HttpStatus.OK);
        assertThat(resp.contentUtf8()).contains("\"live\":true");
    }

    /**
     * The probe reaches the pod with no credentials. In the EKS overlays GATEWAY_ALLOW_ANONYMOUS
     * is false, so a path the authenticator does not treat as public gets 401 — and a liveness
     * probe that 401s restarts the pod on a loop.
     *
     * <p>/health/live is public today only because {@code GatewayAuthenticator} matches public
     * paths by prefix-with-boundary ({@code path.equals(p) || path.startsWith(p + "/")}), so the
     * existing "/health" entry covers it and no config change was needed. That is load-bearing and
     * invisible: tightening the match to exact equality would look like a hardening change and
     * would take down every gateway pod's liveness probe. This is the test that catches it.
     */
    @Test
    void livenessPathIsAnonymouslyReachableUnderTheDeployedPublicPaths() {
        // Exactly the GATEWAY_PUBLIC_PATHS value from k8s/base/configmap.yaml.
        GatewayAuthenticator auth = GatewayAuthenticator.forTesting(
                Set.of("a-real-api-key"),
                Set.of("/health", "/api/catalog/item", "/api/catalog/similar"),
                null);

        RequestHeaders noCredentials = RequestHeaders.of(HttpMethod.GET, "/health/live");

        assertThat(auth.check(noCredentials, "/health/live").rejected())
                .as("the kubelet liveness probe sends no credentials")
                .isFalse();
    }

    /**
     * The gateway registers a {@code prefix:/} catch-all that proxies everything it does not
     * serve itself, so a liveness probe swallowed by it would be answered by an upstream — the
     * exact dependency this route exists to remove, reintroduced invisibly, and looking healthy
     * right up until the upstream is the thing that is down.
     *
     * <p>Armeria is documented to prefer an exact path over a prefix, and {@code /health} has
     * relied on that in production for as long as the catch-all has existed. Measured here
     * anyway, on a real server with a catch-all that answers a distinguishable status, because
     * "the framework surely does X" is how the liveness coupling this change fixes got shipped.
     */
    @Test
    void exactLivenessRouteWinsOverTheCatchAllProxy() {
        Server server = Server.builder()
                .service("prefix:/", (ctx, req) -> HttpResponse.of(CATCH_ALL_MARKER))
                .service("/health/live", new GatewayLivenessService())
                .build();
        server.start().join();
        try {
            AggregatedHttpResponse resp = WebClient.of("http://127.0.0.1:" + server.activeLocalPort())
                    .get("/health/live")
                    .aggregate()
                    .join();

            assertThat(resp.status())
                    .as("the catch-all marker means the probe would have been proxied to an upstream")
                    .isEqualTo(HttpStatus.OK);
            assertThat(resp.contentUtf8()).contains("\"live\":true");
        } finally {
            server.stop().join();
        }
    }
}
