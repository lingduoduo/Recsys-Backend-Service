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
     * The authenticator is <b>not</b> on the probe's path, and this test does not claim it is.
     * {@code GatewayAuthenticator.check} is called from exactly three request-handling services —
     * {@code GatewayProxyService}, {@code RecommendationGatewayService}, {@code LlmProxyService} —
     * and never as a server-wide decorator, so an exact route registered on the {@code
     * ServerBuilder} bypasses it entirely. The gate that <i>does</i> see every request is the
     * origin-secret decorator; {@link #livenessPathIsExemptFromTheOriginSecret} covers that one.
     *
     * <p>What this pins is the fallback. {@code GatewayProxyService.serve} calls {@code check}
     * <i>before</i> {@code routeTable.match}, so if the exact route were ever removed and the
     * request fell through to the catch-all, the authenticator would be reached — and in the EKS
     * overlays, where GATEWAY_ALLOW_ANONYMOUS is false, a non-public path would 401 rather than
     * reach the 404 that {@link #exactLivenessRouteWinsOverTheCatchAllProxy} describes.
     * "/health/live" is public under the deployed config only because {@code matchesPrefix} uses
     * prefix-with-boundary, so the "/health" entry covers it.
     */
    @Test
    void livenessPathWouldStayPublicIfItEverFellThroughToTheCatchAll() {
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
     * The gateway registers a {@code prefix:/} catch-all, so the exact route has to win or the
     * probe is answered by something else entirely. In production that something else is a 404:
     * {@code GatewayProxyService.serve} calls {@code routeTable.match("/health/live")}, every
     * route prefix is {@code /api/...}, so it returns null and the service answers
     * {@code 404 "no route found"} — it does <b>not</b> proxy the probe to an upstream. Either
     * way the probe fails and every pod restarts on a loop; only the status code differs.
     *
     * <p>Armeria is documented to prefer an exact path over a prefix, and {@code /health} has
     * relied on that in production for as long as the catch-all has existed. Measured here
     * anyway, on a real server whose catch-all answers a distinguishable status, because "the
     * framework surely does X" is how the liveness coupling this change fixes got shipped.
     */
    /**
     * The origin-secret decorator is the one server-wide gate the probe really passes through
     * ({@code MicroserviceGatewayServer} registers exactly four: metrics, slow-request logging,
     * this, and API deprecation — and only this one can reject). It is enabled wherever the CDN
     * is, from the {@code recsys-gateway-origin} Secret, and the kubelet reaches the pod
     * directly with no {@code x-origin-secret} header at all.
     *
     * <p>So this is the assertion that keeps the probe alive: {@code isExempt} matches by
     * prefix-with-boundary, which is why "/health/live" inherits the "/health" exemption and
     * needed no config change. Tightening that to exact equality reads as a hardening change,
     * leaves the three existing {@code GatewayOriginSecretTest} cases green — they only cover
     * "/health", "/metrics" and a non-exempt "/healthcheck" — and gives every gateway pod a 403
     * liveness probe, CrashLoopBackOff-ing the sole public entry point with a green CI.
     */
    @Test
    void livenessPathIsExemptFromTheOriginSecret() {
        GatewayOriginSecret secret = GatewayOriginSecret.fromEnvironment(
                name -> "GATEWAY_ORIGIN_SECRET".equals(name) ? "s3cret" : null);
        assertThat(secret.isEnabled())
                .as("the exemption is only meaningful while the gate is on")
                .isTrue();

        RequestHeaders kubeletProbe = RequestHeaders.of(HttpMethod.GET, "/health/live");

        assertThat(secret.isAllowed(kubeletProbe, "/health/live"))
                .as("the kubelet sends no x-origin-secret; a 403 here restarts every pod")
                .isTrue();
        assertThat(secret.isAllowed(kubeletProbe, "/api/catalog/item"))
                .as("the exemption must stay narrow — a data route without the secret is still 403")
                .isFalse();
    }

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
