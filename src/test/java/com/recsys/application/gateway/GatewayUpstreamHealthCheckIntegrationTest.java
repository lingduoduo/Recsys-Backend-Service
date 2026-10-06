package com.recsys.application.gateway;

import com.recsys.ratelimit.GatewayRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.server.AbstractHttpService;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.testing.junit5.server.ServerExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises the health-checked endpoint-group wiring end-to-end through {@link GatewayRequestForwarder}:
 * a healthy upstream is selected and forwarded to, while an upstream whose health check never passes is
 * never added to selection so the gateway fast-fails with 503 instead of hanging until the response timeout.
 * The live-flip <em>detection latency</em> is Armeria's concern (≈ the health-check retry interval) and is
 * intentionally not timed here; these assertions are deterministic.
 */
class GatewayUpstreamHealthCheckIntegrationTest {

    @RegisterExtension
    static final ServerExtension healthyUpstream = new ServerExtension() {
        @Override
        protected void configure(ServerBuilder sb) {
            // GET-only, exactly like the production health handlers (BaseApiService subclasses override
            // doGet alone), so a HEAD probe gets 405 here as it does from the catalog and online services.
            // A lambda HttpService would accept any method and hide a probe-method mismatch.
            sb.service("/health", new AbstractHttpService() {
                @Override
                protected HttpResponse doGet(ServiceRequestContext ctx, HttpRequest req) {
                    return HttpResponse.of(HttpStatus.OK);
                }
            });
            sb.service("prefix:/api", (ctx, req) ->
                    HttpResponse.of(HttpStatus.OK, MediaType.JSON_UTF_8, "{\"ok\":true}"));
        }
    };

    private static MicroserviceRoute catalogRoute(int port) {
        return new MicroserviceRoute("catalog", "/api/catalog", "CATALOG_SERVICE_URL",
                URI.create("http://127.0.0.1:" + port), "/health");
    }

    private static GatewayRequestForwarder forwarder(int port) {
        Map<String, RouteCircuitBreaker> cbs = Map.of("catalog", new RouteCircuitBreaker(50, 5000));
        // Short probe interval; health checking enabled.
        return new GatewayRequestForwarder(
                List.of(catalogRoute(port)), Duration.ofSeconds(2), cbs, GatewayRateLimiter.disabled(),
                new UpstreamEndpointGroups.UpstreamClientConfig(true, 200L));
    }

    private static HttpStatus proxyOnce(GatewayRequestForwarder fwd, int port) {
        MicroserviceRoute route = catalogRoute(port);
        ServiceRequestContext ctx = ServiceRequestContext.builder(
                HttpRequest.of(HttpMethod.GET, "/api/catalog/x")).build();
        AggregatedHttpRequest req = AggregatedHttpRequest.of(HttpMethod.GET, "/api/catalog/x");
        return fwd.forward(ctx, req, route, "/api/x", GatewayPrincipal.anonymous())
                .aggregate().join().status();
    }

    private static boolean pollUntilStatus(GatewayRequestForwarder fwd, int port,
                                           HttpStatus want, Duration atMost) {
        long deadline = System.nanoTime() + atMost.toNanos();
        while (System.nanoTime() < deadline) {
            if (proxyOnce(fwd, port) == want) {
                return true;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return proxyOnce(fwd, port) == want;
    }

    @Test
    void healthyUpstreamIsSelectedAndForwarded() {
        int port = healthyUpstream.httpPort();
        GatewayRequestForwarder fwd = forwarder(port);
        try {
            assertThat(pollUntilStatus(fwd, port, HttpStatus.OK, Duration.ofSeconds(5)))
                    .as("healthy upstream should be selected and return 200").isTrue();
        } finally {
            fwd.close();
        }
    }

    @Test
    void unhealthyUpstreamIsDroppedAndFastFailsWith503() {
        // Nothing listens on this port, so the health check never passes: the endpoint stays out of the
        // group and selection fails fast (bounded by the selection timeout) with 503 rather than hanging.
        int deadPort = healthyUpstream.httpPort() + 1;
        GatewayRequestForwarder fwd = forwarder(deadPort);
        try {
            assertThat(proxyOnce(fwd, deadPort))
                    .as("upstream with no healthy endpoint should yield 503")
                    .isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        } finally {
            fwd.close();
        }
    }
}
