package com.recsys.application.gateway;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.*;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.metric.PrometheusExpositionService;
import com.recsys.ratelimit.GatewayRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins {@code gateway_circuit_settle_total}, which exists to answer a question circuit state
 * alone cannot: a route's circuit can be quiet while clients hang up constantly, because
 * cancellation settles neutrally by design. See {@link GatewayUpstreamResponse}.
 */
class GatewayCircuitMetricsTest {

    private final PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    private final GatewayCircuitMetrics metrics = GatewayCircuitMetrics.create(registry);

    private double count(String route, String outcome) {
        var counter = registry.find(GatewayCircuitMetrics.METER_NAME)
                .tag("route", route).tag("outcome", outcome).counter();
        return counter == null ? 0d : counter.count();
    }

    private void assertOnly(String route, String outcome) {
        for (String other : new String[] {"success", "failure", "neutral"}) {
            assertThat(count(route, other))
                    .as("route=%s outcome=%s", route, other)
                    .isEqualTo(other.equals(outcome) ? 1d : 0d);
        }
    }

    @Test
    void countsACompletedNonServerErrorResponseAsSuccess() throws Exception {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(2, 60000);
        GatewayUpstreamResponse.relay(HttpResponse.of(HttpStatus.OK), cb, cb.tryAcquirePermit(),
                "catalog", metrics).aggregate().get(3, TimeUnit.SECONDS);

        assertOnly("catalog", "success");
    }

    @Test
    void countsAnUpstreamServerErrorAsFailure() throws Exception {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(2, 60000);
        GatewayUpstreamResponse.relay(HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR), cb,
                cb.tryAcquirePermit(), "online", metrics).aggregate().get(3, TimeUnit.SECONDS);

        assertOnly("online", "failure");
    }

    @Test
    void countsAFailureBeforeHeadersAsFailure() throws Exception {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(2, 60000);
        GatewayUpstreamResponse.relay(HttpResponse.ofFailure(new IOException("unreachable")), cb,
                cb.tryAcquirePermit(), "model", metrics).aggregate().get(3, TimeUnit.SECONDS);

        assertOnly("model", "failure");
    }

    @Test
    void countsAStreamErrorAfterHeadersAsFailure() throws Exception {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(2, 60000);
        HttpResponseWriter writer = HttpResponse.streaming();
        HttpResponse relay = GatewayUpstreamResponse.relay(writer, cb, cb.tryAcquirePermit(),
                "catalog", metrics);
        CompletableFuture<String> chunk = new CompletableFuture<>();
        CompletableFuture<Throwable> error = new CompletableFuture<>();
        relay.subscribe(new Subscriber<HttpObject>() {
            public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
            public void onNext(HttpObject obj) {
                if (obj instanceof HttpData data) chunk.complete(data.toStringUtf8());
            }
            public void onError(Throwable t) { error.complete(t); }
            public void onComplete() {}
        });
        writer.write(ResponseHeaders.of(HttpStatus.OK));
        writer.write(HttpData.ofUtf8("partial"));
        chunk.get(3, TimeUnit.SECONDS);
        writer.close(new IOException("body interrupted"));
        error.get(3, TimeUnit.SECONDS);

        assertOnly("catalog", "failure");
    }

    /** The point of the whole meter: a hang-up is visible here and nowhere else. */
    @Test
    void countsADownstreamCancellationAsNeutral() throws Exception {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(2, 60000);
        HttpResponseWriter writer = HttpResponse.streaming();
        HttpResponse relay = GatewayUpstreamResponse.relay(writer, cb, cb.tryAcquirePermit(),
                "catalog", metrics);
        CompletableFuture<Subscription> subscription = new CompletableFuture<>();
        relay.subscribe(new Subscriber<HttpObject>() {
            public void onSubscribe(Subscription s) { subscription.complete(s); }
            public void onNext(HttpObject obj) {}
            public void onError(Throwable t) {}
            public void onComplete() {}
        });
        subscription.get(3, TimeUnit.SECONDS).cancel();

        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> assertThat(count("catalog", "neutral")).isEqualTo(1d));
        assertOnly("catalog", "neutral");
        assertThat(cb.state()).isEqualTo(RouteCircuitBreaker.State.CLOSED);
    }

    @Test
    void countsAnAbortBeforeSubscriptionAsNeutral() {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(2, 60000);
        HttpResponseWriter writer = HttpResponse.streaming();
        GatewayUpstreamResponse.relay(writer, cb, cb.tryAcquirePermit(), "catalog", metrics).abort();

        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3))
                .untilAsserted(() -> assertOnly("catalog", "neutral"));
    }

    /**
     * A cancellation reaches both {@code onCancellation} and the {@code whenComplete} hook, which
     * race. The counter sits inside the settle CAS so the pair can only produce one increment —
     * otherwise the outcome counts would exceed the permits actually taken.
     */
    @Test
    void countsEachPermitExactlyOnceAcrossManyCancellations() {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(RouteCircuitBreaker.DEFAULT_FAILURE_THRESHOLD, 60000);
        int requests = 20;
        for (int i = 0; i < requests; i++) {
            HttpResponseWriter writer = HttpResponse.streaming();
            HttpResponse relay = GatewayUpstreamResponse.relay(writer, cb, cb.tryAcquirePermit(),
                    "catalog", metrics);
            CompletableFuture<Subscription> subscription = new CompletableFuture<>();
            relay.subscribe(new Subscriber<HttpObject>() {
                public void onSubscribe(Subscription s) { subscription.complete(s); }
                public void onNext(HttpObject obj) {}
                public void onError(Throwable t) {}
                public void onComplete() {}
            });
            writer.write(ResponseHeaders.of(HttpStatus.OK));
            subscription.join().cancel();
        }
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5))
                .untilAsserted(() -> assertThat(count("catalog", "neutral")).isEqualTo(requests));

        assertThat(count("catalog", "success") + count("catalog", "failure"))
                .as("no permit settled twice")
                .isZero();
    }

    /**
     * The meter name Micrometer registers is not the name Prometheus scrapes. Verified against a
     * real exposition rather than reasoned about: Micrometer's Prometheus client appends
     * {@code _total} to counters, but does NOT double a name that already carries the suffix —
     * measured, and the reason this meter is spelled with it, matching the gateway's other
     * counters.
     */
    @Test
    void emitsTheSeriesNameAndLabelsPrometheusScrapes() throws Exception {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(2, 60000);
        GatewayUpstreamResponse.relay(HttpResponse.of(HttpStatus.OK), cb, cb.tryAcquirePermit(),
                "catalog", metrics).aggregate().get(3, TimeUnit.SECONDS);

        assertThat(registry.scrape().lines().filter(l -> !l.startsWith("#")).toList())
                .contains("gateway_circuit_settle_total{outcome=\"success\",route=\"catalog\",} 1.0");
    }

    @Test
    void recordsNothingAndDoesNotFailWhenNoRegistryWasSupplied() throws Exception {
        assertThat(GatewayCircuitMetrics.create(null)).isNull();

        RouteCircuitBreaker cb = new RouteCircuitBreaker(2, 60000);
        AggregatedHttpResponse result = GatewayUpstreamResponse.relay(HttpResponse.of(HttpStatus.OK),
                cb, cb.tryAcquirePermit(), "catalog", null).aggregate().get(3, TimeUnit.SECONDS);

        assertThat(result.status()).isEqualTo(HttpStatus.OK);
        assertThat(registry.find(GatewayCircuitMetrics.METER_NAME).counters()).isEmpty();
    }

    @Test
    void separatesCountsPerRoute() throws Exception {
        RouteCircuitBreaker catalog = new RouteCircuitBreaker(2, 60000);
        RouteCircuitBreaker online = new RouteCircuitBreaker(2, 60000);
        GatewayUpstreamResponse.relay(HttpResponse.of(HttpStatus.OK), catalog,
                catalog.tryAcquirePermit(), "catalog", metrics).aggregate().get(3, TimeUnit.SECONDS);
        GatewayUpstreamResponse.relay(HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR), online,
                online.tryAcquirePermit(), "online", metrics).aggregate().get(3, TimeUnit.SECONDS);

        assertOnly("catalog", "success");
        assertOnly("online", "failure");
    }

    /**
     * An in-process registry assertion is not evidence the series is scrapeable: the forwarder
     * must actually be handed the server's registry, and that registry must be the one
     * {@code /metrics} exposes. This repo has been bitten twice by instrumentation that looked
     * present and was observable by nothing (18_Fault_Tolerance §8.2/§8.4), so the path from a
     * real proxied request to a real scrape is exercised here rather than assumed.
     */
    @Test
    void theSeriesIsScrapeableFromTheGatewaysOwnMetricsEndpoint() throws Exception {
        Server upstream = Server.builder().http(0)
                .service("/item", (ctx, req) -> HttpResponse.of(HttpStatus.OK))
                .build();
        upstream.start().join();
        MicroserviceRoute route = new MicroserviceRoute("catalog", "/api/catalog", "UNUSED",
                URI.create("http://127.0.0.1:" + upstream.activeLocalPort()), "/health",
                "recsys-catalog-serving");
        try (GatewayRequestForwarder forwarder = new GatewayRequestForwarder(List.of(route),
                Duration.ofSeconds(10), Map.of("catalog", new RouteCircuitBreaker(5, 10000)),
                GatewayRateLimiter.disabled(),
                new UpstreamEndpointGroups.HealthCheckConfig(false, 1000), registry)) {
            Server gateway = Server.builder().http(0)
                    .service("/metrics", PrometheusExpositionService.of(registry.getPrometheusRegistry()))
                    .service("prefix:/", new GatewayProxyService(List.of(route), forwarder,
                            GatewayAuthenticator.disabled()))
                    .build();
            gateway.start().join();
            try {
                WebClient client = WebClient.of("http://127.0.0.1:" + gateway.activeLocalPort());
                assertThat(client.get("/api/catalog/item").aggregate().get(10, TimeUnit.SECONDS).status())
                        .isEqualTo(HttpStatus.OK);

                String scrape = client.get("/metrics").aggregate().get(10, TimeUnit.SECONDS).contentUtf8();
                assertThat(scrape.lines().filter(l -> !l.startsWith("#")).toList())
                        .contains("gateway_circuit_settle_total{outcome=\"success\",route=\"catalog\",} 1.0");
            } finally {
                gateway.stop().join();
            }
        } finally {
            upstream.stop().join();
        }
    }
}
