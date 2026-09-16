package com.recsys.application.gateway;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.*;
import com.linecorp.armeria.common.metric.MeterIdPrefixFunction;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import com.recsys.infrastructure.observability.SlowRequestLogger;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.recsys.ratelimit.GatewayRateLimiter;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class GatewayResponseStreamingTest {
    @Test
    void forwardsFirstChunkBeforeUpstreamCompletes() throws Exception {
        HttpResponseWriter writer = HttpResponse.streaming();
        Server server = Server.builder().http(0).service("/item", (ctx, req) -> writer).build();
        server.start().join();
        MicroserviceRoute route = new MicroserviceRoute("catalog", "/api/catalog", "UNUSED",
                URI.create("http://127.0.0.1:" + server.activeLocalPort()), "/health",
                "recsys-catalog-serving");
        try (GatewayRequestForwarder forwarder = new GatewayRequestForwarder(List.of(route),
                Duration.ofSeconds(10), Map.of(), GatewayRateLimiter.disabled(),
                new UpstreamEndpointGroups.HealthCheckConfig(false, 1000))) {
            HttpRequest req = HttpRequest.of(HttpMethod.GET, "/api/catalog/item");
            HttpResponse response = forwarder.forward(ServiceRequestContext.of(req),
                    AggregatedHttpRequest.of(HttpMethod.GET, "/api/catalog/item"), route,
                    "/item", GatewayPrincipal.anonymous());
            CompletableFuture<String> first = new CompletableFuture<>();
            response.subscribe(new Subscriber<HttpObject>() {
                public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
                public void onNext(HttpObject obj) {
                    if (obj instanceof HttpData data) first.complete(data.toStringUtf8());
                }
                public void onError(Throwable t) { first.completeExceptionally(t); }
                public void onComplete() {}
            });
            writer.write(ResponseHeaders.of(HttpStatus.OK));
            writer.write(HttpData.ofUtf8("first"));
            try {
                assertEquals("first", first.get(3, TimeUnit.SECONDS));
                assertFalse(response.whenComplete().isDone(), "upstream is still open");
            } finally {
                writer.close();
            }
            response.whenComplete().get(3, TimeUnit.SECONDS);
        } finally {
            writer.close();
            server.stop().join();
        }
    }

    /**
     * The forwarder streaming is necessary but not sufficient: any decorator in the assembled
     * stack that aggregates would silently re-buffer the body and undo this change. This mounts
     * the same server-wide decorators MicroserviceGatewayServer registers around
     * GatewayProxyService and asserts end to end, over a real socket, that the first chunk
     * arrives while the upstream response is still open.
     */
    @Test
    void streamsThroughTheAssembledDecoratorStack() throws Exception {
        HttpResponseWriter writer = HttpResponse.streaming();
        Server upstreamServer = Server.builder().http(0).service("/item", (ctx, req) -> writer).build();
        upstreamServer.start().join();
        MicroserviceRoute route = new MicroserviceRoute("catalog", "/api/catalog", "UNUSED",
                URI.create("http://127.0.0.1:" + upstreamServer.activeLocalPort()), "/health",
                "recsys-catalog-serving");
        try (GatewayRequestForwarder forwarder = new GatewayRequestForwarder(List.of(route),
                Duration.ofSeconds(10), Map.of(), GatewayRateLimiter.disabled(),
                new UpstreamEndpointGroups.HealthCheckConfig(false, 1000))) {
            ApiDeprecationDecorator deprecation = ApiDeprecationDecorator.fromEnvironment(
                    name -> "GATEWAY_DEPRECATION_SUNSET".equals(name) ? "2027-07-27" : null);
            assertTrue(deprecation.isEnabled(), "deprecation decorator must be in the stack");
            Server gateway = Server.builder().http(0)
                    .decorator(MetricCollectingService.newDecorator(
                            MeterIdPrefixFunction.ofDefault("api_gateway")))
                    .decorator(SlowRequestLogger.newDecorator("api-gateway", 1000))
                    .decorator(deprecation.newDecorator())
                    .service("prefix:/", new GatewayProxyService(List.of(route), forwarder,
                            GatewayAuthenticator.disabled()))
                    .build();
            gateway.start().join();
            try {
                CompletableFuture<String> first = new CompletableFuture<>();
                CompletableFuture<Void> done = new CompletableFuture<>();
                WebClient.of("http://127.0.0.1:" + gateway.activeLocalPort())
                        .get("/api/catalog/item")
                        .subscribe(new Subscriber<HttpObject>() {
                            public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
                            public void onNext(HttpObject obj) {
                                if (obj instanceof HttpData data && !data.isEmpty()) {
                                    first.complete(data.toStringUtf8());
                                }
                            }
                            public void onError(Throwable t) {
                                first.completeExceptionally(t);
                                done.completeExceptionally(t);
                            }
                            public void onComplete() { done.complete(null); }
                        });
                writer.write(ResponseHeaders.of(HttpStatus.OK));
                writer.write(HttpData.ofUtf8("first"));
                assertEquals("first", first.get(5, TimeUnit.SECONDS));
                assertFalse(done.isDone(), "the upstream response is still open");
                writer.write(HttpData.ofUtf8("second"));
                writer.close();
                done.get(5, TimeUnit.SECONDS);
            } finally {
                gateway.stop().join();
            }
        } finally {
            writer.close();
            upstreamServer.stop().join();
        }
    }
}
