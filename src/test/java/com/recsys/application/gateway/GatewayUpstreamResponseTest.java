package com.recsys.application.gateway;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.*;
import com.linecorp.armeria.common.stream.SubscriptionOption;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.client.endpoint.EmptyEndpointGroupException;
import com.recsys.resilience.RouteCircuitBreaker;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

class GatewayUpstreamResponseTest {
    private static RouteCircuitBreaker probeBreaker() {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(1, 0);
        cb.recordFailure(cb.tryAcquirePermit());
        assertEquals(RouteCircuitBreaker.State.HALF_OPEN, cb.state());
        return cb;
    }

    @Test
    void preservesHeadersBodyAndTrailersAndSettlesSuccess() throws Exception {
        RouteCircuitBreaker cb = probeBreaker();
        HttpResponse upstream = HttpResponse.of(
                ResponseHeaders.builder(HttpStatus.OK).add("x-upstream", "yes").build(),
                HttpData.ofUtf8("body"), HttpHeaders.of("x-checksum", "done"));
        AggregatedHttpResponse result = GatewayUpstreamResponse.relay(upstream, cb,
                cb.tryAcquirePermit(), "catalog").aggregate().get(3, TimeUnit.SECONDS);
        assertEquals("yes", result.headers().get("x-upstream"));
        assertEquals("body", result.contentUtf8());
        assertEquals("done", result.trailers().get("x-checksum"));
        assertEquals(RouteCircuitBreaker.State.CLOSED, cb.state());
    }

    @Test
    void passes5xxThroughAndCountsExactlyOneFailure() throws Exception {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(2, 60000);
        AggregatedHttpResponse result = GatewayUpstreamResponse.relay(
                HttpResponse.of(HttpStatus.INTERNAL_SERVER_ERROR), cb, cb.tryAcquirePermit(), "catalog")
                .aggregate().get(3, TimeUnit.SECONDS);
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, result.status());
        assertEquals(RouteCircuitBreaker.State.CLOSED, cb.state());
        cb.recordFailure(cb.tryAcquirePermit());
        assertEquals(RouteCircuitBreaker.State.OPEN, cb.state());
    }

    @Test
    void mapsFailureBeforeHeadersTo502WithoutDoubleCounting() throws Exception {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(2, 60000);
        AggregatedHttpResponse result = GatewayUpstreamResponse.relay(
                HttpResponse.ofFailure(new IOException("unreachable")), cb, cb.tryAcquirePermit(), "catalog")
                .aggregate().get(3, TimeUnit.SECONDS);
        assertEquals(HttpStatus.BAD_GATEWAY, result.status());
        assertEquals("no-store", result.headers().get(HttpHeaderNames.CACHE_CONTROL));
        assertEquals(RouteCircuitBreaker.State.CLOSED, cb.state());
        cb.recordFailure(cb.tryAcquirePermit());
        assertEquals(RouteCircuitBreaker.State.OPEN, cb.state());
    }

    @Test
    void mapsNestedNoHealthyEndpointTo503() throws Exception {
        AggregatedHttpResponse result = GatewayUpstreamResponse.relay(HttpResponse.ofFailure(
                new IllegalStateException(EmptyEndpointGroupException.get())), null, null, "catalog")
                .aggregate().get(3, TimeUnit.SECONDS);
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, result.status());
        assertTrue(result.contentUtf8().contains("no healthy endpoint"));
    }

    @Test
    void doesNotSettleSuccessOnHeadersAndDoesNotReplacePartialResponse() throws Exception {
        RouteCircuitBreaker cb = probeBreaker();
        HttpResponseWriter writer = HttpResponse.streaming();
        HttpResponse relay = GatewayUpstreamResponse.relay(writer, cb, cb.tryAcquirePermit(), "catalog");
        CompletableFuture<String> first = new CompletableFuture<>();
        CompletableFuture<Throwable> error = new CompletableFuture<>();
        relay.subscribe(new Subscriber<HttpObject>() {
            public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
            public void onNext(HttpObject obj) {
                if (obj instanceof HttpData data) first.complete(data.toStringUtf8());
            }
            public void onError(Throwable t) { error.complete(t); }
            public void onComplete() { error.completeExceptionally(new AssertionError("must fail")); }
        });
        writer.write(ResponseHeaders.of(HttpStatus.OK));
        writer.write(HttpData.ofUtf8("partial"));
        assertEquals("partial", first.get(3, TimeUnit.SECONDS));
        assertNull(cb.tryAcquirePermit(), "probe stays claimed until completion");
        IOException failure = new IOException("body interrupted");
        writer.close(failure);
        assertSame(failure, error.get(3, TimeUnit.SECONDS));
        assertNotNull(cb.tryAcquirePermit(), "failed probe was released");
    }

    @Test
    void abortBeforeSubscriptionReleasesProbe() throws Exception {
        RouteCircuitBreaker cb = probeBreaker();
        HttpResponseWriter writer = HttpResponse.streaming();
        HttpResponse relay = GatewayUpstreamResponse.relay(writer, cb, cb.tryAcquirePermit(), "catalog");
        relay.abort();
        assertThrows(java.util.concurrent.ExecutionException.class,
                () -> writer.whenComplete().get(3, TimeUnit.SECONDS));
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3))
                .untilAsserted(() -> assertNotNull(cb.tryAcquirePermit(), "aborted probe was released"));
    }

    @Test
    void respectsDemandAndCancelsWithPendingData() throws Exception {
        HttpResponseWriter writer = HttpResponse.streaming();
        HttpResponse relay = GatewayUpstreamResponse.relay(writer, null, null, "catalog");
        var objects = new java.util.concurrent.LinkedBlockingQueue<HttpObject>();
        CompletableFuture<Subscription> subscription = new CompletableFuture<>();
        relay.subscribe(new Subscriber<HttpObject>() {
            public void onSubscribe(Subscription s) { subscription.complete(s); }
            public void onNext(HttpObject obj) { objects.add(obj); }
            public void onError(Throwable t) {}
            public void onComplete() {}
        });
        Subscription demand = subscription.get(3, TimeUnit.SECONDS);
        try {
            writer.write(ResponseHeaders.of(HttpStatus.OK));
            writer.write(HttpData.ofUtf8("first"));
            writer.write(HttpData.ofUtf8("pending"));
            demand.request(1);
            assertInstanceOf(ResponseHeaders.class, objects.poll(3, TimeUnit.SECONDS));
            assertNull(objects.poll(100, TimeUnit.MILLISECONDS), "body requires more demand");
            demand.request(1);
            assertEquals("first", ((HttpData) objects.poll(3, TimeUnit.SECONDS)).toStringUtf8());
            assertNull(objects.poll(100, TimeUnit.MILLISECONDS), "second chunk requires more demand");
        } finally {
            demand.cancel();
        }
        assertThrows(java.util.concurrent.ExecutionException.class,
                () -> writer.whenComplete().get(3, TimeUnit.SECONDS));
    }

    @Test
    void cancellationReachesUpstreamAndReleasesProbe() throws Exception {
        RouteCircuitBreaker cb = probeBreaker();
        HttpResponseWriter writer = HttpResponse.streaming();
        HttpResponse relay = GatewayUpstreamResponse.relay(writer, cb, cb.tryAcquirePermit(), "catalog");
        CompletableFuture<Subscription> subscription = new CompletableFuture<>();
        relay.subscribe(new Subscriber<HttpObject>() {
            public void onSubscribe(Subscription s) { subscription.complete(s); }
            public void onNext(HttpObject obj) { fail("no downstream demand"); }
            public void onError(Throwable t) {}
            public void onComplete() {}
        });
        subscription.get(3, TimeUnit.SECONDS).cancel();
        assertThrows(java.util.concurrent.ExecutionException.class, () -> writer.whenComplete().get(3, TimeUnit.SECONDS));
        assertNotNull(cb.tryAcquirePermit(), "cancelled probe was released");
    }

    /**
     * A caller that hangs up mid-response proves nothing about the upstream. If cancellation
     * counted as a failure, any client could open a route's circuit with five hang-ups — and
     * /api/catalog/item is in the default GATEWAY_PUBLIC_PATHS, so that client need not
     * authenticate. Measured red before the neutral settle: this opened at the fifth cancel.
     */
    @Test
    void cancellationsNeverOpenTheRouteCircuit() throws Exception {
        RouteCircuitBreaker cb = new RouteCircuitBreaker(
                RouteCircuitBreaker.DEFAULT_FAILURE_THRESHOLD, 60000);
        for (int attempt = 1; attempt <= RouteCircuitBreaker.DEFAULT_FAILURE_THRESHOLD * 2; attempt++) {
            HttpResponseWriter writer = HttpResponse.streaming();
            RouteCircuitBreaker.Permit permit = cb.tryAcquirePermit();
            assertNotNull(permit, "breaker stayed closed through cancel #" + attempt);
            HttpResponse relay = GatewayUpstreamResponse.relay(writer, cb, permit, "catalog");
            CompletableFuture<Subscription> subscription = new CompletableFuture<>();
            CompletableFuture<String> chunk = new CompletableFuture<>();
            relay.subscribe(new Subscriber<HttpObject>() {
                public void onSubscribe(Subscription s) { subscription.complete(s); s.request(Long.MAX_VALUE); }
                public void onNext(HttpObject obj) {
                    if (obj instanceof HttpData data) chunk.complete(data.toStringUtf8());
                }
                public void onError(Throwable t) {}
                public void onComplete() {}
            });
            writer.write(ResponseHeaders.of(HttpStatus.OK));
            writer.write(HttpData.ofUtf8("chunk"));
            assertEquals("chunk", chunk.get(3, TimeUnit.SECONDS));
            subscription.get(3, TimeUnit.SECONDS).cancel();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3))
                    .untilAsserted(() -> assertThrows(java.util.concurrent.ExecutionException.class,
                            () -> writer.whenComplete().get(1, TimeUnit.SECONDS)));
            assertEquals(RouteCircuitBreaker.State.CLOSED, cb.state(), "cancel #" + attempt);
        }
    }

    /** A released probe heals nothing either: the breaker stays open and re-probes. */
    @Test
    void releasedProbeNeitherClosesTheBreakerNorLeaksItsSlot() throws Exception {
        RouteCircuitBreaker cb = probeBreaker();
        HttpResponseWriter writer = HttpResponse.streaming();
        HttpResponse relay = GatewayUpstreamResponse.relay(writer, cb, cb.tryAcquirePermit(), "catalog");
        CompletableFuture<Subscription> subscription = new CompletableFuture<>();
        relay.subscribe(new Subscriber<HttpObject>() {
            public void onSubscribe(Subscription s) { subscription.complete(s); }
            public void onNext(HttpObject obj) {}
            public void onError(Throwable t) {}
            public void onComplete() {}
        });
        subscription.get(3, TimeUnit.SECONDS).cancel();
        org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(3))
                .untilAsserted(() -> assertNotNull(cb.tryAcquirePermit(), "probe slot was released"));
        assertNotEquals(RouteCircuitBreaker.State.CLOSED, cb.state(),
                "a cancelled probe must not count as a recovery");
    }

    /**
     * filter() hands pooled buffers straight through, so ownership must reach the final
     * subscriber exactly once. Only a real client response over a real socket produces pooled
     * HttpData; the unpooled HttpData.ofUtf8 used elsewhere in this class cannot exercise it.
     */
    @Test
    void handsPooledBuffersToThePooledSubscriberExactlyOnce() throws Exception {
        byte[] payload = new byte[64 * 1024];
        java.util.Arrays.fill(payload, (byte) 'p');
        Server upstream = Server.builder().http(0)
                .service("/item", (ctx, req) -> HttpResponse.of(HttpStatus.OK,
                        MediaType.OCTET_STREAM, HttpData.wrap(payload)))
                .build();
        upstream.start().join();
        try {
            RouteCircuitBreaker cb = probeBreaker();
            HttpResponse response = WebClient.of("http://127.0.0.1:" + upstream.activeLocalPort())
                    .get("/item");
            HttpResponse relay = GatewayUpstreamResponse.relay(response, cb, cb.tryAcquirePermit(), "catalog");
            CompletableFuture<Integer> bytes = new CompletableFuture<>();
            java.util.concurrent.atomic.AtomicInteger seen = new java.util.concurrent.atomic.AtomicInteger();
            java.util.concurrent.atomic.AtomicBoolean pooled = new java.util.concurrent.atomic.AtomicBoolean();
            relay.subscribe(new Subscriber<HttpObject>() {
                public void onSubscribe(Subscription s) { s.request(Long.MAX_VALUE); }
                public void onNext(HttpObject obj) {
                    if (obj instanceof HttpData data) {
                        if (data.isPooled()) pooled.set(true);
                        seen.addAndGet(data.length());
                        // The pooled subscriber owns the buffer; releasing twice would throw.
                        data.close();
                    }
                }
                public void onError(Throwable t) { bytes.completeExceptionally(t); }
                public void onComplete() { bytes.complete(seen.get()); }
            }, SubscriptionOption.WITH_POOLED_OBJECTS);
            assertEquals(payload.length, bytes.get(10, TimeUnit.SECONDS));
            assertTrue(pooled.get(), "a real client response must deliver pooled buffers");
            assertEquals(RouteCircuitBreaker.State.CLOSED, cb.state());
        } finally {
            upstream.stop().join();
        }
    }
}
