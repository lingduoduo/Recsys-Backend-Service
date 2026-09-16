package com.recsys.application.gateway;

import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.*;
import com.linecorp.armeria.server.Server;
import com.recsys.ratelimit.GatewayRateLimiter;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Opt-in local comparison of full response buffering versus streaming, not a production capacity test. */
@Tag("load")
class GatewayStreamingBenchmarkTest {
    private record Sample(double firstByteMs, double completeMs) {}

    @Test
    void compareBufferedAndStreamingResponses() throws Exception {
        byte[] tail = new byte[256 * 1024];
        Arrays.fill(tail, (byte) 'x');
        Server upstream = Server.builder().http(0).service("/item", (ctx, req) -> {
            HttpResponseWriter writer = HttpResponse.streaming();
            writer.write(ResponseHeaders.of(HttpStatus.OK));
            writer.write(HttpData.ofUtf8("x"));
            ctx.eventLoop().schedule(() -> {
                writer.write(HttpData.wrap(tail));
                writer.close();
            }, 100, TimeUnit.MILLISECONDS);
            return writer;
        }).build();
        upstream.start().join();
        MicroserviceRoute route = new MicroserviceRoute("catalog", "/api/catalog", "UNUSED",
                URI.create("http://127.0.0.1:" + upstream.activeLocalPort()), "/health",
                "recsys-catalog-serving");
        try (GatewayRequestForwarder forwarder = new GatewayRequestForwarder(List.of(route),
                Duration.ofSeconds(10), Map.of(), GatewayRateLimiter.disabled(),
                new UpstreamEndpointGroups.HealthCheckConfig(false, 1000))) {
            GatewayProxyService proxy = new GatewayProxyService(List.of(route), forwarder,
                    GatewayAuthenticator.disabled());
            Server gateway = Server.builder().http(0).service("prefix:/", (ctx, req) -> {
                HttpResponse streamed = proxy.serve(ctx, req);
                // Reintroduce the former whole-body buffering boundary for the control.
                return "true".equals(req.headers().get("x-benchmark-buffer"))
                        ? HttpResponse.of(streamed.aggregate().thenApply(AggregatedHttpResponse::toHttpResponse))
                        : streamed;
            }).build();
            gateway.start().join();
            try {
                WebClient client = WebClient.of("http://127.0.0.1:" + gateway.activeLocalPort());
                for (int warmup = 0; warmup < 6; warmup++) {
                    batch(client, true, 32);
                    batch(client, false, 32);
                }
                List<Sample> buffered = new ArrayList<>();
                List<Sample> streaming = new ArrayList<>();
                double[] seconds = new double[2];
                for (int round = 0; round < 6; round++) {
                    // Alternate order to reduce systematic warm-up bias.
                    for (int pass = 0; pass < 2; pass++) {
                        boolean buffer = (round + pass) % 2 == 0;
                        long start = System.nanoTime();
                        List<Sample> samples = batch(client, buffer, 32);
                        seconds[buffer ? 0 : 1] += (System.nanoTime() - start) / 1e9;
                        (buffer ? buffered : streaming).addAll(samples);
                    }
                }
                report("buffered", buffered, seconds[0]);
                report("streaming", streaming, seconds[1]);
            } finally {
                gateway.stop().join();
            }
        } finally {
            upstream.stop().join();
        }
    }

    private static List<Sample> batch(WebClient client, boolean buffer, int concurrency) throws Exception {
        List<CompletableFuture<Sample>> results = new ArrayList<>();
        for (int i = 0; i < concurrency; i++) {
            long start = System.nanoTime();
            CompletableFuture<Sample> result = new CompletableFuture<>();
            results.add(result);
            client.execute(RequestHeaders.builder(HttpMethod.GET, "/api/catalog/item")
                    .set("x-benchmark-buffer", Boolean.toString(buffer)).build())
                    .subscribe(new Subscriber<HttpObject>() {
                        private long first;
                        private int bytes;
                        public void onSubscribe(Subscription subscription) { subscription.request(Long.MAX_VALUE); }
                        public void onNext(HttpObject obj) {
                            if (obj instanceof HttpData data) {
                                if (first == 0) first = System.nanoTime();
                                bytes += data.length();
                            }
                        }
                        public void onError(Throwable cause) { result.completeExceptionally(cause); }
                        public void onComplete() {
                            if (bytes != 262145) {
                                result.completeExceptionally(new AssertionError("Incomplete body: " + bytes));
                            } else {
                                result.complete(new Sample((first - start) / 1e6,
                                        (System.nanoTime() - start) / 1e6));
                            }
                        }
                    });
        }
        CompletableFuture.allOf(results.toArray(CompletableFuture[]::new)).get(15, TimeUnit.SECONDS);
        assertEquals(concurrency, results.size());
        return results.stream().map(CompletableFuture::join).toList();
    }

    private static void report(String name, List<Sample> samples, double seconds) {
        double[] first = samples.stream().mapToDouble(Sample::firstByteMs).sorted().toArray();
        double[] end = samples.stream().mapToDouble(Sample::completeMs).sorted().toArray();
        System.out.printf(java.util.Locale.ROOT,
                "GATEWAY_BENCH %s n=%d concurrency=32 first_p50_ms=%.2f first_p95_ms=%.2f "
                        + "complete_p50_ms=%.2f complete_p95_ms=%.2f completed_rps=%.2f%n",
                name, first.length, first[first.length / 2], first[(int) (first.length * .95)],
                end[end.length / 2], end[(int) (end.length * .95)], samples.size() / seconds);
    }
}
