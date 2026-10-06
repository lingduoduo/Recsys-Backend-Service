package com.recsys.application.gateway;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Server;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How the gateway's upstream clients spread load across the pods behind one Service address. A
 * per-connection round-robin TCP proxy stands in for kube-proxy, which picks a backend pod once per
 * connection: without connection recycling Armeria multiplexes every request onto one HTTP/2
 * connection, so a single pod takes all of it — including after scale-out.
 */
class UpstreamSelectionTest {

    private final List<AutoCloseable> resources = new ArrayList<>();

    @AfterEach
    void closeAll() throws Exception {
        // Each Armeria server takes ~2 s to stop, so stop the pods together rather than one by one.
        List<CompletableFuture<?>> podStops = new ArrayList<>();
        for (int i = resources.size() - 1; i >= 0; i--) {
            if (resources.get(i) instanceof Pod pod) {
                podStops.add(pod.server.stop());
            } else {
                resources.get(i).close();
            }
        }
        CompletableFuture.allOf(podStops.toArray(CompletableFuture<?>[]::new)).join();
    }

    /** A backend pod: counts /work requests, serves a togglable GET /health. */
    private static final class Pod implements AutoCloseable {
        final AtomicInteger requests = new AtomicInteger();
        final AtomicInteger probes = new AtomicInteger();
        final AtomicBoolean healthy = new AtomicBoolean(true);
        final Server server = Server.builder().http(0)
                .service("/health", (ctx, req) -> {
                    probes.incrementAndGet();
                    return HttpResponse.of(healthy.get() ? HttpStatus.OK : HttpStatus.SERVICE_UNAVAILABLE);
                })
                .service("/work", (ctx, req) -> {
                    requests.incrementAndGet();
                    return HttpResponse.of("ok");
                })
                .build();

        Pod() {
            server.start().join();
        }

        int port() {
            return server.activeLocalPort();
        }

        @Override
        public void close() {
            server.stop().join();
        }
    }

    /** kube-proxy stand-in: each accepted TCP connection goes to the next pod, round-robin. */
    private static final class PerConnectionProxy implements AutoCloseable {
        final ServerSocket socket;
        final List<Pod> pods;
        final AtomicInteger next = new AtomicInteger();

        PerConnectionProxy(List<Pod> pods) throws IOException {
            this.pods = pods;
            this.socket = new ServerSocket(0);
            Thread acceptor = new Thread(this::acceptLoop, "per-connection-proxy");
            acceptor.setDaemon(true);
            acceptor.start();
        }

        private void acceptLoop() {
            while (!socket.isClosed()) {
                try {
                    Socket client = socket.accept();
                    Pod pod = pods.get(Math.floorMod(next.getAndIncrement(), pods.size()));
                    Socket upstream = new Socket();
                    upstream.connect(new InetSocketAddress("127.0.0.1", pod.port()));
                    pipe(client, upstream);
                    pipe(upstream, client);
                } catch (IOException ignored) {
                    // socket closed at test end, or a pod already stopped
                }
            }
        }

        private static void pipe(Socket from, Socket to) {
            Thread t = new Thread(() -> {
                try (InputStream in = from.getInputStream(); OutputStream out = to.getOutputStream()) {
                    in.transferTo(out);
                } catch (IOException ignored) {
                    // peer closed
                } finally {
                    closeQuietly(from);
                    closeQuietly(to);
                }
            }, "per-connection-proxy-pipe");
            t.setDaemon(true);
            t.start();
        }

        private static void closeQuietly(Socket s) {
            try {
                s.close();
            } catch (IOException ignored) {
                // already closed
            }
        }

        int port() {
            return socket.getLocalPort();
        }

        @Override
        public void close() throws IOException {
            socket.close();
        }
    }

    private List<Pod> pods(int n) {
        List<Pod> pods = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Pod pod = new Pod();
            resources.add(pod);
            pods.add(pod);
        }
        return pods;
    }

    private WebClient client(int port, UpstreamEndpointGroups.UpstreamClientConfig config) {
        ClientFactory factory = config.newClientFactory();
        resources.add(factory);
        MicroserviceRoute route = new MicroserviceRoute("catalog", "/api/catalog", "CATALOG_SERVICE_URL",
                URI.create("http://127.0.0.1:" + port), "/health");
        UpstreamEndpointGroups groups = UpstreamEndpointGroups.create(
                List.of(route), Duration.ofSeconds(2), null, config, factory);
        resources.add(groups);
        return groups.clientFor("catalog");
    }

    /** Sends {@code concurrency} requests at once and waits for all of them; returns the non-200 count. */
    private static int burst(WebClient client, int concurrency) {
        AtomicInteger failures = new AtomicInteger();
        CompletableFuture<?>[] inFlight = new CompletableFuture<?>[concurrency];
        for (int i = 0; i < concurrency; i++) {
            inFlight[i] = client.get("/work").aggregate().handle((AggregatedHttpResponse res, Throwable err) -> {
                if (err != null || res.status() != HttpStatus.OK) {
                    failures.incrementAndGet();
                }
                return null;
            });
        }
        CompletableFuture.allOf(inFlight).join();
        return failures.get();
    }

    private static long podsServing(List<Pod> pods) {
        return pods.stream().filter(p -> p.requests.get() > 0).count();
    }

    @Test
    void recyclingSpreadsRequestsAcrossEveryPod() throws Exception {
        List<Pod> pods = pods(4);
        PerConnectionProxy proxy = new PerConnectionProxy(pods);
        resources.add(proxy);
        WebClient client = client(proxy.port(), new UpstreamEndpointGroups.UpstreamClientConfig(true, 200, 1000));

        int failures = 0;
        long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
        while (podsServing(pods) < pods.size() && System.nanoTime() < deadline) {
            failures += burst(client, 32);
            Thread.sleep(20);
        }

        assertThat(podsServing(pods)).as("pods that received requests").isEqualTo(pods.size());
        assertThat(failures).as("recycling must not fail requests").isZero();
    }

    @Test
    void withoutRecyclingOnePodTakesEverything() throws Exception {
        List<Pod> pods = pods(4);
        PerConnectionProxy proxy = new PerConnectionProxy(pods);
        resources.add(proxy);
        WebClient client = client(proxy.port(), new UpstreamEndpointGroups.UpstreamClientConfig(true, 200, 0));

        // Run well past the 1 s age the recycling test uses, so recycling would have spread these.
        long until = System.nanoTime() + Duration.ofSeconds(3).toNanos();
        while (System.nanoTime() < until) {
            assertThat(burst(client, 32)).isZero();
            Thread.sleep(20);
        }

        assertThat(podsServing(pods)).as("pods that received requests").isEqualTo(1);
    }
}
