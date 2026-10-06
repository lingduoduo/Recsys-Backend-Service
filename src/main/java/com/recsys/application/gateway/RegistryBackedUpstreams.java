package com.recsys.application.gateway;

import com.recsys.infrastructure.registry.ServiceRegistryProvider;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.HttpClient;
import com.linecorp.armeria.client.WebClient;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Overlays registry-resolved addresses onto {@link UpstreamEndpointGroups}. Holds a current
 * {@code UpstreamEndpointGroups} built from routes whose base URI is the registered address (when
 * present) or the static route address (fallback). When {@link #rebuildIfChanged()} observes a change
 * in the resolved address map it rebuilds the groups, atomically swaps them, and closes the old set —
 * the {@code ShardTopologyProvider} swap pattern applied to endpoint groups.
 */
final class RegistryBackedUpstreams implements java.io.Closeable {

    private final List<MicroserviceRoute> routes;
    private final Duration timeout;
    private final Function<? super HttpClient, ? extends HttpClient> decorator;
    private final UpstreamEndpointGroups.UpstreamClientConfig healthConfig;
    private final ServiceRegistryProvider provider;
    // One factory for this object's lifetime: a rebuild swaps endpoint groups but must not close the
    // connections that requests still in flight on the old groups are using.
    private final ClientFactory clientFactory;

    private volatile Map<String, String> resolvedAddresses;   // routeName -> effective base URI
    private volatile UpstreamEndpointGroups current;
    private volatile boolean closed;

    RegistryBackedUpstreams(List<MicroserviceRoute> routes,
                            Duration timeout,
                            Function<? super HttpClient, ? extends HttpClient> decorator,
                            UpstreamEndpointGroups.UpstreamClientConfig healthConfig,
                            ServiceRegistryProvider provider) {
        this.routes = List.copyOf(routes);
        this.timeout = timeout;
        this.decorator = decorator;
        this.healthConfig = healthConfig;
        this.provider = provider;
        this.clientFactory = healthConfig.newClientFactory();
        this.resolvedAddresses = resolveAddresses();
        this.current = build(this.resolvedAddresses);
    }

    private Map<String, String> resolveAddresses() {
        Map<String, String> resolved = new LinkedHashMap<>();
        for (MicroserviceRoute route : routes) {
            String effective = provider.resolve(route.serviceName())
                    .orElse(route.baseUri().toString());
            resolved.put(route.name(), effective);
        }
        return resolved;
    }

    private UpstreamEndpointGroups build(Map<String, String> resolved) {
        List<MicroserviceRoute> effectiveRoutes = new ArrayList<>(routes.size());
        for (MicroserviceRoute route : routes) {
            URI effective = URI.create(resolved.get(route.name()));
            effectiveRoutes.add(new MicroserviceRoute(route.name(), route.prefix(), route.envVar(),
                    effective, route.healthPath(), route.serviceName()));
        }
        return UpstreamEndpointGroups.create(effectiveRoutes, timeout, decorator, healthConfig, clientFactory);
    }

    WebClient clientFor(String routeName) {
        return current.clientFor(routeName);
    }

    /** The effective base URI in use for a route (registered address, or static fallback). */
    String resolvedBaseUri(String routeName) {
        return resolvedAddresses.get(routeName);
    }

    synchronized void rebuildIfChanged() {
        if (closed) {
            return;
        }
        Map<String, String> next = resolveAddresses();
        if (next.equals(resolvedAddresses)) {
            return;
        }
        UpstreamEndpointGroups rebuilt = build(next);
        UpstreamEndpointGroups old = current;
        this.current = rebuilt;
        this.resolvedAddresses = next;
        old.close();
    }

    @Override
    public synchronized void close() {
        if (closed) {
            return;
        }
        closed = true;
        current.close();
        clientFactory.close();
    }
}
