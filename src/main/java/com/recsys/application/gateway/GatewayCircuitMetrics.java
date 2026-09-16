package com.recsys.application.gateway;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Counts how each route's circuit-breaker permits settle, so an operator can tell *why* a route's
 * circuit is moving. Circuit state alone cannot: a route whose permits mostly settle neutrally is
 * seeing clients hang up, which is invisible to the breaker by design (see
 * {@link GatewayUpstreamResponse}), whereas a rising failure count is the upstream itself.
 *
 * <p>Counters are resolved once per route/outcome pair and cached, so the request path costs a map
 * lookup rather than a registry lookup. Construction is null-tolerant — no registry means no
 * metrics, matching how the rest of the gateway treats an absent {@link MeterRegistry}.
 */
final class GatewayCircuitMetrics {
    static final String METER_NAME = "gateway_circuit_settle_total";

    private record Key(String route, String outcome) {}

    private final MeterRegistry registry;
    private final ConcurrentMap<Key, Counter> counters = new ConcurrentHashMap<>();

    private GatewayCircuitMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    static GatewayCircuitMetrics create(MeterRegistry registry) {
        return registry == null ? null : new GatewayCircuitMetrics(registry);
    }

    void record(String routeName, String outcome) {
        String route = routeName == null ? "unknown" : routeName;
        counters.computeIfAbsent(new Key(route, outcome), key -> Counter.builder(METER_NAME)
                        .description("Gateway upstream circuit-breaker permits, by how each settled")
                        .tag("route", key.route())
                        .tag("outcome", key.outcome())
                        .register(registry))
                .increment();
    }
}
