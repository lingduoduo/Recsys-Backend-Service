package com.recsys.application.gateway;

import com.linecorp.armeria.client.WebClient;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UpstreamEndpointGroupsTest {

    private static MicroserviceRoute route(String name, String baseUri, String healthPath) {
        return new MicroserviceRoute(name, "/api/" + name, name.toUpperCase() + "_URL",
                URI.create(baseUri), healthPath);
    }

    private static UpstreamEndpointGroups.UpstreamClientConfig cfg(boolean enabled) {
        return new UpstreamEndpointGroups.UpstreamClientConfig(enabled, 10_000L);
    }

    @Test
    void dedupsRoutesSharingHostPortAndHealthPath() {
        List<MicroserviceRoute> routes = List.of(
                route("a", "http://localhost:6010", "/health"),
                route("b", "http://localhost:6010", "/health"),   // same authority + health path as a
                route("c", "http://localhost:8080", "/health/ready"));
        // Dedup happens before any health wrapping, so it is independent of the health-check flag;
        // use the no-probe config to keep the unit test free of background health-check log noise.
        UpstreamEndpointGroups groups = UpstreamEndpointGroups.create(
                routes, Duration.ofSeconds(3), null, cfg(false));
        try {
            // Two unique (host,port,healthPath) keys -> two endpoint groups.
            assertThat(groups.groupCount()).isEqualTo(2);
            // But every route still resolves to its own WebClient.
            assertThat(groups.clientFor("a")).isInstanceOf(WebClient.class);
            assertThat(groups.clientFor("b")).isInstanceOf(WebClient.class);
            assertThat(groups.clientFor("c")).isInstanceOf(WebClient.class);
        } finally {
            groups.close();
        }
    }

    @Test
    void healthCheckDisabledStillBuildsAClientPerRoute() {
        List<MicroserviceRoute> routes = List.of(route("a", "http://localhost:6010", "/health"));
        UpstreamEndpointGroups groups = UpstreamEndpointGroups.create(
                routes, Duration.ofSeconds(3), null, cfg(false));
        try {
            assertThat(groups.groupCount()).isEqualTo(1);
            assertThat(groups.clientFor("a")).isNotNull();
        } finally {
            groups.close();
        }
    }

    @Test
    void closeIsIdempotent() {
        List<MicroserviceRoute> routes = List.of(route("a", "http://localhost:6010", "/health"));
        UpstreamEndpointGroups groups = UpstreamEndpointGroups.create(
                routes, Duration.ofSeconds(3), null, cfg(false));
        groups.close();
        groups.close(); // must not throw
    }

    @Test
    void maxConnectionAgeDefaultsTo30sWhenUnset() {
        assertThat(UpstreamEndpointGroups.UpstreamClientConfig.fromEnvironment(name -> null).maxConnectionAgeMs())
                .isEqualTo(30_000L);
        assertThat(new UpstreamEndpointGroups.UpstreamClientConfig(true, 10_000L).maxConnectionAgeMs())
                .isEqualTo(UpstreamEndpointGroups.UpstreamClientConfig.DEFAULT_MAX_CONNECTION_AGE_MS);
    }

    @Test
    void maxConnectionAgeIsReadFromTheEnvironment() {
        Map<String, String> env = Map.of("GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS", "0");
        assertThat(UpstreamEndpointGroups.UpstreamClientConfig.fromEnvironment(env::get).maxConnectionAgeMs())
                .isZero();
    }

    @Test
    void maxConnectionAgeAcceptsZeroAndAtLeastOneSecond() {
        assertThat(new UpstreamEndpointGroups.UpstreamClientConfig(true, 1000, 0).maxConnectionAgeMs()).isZero();
        assertThat(new UpstreamEndpointGroups.UpstreamClientConfig(true, 1000, 1000).maxConnectionAgeMs())
                .isEqualTo(1000);
    }

    @Test
    void maxConnectionAgeRejectsNegativeAndSubSecondValues() {
        for (long bad : new long[]{-1, 1, 999}) {
            assertThatThrownBy(() -> new UpstreamEndpointGroups.UpstreamClientConfig(true, 1000, bad))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("GATEWAY_UPSTREAM_MAX_CONNECTION_AGE_MS");
        }
    }
}
