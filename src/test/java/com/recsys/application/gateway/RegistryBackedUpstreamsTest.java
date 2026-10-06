package com.recsys.application.gateway;

import com.recsys.infrastructure.registry.ServiceRegistryProvider;
import com.recsys.infrastructure.registry.ServiceRegistryStore;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

class RegistryBackedUpstreamsTest {

    private static MicroserviceRoute route() {
        return new MicroserviceRoute("catalog", "/api/catalog", "CATALOG_SERVICE_URL",
                URI.create("http://static-host:6010"), "/health", "recsys-catalog-serving");
    }

    private static UpstreamEndpointGroups.UpstreamClientConfig noProbe() {
        return new UpstreamEndpointGroups.UpstreamClientConfig(false, 10_000L);
    }

    private static ServiceRegistryProvider providerReturning(Map<String, String> map) {
        ServiceRegistryStore store = Mockito.mock(ServiceRegistryStore.class);
        when(store.lookup(Mockito.anyCollection())).thenReturn(map);
        ServiceRegistryProvider p = new ServiceRegistryProvider(store, map.keySet(), 0L, null);
        p.refresh();
        return p;
    }

    @Test
    void resolvesRegisteredAddressOverStatic() {
        ServiceRegistryProvider provider =
                providerReturning(Map.of("recsys-catalog-serving", "http://registered-host:6010"));
        RegistryBackedUpstreams ru = new RegistryBackedUpstreams(
                List.of(route()), Duration.ofSeconds(2), null, noProbe(), provider);
        try {
            assertThat(ru.resolvedBaseUri("catalog")).isEqualTo("http://registered-host:6010");
            assertThat(ru.clientFor("catalog")).isNotNull();
        } finally {
            ru.close();
        }
    }

    @Test
    void fallsBackToStaticWhenUnregistered() {
        ServiceRegistryProvider provider = providerReturning(Map.of()); // nothing registered
        RegistryBackedUpstreams ru = new RegistryBackedUpstreams(
                List.of(route()), Duration.ofSeconds(2), null, noProbe(), provider);
        try {
            assertThat(ru.resolvedBaseUri("catalog")).isEqualTo("http://static-host:6010");
        } finally {
            ru.close();
        }
    }
}
