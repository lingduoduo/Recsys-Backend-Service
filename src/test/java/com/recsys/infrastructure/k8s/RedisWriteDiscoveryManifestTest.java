package com.recsys.infrastructure.k8s;

import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import static org.assertj.core.api.Assertions.assertThat;
import static com.recsys.infrastructure.k8s.ManifestDocuments.*;

class RedisWriteDiscoveryManifestTest {
    @Test
    void baseAdvertisesDiscoveryAndExcludesStaticWriteServices() throws Exception {
        var docs = allIn(Path.of("k8s/base"));
        var config = ofKind(docs, "ConfigMap").stream()
                .filter(d -> nameOf(d).equals("recsys-config")).findFirst().orElseThrow();
        var data = mapAt(config, "data");
        assertThat(data.get("REDIS_MODE")).isEqualTo("sentinel");
        assertThat(data.get("REDIS_HOST")).isEqualTo("");
        assertThat(ofKind(docs, "Service").stream().map(ManifestDocuments::nameOf))
                .doesNotContain("redis");
        var bootstrap = ofKind(docs, "Service").stream()
                .filter(d -> nameOf(d).equals("redis-primary")).findFirst().orElseThrow();
        assertThat(mapAt(bootstrap, "metadata", "annotations").get("recsys.io/endpoint-purpose"))
                .isEqualTo("bootstrap-only; not an application write endpoint");
    }
}
