package com.recsys.application.gateway;
import com.recsys.application.gateway.MicroserviceRoute;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class GatewayRouteTableTest {

    private final List<MicroserviceRoute> routes = MicroserviceRoute.defaults();

    @Test
    void canonicalStrategiesMapToProductionRoutes() {
        assertThat(RecommendationGatewayService.STRATEGY_ROUTES).containsExactlyInAnyOrderEntriesOf(
                Map.of("embedding", "embed-recall",
                        "model", "model-inference",
                        "online", "online-blend",
                        "sequential", "sequential"));
        assertThat(routes.stream().map(MicroserviceRoute::name))
                .contains("embed-recall", "model-inference", "online-blend", "sequential");
    }

    @Test
    void noDuplicatePrefixes() {
        List<String> prefixes = routes.stream()
                .map(MicroserviceRoute::prefix).toList();
        Set<String> unique = Set.copyOf(prefixes);
        assertThat(prefixes).hasSameSizeAs(unique);
    }

    @Test
    void deadRoutesAreRemoved() {
        Set<String> prefixes = routes.stream()
                .map(MicroserviceRoute::prefix)
                .collect(Collectors.toSet());
        // /api/retrieval is deliberately absent from this list: it is no longer the dead
        // "recommendation-retrieval" route but a live one reaching the merged retrieval surface
        // on model serving. MicroserviceRouteTest still asserts the dead route's *name* is gone,
        // which is what this check was actually protecting.
        assertThat(prefixes).doesNotContain(
                "/api/ranking",
                "/api/agents",
                "/api/observability");
    }

    @Test
    void newProductionRoutesExist() {
        Set<String> prefixes = routes.stream()
                .map(MicroserviceRoute::prefix)
                .collect(Collectors.toSet());
        assertThat(prefixes).contains(
                "/api/recommend/embedding",
                "/api/recommend/model",
                "/api/recommend/online",
                "/api/recommend/sequential",
                "/api/knowledge");
    }

    @Test
    void backwardCompatRoutesAreKept() {
        Set<String> prefixes = routes.stream()
                .map(MicroserviceRoute::prefix)
                .collect(Collectors.toSet());
        assertThat(prefixes).contains(
                "/api/catalog",
                "/api/model",
                "/api/online",
                "/api/users",
                "/api/movies",
                "/api/features");
    }

    @Test
    void recommendPrefixRoutesRewriteToCorrectBackends() {
        MicroserviceRoute embedding = routes.stream()
                .filter(r -> r.prefix().equals("/api/recommend/embedding"))
                .findFirst().orElseThrow();
        assertThat(embedding.rewrite("/api/recommend/embedding/v2/recommend", null).getPath())
                .isEqualTo("/v2/recommend");
        assertThat(embedding.baseUri().getPort()).isEqualTo(6010);

        MicroserviceRoute model = routes.stream()
                .filter(r -> r.prefix().equals("/api/recommend/model"))
                .findFirst().orElseThrow();
        assertThat(model.rewrite("/api/recommend/model/v2/recommend", null).getPath())
                .isEqualTo("/v2/recommend");
        assertThat(model.baseUri().getPort()).isEqualTo(8080);

        MicroserviceRoute online = routes.stream()
                .filter(r -> r.prefix().equals("/api/recommend/online"))
                .findFirst().orElseThrow();
        assertThat(online.rewrite("/api/recommend/online/v2/recommend", null).getPath())
                .isEqualTo("/v2/recommend");
        assertThat(online.baseUri().getPort()).isEqualTo(7010);
    }

    /**
     * The merged retrieval surface. Note the doubled segment in the gateway path: this table's
     * convention is that the gateway path is {@code route.prefix() + backendPath} — the same
     * shape as {@code /api/model/api/v1/recommend} and {@code /api/online/online/features} — and
     * {@code BackendRoutePolicy.userScopedGatewayPaths} derives the never-public guard from
     * exactly that identity. Folding {@code /api/v1/retrieval} into the route's baseUri would
     * read better and quietly break both.
     */
    @Test
    void theRetrievalPrefixResolvesToModelServing() {
        String gatewayPath = "/api/retrieval/api/v1/retrieval/recommend/123";

        MicroserviceRoute retrieval = MicroserviceRoute.match(routes, gatewayPath);

        assertThat(retrieval).isNotNull();
        assertThat(retrieval.name()).isEqualTo("retrieval");
        assertThat(retrieval.serviceName()).isEqualTo("recsys-model-serving");
        assertThat(retrieval.baseUri().getPort()).isEqualTo(8080);

        // The rewritten backend path must be the one BackendRoutePolicy classifies and the one
        // the Spring controller answers on, or the route forwards straight into a 403 then a 404.
        String backendPath = retrieval.rewrite(gatewayPath, null).getPath();
        assertThat(backendPath).isEqualTo("/api/v1/retrieval/recommend/123");
        assertThat(BackendRoutePolicy.lookup(retrieval.serviceName(), backendPath))
                .isEqualTo(new BackendRoutePolicy.Policy(
                        BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.PATH));
    }
}
