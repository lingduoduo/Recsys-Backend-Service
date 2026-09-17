package com.recsys.application.gateway;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.recsys.ratelimit.GatewayRateLimiter;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The edge rejects a path carrying a {@code .} or {@code ..} segment.
 *
 * <p>Armeria decodes percent-escapes, strips matrix params, collapses {@code //} and rejects
 * {@code ..} — but it preserves a single {@code .} segment. Tomcat, which serves 8080, collapses
 * it. So {@code /api/model/api/v1/./recommend} reaches the gateway as a path that
 * {@link BackendRoutePolicy} does not recognise as its declared {@code /api/v1/recommend} entry
 * (its exact matching is by design) and reaches the Spring handler as {@code /api/v1/recommend}.
 * That is a bypass of the user-scope check on every 8080 user-scoped route.
 *
 * <p>The fix rejects the family at the entry point rather than canonicalizing it: route matching,
 * rate-limit keying, and the CloudFront cache key all derive from the same string, so
 * normalize-and-continue would leave three of them keyed on a spelling no client should ever send.
 */
class GatewayPathCanonicalizationTest {

    private static final MicroserviceRoute MODEL = new MicroserviceRoute(
            "model", "/api/model", "MODEL_SERVICE_URL",
            URI.create("http://localhost:8080"), "/health/ready", "recsys-model-serving");

    @Test
    void aDotSegmentIsRejectedAtTheEdge() {
        assertRejected("/api/model/api/v1/./recommend");
    }

    /** Armeria decodes {@code %2E} to {@code .} before the gateway sees it; so must the guard. */
    @Test
    void aPercentEncodedDotSegmentIsRejectedAtTheEdge() {
        assertRejected("/api/model/api/v1/%2E/recommend");
        assertRejected("/api/model/api/v1/%2e/recommend");
    }

    /**
     * Armeria leaves {@code %2F} percent-encoded in {@code ctx.path()}, so {@code .%2Frecommend} is
     * <em>one</em> segment to every gateway control and two to any backend that decodes it — the
     * same gateway/backend disagreement as a dot segment, reached by a different spelling. It is
     * inert only while Tomcat keeps rejecting an encoded solidus by default, which is a setting,
     * not a guarantee. The separator is not negotiable, so an encoded one is refused outright.
     */
    @Test
    void anEncodedPathSeparatorIsRejectedAtTheEdge() {
        assertRejected("/api/model/api/v1/.%2Frecommend");
        assertRejected("/api/model/api/v1/.%2frecommend");
        assertRejected("/api/model/foo%2Fbar");
    }

    /** Not only the last segment: a dot anywhere in the path is non-canonical. */
    @Test
    void aDotSegmentAnywhereInThePathIsRejected() {
        assertRejected("/api/model/./api/v1/recommend");
        assertRejected("/./api/model/api/v1/recommend");
        assertRejected("/api/./v1/catalog/getuser");
        assertRejected("/api/model/api/v1/recommend/.");
    }

    /**
     * Armeria rejects {@code ..} itself, before a request can even be constructed — asserted here
     * so the division of labour is recorded rather than assumed. The guard covers {@code ..} too,
     * so the whole family is closed in one place even if that upstream behaviour ever changes.
     */
    @Test
    void aDotDotSegmentIsRejectedByArmeriaAndAlsoByTheGuard() {
        assertThrows(IllegalArgumentException.class, () -> serve("/api/model/api/v1/../recommend"));
        assertTrue(GatewayProxyService.hasNonCanonicalSegment("/api/model/api/v1/../recommend"));
        assertTrue(GatewayProxyService.hasNonCanonicalSegment("/api/model/.."));
    }

    /** A dot inside a segment is an ordinary character and must keep working. */
    @Test
    void ordinaryPathsWithDotsInsideSegmentsAreUntouched() {
        assertFalse(GatewayProxyService.hasNonCanonicalSegment("/api/catalog/item"));
        assertFalse(GatewayProxyService.hasNonCanonicalSegment("/api/catalog/similar"));
        assertFalse(GatewayProxyService.hasNonCanonicalSegment("/api/model/v1/models/recmodel:predict"));
        assertFalse(GatewayProxyService.hasNonCanonicalSegment("/api/catalog/movie.json"));
        assertFalse(GatewayProxyService.hasNonCanonicalSegment("/api/catalog/1.2.3"));
        assertFalse(GatewayProxyService.hasNonCanonicalSegment("/api/catalog/..foo"));
        assertFalse(GatewayProxyService.hasNonCanonicalSegment("/health"));
        assertFalse(GatewayProxyService.hasNonCanonicalSegment("/"));
    }

    /**
     * The retrieval routes read the userId from a path <em>segment</em>
     * ({@code UserIdSource.PATH}), which reads the segment after the first {@code recommend},
     * {@code predict} or {@code users} marker anywhere in the path. On its own that would let
     * {@code /api/retrieval/api/v1/retrieval/recommend/<own>/../../users/<victim>/profile} name
     * the caller's own id to the gateway and a victim's to the backend — the id the check reads
     * and the id the handler acts on would be different strings.
     *
     * <p>It cannot: this guard runs before routing, so the traversal spelling never reaches the
     * policy lookup at all. Pinned here rather than re-architecting {@code PATH}, because this is
     * the protection that actually holds — and if it is ever relaxed, {@code PATH}'s
     * first-marker rule becomes a live cross-user read, not a style question.
     */
    @Test
    void aTraversalSpellingOfARetrievalPathIsRejectedBeforeRouting() {
        String traversal = "/api/retrieval/api/v1/retrieval/recommend/1/../../users/2/profile";
        // Two layers, as above: Armeria refuses to build the context at all, and the guard would
        // have caught it anyway if that upstream behaviour ever changed.
        assertThrows(IllegalArgumentException.class, () -> serve(traversal));
        assertTrue(GatewayProxyService.hasNonCanonicalSegment(traversal));
        assertThrows(IllegalArgumentException.class,
                () -> serve("/api/retrieval/api/v1/retrieval/recommend/1/%2e%2e/users/2/profile"));
        assertThrows(IllegalArgumentException.class,
                () -> serve("/api/retrieval/api/v1/retrieval/recommend/1/%2E%2E/users/2/profile"));
        // An encoded separator hides a segment boundary from every gateway control while a
        // backend that decodes it sees two segments — the same disagreement, spelled differently.
        // Armeria leaves this one encoded, so it is the gateway's own 400 that closes it.
        assertRejected("/api/retrieval/api/v1/retrieval/recommend/1%2Fusers%2F2");
        assertRejected("/api/retrieval/api/v1/retrieval/recommend/1%2fusers%2f2");
    }

    /**
     * The rejection is the gateway's own 400, not a 404 from route matching — proving it runs
     * before routing, which is what also keeps rate-limit keying and the CDN cache key off the
     * non-canonical spelling.
     */
    @Test
    void theRejectionPrecedesRouting() {
        AggregatedHttpResponse response = serve("/no/such/route/./at/all");
        assertEquals(HttpStatus.BAD_REQUEST, response.status());
        assertTrue(response.contentUtf8().contains("path segment"), response.contentUtf8());
    }

    private static void assertRejected(String path) {
        AggregatedHttpResponse response = serve(path);
        assertEquals(HttpStatus.BAD_REQUEST, response.status(), "expected 400 for " + path);
        assertEquals("no-store", response.headers().get("cache-control"),
                "a rejection must never be cacheable at the edge");
    }

    private static AggregatedHttpResponse serve(String path) {
        GatewayProxyService service = new GatewayProxyService(
                List.of(MODEL),
                new GatewayRequestForwarder(List.of(MODEL), Duration.ofSeconds(1), Map.of(),
                        GatewayRateLimiter.disabled(),
                        new UpstreamEndpointGroups.HealthCheckConfig(false, 0L)),
                GatewayAuthenticator.disabled());
        HttpRequest request = HttpRequest.of(
                RequestHeaders.of(HttpMethod.POST, path), HttpData.ofUtf8("{\"userId\":\"999\"}"));
        ServiceRequestContext ctx = ServiceRequestContext.of(request);
        HttpResponse response = service.serve(ctx, request);
        return response.aggregate().join();
    }
}
