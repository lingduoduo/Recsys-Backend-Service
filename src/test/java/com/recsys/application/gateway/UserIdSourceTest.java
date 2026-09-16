package com.recsys.application.gateway;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.RequestHeaders;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link UserIdSource#PATH}, which the retrieval routes need: they carry the userId in a path
 * segment, so every other source reads nothing there.
 *
 * <p>The QUERY/BODY/BODY_INSTANCES cases are exercised in {@code BackendRoutePolicyTest}
 * alongside the table that selects them.
 *
 * <p>Every "no id here" assertion is {@code ""} rather than {@code null} on purpose:
 * {@code GatewayRequestForwarder.authorizeUserScope} calls {@code requested.isBlank()} on the
 * result, so returning null would NPE the very check that is supposed to deny the request.
 * {@code ""} is the enum's documented denial value.
 */
class UserIdSourceTest {

    private final AggregatedHttpRequest request = AggregatedHttpRequest.of(
            RequestHeaders.of(HttpMethod.GET, "/api/v1/retrieval/recommend/123"), HttpData.empty());

    @Test
    void extractsTheUserIdFromThePathSegment() {
        assertEquals("123", UserIdSource.PATH.extract("/api/v1/retrieval/recommend/123", request));
    }

    @Test
    void extractsTheUserIdWhenMoreSegmentsFollow() {
        // /predict/{user}/{item}: the id is not the last segment, so "take the tail" is wrong.
        assertEquals("123", UserIdSource.PATH.extract("/api/v1/retrieval/predict/123/456", request));
    }

    @Test
    void extractsTheUserIdFromTheUsersRoute() {
        assertEquals("123", UserIdSource.PATH.extract("/api/v1/retrieval/users/123/profile", request));
    }

    @Test
    void returnsBlankWhenNoSegmentFollowsTheMarker() {
        assertEquals("", UserIdSource.PATH.extract("/api/v1/retrieval/recommend", request));
        assertEquals("", UserIdSource.PATH.extract("/api/v1/retrieval/recommend/", request));
    }

    @Test
    void ignoresTheQueryStringWhenReadingTheSegment() {
        assertEquals("123", UserIdSource.PATH.extract("/api/v1/retrieval/recommend/123?limit=5", request));
    }

    @Test
    void returnsBlankWhenNoMarkerSegmentIsPresent() {
        // Fails closed rather than guessing: a path this source was never meant to read is a
        // request whose subject cannot be determined, which the forwarder turns into a 403.
        assertEquals("", UserIdSource.PATH.extract("/api/v1/retrieval/feedback", request));
        assertEquals("", UserIdSource.PATH.extract("/", request));
        assertEquals("", UserIdSource.PATH.extract("", request));
    }
}
