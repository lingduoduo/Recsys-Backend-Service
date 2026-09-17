package com.recsys.application.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.RequestHeaders;
import com.recsys.retrieval.model.FeedbackRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * {@link UserIdSource#PATH}, which the retrieval routes need: they carry the userId in a path
 * segment, so every other source reads nothing there.
 *
 * <p>...and {@link UserIdSource#BODY_USER}, which the feedback route needs for the mirror-image
 * reason: its body spells the subject {@code user}, not {@code userId}.
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

    // ---- BODY_USER: the feedback route's own field name -----------------------------

    /**
     * The case the original defect survived a green suite for: no test ever fed a <em>real</em>
     * request body through the policy. Serializing the actual record is what makes this test
     * track the wire contract instead of restating the source's assumption back to it — rename
     * {@code FeedbackRequest.user} and this fails, which is the point.
     */
    @Test
    void extractsTheUserIdFromARealSerializedFeedbackRequest() throws Exception {
        String json = new ObjectMapper().writeValueAsString(
                new FeedbackRequest("123", "item2", true, 0.8));

        assertEquals("123", UserIdSource.BODY_USER.extract(FEEDBACK, body(json)));
        // And the reason BODY was wrong here: the serialized body carries no `userId` at all, so
        // BODY denied every honest feedback call with 403.
        assertEquals("", UserIdSource.BODY.extract(FEEDBACK, body(json)));
    }

    /**
     * The bypass direction. A caller whose own id is 123 could pass the gateway's check on a
     * `userId` field the backend never reads, while Spring's Jackson (FAIL_ON_UNKNOWN_PROPERTIES
     * off) dropped it and bound `user`: the feedback was written as user 456.
     */
    @Test
    void readsUserAndNotUserIdWhenABodyCarriesBoth() {
        assertEquals("456", UserIdSource.BODY_USER.extract(
                FEEDBACK, body("{\"userId\":\"123\",\"user\":\"456\",\"clicked\":true}")));
    }

    @Test
    void appliesTheSameScalarAndBlankRulesAsBody() {
        assertEquals("42", UserIdSource.BODY_USER.extract(FEEDBACK, body("{\"user\":42}")));
        assertEquals("42", UserIdSource.BODY_USER.extract(FEEDBACK, body("{\"user\":\" 42 \"}")));
        assertEquals("", UserIdSource.BODY_USER.extract(FEEDBACK, body("{\"user\":null}")));
        assertEquals("", UserIdSource.BODY_USER.extract(FEEDBACK, body("{\"user\":{\"id\":1}}")));
        assertEquals("", UserIdSource.BODY_USER.extract(FEEDBACK, body("{\"user\":[1]}")));
        assertEquals("", UserIdSource.BODY_USER.extract(FEEDBACK, body("{\"item\":\"i\"}")));
        assertEquals("", UserIdSource.BODY_USER.extract(FEEDBACK, body("not json")));
        assertEquals("", UserIdSource.BODY_USER.extract(FEEDBACK, body("[1,2]")));
        assertEquals("", UserIdSource.BODY_USER.extract(FEEDBACK, body("")));
    }

    private static final String FEEDBACK = "/api/v1/retrieval/feedback";

    private static AggregatedHttpRequest body(String json) {
        return AggregatedHttpRequest.of(
                RequestHeaders.of(HttpMethod.POST, FEEDBACK), HttpData.ofUtf8(json));
    }
}
