package com.recsys.application.gateway;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.RequestHeaders;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BackendRoutePolicyTest {

    @Test
    void lookup_findsQueryAndBodyRoutes() {
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.QUERY),
                BackendRoutePolicy.lookup("recsys-catalog-serving", "/getuser"));
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.QUERY),
                BackendRoutePolicy.lookup("recsys-online-serving", "/online/features"));
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.BODY),
                BackendRoutePolicy.lookup("recsys-model-serving", "/v2/sequential/recommend"));
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.BODY_INSTANCES),
                BackendRoutePolicy.lookup("recsys-catalog-serving", "/v1/models/recmodel:predict"));
    }

    @Test
    void lookup_isExactNeverPrefix() {
        // Prefix-with-boundary matching is what created the /api/catalog trap that
        // PROTECTED_PREFIXES exists to survive (20_AuthN_AuthZ §3). Not repeated here.
        assertNull(BackendRoutePolicy.lookup("recsys-catalog-serving", "/getuserprofile"));
        assertNull(BackendRoutePolicy.lookup("recsys-catalog-serving", "/getuser/extra"));
    }

    @Test
    void lookup_returnsNullForUnknownOrNullService() {
        assertNull(BackendRoutePolicy.lookup("recsys-llm", "/getuser"));
        assertNull(BackendRoutePolicy.lookup(null, "/getuser"));
    }

    @Test
    void pathWithoutQuery_splitsOnTheFirstQuestionMark() {
        assertEquals("/getuser", BackendRoutePolicy.pathWithoutQuery("/getuser?userId=42"));
        assertEquals("/getuser", BackendRoutePolicy.pathWithoutQuery("/getuser"));
        assertEquals("/getuser", BackendRoutePolicy.pathWithoutQuery("/getuser?a=1?b=2"));
    }

    @Test
    void query_extractsFromTargetPathNotRequestHeaders() {
        // The request's own path is still the pre-rewrite gateway path; the rewritten query
        // lives in targetPath. Reading the wrong one silently extracts nothing.
        AggregatedHttpRequest request = AggregatedHttpRequest.of(
                RequestHeaders.of(HttpMethod.GET, "/api/catalog/getuser?userId=999"),
                HttpData.empty());
        assertEquals("42", UserIdSource.QUERY.extract("/getuser?userId=42", request));
    }

    @Test
    void query_blankWhenAbsent() {
        AggregatedHttpRequest request = AggregatedHttpRequest.of(
                RequestHeaders.of(HttpMethod.GET, "/api/catalog/getuser"), HttpData.empty());
        assertEquals("", UserIdSource.QUERY.extract("/getuser", request));
        assertEquals("", UserIdSource.QUERY.extract("/getuser?limit=5", request));
    }

    @Test
    void body_extractsStringAndNumericUserId() {
        assertEquals("42", UserIdSource.BODY.extract("/v2/recommend", body("{\"userId\":\"42\"}")));
        // 6010 and 7010 bind userId as an int, so the JSON may legitimately be a number.
        assertEquals("42", UserIdSource.BODY.extract("/v2/recommend", body("{\"userId\":42}")));
    }

    @Test
    void body_blankWhenMissingUnparseableOrNotScalar() {
        assertEquals("", UserIdSource.BODY.extract("/v2/recommend", body("{\"limit\":5}")));
        assertEquals("", UserIdSource.BODY.extract("/v2/recommend", body("not json")));
        assertEquals("", UserIdSource.BODY.extract("/v2/recommend", body("[1,2]")));
        assertEquals("", UserIdSource.BODY.extract("/v2/recommend", body("{\"userId\":{\"id\":1}}")));
        assertEquals("", UserIdSource.BODY.extract("/v2/recommend", body("")));
    }

    // ---- BODY_INSTANCES: the TF-Serving predict batch ------------------------------

    private static final String PREDICT = "/v1/models/recmodel:predict";

    @Test
    void instances_extractsWhenEveryElementNamesTheSameUser() {
        assertEquals("42", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":[{\"userId\":42,\"movieId\":1},{\"userId\":42,\"movieId\":2}]}")));
    }

    @Test
    void instances_extractsFromASingleElement() {
        assertEquals("42", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":[{\"userId\":42,\"movieId\":1}]}")));
        // The backend binds userId as an int, but a client may still send it as a string.
        assertEquals("42", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":[{\"userId\":\"42\"}]}")));
    }

    @Test
    void instances_deniesAMixedUserBatch() {
        // The gateway forwards the whole request or none of it — it cannot allow a subset, so a
        // batch naming a second user must be denied outright rather than partially allowed.
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":[{\"userId\":42},{\"userId\":43}]}")));
        // Including when the caller's own id is present but not alone.
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":[{\"userId\":42},{\"userId\":42},{\"userId\":99}]}")));
    }

    @Test
    void instances_blankWhenTheBatchNamesNoUsableId() {
        // Empty array: nothing to authorize.
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":[]}")));
        // Absent instances.
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"userId\":42}")));
        // instances present but not an array.
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":{\"userId\":42}}")));
        // An element missing its userId, or carrying a container instead of a scalar.
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":[{\"movieId\":1}]}")));
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":[{\"userId\":{\"id\":42}}]}")));
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":[{\"userId\":42},{\"movieId\":2}]}")));
        // An element that is not an object at all.
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT,
                body("{\"instances\":[42]}")));
        // Malformed, empty, and non-object bodies stay total.
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT, body("not json")));
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT, body("")));
        assertEquals("", UserIdSource.BODY_INSTANCES.extract(PREDICT, body("[1,2]")));
    }

    @Test
    void instances_isNotReadableByPlainBody() {
        // Why the new kind exists: BODY reads a top-level scalar, so on this shape it would
        // extract nothing and deny every legitimate call while looking like a working control.
        String batch = "{\"instances\":[{\"userId\":42,\"movieId\":1}]}";
        assertEquals("", UserIdSource.BODY.extract(PREDICT, body(batch)));
        assertEquals("42", UserIdSource.BODY_INSTANCES.extract(PREDICT, body(batch)));
    }

    private static AggregatedHttpRequest body(String json) {
        return AggregatedHttpRequest.of(
                RequestHeaders.of(HttpMethod.POST, "/api/recommend"), HttpData.ofUtf8(json));
    }

    // ---- BackendRoutePolicy classification ------------------------------------------------

    @Test
    void telemetryIsClassifiedNoProxy() {
        assertEquals(BackendRoutePolicy.Access.NO_PROXY,
                BackendRoutePolicy.lookup("recsys-catalog-serving", "/metrics").access());
        assertEquals(BackendRoutePolicy.Access.NO_PROXY,
                BackendRoutePolicy.lookup("recsys-online-serving", "/metrics").access());
        assertEquals(BackendRoutePolicy.Access.NO_PROXY,
                BackendRoutePolicy.lookup("recsys-model-serving", "/health/ab-tests").access());
    }

    @Test
    void controlPlaneWritesAreClassifiedOperator() {
        assertEquals(BackendRoutePolicy.Access.OPERATOR,
                BackendRoutePolicy.lookup("recsys-catalog-serving", "/setembedding").access());
        assertEquals(BackendRoutePolicy.Access.OPERATOR,
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/model/versions/activate").access());
        assertEquals(BackendRoutePolicy.Access.OPERATOR,
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/model/versions/rollback").access());
        assertEquals(BackendRoutePolicy.Access.OPERATOR,
                BackendRoutePolicy.lookup("recsys-online-serving", "/online/ops").access());
    }

    @Test
    void ordinaryDataPathsAreClassifiedAuthenticated() {
        assertEquals(BackendRoutePolicy.Access.AUTHENTICATED,
                BackendRoutePolicy.lookup("recsys-catalog-serving", "/item").access());
        assertEquals(BackendRoutePolicy.Access.AUTHENTICATED,
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/token").access());
    }

    @Test
    void anUndeclaredPathHasNoPolicy() {
        assertNull(BackendRoutePolicy.lookup("recsys-catalog-serving", "/nope"));
        assertNull(BackendRoutePolicy.lookup("recsys-catalog-serving", "/getuser/extra"));
        assertNull(BackendRoutePolicy.lookup("no-such-service", "/item"));
        assertNull(BackendRoutePolicy.lookup(null, "/item"));
    }

    @Test
    void prefixEntriesMatchWithABoundaryAndOnlyAfterAnExactMiss() {
        // /actuator is config-driven and /shards is one Armeria pathPrefix — neither is enumerable.
        assertEquals(BackendRoutePolicy.Access.NO_PROXY,
                BackendRoutePolicy.lookup("recsys-model-serving", "/actuator").access());
        assertEquals(BackendRoutePolicy.Access.NO_PROXY,
                BackendRoutePolicy.lookup("recsys-model-serving", "/actuator/prometheus").access());
        assertEquals(BackendRoutePolicy.Access.AUTHENTICATED,
                BackendRoutePolicy.lookup("recsys-online-serving", "/shards/device").access());
        // Boundary: a longer name that merely starts with the prefix is not a match.
        assertNull(BackendRoutePolicy.lookup("recsys-model-serving", "/actuatorx"));
        assertNull(BackendRoutePolicy.lookup("recsys-online-serving", "/shardsx"));
    }

    @Test
    void userScopedPolicyCarriesItsSourceAndOthersDoNot() {
        assertEquals(UserIdSource.BODY_INSTANCES,
                BackendRoutePolicy.lookup("recsys-catalog-serving", "/v1/models/recmodel:predict").userIdSource());
        assertNull(BackendRoutePolicy.lookup("recsys-catalog-serving", "/item").userIdSource());
    }

    @Test
    void aPolicyCannotClaimUserScopeWithoutASource() {
        // The invariant is enforced in the record, not left to the table author's discipline.
        assertThrows(IllegalArgumentException.class,
                () -> new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.USER_SCOPED, null));
        assertThrows(IllegalArgumentException.class,
                () -> new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.AUTHENTICATED, UserIdSource.QUERY));
    }

    // ---- the retrieval surface (/api/v1/retrieval) ----------------------------------------

    @Test
    void classifiesRetrievalModelReloadAsOperator() {
        // It reloads the live ONNX session for everyone — the same class of mutation as
        // /api/v1/model/versions/activate, and it arrived with no authorization at all.
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.OPERATOR, null),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/model/reload"));
    }

    @Test
    void classifiesProfileAuditAndItsPerUserPathAsOperator() {
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.OPERATOR, null),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/profile-audit"));
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.OPERATOR, null),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/profile-audit/123"));
    }

    @Test
    void scopesTheRetrievalUserRoutesToTheCallersOwnUserId() {
        assertEquals(new BackendRoutePolicy.Policy(
                        BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.PATH),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/recommend/123"));
        assertEquals(new BackendRoutePolicy.Policy(
                        BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.PATH),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/users/123/profile"));
        assertEquals(new BackendRoutePolicy.Policy(
                        BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.PATH),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/predict/123/456"));
    }

    @Test
    void theRetrievalPredictPrefixDoesNotSwallowItsTwoExactSiblings() {
        // Exact is tried before prefix, so /predict/id and /predict/metadata keep their own
        // classification even though /api/v1/retrieval/predict is a USER_SCOPED prefix. Neither
        // names a user, so USER_SCOPED would deny every call to them.
        //
        // DO NOT "helpfully" promote these to AUTHENTICATED. /predict/id is an UNSCOPED ALIAS
        // for the user-scoped /predict/{user}/{item}: RetrievalRecommendationController.predictById
        // reaches the same predictionService.predict(userId, itemId), addressed by raw model
        // indices. AUTHENTICATED here hands a user-tier caller the score they were just 403'd
        // for on the templated spelling — and the indices are dense and bounded by
        // userLookup.size(), so they enumerate rather than having to be guessed.
        // /predict/metadata publishes that bound. They are debug/index surfaces; NO_PROXY is the
        // only classification that does not reopen the scope check the prefix exists to enforce.
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.NO_PROXY, null),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/predict/id"));
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.NO_PROXY, null),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/predict/metadata"));
    }

    @Test
    void classifiesTheRemainingRetrievalRoutes() {
        // BODY_USER, not BODY: FeedbackRequest's field is `user`. BODY read `userId`, which this
        // body never carries — so it denied every honest call, and a body naming both keys passed
        // on the one the backend ignores.
        assertEquals(new BackendRoutePolicy.Policy(
                        BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.BODY_USER),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/feedback"));
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.AUTHENTICATED, null),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/embedding/item1"));
        // Serving telemetry, not a data path: reachable on the pod only, like every other
        // /metrics in this table.
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.NO_PROXY, null),
                BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/metrics"));
    }
}
