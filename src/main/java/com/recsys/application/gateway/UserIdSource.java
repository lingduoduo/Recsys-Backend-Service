package com.recsys.application.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.QueryParams;

import java.util.Set;

/**
 * Where a user-scoped backend route carries the {@code userId} it acts on.
 *
 * <p>Extraction is deliberately total: anything it cannot read as a scalar id — absent, blank,
 * malformed, an object, an array — comes back as {@code ""}, which the caller treats as a denial.
 * A request whose subject cannot be determined is a request that cannot be authorized.
 */
enum UserIdSource {

    QUERY {
        @Override
        String extract(String targetPath, AggregatedHttpRequest request) {
            int mark = targetPath.indexOf('?');
            if (mark < 0) {
                return "";
            }
            String value = QueryParams.fromQueryString(targetPath.substring(mark + 1)).get(PARAM);
            return value == null ? "" : value.trim();
        }
    },

    BODY {
        @Override
        String extract(String targetPath, AggregatedHttpRequest request) {
            try {
                JsonNode root = MAPPER.readTree(request.contentUtf8());
                if (root == null || !root.isObject()) {
                    return "";
                }
                return scalarText(root.get(PARAM));
            } catch (Exception e) {
                return "";
            }
        }
    },

    /**
     * A TF-Serving-shaped batch: {@code {"instances":[{"userId":1,"movieId":2}, ...]}}. The id is
     * inside the array elements, not at the top level, so {@link #BODY} would read {@code ""} here
     * and deny every legitimate call while looking like a working control.
     *
     * <p>Returns the id only when the batch names exactly one user: the array is non-empty and
     * every element carries the same scalar {@code userId}. A batch mixing users is denied rather
     * than partially allowed — the gateway authorizes the whole request or none of it, and it has
     * no way to forward a subset. `/v1/models/recmodel:predict` scores against
     * {@code u2vEmb:<userId>}, so a mixed batch is a read of someone else's embedding.
     */
    BODY_INSTANCES {
        @Override
        String extract(String targetPath, AggregatedHttpRequest request) {
            try {
                JsonNode root = MAPPER.readTree(request.contentUtf8());
                if (root == null || !root.isObject()) {
                    return "";
                }
                JsonNode instances = root.get(INSTANCES);
                if (instances == null || !instances.isArray() || instances.size() == 0) {
                    return "";
                }
                String agreed = null;
                for (JsonNode instance : instances) {
                    if (instance == null || !instance.isObject()) {
                        return "";
                    }
                    String id = scalarText(instance.get(PARAM));
                    if (id.isEmpty() || (agreed != null && !agreed.equals(id))) {
                        return "";
                    }
                    agreed = id;
                }
                return agreed;
            } catch (Exception e) {
                return "";
            }
        }
    },

    /**
     * A path segment: {@code /api/v1/retrieval/recommend/123}. The retrieval routes spell the
     * userId as a path variable, which no other source can read, so without this case they could
     * only be classified AUTHENTICATED — and any authenticated caller could then read another
     * user's profile by editing the path.
     *
     * <p>Reads the segment <em>after</em> a known marker rather than the last segment:
     * {@code /predict/{user}/{item}} puts the item last, so "take the tail" would compare the
     * caller's id against a movie id and deny every legitimate call.
     */
    PATH {
        @Override
        String extract(String targetPath, AggregatedHttpRequest request) {
            if (targetPath == null) {
                return "";
            }
            int mark = targetPath.indexOf('?');
            String path = mark < 0 ? targetPath : targetPath.substring(0, mark);
            String[] segments = path.split("/");
            // Stops one short of the end: a marker with nothing after it carries no id.
            for (int i = 0; i < segments.length - 1; i++) {
                if (MARKERS.contains(segments[i])) {
                    return segments[i + 1].trim();
                }
            }
            return "";
        }
    };

    /** The parameter and JSON field name is `userId` on every route in the table. */
    static final String PARAM = "userId";

    /**
     * The segments {@link #PATH} reads an id from — the segment that follows one of these is the
     * userId. Kept as a closed set rather than a positional index because the prefix length is not
     * fixed: the same handler is reachable under several gateway spellings, and a positional rule
     * would silently read the wrong segment under any of them.
     */
    private static final Set<String> MARKERS = Set.of("recommend", "predict", "users");

    /** The array field wrapping a TF-Serving predict batch. */
    private static final String INSTANCES = "instances";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * The one place the scalar rule lives: a container, a null, or an absent field is not an id.
     * Jackson's {@code asText("")} would happily stringify an object, so the guard runs first.
     */
    private static String scalarText(JsonNode value) {
        if (value == null || value.isNull() || value.isContainerNode()) {
            return "";
        }
        return value.asText("").trim();
    }

    /**
     * @param targetPath the rewritten backend path, including its query string
     * @param request    the already-aggregated request; reading it here costs no extra buffering
     */
    abstract String extract(String targetPath, AggregatedHttpRequest request);
}
