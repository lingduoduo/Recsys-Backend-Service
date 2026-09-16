package com.recsys.application.gateway;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.recsys.api.serving.BaseApiService;

import java.util.Map;

/**
 * GET /health/live — liveness probe. Reports process viability and nothing else.
 *
 * <p>Deliberately distinct from {@link GatewayHealthService}, which reports upstream
 * reachability and circuit state and returns 503 when an upstream is down. That answer is right
 * for readiness — take this pod out of the load balancer — and wrong for liveness, where 503
 * means "restart this container". An upstream outage would otherwise restart every gateway pod,
 * discarding warm connection pools and circuit state, and removing the component still capable
 * of serving the routes whose upstreams are healthy.
 *
 * <p>Mirrors {@code OnlineServices.Live} on 7010.
 */
public final class GatewayLivenessService extends BaseApiService {
    @Override
    protected HttpResponse doGet(ServiceRequestContext ctx, HttpRequest req) {
        return writeJson(HttpStatus.OK, Map.of(
                "ok", true,
                "live", true,
                "service", "api-gateway"
        ));
    }
}
