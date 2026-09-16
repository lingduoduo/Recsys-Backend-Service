package com.recsys.application.gateway;

import com.linecorp.armeria.client.endpoint.EmptyEndpointGroupException;
import com.linecorp.armeria.client.endpoint.EndpointSelectionTimeoutException;
import com.linecorp.armeria.common.FilteredHttpResponse;
import com.linecorp.armeria.common.HttpObject;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.common.stream.AbortedStreamException;
import com.linecorp.armeria.common.stream.CancelledSubscriptionException;
import com.recsys.resilience.RouteCircuitBreaker;
import org.reactivestreams.Subscriber;

import java.util.concurrent.atomic.AtomicBoolean;

/** Relays response objects with demand and cancellation propagation, without collecting the body. */
final class GatewayUpstreamResponse {
    private GatewayUpstreamResponse() {}

    /** How a terminated relay should settle its circuit permit. */
    private enum Outcome {
        SUCCESS("success"), FAILURE("failure"), NEUTRAL("neutral");

        private final String label;

        Outcome(String label) { this.label = label; }
    }

    static HttpResponse relay(HttpResponse upstream, RouteCircuitBreaker breaker,
                              RouteCircuitBreaker.Permit permit, String routeName,
                              GatewayCircuitMetrics metrics) {
        AtomicBoolean settled = new AtomicBoolean();
        // A deferred downstream can abort the relay before subscribing. Filter callbacks do
        // not run in that case, but the upstream still completes exceptionally.
        upstream.whenComplete().whenComplete((unused, cause) -> {
            if (cause != null) settle(breaker, permit, settled, classify(cause), routeName, metrics);
        });
        HttpResponse observed = new FilteredHttpResponse(upstream, true) {
            private HttpStatus status;

            @Override
            protected HttpObject filter(HttpObject obj) {
                if (obj instanceof ResponseHeaders headers && !headers.status().isInformational()) {
                    status = headers.status();
                }
                // Pass ownership through unchanged, including pooled data and trailers.
                return obj;
            }

            @Override
            protected void beforeComplete(Subscriber<? super HttpObject> subscriber) {
                settle(breaker, permit, settled, status != null && !status.isServerError()
                        ? Outcome.SUCCESS : Outcome.FAILURE, routeName, metrics);
            }

            @Override
            protected Throwable beforeError(Subscriber<? super HttpObject> subscriber, Throwable cause) {
                settle(breaker, permit, settled, classify(cause), routeName, metrics);
                return cause;
            }

            @Override
            protected void onCancellation(Subscriber<? super HttpObject> subscriber) {
                // A caller that hangs up says nothing about upstream health. Counting it as a
                // failure would let any client — including an unauthenticated one on a public
                // route — open this route's circuit by cancelling repeatedly. Release the
                // half-open probe slot instead, so the next real request still gets to probe.
                settle(breaker, permit, settled, Outcome.NEUTRAL, routeName, metrics);
            }
        };
        // Recovery is possible before final headers (informational headers are allowed).
        // After final headers a failed stream terminates instead of emitting a second response.
        return observed.recover(cause -> {
            boolean unavailable = noHealthyEndpoint(cause);
            return GatewayProxyService.gatewayError(
                    unavailable ? HttpStatus.SERVICE_UNAVAILABLE : HttpStatus.BAD_GATEWAY,
                    unavailable ? routeName + " upstream unavailable — no healthy endpoint"
                            : "upstream unreachable");
        });
    }

    // Downstream cancellation reaches the upstream as a cancel/abort, and can also surface
    // through whenComplete() rather than onCancellation() depending on which side terminates
    // first. Classify it the same way wherever it arrives, so the two paths cannot race to
    // different verdicts.
    private static Outcome classify(Throwable cause) {
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (current instanceof CancelledSubscriptionException
                    || current instanceof AbortedStreamException) return Outcome.NEUTRAL;
        }
        return Outcome.FAILURE;
    }

    private static void settle(RouteCircuitBreaker breaker, RouteCircuitBreaker.Permit permit,
                               AtomicBoolean settled, Outcome outcome, String routeName,
                               GatewayCircuitMetrics metrics) {
        if (breaker == null || permit == null || !settled.compareAndSet(false, true)) return;
        switch (outcome) {
            case SUCCESS -> breaker.recordSuccess(permit);
            case FAILURE -> breaker.recordFailure(permit);
            case NEUTRAL -> breaker.releasePermit(permit);
        }
        // Inside the CAS deliberately: the counter inherits settle-exactly-once rather than
        // needing its own guard, so the three outcomes always sum to the permits taken.
        if (metrics != null) metrics.record(routeName, outcome.label);
    }

    private static boolean noHealthyEndpoint(Throwable cause) {
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (current instanceof EmptyEndpointGroupException
                    || current instanceof EndpointSelectionTimeoutException) return true;
        }
        return false;
    }
}
