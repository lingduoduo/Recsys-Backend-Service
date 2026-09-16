package com.recsys.resilience;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Per-route circuit breaker for the API gateway. Delegates the CLOSED/OPEN/HALF_OPEN
 * state machine to the shared {@link CircuitBreaker}; keeps this class's public State
 * enum and method surface for gateway callers.
 */
public final class RouteCircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    public record Permit(CircuitBreaker.Permit delegate) {
        public Permit {
            Objects.requireNonNull(delegate, "delegate");
        }
    }

    public static final int  DEFAULT_FAILURE_THRESHOLD = 5;
    public static final long DEFAULT_COOLDOWN_MS       = 10_000L;

    private final CircuitBreaker delegate;

    public RouteCircuitBreaker() {
        this(DEFAULT_FAILURE_THRESHOLD, DEFAULT_COOLDOWN_MS);
    }

    public RouteCircuitBreaker(int failureThreshold, long cooldownMs) {
        this.delegate = new CircuitBreaker(failureThreshold, cooldownMs);
    }

    RouteCircuitBreaker(int failureThreshold, long cooldownMs, LongSupplier clockMs) {
        this.delegate = new CircuitBreaker(failureThreshold, cooldownMs, clockMs);
    }

    public State state() {
        return switch (delegate.state()) {
            case CLOSED    -> State.CLOSED;
            case OPEN      -> State.OPEN;
            case HALF_OPEN -> State.HALF_OPEN;
        };
    }

    public Permit tryAcquirePermit() {
        CircuitBreaker.Permit permit = delegate.tryAcquirePermit();
        return permit == null ? null : new Permit(permit);
    }

    public void recordSuccess(Permit permit) {
        delegate.recordSuccess(permit.delegate());
    }

    public void recordFailure(Permit permit) {
        delegate.recordFailure(permit.delegate());
    }

    /**
     * Settles a permit neutrally, for a request that ended with no evidence about upstream
     * health — a client that cancelled or aborted mid-response. Frees a half-open probe slot
     * without counting for or against the route. See {@link CircuitBreaker#releasePermit}.
     */
    public void releasePermit(Permit permit) {
        delegate.releasePermit(permit.delegate());
    }
}
