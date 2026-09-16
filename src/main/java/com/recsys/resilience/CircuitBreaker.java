package com.recsys.resilience;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Shared CLOSED/OPEN/HALF_OPEN circuit-breaker state machine, consolidated from
 * {@code RouteCircuitBreaker} (gateway) and {@code RedisRateLimiter}'s embedded breaker.
 *
 * Thread-safe: all transitions are CAS-based, no global lock. The clock is injectable so
 * cooldown transitions can be tested deterministically.
 */
public final class CircuitBreaker {

    public enum State { CLOSED, OPEN, HALF_OPEN }

    public record Permit(long generation, boolean probe) {}

    private final int failureThreshold;
    private final long cooldownMs;
    private final LongSupplier clockMs;

    private final AtomicReference<BreakerState> breakerState =
            new AtomicReference<>(new BreakerState(0L, 0, 0L, -1L));

    public CircuitBreaker(int failureThreshold, long cooldownMs) {
        this(failureThreshold, cooldownMs, System::currentTimeMillis);
    }

    public CircuitBreaker(int failureThreshold, long cooldownMs, LongSupplier clockMs) {
        if (failureThreshold < 1) throw new IllegalArgumentException("failureThreshold must be >= 1");
        if (cooldownMs < 0)       throw new IllegalArgumentException("cooldownMs must be non-negative");
        this.failureThreshold = failureThreshold;
        this.cooldownMs = cooldownMs;
        this.clockMs = clockMs;
    }

    public State state() {
        return state(breakerState.get());
    }

    private State state(BreakerState snapshot) {
        if (snapshot.consecutiveFailures() < failureThreshold) return State.CLOSED;
        long elapsed = clockMs.getAsLong() - snapshot.openedAtMs();
        return elapsed >= cooldownMs ? State.HALF_OPEN : State.OPEN;
    }

    public Permit tryAcquirePermit() {
        while (true) {
            BreakerState snapshot = breakerState.get();
            State current = state(snapshot);
            if (current == State.CLOSED) return new Permit(snapshot.generation(), false);
            if (current == State.OPEN) return null;
            if (snapshot.probeGeneration() == snapshot.generation()) return null;

            BreakerState claimed = new BreakerState(
                    snapshot.generation(),
                    snapshot.consecutiveFailures(),
                    snapshot.openedAtMs(),
                    snapshot.generation());
            if (breakerState.compareAndSet(snapshot, claimed)) {
                return new Permit(snapshot.generation(), true);
            }
        }
    }

    public void recordSuccess(Permit permit) {
        if (permit == null) return;
        while (true) {
            BreakerState snapshot = breakerState.get();
            if (permit.generation() != snapshot.generation()) return;
            if (permit.probe() && snapshot.probeGeneration() != permit.generation()) return;
            if (!permit.probe() && snapshot.consecutiveFailures() >= failureThreshold) return;
            if (!permit.probe() && snapshot.consecutiveFailures() == 0) return;

            BreakerState recovered = new BreakerState(
                    permit.probe() ? snapshot.generation() + 1 : snapshot.generation(),
                    0,
                    0L,
                    -1L);
            if (breakerState.compareAndSet(snapshot, recovered)) return;
        }
    }

    public void recordFailure(Permit permit) {
        if (permit == null) return;
        while (true) {
            BreakerState snapshot = breakerState.get();
            if (permit.generation() != snapshot.generation()) return;
            if (permit.probe() && snapshot.probeGeneration() != permit.generation()) return;
            if (!permit.probe() && snapshot.consecutiveFailures() >= failureThreshold) return;

            int failures = permit.probe()
                    ? failureThreshold
                    : snapshot.consecutiveFailures() + 1;
            boolean opens = permit.probe() || failures >= failureThreshold;
            BreakerState failed = new BreakerState(
                    opens ? snapshot.generation() + 1 : snapshot.generation(),
                    failures,
                    opens ? clockMs.getAsLong() : snapshot.openedAtMs(),
                    opens ? -1L : snapshot.probeGeneration());
            if (breakerState.compareAndSet(snapshot, failed)) return;
        }
    }

    /**
     * Settles a permit without recording an outcome, for a call that ended without evidence
     * about upstream health (for example a caller that hung up mid-response). A probe permit
     * releases its half-open slot so the next request can probe; the failure count and the
     * generation are left untouched, so this neither opens nor closes the breaker. A non-probe
     * permit holds no slot, so releasing it is a no-op.
     */
    public void releasePermit(Permit permit) {
        if (permit == null || !permit.probe()) return;
        while (true) {
            BreakerState snapshot = breakerState.get();
            if (permit.generation() != snapshot.generation()) return;
            if (snapshot.probeGeneration() != permit.generation()) return;

            BreakerState released = new BreakerState(
                    snapshot.generation(),
                    snapshot.consecutiveFailures(),
                    snapshot.openedAtMs(),
                    -1L);
            if (breakerState.compareAndSet(snapshot, released)) return;
        }
    }

    public int failureCount() {
        return breakerState.get().consecutiveFailures();
    }

    private record BreakerState(
            long generation,
            int consecutiveFailures,
            long openedAtMs,
            long probeGeneration
    ) {}
}
