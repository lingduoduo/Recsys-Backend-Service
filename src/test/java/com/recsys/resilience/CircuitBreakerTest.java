package com.recsys.resilience;

import org.junit.jupiter.api.Test;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import static com.recsys.resilience.CircuitBreaker.State.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CircuitBreakerTest {

    @Test
    void staleClosedSuccessCannotCloseHalfOpenGeneration() {
        AtomicLong clock = new AtomicLong();
        CircuitBreaker cb = new CircuitBreaker(1, 100L, clock::get);

        CircuitBreaker.Permit stale = cb.tryAcquirePermit();
        cb.recordFailure(stale);                 // generation 0 opens generation 1
        clock.set(100L);
        CircuitBreaker.Permit probe = cb.tryAcquirePermit();
        assertThat(probe).isNotNull();

        cb.recordSuccess(stale);                 // late completion from generation 0

        assertThat(cb.state()).isEqualTo(HALF_OPEN);
        assertThat(cb.tryAcquirePermit()).isNull();
        cb.recordFailure(probe);
        assertThat(cb.state()).isEqualTo(OPEN);
    }

    @Test
    void releasingAProbeFreesItsSlotWithoutClosingTheBreaker() {
        AtomicLong clock = new AtomicLong();
        CircuitBreaker cb = new CircuitBreaker(1, 100L, clock::get);

        cb.recordFailure(cb.tryAcquirePermit());
        clock.set(100L);
        CircuitBreaker.Permit probe = cb.tryAcquirePermit();
        assertThat(probe).isNotNull();
        assertThat(cb.tryAcquirePermit()).isNull();   // slot is claimed

        cb.releasePermit(probe);

        assertThat(cb.state()).isEqualTo(HALF_OPEN);  // proved nothing, so still not closed
        assertThat(cb.failureCount()).isEqualTo(1);
        assertThat(cb.tryAcquirePermit()).isNotNull();  // the next request may probe
    }

    @Test
    void releasingAClosedPermitIsANoOpAndReleaseIsIdempotent() {
        AtomicLong clock = new AtomicLong();
        CircuitBreaker cb = new CircuitBreaker(2, 100L, clock::get);

        cb.recordFailure(cb.tryAcquirePermit());
        CircuitBreaker.Permit closedPermit = cb.tryAcquirePermit();
        assertThat(closedPermit).isNotNull();

        cb.releasePermit(closedPermit);
        cb.releasePermit(closedPermit);
        cb.releasePermit(null);

        assertThat(cb.state()).isEqualTo(CLOSED);
        assertThat(cb.failureCount()).isEqualTo(1);   // release neither counts nor clears

        cb.recordFailure(cb.tryAcquirePermit());
        assertThat(cb.state()).isEqualTo(OPEN);
    }

    @Test
    void aStaleReleaseCannotFreeTheCurrentGenerationsProbe() {
        AtomicLong clock = new AtomicLong();
        CircuitBreaker cb = new CircuitBreaker(1, 100L, clock::get);

        cb.recordFailure(cb.tryAcquirePermit());
        clock.set(100L);
        CircuitBreaker.Permit oldProbe = cb.tryAcquirePermit();
        cb.recordFailure(oldProbe);                  // re-opens into a new generation
        clock.set(200L);
        CircuitBreaker.Permit currentProbe = cb.tryAcquirePermit();
        assertThat(currentProbe).isNotNull();

        cb.releasePermit(oldProbe);                  // late arrival from the old generation

        assertThat(cb.tryAcquirePermit()).isNull();  // current probe still owns the slot
        cb.releasePermit(currentProbe);
        assertThat(cb.tryAcquirePermit()).isNotNull();
    }

    @Test
    void onlyOnePermitOwnsHalfOpenProbe() {
        AtomicLong clock = new AtomicLong();
        CircuitBreaker cb = new CircuitBreaker(1, 100L, clock::get);

        CircuitBreaker.Permit initial = cb.tryAcquirePermit();
        cb.recordFailure(initial);
        clock.set(100L);

        assertThat(cb.tryAcquirePermit()).isNotNull();
        assertThat(cb.tryAcquirePermit()).isNull();
    }

    @Test
    void staleFailureCannotReopenClosedRecoveredGeneration() {
        AtomicLong clock = new AtomicLong();
        CircuitBreaker cb = new CircuitBreaker(1, 100L, clock::get);

        CircuitBreaker.Permit stale = cb.tryAcquirePermit();
        cb.recordFailure(stale);
        clock.set(100L);
        CircuitBreaker.Permit probe = cb.tryAcquirePermit();
        cb.recordSuccess(probe);

        cb.recordFailure(stale);

        assertThat(cb.state()).isEqualTo(CLOSED);
        assertThat(cb.failureCount()).isZero();
    }

    @Test
    void currentProbeSuccessClosesAndAllowsNewClosedPermit() {
        AtomicLong clock = new AtomicLong();
        CircuitBreaker cb = new CircuitBreaker(1, 100L, clock::get);

        CircuitBreaker.Permit initial = cb.tryAcquirePermit();
        cb.recordFailure(initial);
        clock.set(100L);
        CircuitBreaker.Permit probe = cb.tryAcquirePermit();
        cb.recordSuccess(probe);

        CircuitBreaker.Permit closed = cb.tryAcquirePermit();
        assertThat(cb.state()).isEqualTo(CLOSED);
        assertThat(closed).isNotNull();
        assertThat(closed.probe()).isFalse();
    }

    @Test
    void acquisitionCannotPublishPermitFromSnapshotInvalidatedByProbeSuccess()
            throws ExecutionException, InterruptedException, TimeoutException {
        InterleavingClock clock = new InterleavingClock();
        CircuitBreaker cb = new CircuitBreaker(1, 100L, clock);
        CircuitBreaker.Permit initial = cb.tryAcquirePermit();
        cb.recordFailure(initial);
        clock.set(100L);
        CircuitBreaker.Permit probe = cb.tryAcquirePermit();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<CircuitBreaker.Permit> racingAcquire = executor.submit(() -> {
                clock.blockCurrentThreadOnNextRead();
                return cb.tryAcquirePermit();
            });
            clock.awaitBlockedRead();

            cb.recordSuccess(probe);
            clock.releaseBlockedRead();

            assertThat(racingAcquire.get(5, TimeUnit.SECONDS)).isNull();
            assertThat(cb.state()).isEqualTo(CLOSED);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void failureCompletionCannotMutateSnapshotAfterCompetingProbeSuccess()
            throws ExecutionException, InterruptedException, TimeoutException {
        InterleavingClock clock = new InterleavingClock();
        CircuitBreaker cb = new CircuitBreaker(1, 100L, clock);
        CircuitBreaker.Permit initial = cb.tryAcquirePermit();
        cb.recordFailure(initial);
        clock.set(100L);
        CircuitBreaker.Permit probe = cb.tryAcquirePermit();

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> racingFailure = executor.submit(() -> {
                clock.blockCurrentThreadOnNextRead();
                cb.recordFailure(probe);
            });
            clock.awaitBlockedRead();

            cb.recordSuccess(probe);
            clock.releaseBlockedRead();
            racingFailure.get(5, TimeUnit.SECONDS);

            assertThat(cb.state()).isEqualTo(CLOSED);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void completedProbeCannotReopenRecoveredGeneration() {
        AtomicLong clock = new AtomicLong();
        CircuitBreaker cb = new CircuitBreaker(1, 100L, clock::get);
        CircuitBreaker.Permit initial = cb.tryAcquirePermit();
        cb.recordFailure(initial);
        clock.set(100L);
        CircuitBreaker.Permit probe = cb.tryAcquirePermit();
        cb.recordSuccess(probe);

        cb.recordFailure(probe);

        assertThat(cb.state()).isEqualTo(CLOSED);
        assertThat(cb.failureCount()).isZero();
    }

    @Test
    void startsClosedAndAllows() {
        CircuitBreaker cb = new CircuitBreaker(3, 10_000L);
        assertThat(cb.state()).isEqualTo(CLOSED);
        assertThat(cb.tryAcquirePermit()).isNotNull();
    }

    @Test
    void opensAtThresholdThenHalfOpensAfterCooldown() {
        AtomicLong clock = new AtomicLong(0L);
        CircuitBreaker cb = new CircuitBreaker(2, 100L, clock::get);
        cb.recordFailure(cb.tryAcquirePermit());
        assertThat(cb.state()).isEqualTo(CLOSED);
        cb.recordFailure(cb.tryAcquirePermit());  // threshold reached
        assertThat(cb.state()).isEqualTo(OPEN);
        assertThat(cb.tryAcquirePermit()).isNull();
        clock.set(100L);                          // exactly cooldown elapsed
        assertThat(cb.state()).isEqualTo(HALF_OPEN);
        assertThat(cb.tryAcquirePermit()).isNotNull(); // single probe wins
        assertThat(cb.tryAcquirePermit()).isNull();    // second concurrent caller fails
    }

    @Test
    void successResetsAndProbeFailureReopens() {
        AtomicLong clock = new AtomicLong(0L);
        CircuitBreaker cb = new CircuitBreaker(1, 50L, clock::get);
        cb.recordFailure(cb.tryAcquirePermit());  // opens (threshold 1)
        assertThat(cb.state()).isEqualTo(OPEN);
        clock.set(50L);
        assertThat(cb.state()).isEqualTo(HALF_OPEN);
        cb.recordSuccess(cb.tryAcquirePermit());
        assertThat(cb.state()).isEqualTo(CLOSED);
        cb.recordFailure(cb.tryAcquirePermit());  // reopen
        clock.set(100L);
        assertThat(cb.state()).isEqualTo(HALF_OPEN);
        cb.recordFailure(cb.tryAcquirePermit());  // probe failed → push window forward
        clock.set(120L);
        assertThat(cb.state()).isEqualTo(OPEN);   // 120 - 100 = 20 < 50 cooldown
    }

    @Test
    void rejectsInvalidArgs() {
        assertThatThrownBy(() -> new CircuitBreaker(0, 10L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("failureThreshold");
        assertThatThrownBy(() -> new CircuitBreaker(1, -1L))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("cooldownMs");
    }

    private static final class InterleavingClock implements LongSupplier {
        private final AtomicLong now = new AtomicLong();
        private final AtomicReference<Thread> blockedThread = new AtomicReference<>();
        private final CyclicBarrier barrier = new CyclicBarrier(2);

        void set(long value) {
            now.set(value);
        }

        void blockCurrentThreadOnNextRead() {
            blockedThread.set(Thread.currentThread());
        }

        void awaitBlockedRead() {
            awaitBarrier();
        }

        void releaseBlockedRead() {
            awaitBarrier();
        }

        @Override
        public long getAsLong() {
            long captured = now.get();
            if (blockedThread.compareAndSet(Thread.currentThread(), null)) {
                awaitBarrier();
                awaitBarrier();
            }
            return captured;
        }

        private void awaitBarrier() {
            try {
                barrier.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            } catch (BrokenBarrierException | TimeoutException e) {
                throw new AssertionError(e);
            }
        }
    }
}
