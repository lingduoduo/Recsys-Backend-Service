package com.demo.retrieval.measurement;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ThroughputWindowTest {

    @Test
    void reportsNullRateBeforeAnyRequestIsRecorded() {
        Map<String, Object> snapshot = new ThroughputWindow(60, () -> 0L).snapshot();

        assertNull(snapshot.get("qps"));
        assertEquals(0L, snapshot.get("windowRequests"));
        assertEquals(0L, snapshot.get("observedSeconds"));
        assertEquals(60, snapshot.get("windowSeconds"));
    }

    @Test
    void dividesBurstByObservedSecondsRatherThanWindowWidth() {
        AtomicLong clock = new AtomicLong(0L);
        ThroughputWindow window = new ThroughputWindow(60, clock::get);

        for (int second = 0; second < 3; second++) {
            clock.set(second * 1_000L);
            for (int i = 0; i < 10; i++) {
                window.record();
            }
        }

        Map<String, Object> snapshot = window.snapshot();
        assertEquals(30L, snapshot.get("windowRequests"));
        assertEquals(3L, snapshot.get("observedSeconds"));
        assertEquals(10.0, snapshot.get("qps"));
    }

    @Test
    void dropsBucketsThatFallOutOfTheTrailingWindow() {
        AtomicLong clock = new AtomicLong(0L);
        ThroughputWindow window = new ThroughputWindow(60, clock::get);

        for (int i = 0; i < 10; i++) {
            window.record();
        }
        clock.set(120_000L);

        Map<String, Object> snapshot = window.snapshot();
        assertEquals(0L, snapshot.get("windowRequests"));
        assertEquals(60L, snapshot.get("observedSeconds"));
        assertEquals(0.0, snapshot.get("qps"));
    }

    @Test
    void countsEveryRequestUnderConcurrentRecording() throws Exception {
        ThroughputWindow window = new ThroughputWindow(60, () -> 0L);
        ExecutorService pool = Executors.newFixedThreadPool(4);
        CountDownLatch start = new CountDownLatch(1);

        for (int t = 0; t < 4; t++) {
            pool.submit(() -> {
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(e);
                }
                for (int i = 0; i < 1_000; i++) {
                    window.record();
                }
            });
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(4_000L, window.snapshot().get("windowRequests"));
    }
}
