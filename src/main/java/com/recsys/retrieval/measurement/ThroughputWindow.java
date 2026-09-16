package com.demo.retrieval.measurement;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * Request rate over a trailing window, as a ring of one-second counters.
 *
 * Rate is reported over the time actually observed rather than the full window
 * width: a three-second burst divided by a sixty-second window would understate
 * throughput by twenty times. {@code observedSeconds} publishes that denominator
 * so a reader can see how thin the support is.
 */
final class ThroughputWindow {

    private static final long NO_REQUESTS = Long.MIN_VALUE;

    private final int windowSeconds;
    private final LongSupplier nowMillis;
    private final long[] counts;
    private final long[] seconds;
    private long firstSecond = NO_REQUESTS;

    ThroughputWindow(int windowSeconds, LongSupplier nowMillis) {
        this.windowSeconds = windowSeconds;
        this.nowMillis = nowMillis;
        this.counts = new long[windowSeconds];
        this.seconds = new long[windowSeconds];
        Arrays.fill(this.seconds, NO_REQUESTS);
    }

    synchronized void record() {
        long second = currentSecond();
        int slot = slotFor(second);
        if (seconds[slot] != second) {
            seconds[slot] = second;
            counts[slot] = 0L;
        }
        counts[slot]++;
        if (firstSecond == NO_REQUESTS) {
            firstSecond = second;
        }
    }

    synchronized Map<String, Object> snapshot() {
        long second = currentSecond();
        long cutoff = second - windowSeconds + 1;
        long requests = 0L;
        for (int slot = 0; slot < windowSeconds; slot++) {
            if (seconds[slot] >= cutoff && seconds[slot] <= second) {
                requests += counts[slot];
            }
        }
        long observed = firstSecond == NO_REQUESTS
            ? 0L
            : Math.min(windowSeconds, second - firstSecond + 1);

        Map<String, Object> values = new LinkedHashMap<>();
        values.put("qps", observed <= 0 ? null : round((double) requests / observed));
        values.put("windowRequests", requests);
        values.put("windowSeconds", windowSeconds);
        values.put("observedSeconds", observed);
        return values;
    }

    private long currentSecond() {
        return Math.floorDiv(nowMillis.getAsLong(), 1_000L);
    }

    private int slotFor(long second) {
        return (int) Math.floorMod(second, (long) windowSeconds);
    }

    private static Double round(double value) {
        return Math.round(value * 10_000.0) / 10_000.0;
    }
}
