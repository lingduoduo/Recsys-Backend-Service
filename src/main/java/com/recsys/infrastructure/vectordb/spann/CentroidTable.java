package com.recsys.infrastructure.vectordb.spann;

import java.util.Arrays;

/**
 * Immutable in-heap centroid arrays. Every mutator copies the (small) arrays and returns a new
 * table; the index publishes tables through one {@code volatile} reference so a reader sees a
 * consistent set of offsets. Slot numbers are stable for the life of the index — dead slots
 * are never reused or renumbered, because the concurrent id map stores slot numbers and
 * renumbering would make a reader's liveness test lie during the swap.
 */
final class CentroidTable {

    private final float[][] centroid;
    private final long[] offset;
    private final int[] live;
    private final boolean[] alive;
    private final int size;
    private final int aliveCount;

    private CentroidTable(float[][] centroid, long[] offset, int[] live, boolean[] alive, int size, int aliveCount) {
        this.centroid = centroid;
        this.offset = offset;
        this.live = live;
        this.alive = alive;
        this.size = size;
        this.aliveCount = aliveCount;
    }

    static CentroidTable empty() {
        return new CentroidTable(new float[8][], new long[8], new int[8], new boolean[8], 0, 0);
    }

    int size() { return size; }
    int aliveCount() { return aliveCount; }
    boolean alive(int slot) { return alive[slot]; }
    float[] centroid(int slot) { return centroid[slot]; }
    long offset(int slot) { return offset[slot]; }
    int live(int slot) { return live[slot]; }

    int[] aliveSlots() {
        int[] out = new int[aliveCount];
        int j = 0;
        for (int i = 0; i < size; i++) if (alive[i]) out[j++] = i;
        return out;
    }

    CentroidTable withAdded(float[] c, long off, int liveCount) {
        int cap = centroid.length;
        int newCap = size == cap ? cap * 2 : cap;
        float[][] nc = Arrays.copyOf(centroid, newCap);
        long[] no = Arrays.copyOf(offset, newCap);
        int[] nl = Arrays.copyOf(live, newCap);
        boolean[] na = Arrays.copyOf(alive, newCap);
        nc[size] = c;
        no[size] = off;
        nl[size] = liveCount;
        na[size] = true;
        return new CentroidTable(nc, no, nl, na, size + 1, aliveCount + 1);
    }

    CentroidTable withRepointed(int slot, long off, int liveCount) {
        long[] no = offset.clone();
        int[] nl = live.clone();
        no[slot] = off;
        nl[slot] = liveCount;
        return new CentroidTable(centroid, no, nl, alive, size, aliveCount);
    }

    CentroidTable withLive(int slot, int liveCount) {
        int[] nl = live.clone();
        nl[slot] = liveCount;
        return new CentroidTable(centroid, offset, nl, alive, size, aliveCount);
    }

    CentroidTable withKilled(int slot) {
        if (!alive[slot]) return this;
        boolean[] na = alive.clone();
        int[] nl = live.clone();
        // Copy-on-write, never an in-place write: other published tables share this array, and
        // mutating it in place would corrupt a pinned reader's view.
        float[][] nc = centroid.clone();
        na[slot] = false;
        nl[slot] = 0;
        nc[slot] = null;
        return new CentroidTable(nc, offset, nl, na, size, aliveCount - 1);
    }

    int nearest(float[] q) {
        int best = -1;
        double bestD = Double.POSITIVE_INFINITY;
        for (int i = 0; i < size; i++) {
            if (!alive[i]) continue;
            double d = KMeans.l2sq(q, centroid[i]);
            if (d < bestD) { bestD = d; best = i; }
        }
        return best;
    }

    int[] nearestN(float[] q, int n, int... exclude) {
        int[] slots = aliveSlots();
        double[] d = distancesToAlive(q, slots);
        Integer[] order = new Integer[slots.length];
        for (int i = 0; i < slots.length; i++) order[i] = i;
        Arrays.sort(order, (a, b) -> Double.compare(d[a], d[b]));
        int[] out = new int[Math.min(n, slots.length)];
        int j = 0;
        for (Integer i : order) {
            if (j == out.length) break;
            int slot = slots[i];
            boolean excluded = false;
            for (int e : exclude) if (e == slot) { excluded = true; break; }
            if (!excluded) out[j++] = slot;
        }
        return j == out.length ? out : Arrays.copyOf(out, j);
    }

    double[] distancesToAlive(float[] q, int[] aliveSlots) {
        double[] d = new double[aliveSlots.length];
        for (int i = 0; i < aliveSlots.length; i++) d[i] = KMeans.l2sq(q, centroid[aliveSlots[i]]);
        return d;
    }
}
