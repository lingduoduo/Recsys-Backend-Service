package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.SearchResult;
import com.recsys.infrastructure.vectordb.VectorIndex;
import com.recsys.infrastructure.vectordb.VectorMath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 * SPANN-style disk-resident index: centroids in heap, posting lists as immutable blocks in a
 * {@link PostingStore}, SPFresh-style in-place updates. See
 * docs/superpowers/specs/2026-09-13-spann-spfresh-vector-index-design.md.
 *
 * <p>Liveness rule: an entry read from centroid slot {@code c} is live iff {@code idMap.get(id) == c}.
 * Readers pin one {@link Snapshot} (table + store) per search and never block; the single writer
 * publishes a new snapshot after every step and flips {@code idMap} as the linearisation point.
 */
public final class SpannVectorIndex implements VectorIndex, Closeable {

    private static final Logger log = LoggerFactory.getLogger(SpannVectorIndex.class);
    private static final int SAMPLE_LIMIT = 20_000;
    private static final int BUILD_ITERATIONS = 10;
    private static final int SPLIT_ITERATIONS = 10;

    /** Heap head is the worst retained hit: lowest score, then highest id (cf. ExactMultiVectorIndex). */
    static final Comparator<SearchResult> WORST_FIRST =
            Comparator.comparingDouble(SearchResult::score).thenComparing(SearchResult::id, Comparator.reverseOrder());

    record Snapshot(CentroidTable table, PostingStore store) {}

    private final SpannConfig cfg;
    private final Supplier<PostingStore> storeFactory;
    private final ReentrantLock writer = new ReentrantLock();
    private final ConcurrentHashMap<Integer, Integer> idMap = new ConcurrentHashMap<>();
    private final LongAdder distanceComputations = new LongAdder();
    private final AtomicLong splits = new AtomicLong();
    private final AtomicLong merges = new AtomicLong();
    private final AtomicLong reassigned = new AtomicLong();
    private final AtomicLong compactions = new AtomicLong();
    private final AtomicLong fallbackWidenings = new AtomicLong();
    private volatile Snapshot snap;
    private volatile int dim;
    private volatile long deadBytes;       // written under the writer lock only
    private volatile boolean closed;

    public SpannVectorIndex(Map<Integer, float[]> embeddings, SpannConfig cfg) {
        this(embeddings, cfg, () -> new MappedPostingStore(cfg.dir(), cfg.regionBytes()));
    }

    public SpannVectorIndex(Map<Integer, float[]> embeddings, SpannConfig cfg, Supplier<PostingStore> storeFactory) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.storeFactory = Objects.requireNonNull(storeFactory, "storeFactory");
        PostingStore store = storeFactory.get();
        try {
            this.snap = new Snapshot(build(embeddings, store), store);
            rebalanceAfterBuild();
        } catch (RuntimeException e) {
            store.close();
            throw e;
        }
        log.info("SPANN index built: {} entries, {} centroids, {} bytes on disk", idMap.size(),
                snap.table().aliveCount(), store.bytes());
    }

    // ---------------------------------------------------------------- build

    private CentroidTable build(Map<Integer, float[]> embeddings, PostingStore store) {
        CentroidTable table = CentroidTable.empty();
        if (embeddings.isEmpty()) {
            dim = 0;
            return table;
        }
        List<Integer> ids = new ArrayList<>(embeddings.keySet());
        Collections.sort(ids);
        int d = embeddings.get(ids.get(0)).length;
        for (int id : ids) {
            float[] v = embeddings.get(id);
            if (v == null || v.length != d) {
                throw new IllegalArgumentException("vector dimension mismatch for id " + id + ": expected " + d
                        + ", got " + (v == null ? "null" : v.length));
            }
        }
        dim = d;
        int n = ids.size();
        int k = Math.max(1, (int) Math.ceil(n / (cfg.postingMax() / 2.0)));
        List<float[]> sample = new ArrayList<>();
        if (n <= SAMPLE_LIMIT) {
            for (int id : ids) sample.add(embeddings.get(id));
        } else {
            List<Integer> shuffled = new ArrayList<>(ids);
            Collections.shuffle(shuffled, new Random(cfg.seed()));
            for (int i = 0; i < SAMPLE_LIMIT; i++) sample.add(embeddings.get(shuffled.get(i)));
        }
        float[][] centroids = KMeans.cluster(sample, k, cfg.seed(), BUILD_ITERATIONS);
        List<List<Integer>> members = new ArrayList<>(centroids.length);
        for (int c = 0; c < centroids.length; c++) members.add(new ArrayList<>());
        for (int id : ids) {
            members.get(KMeans.nearest(embeddings.get(id), centroids, null)).add(id);
        }
        distanceComputations.add((long) n * centroids.length);
        for (int c = 0; c < centroids.length; c++) {
            List<Integer> m = members.get(c);
            if (m.isEmpty()) continue;
            int[] bid = new int[m.size()];
            float[][] bvec = new float[m.size()][];
            for (int i = 0; i < m.size(); i++) {
                bid[i] = m.get(i);
                bvec[i] = embeddings.get(m.get(i));
            }
            int slot = table.size();
            long off = store.append(new PostingStore.Block(bid, bvec));
            table = table.withAdded(centroids[c], off, m.size());
            for (int id : m) idMap.put(id, slot);
        }
        return table;
    }

    // ---------------------------------------------------------------- search

    @Override
    public List<SearchResult> search(float[] query, int k, Set<Integer> excludeIds) {
        Snapshot s = snap;
        if (closed || s == null || query == null || k <= 0 || s.table().aliveCount() == 0) return List.of();
        if (query.length != dim) return List.of();
        Set<Integer> excluded = Objects.requireNonNullElse(excludeIds, Set.of());
        CentroidTable t = s.table();
        int[] slots = t.aliveSlots();
        // Probe by the SCORING metric (inner product), as FAISS's inner-product IVF does: the
        // partition is L2 (k-means), but the postings holding the highest <query, entry> are the
        // ones whose centroids have the highest <query, centroid>. L2-nearest postings would miss
        // them — see the spec's Search section.
        double[] ip = new double[slots.length];
        for (int i = 0; i < slots.length; i++) ip[i] = VectorMath.innerProduct(query, t.centroid(slots[i]));
        distanceComputations.add(slots.length);
        Integer[] order = new Integer[slots.length];
        for (int i = 0; i < slots.length; i++) order[i] = i;
        Arrays.sort(order, (a, b) -> Double.compare(ip[b], ip[a]));          // descending

        PriorityQueue<SearchResult> best = new PriorityQueue<>(WORST_FIRST);
        Set<Integer> seen = new HashSet<>();
        int scanned = 0;
        int probe = Math.min(cfg.nprobe(), slots.length);
        while (true) {
            for (; scanned < probe; scanned++) {
                scanBlock(s, slots[order[scanned]], query, k, excluded, seen, best);
            }
            if (best.size() >= k || probe == slots.length) break;
            probe = Math.min(probe * 2, slots.length);                          // widening fallback
            fallbackWidenings.incrementAndGet();
        }
        List<SearchResult> results = new ArrayList<>(best);
        results.sort(WORST_FIRST.reversed());
        return results;
    }

    private void scanBlock(Snapshot s, int slot, float[] query, int k, Set<Integer> excluded,
                           Set<Integer> seen, PriorityQueue<SearchResult> best) {
        // Read through the pinned snapshot: its offset and its store were published together.
        PostingStore.Block b = s.store().read(s.table().offset(slot));
        for (int i = 0; i < b.count(); i++) {
            int id = b.ids()[i];
            if (excluded.contains(id)) continue;
            Integer owner = idMap.get(id);
            if (owner == null || owner != slot) continue;      // stale: superseded or moved
            if (!seen.add(id)) continue;                        // observed in another block mid-move
            double score = VectorMath.innerProduct(query, b.vectors()[i]);
            distanceComputations.increment();
            if (score == Double.NEGATIVE_INFINITY) continue;
            SearchResult hit = new SearchResult(id, score);
            if (best.size() < k) {
                best.offer(hit);
            } else if (WORST_FIRST.compare(hit, best.peek()) > 0) {
                best.poll();
                best.offer(hit);
            }
        }
    }

    // ---------------------------------------------------------------- SPFresh: insert / overwrite

    @Override
    public void addOrUpdate(int id, float[] vec) {
        if (vec == null) throw new IllegalArgumentException("vector must not be null");
        writer.lock();
        try {
            if (closed) throw new IllegalStateException("SPANN index is closed");
            Snapshot s = snap;
            if (s.table().aliveCount() == 0) {
                dim = vec.length;
                long off = s.store().append(new PostingStore.Block(new int[]{id}, new float[][]{vec.clone()}));
                CentroidTable t = s.table().withAdded(vec.clone(), off, 1);
                publish(new Snapshot(t, s.store()));
                idMap.put(id, t.size() - 1);
                return;
            }
            if (vec.length != dim) {
                throw new IllegalArgumentException("vector dimension mismatch: expected " + dim + ", got " + vec.length);
            }
            Integer old = idMap.get(id);
            int target = s.table().nearest(vec);
            distanceComputations.add(s.table().aliveCount());

            List<int[]> ids = new ArrayList<>();
            List<float[]> vecs = new ArrayList<>();
            int liveBefore = collectLive(s, target, id, ids, vecs);
            ids.add(new int[]{id});
            vecs.add(vec.clone());
            long off = s.store().append(block(ids, vecs));
            CentroidTable t = s.table().withRepointed(target, off, ids.size());
            deadBytes += 8L + (long) liveBefore * entryBytes(dim);       // the old block is unreferenced now
            if (old != null && old != target) {
                t = t.withLive(old, t.live(old) - 1);
                deadBytes += entryBytes(dim);                            // stale entry left inside old's block
            }
            publish(new Snapshot(t, s.store()));
            idMap.put(id, target);                                      // linearisation point
            if (t.live(target) > cfg.postingMax()) {
                t = split(snap, t, target, true);
            }
            if (old != null && old != target) {
                t = mergeIfUnderfull(snap, t, old);
            }
        } finally {
            writer.unlock();
        }
    }

    /**
     * Collects the live entries of {@code slot} (those the id map attributes to it), skipping
     * {@code skipId}. Returns the number of entries that were live before the skip, i.e. the
     * live count the old block carried, for dead-bytes accounting.
     */
    private int collectLive(Snapshot s, int slot, int skipId, List<int[]> ids, List<float[]> vecs) {
        PostingStore.Block b = s.store().read(s.table().offset(slot));
        int liveBefore = 0;
        for (int i = 0; i < b.count(); i++) {
            int eid = b.ids()[i];
            Integer owner = idMap.get(eid);
            if (owner == null || owner != slot) continue;
            liveBefore++;
            if (eid == skipId) continue;
            ids.add(new int[]{eid});
            vecs.add(b.vectors()[i]);
        }
        return liveBefore;
    }

    private static PostingStore.Block block(List<int[]> ids, List<float[]> vecs) {
        int[] bid = new int[ids.size()];
        float[][] bvec = new float[ids.size()][];
        for (int i = 0; i < ids.size(); i++) {
            bid[i] = ids.get(i)[0];
            bvec[i] = vecs.get(i);
        }
        return new PostingStore.Block(bid, bvec);
    }

    private void publish(Snapshot s) {
        snap = s;
    }

    // ---------------------------------------------------------------- SPFresh: split / reassign

    private void rebalanceAfterBuild() {
        CentroidTable t = snap.table();
        for (int slot = 0; slot < t.size(); slot++) {
            if (t.alive(slot) && t.live(slot) > cfg.postingMax()) {
                t = split(snap, t, slot, false);
            }
        }
    }

    /**
     * 2-means the live entries of {@code slot} into two new centroids, then reassigns the
     * neighbourhood. Returns the table in service afterwards. A posting whose entries cannot be
     * separated (fewer than two live, or all identical) is left as is.
     */
    private CentroidTable split(Snapshot s, CentroidTable t, int slot, boolean cascade) {
        List<int[]> ids = new ArrayList<>();
        List<float[]> vecs = new ArrayList<>();
        collectLive(new Snapshot(t, s.store()), slot, Integer.MIN_VALUE, ids, vecs);
        // MIN_VALUE is never an id in practice; collectLive's skip is a no-op here.
        if (ids.size() < 2) return t;
        float[][] c = KMeans.cluster(vecs, 2, cfg.seed(), SPLIT_ITERATIONS);
        if (c.length < 2) return t;
        List<int[]> idsA = new ArrayList<>(), idsB = new ArrayList<>();
        List<float[]> vecA = new ArrayList<>(), vecB = new ArrayList<>();
        for (int i = 0; i < ids.size(); i++) {
            if (KMeans.l2sq(vecs.get(i), c[0]) <= KMeans.l2sq(vecs.get(i), c[1])) { idsA.add(ids.get(i)); vecA.add(vecs.get(i)); }
            else { idsB.add(ids.get(i)); vecB.add(vecs.get(i)); }
        }
        distanceComputations.add(2L * ids.size());
        if (idsA.isEmpty() || idsB.isEmpty()) return t;

        long offA = s.store().append(block(idsA, vecA));
        long offB = s.store().append(block(idsB, vecB));
        t = t.withAdded(c[0], offA, idsA.size()).withAdded(c[1], offB, idsB.size());
        int a = t.size() - 2, b = t.size() - 1;
        publish(new Snapshot(t, s.store()));                            // old slot still referenced
        for (int[] e : idsA) idMap.put(e[0], a);
        for (int[] e : idsB) idMap.put(e[0], b);
        t = t.withKilled(slot);
        deadBytes += 8L + (long) ids.size() * entryBytes(dim);
        publish(new Snapshot(t, s.store()));
        splits.incrementAndGet();
        log.info("SPANN split: slot {} ({} entries) -> {} ({}) + {} ({})", slot, ids.size(), a, idsA.size(), b, idsB.size());
        return reassign(s, t, a, b, cascade);
    }

    /**
     * SPFresh's bounded rebalancing: only the postings of the {@code reassignProbe} centroids
     * nearest to each new centroid are examined; an entry moves iff its nearest live centroid
     * is now one of the two new ones. A far-away entry in a now-suboptimal posting stays until
     * it is next touched — that is the trade, and 13_DB_Indexing §5 names it.
     */
    private CentroidTable reassign(Snapshot s, CentroidTable t, int a, int b, boolean cascade) {
        if (cfg.reassignProbe() == 0) return t;
        Set<Integer> neighbours = new java.util.LinkedHashSet<>();
        for (int n : t.nearestN(t.centroid(a), cfg.reassignProbe(), a, b)) neighbours.add(n);
        for (int n : t.nearestN(t.centroid(b), cfg.reassignProbe(), a, b)) neighbours.add(n);
        for (int n : neighbours) {
            if (!t.alive(n)) continue;
            List<int[]> ids = new ArrayList<>();
            List<float[]> vecs = new ArrayList<>();
            collectLive(new Snapshot(t, s.store()), n, Integer.MIN_VALUE, ids, vecs);
            List<int[]> stayIds = new ArrayList<>(), toA = new ArrayList<>(), toB = new ArrayList<>();
            List<float[]> stayVec = new ArrayList<>(), vecToA = new ArrayList<>(), vecToB = new ArrayList<>();
            for (int i = 0; i < ids.size(); i++) {
                int nearest = t.nearest(vecs.get(i));
                if (nearest == a) { toA.add(ids.get(i)); vecToA.add(vecs.get(i)); }
                else if (nearest == b) { toB.add(ids.get(i)); vecToB.add(vecs.get(i)); }
                else { stayIds.add(ids.get(i)); stayVec.add(vecs.get(i)); }
            }
            distanceComputations.add((long) ids.size() * t.aliveCount());
            if (toA.isEmpty() && toB.isEmpty()) continue;

            // Destination blocks first (moved entries are stale there until the flip)…
            if (!toA.isEmpty()) t = appendTo(s, t, a, toA, vecToA);
            if (!toB.isEmpty()) t = appendTo(s, t, b, toB, vecToB);
            long offStay = s.store().append(block(stayIds, stayVec));
            publish(new Snapshot(t, s.store()));                        // n still points at its old block
            for (int[] e : toA) idMap.put(e[0], a);
            for (int[] e : toB) idMap.put(e[0], b);
            deadBytes += 8L + (long) ids.size() * entryBytes(dim);
            t = t.withRepointed(n, offStay, stayIds.size());
            publish(new Snapshot(t, s.store()));
            reassigned.addAndGet(toA.size() + toB.size());
            if (cascade && stayIds.size() < cfg.postingMin()) {
                t = mergeIfUnderfull(s, t, n);
            }
        }
        return t;
    }

    /** Rewrites {@code slot}'s block as its current live entries plus {@code extra}; no id-map change. */
    private CentroidTable appendTo(Snapshot s, CentroidTable t, int slot, List<int[]> extraIds, List<float[]> extraVecs) {
        List<int[]> ids = new ArrayList<>();
        List<float[]> vecs = new ArrayList<>();
        int liveBefore = collectLive(new Snapshot(t, s.store()), slot, Integer.MIN_VALUE, ids, vecs);
        ids.addAll(extraIds);
        vecs.addAll(extraVecs);
        long off = s.store().append(block(ids, vecs));
        deadBytes += 8L + (long) liveBefore * entryBytes(dim);
        return t.withRepointed(slot, off, ids.size());
    }

    /**
     * Merges an underfull posting into its nearest live neighbour. The destination centroid's
     * vector is left unchanged (as SPFresh does). If the destination overflows it is split once,
     * without cascading merges — a second overflow waits for the next update.
     */
    private CentroidTable mergeIfUnderfull(Snapshot s, CentroidTable t, int slot) {
        if (!t.alive(slot) || t.live(slot) >= cfg.postingMin() || t.aliveCount() <= 1) return t;
        List<int[]> ids = new ArrayList<>();
        List<float[]> vecs = new ArrayList<>();
        collectLive(new Snapshot(t, s.store()), slot, Integer.MIN_VALUE, ids, vecs);
        int[] nearest = t.nearestN(t.centroid(slot), 1, slot);
        if (nearest.length == 0) return t;
        int dest = nearest[0];
        distanceComputations.add(t.aliveCount());
        t = appendTo(s, t, dest, ids, vecs);
        publish(new Snapshot(t, s.store()));                            // slot still referenced
        for (int[] e : ids) idMap.put(e[0], dest);
        t = t.withKilled(slot);
        deadBytes += 8L + (long) ids.size() * entryBytes(dim);
        publish(new Snapshot(t, s.store()));
        merges.incrementAndGet();
        log.info("SPANN merge: slot {} ({} entries) -> {} (now {})", slot, ids.size(), dest, t.live(dest));
        if (t.live(dest) > cfg.postingMax()) {
            t = split(s, t, dest, false);
        }
        return t;
    }

    // ---------------------------------------------------------------- misc

    @Override
    public String name() {
        return "spann";
    }

    public int dimension() {
        return dim;
    }

    public SpannStats stats() {
        Snapshot s = snap;
        CentroidTable t = s == null ? CentroidTable.empty() : s.table();
        long bytes = s == null ? 0 : s.store().bytes();
        return new SpannStats(t.aliveCount(), t.size(), idMap.size(), bytes, deadBytes, splits.get(), merges.get(),
                reassigned.get(), compactions.get(), distanceComputations.sum(), fallbackWidenings.get());
    }

    @Override
    public void close() {
        writer.lock();
        try {
            if (closed) return;
            closed = true;
            Snapshot s = snap;
            snap = null;
            idMap.clear();
            if (s != null) s.store().close();
        } finally {
            writer.unlock();
        }
    }

    private static int entryBytes(int dim) {
        return 4 + 4 * dim;
    }
}
