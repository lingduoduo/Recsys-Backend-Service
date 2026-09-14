package com.recsys.infrastructure.vectordb.spann;

import com.recsys.infrastructure.vectordb.SearchResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures what a reader sees when an update MOVES an id to another posting after that reader has
 * pinned its snapshot — the one interleaving the multi-threaded test cannot force. Blocks the
 * reader inside its first block read, completes the move, then lets the reader finish.
 */
class SpannVectorIndexPinnedReaderTest {

    @TempDir Path dir;

    @Test
    void readerPinnedBeforeAMove_neverSeesTheIdTwice() throws Exception {
        Map<Integer, float[]> data = new LinkedHashMap<>();
        for (int i = 0; i < 10; i++) data.put(i, new float[]{i * 0.1f, 0f});      // blob A
        for (int i = 10; i < 20; i++) data.put(i, new float[]{50f + i * 0.1f, 0f}); // blob B
        SpannConfig cfg = SpannConfig.defaults().withDir(dir).withPostingMin(0).withPostingMax(16)
                .withNprobe(1_000_000).withRegionBytes(1 << 20);

        CountDownLatch reading = new CountDownLatch(1);
        CountDownLatch moved = new CountDownLatch(1);
        AtomicBoolean gate = new AtomicBoolean(true);

        try (SpannVectorIndex idx = new SpannVectorIndex(data, cfg, () -> new PostingStore() {
            private final MappedPostingStore real = new MappedPostingStore(dir, 1 << 20);
            @Override public long append(Block b) { return real.append(b); }
            @Override public Block read(long offset) {
                if (gate.get() && "pinned-reader".equals(Thread.currentThread().getName())) {
                    gate.set(false);
                    reading.countDown();
                    try { moved.await(10, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                }
                return real.read(offset);
            }
            @Override public long bytes() { return real.bytes(); }
            @Override public void close() { real.close(); }
        })) {
            AtomicReference<List<SearchResult>> result = new AtomicReference<>();
            Thread reader = new Thread(() -> result.set(idx.search(new float[]{1f, 0f}, 100, Set.of())), "pinned-reader");
            reader.start();
            assertThat(reading.await(10, TimeUnit.SECONDS)).isTrue();   // pinned, inside its first read

            idx.addOrUpdate(3, new float[]{51.05f, 0f});                // move id 3: blob A -> blob B
            moved.countDown();
            reader.join(10_000);

            List<SearchResult> hits = result.get();
            assertThat(hits).extracting(SearchResult::id).doesNotHaveDuplicates();
            long occurrences = hits.stream().filter(h -> h.id() == 3).count();
            System.out.println("PINNED READER MEASUREMENT: id 3 occurrences = " + occurrences + ", hits = " + hits.size());
            // Measured: blocks are read through the reader's PINNED snapshot/table (fixed at
            // idx.search()'s first `Snapshot s = snap;`), while liveness is checked against the
            // LIVE idMap (a plain ConcurrentHashMap, always current). The move published by
            // addOrUpdate happens entirely between the reader's blocking first read and its second
            // read, so by the time the reader resumes: idMap.get(3) == the new (blob-B) slot. The
            // reader's pinned table still has id 3's old (blob-A) slot pointing at the OLD block
            // (pre-move, offset unchanged in the reader's stale table) — scanning that block finds
            // id 3 there, but the ownership check `idMap.get(3) == oldSlot` now fails (owner moved),
            // so it is correctly skipped as stale. The pinned table's blob-B slot still points at
            // the block from BEFORE the move (the move appended a brand-new block and only the
            // live/published table — which this reader never re-reads — points at it), so id 3
            // never appears in blob-B's posting from this reader's point of view either. Net effect:
            // id 3 is simply ABSENT from this reader's results, not duplicated and not stale-but-
            // present — 0 occurrences.
            assertThat(occurrences).isEqualTo(0L);
        }
    }
}
