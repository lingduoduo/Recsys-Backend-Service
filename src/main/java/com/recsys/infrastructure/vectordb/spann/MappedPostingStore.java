package com.recsys.infrastructure.vectordb.spann;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * One memory-mapped file per index instance, mapped READ_WRITE in fixed-size regions. The
 * single writer puts bytes through the mapping and readers do absolute gets on the same
 * buffers, so there is no channel-write/page-cache coherence question; the index's
 * {@code volatile} snapshot publish is the happens-before edge that makes a block visible.
 *
 * <p>Regions are never remapped: growth maps the next region and appends it to a
 * copy-on-write list, so a reader holding an earlier region is unaffected. The region list is
 * deliberately kept after {@link #close()} so a reader still holding a pre-close snapshot can
 * finish its reads; the mapped buffers are released by GC once the store object itself becomes
 * unreachable — Java offers no safe explicit unmap.
 */
public final class MappedPostingStore implements PostingStore {

    private final Path path;
    private final int regionBytes;
    private final FileChannel channel;
    private final List<MappedByteBuffer> regions = new CopyOnWriteArrayList<>();
    private long tail;            // written only under the index's writer lock
    private volatile boolean closed;

    public MappedPostingStore(Path dir, int regionBytes) {
        this.regionBytes = regionBytes;
        this.path = dir.resolve("spann-" + ProcessHandle.current().pid() + "-" + System.nanoTime() + ".bin");
        try {
            Files.createDirectories(dir);
            this.channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            path.toFile().deleteOnExit();
        } catch (IOException e) {
            throw new UncheckedIOException("cannot create SPANN index file " + path, e);
        }
    }

    public Path path() {
        return path;
    }

    @Override
    public long append(Block block) {
        if (closed) throw new IllegalStateException("posting store is closed: " + path);
        int dim = block.count() == 0 ? 0 : block.vectors()[0].length;
        int size = PostingStore.encodedBytes(block.count(), dim);
        if (size > regionBytes) {
            throw new IllegalArgumentException("block of " + size + " bytes exceeds region size " + regionBytes);
        }
        long regionStart = (tail / regionBytes) * regionBytes;
        if (tail + size > regionStart + regionBytes) {
            tail = regionStart + regionBytes;            // pad to next region; a block never straddles
        }
        int regionIdx = (int) (tail / regionBytes);
        while (regions.size() <= regionIdx) mapRegion(regions.size());
        MappedByteBuffer buf = regions.get(regionIdx);
        int pos = (int) (tail - (long) regionIdx * regionBytes);
        long offset = tail;
        buf.putInt(pos, block.count());
        buf.putInt(pos + 4, dim);
        int p = pos + 8;
        for (int i = 0; i < block.count(); i++) {
            buf.putInt(p, block.ids()[i]);
            p += 4;
            float[] v = block.vectors()[i];
            for (int d = 0; d < dim; d++) { buf.putFloat(p, v[d]); p += 4; }
        }
        tail = align8(tail + size);
        return offset;
    }

    @Override
    public Block read(long offset) {
        int regionIdx = (int) (offset / regionBytes);
        MappedByteBuffer buf = regions.get(regionIdx);
        int pos = (int) (offset - (long) regionIdx * regionBytes);
        int count = buf.getInt(pos);
        int dim = buf.getInt(pos + 4);
        int[] ids = new int[count];
        float[][] vectors = new float[count][];
        int p = pos + 8;
        for (int i = 0; i < count; i++) {
            ids[i] = buf.getInt(p);
            p += 4;
            float[] v = new float[dim];
            for (int d = 0; d < dim; d++) { v[d] = buf.getFloat(p); p += 4; }
            vectors[i] = v;
        }
        return new Block(ids, vectors);
    }

    @Override
    public long bytes() {
        return tail;
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        try {
            channel.close();
        } catch (IOException ignored) {
            // nothing to do: the file is deleted below either way
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot delete SPANN index file " + path, e);
        }
    }

    private void mapRegion(int idx) {
        try {
            regions.add(channel.map(FileChannel.MapMode.READ_WRITE, (long) idx * regionBytes, regionBytes));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot map region " + idx + " of " + path, e);
        }
    }

    private static long align8(long v) {
        return (v + 7) & ~7L;
    }
}
