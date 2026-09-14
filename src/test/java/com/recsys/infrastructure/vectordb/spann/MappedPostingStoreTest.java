package com.recsys.infrastructure.vectordb.spann;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MappedPostingStoreTest {

    @TempDir Path dir;

    private static PostingStore.Block block(int... ids) {
        float[][] v = new float[ids.length][];
        for (int i = 0; i < ids.length; i++) v[i] = new float[]{ids[i], ids[i] * 0.5f, -ids[i]};
        return new PostingStore.Block(ids, v);
    }

    @Test
    void appendThenRead_roundTrips() {
        try (MappedPostingStore s = new MappedPostingStore(dir, 4096)) {
            long off = s.append(block(1, 2, 3));
            PostingStore.Block b = s.read(off);
            assertThat(b.ids()).containsExactly(1, 2, 3);
            assertThat(b.vectors()[1]).containsExactly(2f, 1f, -2f);
            assertThat(s.bytes()).isEqualTo(PostingStore.encodedBytes(3, 3));
        }
    }

    @Test
    void blocksAreEightByteAligned() {
        try (MappedPostingStore s = new MappedPostingStore(dir, 4096)) {
            s.append(block(1));                       // 8 + 16 = 24 bytes
            long second = s.append(block(2));
            assertThat(second % 8).isZero();
            assertThat(second).isEqualTo(24);
        }
    }

    @Test
    void blockNeverStraddlesARegion() {
        // region 4096; each 3-dim block of 100 entries is 8 + 100*16 = 1608 bytes; the third would
        // cross the boundary at 4096, so it must start at 4096.
        try (MappedPostingStore s = new MappedPostingStore(dir, 4096)) {
            int[] ids = new int[100];
            for (int i = 0; i < 100; i++) ids[i] = i;
            s.append(block(ids));
            s.append(block(ids));
            long third = s.append(block(ids));
            assertThat(third).isEqualTo(4096);
            assertThat(s.read(third).ids()).hasSize(100);
            assertThat(s.bytes()).isEqualTo(4096 + 1608);
        }
    }

    @Test
    void blockLargerThanARegionIsRejected() {
        try (MappedPostingStore s = new MappedPostingStore(dir, 4096)) {
            int[] ids = new int[300];
            assertThatThrownBy(() -> s.append(block(ids))).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void emptyBlockRoundTrips() {
        try (MappedPostingStore s = new MappedPostingStore(dir, 4096)) {
            long off = s.append(new PostingStore.Block(new int[0], new float[0][]));
            assertThat(s.read(off).count()).isZero();
        }
    }

    @Test
    void fileExistsWhileOpenAndIsDeletedOnClose() {
        MappedPostingStore s = new MappedPostingStore(dir, 4096);
        Path p = s.path();
        assertThat(Files.exists(p)).isTrue();
        assertThat(p.getFileName().toString()).startsWith("spann-").endsWith(".bin");
        s.close();
        assertThat(Files.exists(p)).isFalse();
        s.close(); // idempotent
    }

    @Test
    void appendAfterCloseThrows() {
        MappedPostingStore s = new MappedPostingStore(dir, 4096);
        s.close();
        assertThatThrownBy(() -> s.append(block(1))).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void readAfterClose_stillServesMappedBlocks() {
        // A reader pinned on a pre-compaction snapshot may read after the index has closed this
        // store; the mapped buffers outlive the channel and the directory entry.
        MappedPostingStore s = new MappedPostingStore(dir, 4096);
        long off = s.append(block(4, 5, 6));
        s.close();
        PostingStore.Block b = s.read(off);
        assertThat(b.ids()).containsExactly(4, 5, 6);
        assertThat(b.vectors()[2]).containsExactly(6f, 3f, -6f);
    }
}
