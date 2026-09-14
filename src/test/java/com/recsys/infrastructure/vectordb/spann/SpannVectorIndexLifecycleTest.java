package com.recsys.infrastructure.vectordb.spann;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannVectorIndexLifecycleTest {

    @TempDir Path dir;

    @Test
    void fileExistsWhileOpen_andIsGoneAfterClose() throws Exception {
        SpannConfig cfg = SpannConfig.defaults().withDir(dir).withRegionBytes(1 << 20);
        SpannVectorIndex idx = new SpannVectorIndex(Map.of(1, new float[]{1f, 0f}, 2, new float[]{0f, 1f}), cfg);
        assertThat(Files.list(dir).count()).isEqualTo(1);
        idx.close();
        assertThat(Files.list(dir).count()).isZero();
        assertThat(idx.search(new float[]{1f, 0f}, 1, Set.of())).isEmpty();
        assertThatThrownBy(() -> idx.addOrUpdate(3, new float[]{1f, 1f})).isInstanceOf(IllegalStateException.class);
        idx.close(); // idempotent
    }

    @Test
    void closeViaTheVectorIndexInterface_isTheSameClose() throws Exception {
        SpannConfig cfg = SpannConfig.defaults().withDir(dir).withRegionBytes(1 << 20);
        com.recsys.infrastructure.vectordb.VectorIndex idx = new SpannVectorIndex(Map.of(1, new float[]{1f}), cfg);
        idx.close();
        assertThat(Files.list(dir).count()).isZero();
    }
}
