package com.recsys.infrastructure.vectordb.spann;

import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SpannConfigTest {

    @Test
    void defaults_matchTheSpecTable() {
        SpannConfig c = SpannConfig.defaults();
        assertThat(c.dir()).isEqualTo(Path.of(System.getProperty("java.io.tmpdir")));
        assertThat(c.postingMax()).isEqualTo(128);
        assertThat(c.postingMin()).isEqualTo(16);
        assertThat(c.nprobe()).isEqualTo(128);
        assertThat(c.reassignProbe()).isEqualTo(4);
        assertThat(c.compactRatio()).isEqualTo(0.5);
        assertThat(c.seed()).isEqualTo(42L);
        assertThat(c.regionBytes()).isEqualTo(64 * 1024 * 1024);
    }

    @Test
    void fromEnv_readsEveryVariable() {
        Map<String, String> env = Map.of(
                "RECSYS_SPANN_DIR", "/tmp/spann-test",
                "RECSYS_SPANN_POSTING_MAX", "64",
                "RECSYS_SPANN_POSTING_MIN", "8",
                "RECSYS_SPANN_NPROBE", "3",
                "RECSYS_SPANN_REASSIGN_PROBE", "2",
                "RECSYS_SPANN_COMPACT_RATIO", "0.4",
                "RECSYS_SPANN_SEED", "7");
        SpannConfig c = SpannConfig.fromEnv(env::get);
        assertThat(c.dir()).isEqualTo(Path.of("/tmp/spann-test"));
        assertThat(c.postingMax()).isEqualTo(64);
        assertThat(c.postingMin()).isEqualTo(8);
        assertThat(c.nprobe()).isEqualTo(3);
        assertThat(c.reassignProbe()).isEqualTo(2);
        assertThat(c.compactRatio()).isEqualTo(0.4);
        assertThat(c.seed()).isEqualTo(7L);
    }

    @Test
    void fromEnv_blankMeansDefault() {
        assertThat(SpannConfig.fromEnv(name -> null)).isEqualTo(SpannConfig.defaults());
    }

    @Test
    void postingMin_mustBeBelowHalfOfPostingMax() {
        // 8 >= 16/2 would let merge and split oscillate on the same posting.
        assertThatThrownBy(() -> SpannConfig.defaults().withPostingMin(8).withPostingMax(16))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("RECSYS_SPANN_POSTING_MIN");
    }

    @Test
    void rejectsNonPositiveNprobeAndPostingMax() {
        assertThatThrownBy(() -> SpannConfig.defaults().withNprobe(0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("RECSYS_SPANN_NPROBE");
        assertThatThrownBy(() -> SpannConfig.defaults().withPostingMax(1))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("RECSYS_SPANN_POSTING_MAX");
    }

    @Test
    void rejectsCompactRatioOutsideZeroToOne() {
        assertThatThrownBy(() -> SpannConfig.defaults().withCompactRatio(0.0))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("RECSYS_SPANN_COMPACT_RATIO");
        assertThatThrownBy(() -> SpannConfig.defaults().withCompactRatio(1.5))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("RECSYS_SPANN_COMPACT_RATIO");
    }

    @Test
    void fromEnv_nonNumericFailsNamingTheVariable() {
        assertThatThrownBy(() -> SpannConfig.fromEnv(name -> "RECSYS_SPANN_NPROBE".equals(name) ? "many" : null))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("RECSYS_SPANN_NPROBE");
    }
}
