package com.recsys.infrastructure.vectordb.spann;

import com.recsys.config.EnvVars;

import java.nio.file.Path;
import java.util.Objects;

/**
 * Configuration for {@link SpannVectorIndex}. Read once at construction; every validation
 * failure names the environment variable so a bad value is a startup crash, not a silent default.
 */
public record SpannConfig(Path dir, int postingMax, int postingMin, int nprobe,
                          int reassignProbe, double compactRatio, long seed, int regionBytes) {

    public static final int DEFAULT_REGION_BYTES = 64 * 1024 * 1024;

    public SpannConfig {
        Objects.requireNonNull(dir, "RECSYS_SPANN_DIR");
        if (postingMax < 2) throw new IllegalArgumentException("RECSYS_SPANN_POSTING_MAX must be >= 2, got " + postingMax);
        if (postingMin < 0 || postingMin >= postingMax / 2) {
            throw new IllegalArgumentException("RECSYS_SPANN_POSTING_MIN must be in [0, RECSYS_SPANN_POSTING_MAX/2), got "
                    + postingMin + " with max " + postingMax);
        }
        if (nprobe < 1) throw new IllegalArgumentException("RECSYS_SPANN_NPROBE must be >= 1, got " + nprobe);
        if (reassignProbe < 0) throw new IllegalArgumentException("RECSYS_SPANN_REASSIGN_PROBE must be >= 0, got " + reassignProbe);
        if (!(compactRatio > 0 && compactRatio <= 1)) {
            throw new IllegalArgumentException("RECSYS_SPANN_COMPACT_RATIO must be in (0, 1], got " + compactRatio);
        }
        if (regionBytes < 4096) throw new IllegalArgumentException("regionBytes must be >= 4096, got " + regionBytes);
    }

    public static SpannConfig defaults() {
        return new SpannConfig(Path.of(System.getProperty("java.io.tmpdir")), 128, 16, 8, 4, 0.5, 42L,
                DEFAULT_REGION_BYTES);
    }

    public static SpannConfig fromEnv(EnvVars.EnvReader env) {
        SpannConfig d = defaults();
        String dir = env.get("RECSYS_SPANN_DIR");
        return new SpannConfig(
                dir == null || dir.isBlank() ? d.dir() : Path.of(dir.trim()),
                EnvVars.readInt(env, "RECSYS_SPANN_POSTING_MAX", d.postingMax()),
                EnvVars.readInt(env, "RECSYS_SPANN_POSTING_MIN", d.postingMin()),
                EnvVars.readInt(env, "RECSYS_SPANN_NPROBE", d.nprobe()),
                EnvVars.readInt(env, "RECSYS_SPANN_REASSIGN_PROBE", d.reassignProbe()),
                EnvVars.readDouble(env, "RECSYS_SPANN_COMPACT_RATIO", d.compactRatio()),
                EnvVars.readLong(env, "RECSYS_SPANN_SEED", d.seed()),
                d.regionBytes());
    }

    public SpannConfig withPostingMax(int v) { return new SpannConfig(dir, v, postingMin, nprobe, reassignProbe, compactRatio, seed, regionBytes); }
    public SpannConfig withPostingMin(int v) { return new SpannConfig(dir, postingMax, v, nprobe, reassignProbe, compactRatio, seed, regionBytes); }
    public SpannConfig withNprobe(int v) { return new SpannConfig(dir, postingMax, postingMin, v, reassignProbe, compactRatio, seed, regionBytes); }
    public SpannConfig withReassignProbe(int v) { return new SpannConfig(dir, postingMax, postingMin, nprobe, v, compactRatio, seed, regionBytes); }
    public SpannConfig withCompactRatio(double v) { return new SpannConfig(dir, postingMax, postingMin, nprobe, reassignProbe, v, seed, regionBytes); }
    public SpannConfig withDir(Path v) { return new SpannConfig(v, postingMax, postingMin, nprobe, reassignProbe, compactRatio, seed, regionBytes); }
    public SpannConfig withRegionBytes(int v) { return new SpannConfig(dir, postingMax, postingMin, nprobe, reassignProbe, compactRatio, seed, v); }
    public SpannConfig withSeed(long v) { return new SpannConfig(dir, postingMax, postingMin, nprobe, reassignProbe, compactRatio, v, regionBytes); }
}
