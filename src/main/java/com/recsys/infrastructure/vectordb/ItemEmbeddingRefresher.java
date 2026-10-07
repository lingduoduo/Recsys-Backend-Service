package com.recsys.infrastructure.vectordb;

import com.recsys.config.EnvVars;
import com.recsys.infrastructure.redis.RedisEmbeddingStore;
import com.recsys.resilience.GuardedLoop;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Keeps in-memory item embeddings (the recall index; 6010's /similar cache) tracking Redis. Every
 * interval it reads every catalog id from Redis, and applies to its sinks only vectors that differ
 * from the last one it applied — load-bearing for SPANN, whose addOrUpdate appends a posting block
 * per call. Redis stays off the request path: a failed pass changes nothing in memory and is
 * recorded by the GuardedLoop, which the RecsysLoopStale alert watches.
 *
 * <p>A catalog id absent from Redis keeps its current vector (keys evict and expire; absence is not
 * deletion). One accepted race with /setembedding: a pass that read v1 before /setembedding wrote v2
 * can re-apply v1 after it; the next pass reads v2 and repairs it, so it lasts at most one interval.
 */
public final class ItemEmbeddingRefresher implements AutoCloseable {

    public static final String LOOP_NAME = "item-embedding-refresh";
    public static final String APPLIED = "recsys.item_embedding.refresh.applied";
    public static final String SKIPPED = "recsys.item_embedding.refresh.skipped";
    public static final String INTERVAL_ENV = "ITEM_EMBEDDING_REFRESH_INTERVAL_MS";
    public static final long DEFAULT_INTERVAL_MS = 60_000L;

    private final Supplier<? extends Collection<Integer>> catalogIds;
    private final ItemEmbeddingReader reader;
    private final int dimension;
    private final List<ItemEmbeddingSink> sinks;
    private final Map<Integer, float[]> lastApplied;
    private final MeterRegistry registry;
    private final Counter applied;
    private final Counter skippedDimension;
    private final Counter skippedCorrupt;
    private final GuardedLoop loop = new GuardedLoop(LOOP_NAME, this::refreshOnce);
    private ScheduledExecutorService scheduler;

    public ItemEmbeddingRefresher(Supplier<? extends Collection<Integer>> catalogIds, ItemEmbeddingReader reader,
                                  int dimension, Map<Integer, float[]> initiallyApplied,
                                  List<ItemEmbeddingSink> sinks, MeterRegistry registry) {
        this.catalogIds = Objects.requireNonNull(catalogIds, "catalogIds");
        this.reader = Objects.requireNonNull(reader, "reader");
        this.dimension = dimension;
        this.sinks = List.copyOf(sinks);
        this.lastApplied = new ConcurrentHashMap<>(initiallyApplied == null ? Map.of() : initiallyApplied);
        this.registry = registry == null ? new SimpleMeterRegistry() : registry;
        this.applied = Counter.builder(APPLIED)
                .description("Item embeddings re-read from Redis that differed and were applied in memory")
                .register(this.registry);
        this.skippedDimension = skipped("dimension");
        this.skippedCorrupt = skipped("corrupt");
    }

    /** Wires the refresher to a generator: its index is always a sink, its seed vectors the baseline. */
    public static ItemEmbeddingRefresher forGenerator(CandidateGenerator generator,
                                                      Supplier<? extends Collection<Integer>> catalogIds,
                                                      RedisEmbeddingStore itemStore,
                                                      List<ItemEmbeddingSink> extraSinks,
                                                      MeterRegistry registry) {
        Objects.requireNonNull(itemStore, "itemStore");
        return forGenerator(generator, catalogIds, itemStore, extraSinks, registry, itemStore::getEmbeddingsLenient);
    }

    static ItemEmbeddingRefresher forGenerator(CandidateGenerator generator,
                                               Supplier<? extends Collection<Integer>> catalogIds,
                                               RedisEmbeddingStore itemStore,
                                               List<ItemEmbeddingSink> extraSinks,
                                               MeterRegistry registry,
                                               ItemEmbeddingReader reader) {
        List<ItemEmbeddingSink> sinks = new ArrayList<>();
        sinks.add(generator::updateEmbedding);
        sinks.addAll(extraSinks);
        return new ItemEmbeddingRefresher(catalogIds, reader, generator.embeddingDimension(),
                generator.seedEmbeddings(), sinks, registry);
    }

    public static long intervalFromEnv(EnvVars.EnvReader env) {
        long ms = EnvVars.readLong(env, INTERVAL_ENV, DEFAULT_INTERVAL_MS);
        if (ms < 0) {
            throw new IllegalArgumentException(INTERVAL_ENV + " must be >= 0 (0 disables), was " + ms);
        }
        return ms;
    }

    public void refreshOnce() {
        Map<Integer, float[]> current = reader.read(catalogIds.get(), id -> skippedCorrupt.increment());
        for (Map.Entry<Integer, float[]> e : current.entrySet()) {
            int id = e.getKey();
            float[] vector = e.getValue();
            if (dimension > 0 && vector.length != dimension) {
                skippedDimension.increment();
                continue;
            }
            if (Arrays.equals(vector, lastApplied.get(id))) continue;
            for (ItemEmbeddingSink sink : sinks) {
                sink.apply(id, vector);   // a throw leaves lastApplied untouched: retried next pass
            }
            lastApplied.put(id, vector);
            applied.increment();
        }
    }

    public GuardedLoop loop() {
        return loop;
    }

    public synchronized void start(long intervalMs) {
        if (intervalMs <= 0 || scheduler != null) return;
        loop.bindTo(registry);
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, LOOP_NAME);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(loop, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void close() {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    private Counter skipped(String reason) {
        return Counter.builder(SKIPPED)
                .description("Item embeddings re-read from Redis that were not applied, by reason")
                .tag("reason", reason)
                .register(registry);
    }
}
