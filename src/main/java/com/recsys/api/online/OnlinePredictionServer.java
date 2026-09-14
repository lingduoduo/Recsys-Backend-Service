package com.recsys.api.online;

import ch.qos.logback.classic.LoggerContext;
import com.recsys.application.online.OnlineServices;
import com.recsys.application.online.OnlineBlendingPipeline;
import com.recsys.application.online.OnlineRecommendationService;
import com.recsys.application.pagination.RecommendationPaginationRuntime;
import com.recsys.application.consistency.ConsistencyTokenCodec;
import com.recsys.application.consistency.ConsistencyWaiter;
import com.recsys.application.consistency.RedisLineageReader;
import com.recsys.application.outbox.DurableEventPublisher;
import com.recsys.application.auth.AdminTokenGuard;
import com.recsys.metrics.OnlineServingMetricsService;
import com.recsys.metrics.ConsistencyMetrics;
import com.recsys.metrics.RedisCacheMetrics;
import com.recsys.metrics.JvmMetricsBinder;
import com.recsys.metrics.QueueMetrics;
import com.recsys.metrics.RequestDurationHistogram;
import com.recsys.metrics.SplunkHecMetrics;
import com.recsys.infrastructure.redis.RedisCacheStatsProbe;
import com.recsys.infrastructure.redis.RedisPersistentKeyProbe;
import com.recsys.infrastructure.redis.RedisReplicaLagProbe;
import com.recsys.infrastructure.redis.RedisFeatureVersionSampler;
import com.recsys.jvm.GcEventTracker;
import com.recsys.infrastructure.observability.SlowRequestLogger;

import com.linecorp.armeria.server.Route;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import com.linecorp.armeria.server.metric.PrometheusExpositionService;
import com.linecorp.armeria.common.metric.MeterIdPrefixFunction;
import com.linecorp.armeria.common.metric.PrometheusMeterRegistries;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import com.recsys.infrastructure.dataloading.DataManager;
import com.recsys.infrastructure.cache.LogicalExpiryEmbeddingCache;
import com.recsys.infrastructure.redis.GlobalPopularityStore;
import com.recsys.infrastructure.redis.LettuceClientFactory;
import com.recsys.infrastructure.redis.RedisEmbeddingStore;
import com.recsys.infrastructure.redis.RedisExecutor;
import com.recsys.infrastructure.vectordb.EmbeddingStore;
import com.recsys.infrastructure.redis.ShardedTopKStore;
import com.recsys.infrastructure.redis.sharding.ConsistentHashRing;
import com.recsys.infrastructure.redis.sharding.SequenceGenerator;
import com.recsys.infrastructure.redis.sharding.ShardedRecordStore;
import com.recsys.infrastructure.redis.sharding.ShardTopology;
import com.recsys.infrastructure.redis.sharding.ShardTopologyProvider;
import com.recsys.infrastructure.redis.sharding.ShardTopologyStore;
import com.recsys.infrastructure.vectordb.CandidateGenerator;
import com.recsys.infrastructure.messaging.AsyncEventPublisher;
import com.recsys.application.online.LearnerFlushScheduler;
import com.recsys.application.online.OnlineLearner;
import com.recsys.config.EnvConfig;
import com.recsys.resilience.FaultInjector;
import com.recsys.resilience.WorkerBulkhead;
import com.recsys.loadshed.GracefulExecutors;
import com.recsys.loadshed.GracefulServers;
import com.recsys.loadshed.OnlineAdmissionControl;
import com.recsys.health.OnlineCapacityService;
import com.recsys.health.OnlineHealthService;
import com.recsys.loadshed.OnlineLoadShedder;
import com.recsys.health.OnlineOpsService;
import com.recsys.infrastructure.store.ShardedRecordService;
import com.recsys.ratelimit.RedisRateLimiter;
import com.recsys.infrastructure.store.OnlineFeatureStore;
import com.recsys.infrastructure.store.TrendingStore;
import com.recsys.infrastructure.messaging.AsyncEventPublisherFactory;
import com.recsys.infrastructure.outbox.MySqlOutboxRepository;
import com.recsys.infrastructure.persistence.MySqlConnectionSettings;
import com.recsys.infrastructure.persistence.TransactionalMySql;
import com.recsys.application.retrieval.channels.Channels;
import com.recsys.application.retrieval.coldstart.ColdStartChannel;
import com.recsys.application.retrieval.coldstart.QuotaPolicy;
import com.recsys.application.retrieval.multichannel.ChannelHealthMonitor;
import com.recsys.application.retrieval.multichannel.MultiChannelRecallService;
import com.recsys.application.retrieval.multichannel.RecallConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.time.Clock;
import java.time.Duration;

public final class OnlinePredictionServer {
    private static final Logger log = LoggerFactory.getLogger(OnlinePredictionServer.class);
    private static final int DEFAULT_PORT = 7010;

    private OnlinePredictionServer() {}

    public static void main(String[] args) throws Exception {
        int port = readIntEnv("ONLINE_DEMO_PORT", DEFAULT_PORT);
        int requestTimeoutMs = readIntEnv("ONLINE_REQUEST_TIMEOUT_MS", 500);
        DurableConfig durableConfig = durableConfig(System.getenv());
        RedisExecutor jedisPool = null;
        com.recsys.infrastructure.registry.ServiceRegistrar registrar = null;
        AsyncEventPublisher asyncEventPublisher = null;
        TransactionalMySql transactionalMySql = null;
        LearnerFlushScheduler learnerFlushScheduler = null;
        ExecutorService recallExecutor = null;
        ShardTopologyProvider topologyProvider = null;
        RedisReplicaLagProbe replicaLagProbe = null;
        RedisCacheStatsProbe cacheStatsProbe = null;
        RedisPersistentKeyProbe persistentKeyProbe = null;
        RedisFeatureVersionSampler featureVersionSampler = null;
        MySqlOutboxRepository outboxRepository = null;

        try {
            jedisPool = LettuceClientFactory.routingFromEnv();
            registrar = com.recsys.infrastructure.registry.ServiceRegistrar.fromEnvironment(
                    new com.recsys.infrastructure.registry.ServiceRegistryStore(
                            jedisPool, com.recsys.infrastructure.registry.ServiceRegistryStore.DEFAULT_KEY_PREFIX));
            if (registrar != null) registrar.start();
            asyncEventPublisher = createAsyncEventPublisher();
            DurableEventPublisher durableEventPublisher = null;
            ConsistencyTokenCodec consistencyTokenCodec = null;
            if (durableConfig.enabled()) {
                transactionalMySql = new TransactionalMySql(MySqlConnectionSettings.fromEnv());
                outboxRepository = new MySqlOutboxRepository(transactionalMySql);
                durableEventPublisher = new DurableEventPublisher(outboxRepository);
                consistencyTokenCodec = new ConsistencyTokenCodec(durableConfig.tokenSecret());
            }
            DataManager dataManager = DataManager.getInstance();
            RedisEmbeddingStore userEmbeddingStore = new RedisEmbeddingStore(jedisPool, "u2vEmb");
            // u2vEmb is continuously rewritten by Flink: use a soft-TTL cache (serve-stale +
            // background refresh, no Bloom guard) so new users are still found and updates land
            // within ~one soft TTL. Default 30s, overridable for tuning.
            int userEmbSoftTtlSeconds = readIntEnv("ONLINE_USER_EMB_SOFT_TTL_SECONDS", 30);
            EmbeddingStore userEmbCache =
                    new LogicalExpiryEmbeddingCache(userEmbeddingStore, userEmbSoftTtlSeconds);
            CandidateGenerator candidateGenerator = new CandidateGenerator(dataManager, userEmbCache);
            TrendingStore topkStore = new ShardedTopKStore(jedisPool, "topk:");
            OnlineFeatureStore onlineFeatureStore = new OnlineFeatureStore(jedisPool);
            OnlineLearner onlineLearner = new OnlineLearner();
            GlobalPopularityStore globalPopStore = new GlobalPopularityStore(jedisPool);
            int recallPoolSize = Runtime.getRuntime().availableProcessors() * 2;
            WorkerBulkhead recallBulkhead = new WorkerBulkhead("recall-online", recallPoolSize,
                    EnvConfig.readInt("RECALL_BULKHEAD_QUEUE_CAPACITY", recallPoolSize * 4));
            recallExecutor = recallBulkhead.asExecutorService();
            MultiChannelRecallService recallService = MultiChannelRecallService.from(
                    RecallConfig.builder()
                            .channels(List.of(
                                    new Channels.Embedding(candidateGenerator),
                                    new Channels.OnlineRecentHistory(onlineFeatureStore, dataManager),
                                    new Channels.UserSimilarity(dataManager),
                                    new Channels.Trending(topkStore, List.of("last_hour", "last_day")),
                                    new Channels.Popularity(dataManager, globalPopStore),
                                    new ColdStartChannel(topkStore, globalPopStore)))
                            .quotaPolicy(QuotaPolicy.defaultOnline())
                            .healthMonitor(new ChannelHealthMonitor())
                            .executor(recallExecutor)
                            .faultInjector(FaultInjector.NOOP)
                            .userEmbeddingStore(userEmbCache)
                            .build());
            OnlineRecommendationService recommendationService = new OnlineRecommendationService(
                    dataManager, recallService, onlineFeatureStore, topkStore, onlineLearner);
            PrometheusMeterRegistry registry = PrometheusMeterRegistries.defaultRegistry();
            // Must be the first thing that touches this registry: a MeterFilter only applies to
            // meters registered after it is installed, and SplunkHecMetrics.register(...) below
            // registers real meters (FunctionCounters/Gauge) immediately whenever a SPLUNK
            // appender is present, regardless of whether SPLUNK_HEC_TOKEN is set. Also covers
            // JvmMetricsBinder.bindTo(...) below and the meterRegistry(registry).decorator(
            // MetricCollectingService...) call further down, both of which register meters too.
            // Any of these running first triggers a "MeterFilter configured after a Meter
            // registered" WARN on every startup.
            RequestDurationHistogram.configure(registry);
            // The Splunk appender was built by Logback long before this registry existed, so it
            // cannot register itself. No-op when SPLUNK_HEC_TOKEN is unset.
            SplunkHecMetrics.register(registry);
            // Armeria's configureRegistry is a no-op, so nothing binds the JVM metrics for us.
            JvmMetricsBinder.bindTo(registry);
            QueueMetrics.register(registry, "recall-online", recallBulkhead);
            // Spring constructs its own; the Armeria mains have no container to do it for them.
            GcEventTracker gcEventTracker = new GcEventTracker();
            gcEventTracker.start();
            Runtime.getRuntime().addShutdownHook(new Thread(gcEventTracker::stop));
            RecommendationPaginationRuntime pagination =
                    RecommendationPaginationRuntime.fromEnvironment(
                            registry, Clock.systemUTC());
            OnlineBlendingPipeline blendingPipeline = new OnlineBlendingPipeline(
                    recommendationService,
                    pagination.coordinator(),
                    pagination.maxCandidates());
            learnerFlushScheduler =
                    new LearnerFlushScheduler(onlineLearner, jedisPool, "bias:item", 30L);
            learnerFlushScheduler.loop().bindTo(registry);
            learnerFlushScheduler.start();
            MySqlOutboxRepository metricsOutboxRepository = outboxRepository;
            ConsistencyMetrics consistencyMetrics = metricsOutboxRepository == null
                    ? new ConsistencyMetrics(registry)
                    : new ConsistencyMetrics(registry,
                            () -> metricsOutboxRepository.countRetryableBacklog(Clock.systemUTC().instant()));
            asyncEventPublisher.bindConsistencyMetrics(consistencyMetrics);
            replicaLagProbe = new RedisReplicaLagProbe(jedisPool, Clock.systemUTC());
            replicaLagProbe.start(Duration.ofSeconds(readIntEnv("REDIS_REPLICA_LAG_PROBE_SECONDS", 10)),
                    result -> consistencyMetrics.updateReplicaLag(
                            new ConsistencyMetrics.ReplicaLag(result.available(), result.lagSeconds())));
            // Redis's own eviction/memory counters: without them, a cache silently evicting
            // and a Redis briefly unreachable look identical from the application side.
            RedisCacheMetrics cacheMetrics = new RedisCacheMetrics(registry);
            cacheStatsProbe = new RedisCacheStatsProbe(jedisPool);
            cacheStatsProbe.start(Duration.ofSeconds(readIntEnv("REDIS_CACHE_STATS_PROBE_SECONDS", 30)),
                    cacheMetrics::update);
            persistentKeyProbe = new RedisPersistentKeyProbe(jedisPool);
            persistentKeyProbe.start(
                    Duration.ofSeconds(readIntEnv("REDIS_PERSISTENT_KEY_PROBE_SECONDS", 60)),
                    cacheMetrics::updateKeyspace);
            featureVersionSampler = new RedisFeatureVersionSampler(jedisPool, consistencyMetrics,
                    Clock.systemUTC(), readIntEnv("REDIS_FEATURE_VERSION_SAMPLE_LIMIT", 1000));
            featureVersionSampler.loop().bindTo(registry);
            featureVersionSampler.start(Duration.ofSeconds(readIntEnv("REDIS_FEATURE_VERSION_SAMPLE_SECONDS", 30)));
            OnlineServingMetricsService metricsService = new OnlineServingMetricsService();
            OnlineLoadShedder loadShedder = new OnlineLoadShedder();
            OnlineCapacityService capacityService = new OnlineCapacityService();
            RedisRateLimiter redisRateLimiter = new RedisRateLimiter(jedisPool);
            redisRateLimiter.registerMetrics(registry);

            int shardCount = readIntEnv("SHARDED_RECORD_SHARD_COUNT", 2);
            long refreshMs = readIntEnv("SHARD_TOPOLOGY_REFRESH_SECONDS", 30) * 1000L;
            ShardTopologyStore topologyStore = new ShardTopologyStore(jedisPool, "shard:topology");
            topologyProvider = new ShardTopologyProvider(
                    topologyStore, 150, shardCount, refreshMs,
                    System::currentTimeMillis);
            topologyProvider.loop().bindTo(registry);
            topologyProvider.start();
            SequenceGenerator seqGen = new SequenceGenerator(jedisPool, "sr:");
            ShardedRecordStore shardedRecordStore = new ShardedRecordStore(
                    jedisPool, jedisPool, topologyProvider, seqGen, "sr:");
            startSequenceCounterRepair(seqGen, topologyProvider);

            OnlineServices.Features featuresService = durableConfig.enabled()
                    ? new OnlineServices.Features(recommendationService, metricsService, loadShedder,
                            redisRateLimiter, durableEventPublisher, consistencyTokenCodec, true)
                    : new OnlineServices.Features(recommendationService, metricsService, loadShedder,
                            redisRateLimiter, null, true);
            OnlineServices.Prediction predictionService = durableConfig.enabled()
                    ? new OnlineServices.Prediction(recommendationService, metricsService, loadShedder,
                            redisRateLimiter, true, consistencyTokenCodec,
                            new ConsistencyWaiter(new RedisLineageReader(jedisPool), consistencyMetrics),
                            consistencyMetrics)
                    : new OnlineServices.Prediction(recommendationService, metricsService,
                            loadShedder, redisRateLimiter, true);
            ServerBuilder sb = Server.builder();
            sb.http(port)
              .requestTimeoutMillis(requestTimeoutMs)
              .meterRegistry(registry)
              .decorator(MetricCollectingService.newDecorator(
                      MeterIdPrefixFunction.ofDefault("online_serving")))
              .decorator(SlowRequestLogger.newDecorator("online-serving",
                      EnvConfig.readLong("SLOW_REQUEST_LOG_THRESHOLD_MS", 300)))
              .service("/health/live", new OnlineServices.Live())
              .service("/health/ready",
                      new OnlineHealthService(metricsService, loadShedder))
              .service("/health",
                      new OnlineHealthService(metricsService, loadShedder))
              .service("/metrics", PrometheusExpositionService.of(registry.getPrometheusRegistry()))
              .service("/online/features",
                      new OnlineAdmissionControl(
                              featuresService,
                              loadShedder, metricsService))
              .service("/online/recommendation",
                      new OnlineAdmissionControl(
                              predictionService,
                              loadShedder, metricsService))
              .service("/online/ops",
                      new OnlineOpsService(metricsService, loadShedder, capacityService,
                              redisRateLimiter, asyncEventPublisher)
                              .decorate(AdminTokenGuard.newDecorator(
                                      new AdminTokenGuard(System.getenv("SHARD_ADMIN_TOKEN")))))
              // "v2" is the PIPELINE name (recall -> rank -> hydrate -> paginate), not API version 2.
              // API versions live only at the gateway edge as /api/v{n} — see docs/system_design/09_API_Gateway.md#the-compatibility-contract.
              //
              // Admission-controlled to match /online/recommendation above. The canonical
              // POST /api/recommend reaches THIS route, so leaving it unwrapped gave the busiest
              // path on this service unbounded concurrency while its neighbour was protected.
              .service("/v2/recommend",
                      new OnlineAdmissionControl(
                              new OnlineServices.RecommendV2(blendingPipeline),
                              loadShedder, metricsService))
              .service(Route.builder().pathPrefix("/shards/").build(),
                      new ShardedRecordService(shardedRecordStore, topologyStore,
                              System.getenv("SHARD_ADMIN_TOKEN"),
                              readIntEnv("SHARDED_RECORD_MAX_TTL_SECONDS", 86_400) * 1000L,
                              System::currentTimeMillis));

            GracefulServers.applyShutdownWindow(sb);

            Server server = sb.build();
            metricsService.registerGauges(registry);
            LearnerFlushScheduler activeLearnerFlushScheduler = learnerFlushScheduler;
            ShardTopologyProvider activeTopologyProvider = topologyProvider;
            AsyncEventPublisher activeAsyncEventPublisher = asyncEventPublisher;
            TransactionalMySql activeTransactionalMySql = transactionalMySql;
            RedisExecutor activeJedisPool = jedisPool;
            com.recsys.infrastructure.registry.ServiceRegistrar activeRegistrar = registrar;
            ExecutorService activeRecallExecutor = recallExecutor;
            RedisReplicaLagProbe activeReplicaLagProbe = replicaLagProbe;
            RedisCacheStatsProbe activeCacheStatsProbe = cacheStatsProbe;
            RedisPersistentKeyProbe activePersistentKeyProbe = persistentKeyProbe;
            RedisFeatureVersionSampler activeFeatureVersionSampler = featureVersionSampler;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                loadShedder.markShuttingDown();   // flip readiness to 503 + shed new load before draining
                server.stop().join();
                try {
                    candidateGenerator.close();
                } catch (RuntimeException e) {
                    log.warn("vector index close failed during shutdown", e);
                }
                activeAsyncEventPublisher.close();
                activeReplicaLagProbe.close();
                activeCacheStatsProbe.close();
                activePersistentKeyProbe.close();
                activeFeatureVersionSampler.close();
                if (activeTransactionalMySql != null) activeTransactionalMySql.close();
                activeLearnerFlushScheduler.close();
                GracefulExecutors.shutdownGracefully(activeRecallExecutor);
                activeTopologyProvider.stop();
                if (activeRegistrar != null) {
                    activeRegistrar.close();
                }
                activeJedisPool.close();
                // Flushes the Splunk appender's queued events. Must run last: stopping the
                // LoggerContext detaches and stops every appender (console included), so any
                // logging after this point would be silently dropped.
                if (LoggerFactory.getILoggerFactory() instanceof LoggerContext loggerContext) {
                    loggerContext.stop();
                }
            }));

            server.start().join();
            server.blockUntilShutdown();
        } catch (Exception e) {
            if (asyncEventPublisher != null) asyncEventPublisher.close();
            if (transactionalMySql != null) transactionalMySql.close();
            if (learnerFlushScheduler != null) learnerFlushScheduler.close();
            if (recallExecutor != null) GracefulExecutors.shutdownGracefully(recallExecutor);
            if (topologyProvider != null) topologyProvider.stop();
            if (replicaLagProbe != null) replicaLagProbe.close();
            if (cacheStatsProbe != null) cacheStatsProbe.close();
            if (persistentKeyProbe != null) persistentKeyProbe.close();
            if (featureVersionSampler != null) featureVersionSampler.close();
            if (registrar != null) registrar.close();
            if (jedisPool != null) jedisPool.close();
            throw e;
        }
    }

    static DurableConfig durableConfig(Map<String, String> env) {
        boolean enabled = Boolean.parseBoolean(env.getOrDefault("ONLINE_DURABLE_EVENTS_ENABLED", "false"));
        if (!enabled) return new DurableConfig(false, null);
        if (!Boolean.parseBoolean(env.getOrDefault("MYSQL_ENABLED", "false"))) {
            throw new IllegalStateException("MYSQL_ENABLED=true is required for durable online event acceptance");
        }
        String secret = env.get("ONLINE_CONSISTENCY_TOKEN_SECRET");
        new ConsistencyTokenCodec(secret);
        return new DurableConfig(true, secret);
    }

    record DurableConfig(boolean enabled, String tokenSecret) {}

    /**
     * Repairs shard sequence counters that a Redis partial flush left behind the highest
     * sequence number still present — a stale counter reissues that number, the device
     * index's {@code ZADD NX} becomes a no-op, and the record is silently dropped.
     *
     * <p>Runs on a daemon thread, not the boot thread: the guard SCANs every device ZSet and
     * issues a ZREVRANGEBYSCORE per key, so doing it synchronously would block startup in
     * proportion to keyspace size. Fails soft — a Redis problem here must not stop the server
     * from serving.
     */
    private static void startSequenceCounterRepair(SequenceGenerator seqGen,
                                                   ShardTopologyProvider provider) {
        if (!Boolean.parseBoolean(
                System.getenv().getOrDefault("SHARDED_RECORD_SEQ_REPAIR_ENABLED", "true"))) {
            log.info("Shard sequence-counter repair disabled (SHARDED_RECORD_SEQ_REPAIR_ENABLED=false)");
            return;
        }
        long budgetMs = readIntEnv("SHARDED_RECORD_SEQ_REPAIR_TIMEOUT_MS", 30_000);
        Thread t = new Thread(() -> {
            try {
                ShardTopology topo = provider.current();
                for (int shard = 0; shard < topo.shardCount(); shard++) {
                    if (!seqGen.ensureCounterValid(topo, shard, budgetMs)) {
                        log.warn("Sequence-counter repair for generation {} shard {} exceeded its {}ms "
                                        + "budget; counter raised only as far as the partial scan reached",
                                topo.version(), shard, budgetMs);
                    }
                }
                log.info("Sequence-counter repair complete for generation {} ({} shards)",
                        topo.version(), topo.shardCount());
            } catch (Exception e) {
                log.warn("Sequence-counter repair failed; continuing without it: {}", e.toString());
            }
        }, "shard-seq-repair");
        t.setDaemon(true);
        t.start();
    }

    private static int readIntEnv(String envName, int defaultValue) {
        String raw = System.getenv(envName);
        if (raw == null || raw.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    static AsyncEventPublisher createAsyncEventPublisher() {
        return AsyncEventPublisherFactory.fromEnvironment("ONLINE_EVENTS");
    }
}
