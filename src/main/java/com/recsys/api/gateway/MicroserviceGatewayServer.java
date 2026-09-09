package com.recsys.api.gateway;
import ch.qos.logback.classic.LoggerContext;
import com.recsys.application.auth.AdminTokenGuard;
import com.recsys.application.gateway.ApiDeprecationDecorator;
import com.recsys.application.gateway.ApiVersion;
import com.recsys.application.gateway.GatewayProxyService;
import com.recsys.application.gateway.GatewayRequestForwarder;
import com.recsys.application.gateway.LlmProxyService;
import com.recsys.application.gateway.GatewayHealthService;
import com.recsys.application.gateway.GatewayAuthenticator;
import com.recsys.application.gateway.GatewayOriginSecret;
import com.recsys.application.gateway.MicroserviceRoute;
import com.recsys.application.gateway.RecommendationGatewayService;
import com.recsys.config.EnvConfig;
import com.recsys.config.EnvVars;
import com.recsys.ratelimit.LlmTokenRateLimiter;
import com.recsys.ratelimit.GatewayRateLimiter;
import com.recsys.resilience.RouteCircuitBreaker;
import com.recsys.infrastructure.cache.LlmResponseCache;
import com.recsys.infrastructure.redis.LettuceClientFactory;
import com.recsys.infrastructure.redis.RedisExecutor;
import com.recsys.infrastructure.observability.SlowRequestLogger;
import com.recsys.infrastructure.registry.ServiceRegistryProvider;
import com.recsys.infrastructure.registry.ServiceRegistryStore;
import com.recsys.jvm.GcEventTracker;
import com.recsys.loadshed.GracefulServers;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.common.metric.MeterIdPrefixFunction;
import com.linecorp.armeria.common.metric.PrometheusMeterRegistries;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import com.linecorp.armeria.server.metric.PrometheusExpositionService;
import com.recsys.metrics.GatewayRegistryMetrics;
import com.recsys.metrics.JvmMetricsBinder;
import com.recsys.metrics.RequestDurationHistogram;
import com.recsys.metrics.SplunkHecMetrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public final class MicroserviceGatewayServer {
    private static final Logger log = LoggerFactory.getLogger(MicroserviceGatewayServer.class);
    private static final int DEFAULT_PORT = 8010;
    private static final Set<String> LLM_ROUTE_NAMES = Set.of("llm", "llm-explanation");
    // Cloud Map DNS TTL is 15–30 s. Cap the JVM cache so Blue/Green endpoint changes propagate.
    private static final String CLOUD_MAP_DNS_TTL_SECONDS = "30";

    private MicroserviceGatewayServer() {}

    public static void main(String[] args) throws Exception {
        int port = EnvVars.readInt("GATEWAY_PORT", DEFAULT_PORT);
        int timeoutMs = EnvVars.readInt("GATEWAY_TIMEOUT_MS", 3000);
        Duration timeout = Duration.ofMillis(timeoutMs);
        List<MicroserviceRoute> allRoutes = MicroserviceRoute.defaults();

        // Respect Cloud Map DNS TTL. The JVM caches successful lookups indefinitely by default,
        // which prevents new Cloud Map endpoint registrations from being picked up during
        // blue/green deployments. Only set if the caller hasn't already configured it.
        if (java.security.Security.getProperty("networkaddress.cache.ttl") == null) {
            java.security.Security.setProperty("networkaddress.cache.ttl", CLOUD_MAP_DNS_TTL_SECONDS);
        }

        // One circuit breaker per route — shared between proxy (records outcomes) and
        // health service (exposes state in the /health response body).
        int cbFailureThreshold = EnvVars.readInt("GATEWAY_CB_FAILURE_THRESHOLD", RouteCircuitBreaker.DEFAULT_FAILURE_THRESHOLD);
        long cbCooldownMs = EnvVars.readLong("GATEWAY_CB_COOLDOWN_MS", RouteCircuitBreaker.DEFAULT_COOLDOWN_MS);
        Map<String, RouteCircuitBreaker> circuitBreakers = allRoutes.stream()
                .collect(Collectors.toUnmodifiableMap(MicroserviceRoute::name,
                        r -> new RouteCircuitBreaker(cbFailureThreshold, cbCooldownMs)));

        // Split routes: LLM-backed routes get dedicated services with long timeouts and token budgets.
        List<MicroserviceRoute> llmRoutes = allRoutes.stream()
                .filter(r -> LLM_ROUTE_NAMES.contains(r.name()))
                .toList();
        List<MicroserviceRoute> proxyRoutes = allRoutes.stream()
                .filter(r -> !LLM_ROUTE_NAMES.contains(r.name()))
                .toList();

        GatewayRateLimiter rateLimiter = GatewayRateLimiter.fromEnvironment(proxyRoutes);
        GatewayAuthenticator authenticator = GatewayAuthenticator.fromEnvironment();

        // Created before the forwarder so it can register gateway_user_scope_rejected_total.
        // PrometheusMeterRegistries.defaultRegistry() is a JVM-wide singleton; order is free.
        PrometheusMeterRegistry meterRegistry = PrometheusMeterRegistries.defaultRegistry();
        // Must run before sb.decorator(MetricCollectingService...) below registers any
        // request-duration timer, so the histogram buckets apply from the first request. Also
        // run before JvmMetricsBinder.bindTo(...) below: a MeterFilter only applies to meters
        // registered after it is installed, and binding the JVM gauges first triggers a
        // "MeterFilter configured after a Meter registered" WARN on every startup.
        RequestDurationHistogram.configure(meterRegistry);
        // Armeria's configureRegistry is a no-op, so nothing binds the JVM metrics for us.
        JvmMetricsBinder.bindTo(meterRegistry);
        // Spring constructs its own; the Armeria mains have no container to do it for them.
        GcEventTracker gcEventTracker = new GcEventTracker();
        gcEventTracker.start();
        Runtime.getRuntime().addShutdownHook(new Thread(gcEventTracker::stop));

        // Same operator credential as 7010's AdminTokenGuard: one operator tier system-wide.
        AdminTokenGuard operatorGuard = new AdminTokenGuard(System.getenv("SHARD_ADMIN_TOKEN"));
        if (!operatorGuard.isConfigured()) {
            log.warn("SHARD_ADMIN_TOKEN is not set: operator-class routes (setembedding, model "
                    + "version activate/rollback/preload, /online/ops) will reject every request "
                    + "with 403. See docs/runbooks/gateway-auth.md.");
        }

        // Upstream addressing: static route/env addresses by default. When the service registry is
        // enabled, the gateway resolves upstreams from Redis (falling back to the static address per
        // route) and rebuilds its endpoint groups when a resolved address changes. Redis is only
        // opened when the flag is on.
        boolean registryEnabled = EnvVars.readBool("SERVICE_REGISTRY_ENABLED", false);
        RedisExecutor registryRedis = null;
        ServiceRegistryProvider registryProvider = null;
        GatewayRequestForwarder forwarder;
        if (registryEnabled) {
            registryRedis = LettuceClientFactory.routingFromEnv();
            ServiceRegistryStore registryStore =
                    new ServiceRegistryStore(registryRedis, ServiceRegistryStore.DEFAULT_KEY_PREFIX);
            List<String> serviceNames = proxyRoutes.stream()
                    .map(MicroserviceRoute::serviceName)
                    .filter(java.util.Objects::nonNull)
                    .distinct()
                    .toList();
            long refreshMs = EnvVars.readLong("SERVICE_REGISTRY_REFRESH_MS", 10_000L);
            GatewayRequestForwarder[] holder = new GatewayRequestForwarder[1];
            registryProvider = new ServiceRegistryProvider(registryStore, serviceNames, refreshMs,
                    () -> { if (holder[0] != null) holder[0].rebuildUpstreamsIfChanged(); });
            forwarder = GatewayRequestForwarder.registryBacked(
                    proxyRoutes, timeout, circuitBreakers, rateLimiter, registryProvider, meterRegistry,
                    operatorGuard);
            holder[0] = forwarder;
            registryProvider.start();
            log.info("Service registry consumer enabled ({} services, refresh {} ms)",
                    serviceNames.size(), refreshMs);
        } else {
            forwarder = new GatewayRequestForwarder(
                    proxyRoutes, timeout, circuitBreakers, rateLimiter, meterRegistry, operatorGuard);
        }

        RecommendationGatewayService recommendationService =
                new RecommendationGatewayService(proxyRoutes, forwarder, authenticator);

        // LLM requests can take much longer than regular API calls (large context, slow inference).
        // Use a separate timeout so LLM latency does not block the shared proxy pool.
        int llmTimeoutMs = EnvVars.readInt("LLM_TIMEOUT_MS", LlmProxyService.DEFAULT_TIMEOUT_MS);
        Duration llmTimeout = Duration.ofMillis(llmTimeoutMs);
        LlmTokenRateLimiter llmTokenRateLimiter = LlmTokenRateLimiter.fromEnvironment();
        LlmResponseCache llmResponseCache = LlmResponseCache.fromEnvironment();
        int llmDefaultTokenEstimate = EnvVars.readInt("LLM_DEFAULT_TOKEN_ESTIMATE", LlmProxyService.DEFAULT_TOKEN_ESTIMATE);
        long llmMaxRetryWaitMs = EnvVars.readLong("LLM_MAX_RETRY_WAIT_MS", LlmProxyService.DEFAULT_MAX_RETRY_WAIT_MS);

        ServerBuilder sb = Server.builder().http(port);

        // Request-duration histogram. Tagged by method/status/service only — the catch-all
        // "prefix:/" data path does NOT produce a per-URL label (verified against
        // DefaultMeterIdPrefixFunction in armeria-1.28.4).
        //
        // meterRegistry(meterRegistry) is required here: Armeria's decorator records into
        // ctx.meterRegistry(), which falls back to Flags.meterRegistry() (Micrometer's global
        // registry) unless the ServerBuilder is told otherwise. Without this call the decorator
        // silently records into a registry that /metrics never exposes (verified empirically:
        // requests succeeded but api_gateway_* series never appeared). RecSysServer and
        // OnlinePredictionServer already bind this the same way.
        sb.meterRegistry(meterRegistry);
        sb.decorator(MetricCollectingService.newDecorator(
                MeterIdPrefixFunction.ofDefault("api_gateway")));
        sb.decorator(SlowRequestLogger.newDecorator("api-gateway",
                EnvConfig.readLong("SLOW_REQUEST_LOG_THRESHOLD_MS", 1000)));

        // Prometheus metrics endpoint (always present, matching the other services). Registry meters
        // are registered only when the registry consumer is active. The registry itself is created
        // above, before the forwarder, so the forwarder can register its own meters into it.
        sb.service("/metrics", PrometheusExpositionService.of(meterRegistry.getPrometheusRegistry()));
        // The Splunk appender was built by Logback long before this registry existed, so it
        // cannot register itself. No-op when SPLUNK_HEC_TOKEN is unset.
        SplunkHecMetrics.register(meterRegistry);
        if (registryProvider != null) {
            List<String> registrySvcNames = proxyRoutes.stream()
                    .map(MicroserviceRoute::serviceName)
                    .filter(java.util.Objects::nonNull)
                    .distinct()
                    .toList();
            GatewayRegistryMetrics.register(meterRegistry, registryProvider, registrySvcNames,
                    System::currentTimeMillis);
        }

        // Origin lockdown: when CloudFront fronts this gateway, reject anything that did not come
        // through our distribution. No-op when GATEWAY_ORIGIN_SECRET is unset (local dev).
        // Registered after meterRegistry so rejections are counted and scrapeable at /metrics.
        GatewayOriginSecret originSecret = GatewayOriginSecret.fromEnvironment(System::getenv);
        if (originSecret.isEnabled()) {
            sb.decorator(GatewayOriginSecret.newDecorator(originSecret, meterRegistry));
        }

        // Deprecation signalling for unversioned spellings and back-compat alias routes.
        // Registered as a server-wide decorator so every entry point — catch-all, canonical
        // recommend, and the LLM routes — is covered from one place. No-op when
        // GATEWAY_DEPRECATION_SUNSET is unset.
        ApiDeprecationDecorator deprecation =
                ApiDeprecationDecorator.fromEnvironment(System::getenv);
        if (deprecation.isEnabled()) {
            sb.decorator(deprecation.newDecorator());
        }

        // Health endpoint — exposes per-route circuit state and upstream reachability.
        sb.service("/health", new GatewayHealthService(allRoutes, timeout, circuitBreakers, port, registryProvider));

        // LLM path: build a tuned, shared ClientFactory (only when LLM routes exist) and register
        // each LLM route from it. Register LLM routes before the catch-all so Armeria's
        // more-specific prefix wins. Connections are established lazily on the first request.
        ClientFactory llmClientFactory = null;
        if (!llmRoutes.isEmpty()) {
            llmClientFactory = buildLlmClientFactory(System::getenv);
            registerLlmRoutes(sb, llmRoutes, llmClientFactory, llmTimeout, circuitBreakers,
                    llmTokenRateLimiter, llmResponseCache, llmDefaultTokenEstimate, llmMaxRetryWaitMs,
                    authenticator);
        }

        registerRecommendRoutes(sb, recommendationService);

        // Catch-all proxy — handles all non-LLM routes using the same forwarding pipeline.
        sb.service("prefix:/",
                new GatewayProxyService(proxyRoutes, forwarder, authenticator));

        GracefulServers.applyShutdownWindow(sb);

        Server server = sb.build();

        final ClientFactory llmFactoryToClose = llmClientFactory;
        final ServiceRegistryProvider providerToStop = registryProvider;
        final RedisExecutor redisToClose = registryRedis;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down API gateway...");
            server.stop().join();
            forwarder.close();
            if (providerToStop != null) {
                providerToStop.stop();
            }
            if (redisToClose != null) {
                redisToClose.close();
            }
            if (llmFactoryToClose != null) {
                llmFactoryToClose.close();
            }
            // Flushes the Splunk appender's queued events. Must run last: stopping the
            // LoggerContext detaches and stops every appender (console included), so any
            // logging after this point would be silently dropped.
            if (LoggerFactory.getILoggerFactory() instanceof LoggerContext loggerContext) {
                loggerContext.stop();
            }
        }));

        log.info("Starting RecSys API gateway on port {}", port);
        log.info("Canonical recommendation routing is available at /api/recommend and defaults to model");
        for (MicroserviceRoute route : allRoutes) {
            log.info("Route {} {} -> {}", route.name(), route.prefix(), route.baseUri());
        }
        if (rateLimiter.isEnabled()) {
            log.info("Gateway local rate limiting enabled");
        }
        if (authenticator.isEnabled()) {
            log.info("Gateway API-key authentication enabled");
        }
        if (originSecret.isEnabled()) {
            log.info("Gateway origin-secret enforcement enabled");
        }
        if (llmTokenRateLimiter.isEnabled()) {
            log.info("LLM token rate limiting enabled");
        }
        if (llmResponseCache.isEnabled()) {
            log.info("LLM response cache enabled");
        }

        server.start().join();
        server.blockUntilShutdown();
    }

    // Public so LlmProxyStreamConcurrencyTest (a different package) measures the factory
    // production actually uses, not a hand-copied approximation of it.
    public static ClientFactory buildLlmClientFactory(EnvVars.EnvReader env) {
        long connectMs = EnvVars.readLong(env, "LLM_CONNECT_TIMEOUT_MS", 2000L);
        long idleMs = EnvVars.readLong(env, "LLM_IDLE_TIMEOUT_MS", 60_000L);
        long pingMs = EnvVars.readLong(env, "LLM_PING_INTERVAL_MS", 20_000L);
        return ClientFactory.builder()
                .connectTimeout(Duration.ofMillis(connectMs))
                .idleTimeout(Duration.ofMillis(idleMs))
                .pingIntervalMillis(pingMs)
                .build();
    }

    // Canonical recommendation endpoint — exact path takes precedence over the catch-all. Both
    // spellings are registered because this is an exact Armeria route, not a route-table entry:
    // the catch-all would normalize /api/v1/recommend to /api/recommend, which matches no
    // route-table prefix and would 404. Extracted (mirroring registerLlmRoutes) so the
    // registration can be exercised directly in MicroserviceGatewayServerTest.
    static void registerRecommendRoutes(ServerBuilder sb, RecommendationGatewayService recommendationService) {
        sb.service("/api/recommend", recommendationService);
        sb.service(ApiVersion.versioned(ApiVersion.DEFAULT_VERSION, "/api/recommend"),
                recommendationService);
    }

    static void registerLlmRoutes(
            ServerBuilder sb,
            List<MicroserviceRoute> llmRoutes,
            ClientFactory llmClientFactory,
            Duration llmTimeout,
            Map<String, RouteCircuitBreaker> circuitBreakers,
            LlmTokenRateLimiter tokenRateLimiter,
            LlmResponseCache responseCache,
            int defaultTokenEstimate,
            long maxRetryWaitMs,
            GatewayAuthenticator authenticator) {
        for (MicroserviceRoute llmRoute : llmRoutes) {
            LlmProxyService llmProxyService = new LlmProxyService(
                    llmRoute,
                    llmTimeout,
                    circuitBreakers.get(llmRoute.name()),
                    tokenRateLimiter,
                    responseCache,
                    defaultTokenEstimate,
                    maxRetryWaitMs,
                    authenticator,
                    llmClientFactory);
            // LLM routes are filtered out of proxyRoutes, so the catch-all cannot serve them.
            // Register the versioned twin explicitly or /api/v1/llm/... would 404.
            sb.service(
                    com.linecorp.armeria.server.Route.builder()
                            .pathPrefix(llmRoute.prefix() + "/")
                            .build(),
                    llmProxyService);
            sb.service(
                    com.linecorp.armeria.server.Route.builder()
                            .pathPrefix(ApiVersion.versioned(
                                    ApiVersion.DEFAULT_VERSION, llmRoute.prefix()) + "/")
                            .build(),
                    llmProxyService);
        }
    }
}
