package com.recsys.infrastructure.redis;

import com.recsys.config.RedisProperties;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.TimeoutOptions;
import io.lettuce.core.api.StatefulRedisConnection;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Builds Lettuce-backed {@link RedisExecutor}s and AZ-aware
 * {@link RedisReadReplicaRouter}s from environment variables or Spring-managed
 * {@link RedisProperties}. Drop-in replacement for the previous
 * {@code RedisConnectionFactory}; preserves all env-var names, defaults, and the
 * latency-capped variant used by the model-serving recall path.
 */
public final class LettuceClientFactory {

    private static final Logger log = LoggerFactory.getLogger(LettuceClientFactory.class);

    static final int DEFAULT_MAX_TOTAL   = 50;
    static final int DEFAULT_MAX_IDLE    = 10;
    static final int DEFAULT_MIN_IDLE    = 2;
    static final int DEFAULT_MAX_WAIT_MS = 250;
    static final int DEFAULT_TIMEOUT_MS  = 2000; // matches Jedis Protocol.DEFAULT_TIMEOUT

    private LettuceClientFactory() {}

    /**
     * Refuses to open a connection to an unauthenticated Redis unless something says so out loud.
     * Mirrors GatewayAuthenticator.fromEnvironment: REDIS_PASSWORD was supported by this class and
     * set by no manifest, which is exactly how every service ended up connecting as the
     * unauthenticated default user. A warning would not have changed that, which is why this
     * throws. The opt-out still warns, on the same precedent: it makes a cluster that took the
     * escape hatch visible in its own logs, where the refusal is what keeps it out of production.
     *
     * <p>Guards the public entry points only. The package-private URI builders and the map-taking
     * router overload stay unguarded so unit tests can construct and inspect URIs without a
     * credential.
     */
    /**
     * Public entry point for callers outside this package that build their own client from
     * {@link RedisProperties} — today, {@code RetrievalRedisConfig}, which hands Spring Data
     * Redis a connection factory. Without this, the Spring Data path would be the one way into
     * Redis that the credential guard does not cover.
     */
    public static void requireAuthentication(RedisProperties props, Map<String, String> env) {
        requireAuthentication(props.getPassword(), env);
    }

    static void requireAuthentication(String password, Map<String, String> env) {
        if (password != null && !password.isBlank()) return;
        if (Boolean.parseBoolean(env.getOrDefault("REDIS_ALLOW_NO_AUTH", "false"))) {
            log.warn("Redis authentication is DISABLED (REDIS_ALLOW_NO_AUTH=true): this service "
                    + "connects to Redis as the unauthenticated default user. Never use this in "
                    + "production.");
            return;
        }
        throw new IllegalStateException(
                "REDIS_PASSWORD is not set, so this service would connect to Redis as the "
                        + "unauthenticated default user. Redis holds the item and user embeddings, "
                        + "device history, the service registry, and the login-token to API-key "
                        + "mapping. Set REDIS_PASSWORD — in Kubernetes, the redis-password key of "
                        + "the recsys-secrets Secret — or set REDIS_ALLOW_NO_AUTH=true to accept an "
                        + "unauthenticated connection in local development.");
    }

    // ── Single executors ──────────────────────────────────────────────────────

    public static RedisExecutor fromEnv() {
        return fromEnv(Integer.MAX_VALUE);
    }

    /**
     * Latency-sensitive variant: caps the command timeout to {@code maxTimeoutMs}
     * so a down/slow Redis fails fast (used by the recall pool).
     */
    public static RedisExecutor fromEnv(int maxTimeoutMs) {
        Map<String, String> env = System.getenv();
        requireAuthentication(env.getOrDefault("REDIS_PASSWORD", ""), env);
        return executor(uriFromEnv(env, maxTimeoutMs), poolConfig(defaultPoolKnobs(env)));
    }

    public static RedisExecutor from(RedisProperties props) {
        requireAuthentication(props.getPassword(), System.getenv());
        return executor(uriFrom(props), poolConfig(props.getPool()));
    }

    // ── Routing executors (execute→primary, executeRead→replica) ──────────────

    /** A read/write-splitting executor: reads use a replica (reader endpoint) when
     *  {@code REDIS_REPLICA_NODES} is set, writes use the primary. */
    public static RedisExecutor routingFromEnv() {
        Map<String, String> env = System.getenv();
        requireAuthentication(env.getOrDefault("REDIS_PASSWORD", ""), env);
        return new RoutingRedisExecutor(routerFromEnv(env));
    }

    /** Latency-capped routing variant (recall pool): caps primary and replica
     *  command timeouts to {@code maxTimeoutMs}. */
    public static RedisExecutor routingFromEnv(int maxTimeoutMs) {
        Map<String, String> env = System.getenv();
        requireAuthentication(env.getOrDefault("REDIS_PASSWORD", ""), env);
        return new RoutingRedisExecutor(routerFromEnv(env, maxTimeoutMs));
    }

    // ── Routers ───────────────────────────────────────────────────────────────

    public static RedisReadReplicaRouter routerFromEnv() {
        Map<String, String> env = System.getenv();
        requireAuthentication(env.getOrDefault("REDIS_PASSWORD", ""), env);
        return routerFromEnv(env);
    }

    static RedisReadReplicaRouter routerFromEnv(Map<String, String> env) {
        return routerFromEnv(env, Integer.MAX_VALUE);
    }

    static RedisReadReplicaRouter routerFromEnv(Map<String, String> env, int maxTimeoutMs) {
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolCfg = poolConfig(defaultPoolKnobs(env));
        int timeoutMs = Math.min(readPositiveInt(env, "REDIS_TIMEOUT_MS", DEFAULT_TIMEOUT_MS), maxTimeoutMs);
        String username = env.getOrDefault("REDIS_USERNAME", "");
        String password = env.getOrDefault("REDIS_PASSWORD", "");
        boolean tls = Boolean.parseBoolean(env.getOrDefault("REDIS_TLS", "false"));
        RedisExecutor primary = executor(uriFromEnv(env, maxTimeoutMs), poolCfg);
        String localAz = env.getOrDefault("AWS_AZ", env.getOrDefault("AVAILABILITY_ZONE", "unknown"));

        List<RedisReadReplicaRouter.AzExecutor> replicas = new ArrayList<>();
        String spec = env.getOrDefault("REDIS_REPLICA_NODES", "");
        if (!spec.isBlank()) {
            for (String node : spec.split(",")) {
                node = node.strip();
                if (node.isEmpty()) continue;
                ReplicaConfig cfg = ReplicaConfig.parse(node);
                RedisURI uri = replicaUri(cfg, username, password, tls, timeoutMs);
                replicas.add(new RedisReadReplicaRouter.AzExecutor(executor(uri, poolCfg), cfg.az()));
            }
        }
        return new RedisReadReplicaRouter(primary, replicas, localAz);
    }

    public static RedisReadReplicaRouter routerFrom(RedisProperties props) {
        requireAuthentication(props.getPassword(), System.getenv());
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolCfg = poolConfig(props.getPool());
        RedisExecutor primary = from(props);
        String localAz = System.getenv().getOrDefault("AWS_AZ",
                System.getenv().getOrDefault("AVAILABILITY_ZONE", "unknown"));

        List<RedisReadReplicaRouter.AzExecutor> replicas = new ArrayList<>();
        String spec = props.getReplicaNodes();
        if (spec != null && !spec.isBlank()) {
            for (String node : spec.split(",")) {
                node = node.strip();
                if (node.isEmpty()) continue;
                ReplicaConfig cfg = ReplicaConfig.parse(node);
                replicas.add(new RedisReadReplicaRouter.AzExecutor(
                        executor(replicaUriFrom(cfg, props), poolCfg), cfg.az()));
            }
        }
        return new RedisReadReplicaRouter(primary, replicas, localAz);
    }

    // ── URI construction ────────────────────────────────────────────────────────

    static RedisExecutor executor(RedisURI uri,
                                  GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolCfg) {
        RedisClient client = RedisClient.create(uri);
        client.setOptions(failFastOptions());
        // Connection is opened lazily by the executor (Jedis-pool parity).
        return new LettuceRedisExecutor(client, poolCfg, true);
    }

    /**
     * Bounds command latency the way the previous Jedis socket timeout did. The
     * {@link RedisURI} timeout governs the per-command deadline; {@code TimeoutOptions.enabled()}
     * makes that deadline apply even to commands queued while disconnected, and
     * {@code REJECT_COMMANDS} fails commands immediately when the connection is down
     * instead of buffering them — so a down Redis fails fast (e.g. the recall path's
     * 150 ms cap) instead of stalling callers past their budget.
     */
    static ClientOptions failFastOptions() {
        return ClientOptions.builder()
                .timeoutOptions(TimeoutOptions.enabled())
                .disconnectedBehavior(ClientOptions.DisconnectedBehavior.REJECT_COMMANDS)
                .build();
    }

    static RedisURI uriFromEnv(Map<String, String> env, int maxTimeoutMs) {
        String mode = env.getOrDefault("REDIS_MODE", "standalone");
        String username = env.getOrDefault("REDIS_USERNAME", "");
        String password = env.getOrDefault("REDIS_PASSWORD", "");
        boolean tls = Boolean.parseBoolean(env.getOrDefault("REDIS_TLS", "false"));
        int timeoutMs = Math.min(readPositiveInt(env, "REDIS_TIMEOUT_MS", DEFAULT_TIMEOUT_MS),
                Math.max(1, maxTimeoutMs));
        if ("sentinel".equalsIgnoreCase(mode)) {
            String master = env.getOrDefault("REDIS_SENTINEL_MASTER", "mymaster");
            String nodes = env.getOrDefault("REDIS_SENTINEL_NODES", "localhost:26379");
            return sentinelUri(master, nodes, username, password, tls, timeoutMs);
        }
        return standaloneUri(env.getOrDefault("REDIS_HOST", "localhost"),
                parsePort(env.getOrDefault("REDIS_PORT", "6379")), username, password, tls, timeoutMs);
    }

    static RedisURI uriFrom(RedisProperties props) {
        if (props.isSentinelMode()) {
            String nodes = props.getSentinelNodes() == null || props.getSentinelNodes().isBlank()
                    ? "localhost:26379" : props.getSentinelNodes();
            return sentinelUri(props.getSentinelMaster(), nodes, props.getUsername(),
                    props.getPassword(), props.isTls(), props.getTimeoutMs());
        }
        return standaloneUri(props.getHost(), props.getPort(), props.getUsername(),
                props.getPassword(), props.isTls(), props.getTimeoutMs());
    }

    /**
     * AUTH with a username is a Redis 6 ACL login; without one it is the legacy default-user
     * AUTH. Kept in one place so the standalone, sentinel and replica paths cannot drift apart —
     * the replica URIs are built separately and are the easiest of the three to forget.
     */
    private static RedisURI.Builder withAuth(RedisURI.Builder b, String username, String password) {
        boolean hasPassword = password != null && !password.isBlank();
        if (username != null && !username.isBlank()) {
            return b.withAuthentication(username, hasPassword ? password : "");
        }
        return hasPassword ? b.withPassword((CharSequence) password) : b;
    }

    static RedisURI standaloneUri(String host, int port, String username, String password,
                                  boolean tls, int timeoutMs) {
        RedisURI.Builder b = RedisURI.builder()
                .withHost(host)
                .withPort(port)
                .withTimeout(Duration.ofMillis(Math.max(1, timeoutMs)));
        b = withAuth(b, username, password);
        if (tls) b = b.withSsl(true);
        return b.build();
    }

    static RedisURI sentinelUri(String master, String nodes, String username, String password,
                                boolean tls, int timeoutMs) {
        RedisURI.Builder b = RedisURI.builder().withSentinelMasterId(master);
        for (String node : nodes.split(",")) {
            node = node.strip();
            if (node.isEmpty()) continue;
            int c = node.lastIndexOf(':');
            if (c > 0) {
                b = b.withSentinel(node.substring(0, c), parsePort(node.substring(c + 1)));
            } else {
                b = b.withSentinel(node, 26379);
            }
        }
        b = withAuth(b, username, password);
        if (tls) b = b.withSsl(true);
        return b.withTimeout(Duration.ofMillis(Math.max(1, timeoutMs))).build();
    }

    /**
     * A read-replica URI. Extracted from the router loops so the auth and TLS wiring is directly
     * assertable: these URIs are built outside uriFromEnv and are where credentials go missing.
     */
    static RedisURI replicaUri(ReplicaConfig cfg, String username, String password,
                               boolean tls, int timeoutMs) {
        return standaloneUri(cfg.host(), cfg.port(), username, password, tls, timeoutMs);
    }

    /**
     * The Spring-properties replica URI, extracted for the same reason as {@link #replicaUri}:
     * this is the model-serving production path, and reading the credentials off the wrong object
     * — or off none at all — is invisible in a diff of {@link #routerFrom}.
     */
    static RedisURI replicaUriFrom(ReplicaConfig cfg, RedisProperties props) {
        return replicaUri(cfg, props.getUsername(), props.getPassword(),
                props.isTls(), props.getTimeoutMs());
    }

    // ── Pool config ─────────────────────────────────────────────────────────────

    static GenericObjectPoolConfig<StatefulRedisConnection<String, String>> poolConfig(RedisProperties.Pool pool) {
        GenericObjectPoolConfig<StatefulRedisConnection<String, String>> cfg = new GenericObjectPoolConfig<>();
        cfg.setMaxTotal(pool.getMaxTotal());
        cfg.setMaxIdle(pool.getMaxIdle());
        cfg.setMinIdle(pool.getMinIdle());
        cfg.setTestOnBorrow(pool.isTestOnBorrow());
        cfg.setBlockWhenExhausted(true);
        cfg.setMaxWait(Duration.ofMillis(pool.getMaxWaitMs()));
        cfg.setTestWhileIdle(true);
        cfg.setNumTestsPerEvictionRun(-1);
        cfg.setTimeBetweenEvictionRuns(Duration.ofMillis(30_000L));
        cfg.setMinEvictableIdleDuration(Duration.ofMillis(60_000L));
        return cfg;
    }

    static RedisProperties.Pool defaultPoolKnobs(Map<String, String> env) {
        RedisProperties.Pool p = new RedisProperties.Pool();
        p.setMaxTotal(readPositiveInt(env, "REDIS_POOL_MAX_TOTAL", DEFAULT_MAX_TOTAL));
        p.setMaxIdle(readPositiveInt(env, "REDIS_POOL_MAX_IDLE", DEFAULT_MAX_IDLE));
        p.setMinIdle(readNonNegativeInt(env, "REDIS_POOL_MIN_IDLE", DEFAULT_MIN_IDLE));
        p.setMaxWaitMs(readPositiveInt(env, "REDIS_POOL_MAX_WAIT_MS", DEFAULT_MAX_WAIT_MS));
        p.setTestOnBorrow(Boolean.parseBoolean(env.getOrDefault("REDIS_POOL_TEST_ON_BORROW", "true")));
        return p;
    }

    static int parsePort(String value) {
        if (value == null) return 6379;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 6379;
        }
    }

    private static int readPositiveInt(Map<String, String> env, String name, int defaultValue) {
        return Math.max(1, readInt(env.get(name), defaultValue));
    }

    private static int readNonNegativeInt(Map<String, String> env, String name, int defaultValue) {
        return Math.max(0, readInt(env.get(name), defaultValue));
    }

    private static int readInt(String value, int defaultValue) {
        if (value == null || value.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
