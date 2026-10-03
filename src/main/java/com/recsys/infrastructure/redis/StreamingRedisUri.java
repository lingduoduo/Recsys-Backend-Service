package com.recsys.infrastructure.redis;

import io.lettuce.core.RedisURI;

/**
 * Builds a Redis URI for the Flink and Spark jobs, on the same terms as every service.
 *
 * <p>Those jobs live in {@code online/flink/} and {@code training/embedding/}, which are excluded
 * from the Maven compile because they need Flink and Spark classpaths. Nothing there can be
 * compiled, tested, or made to fail a build — so the decision about what a Redis URI should look
 * like lives here instead, and each job calls this in one line.
 *
 * <p>They previously called {@code RedisURI.create(host, port)} with no credentials at all, which
 * is not a hardening gap but an outage waiting on a deploy: the moment {@code requirepass} is
 * applied they fail {@code NOAUTH}, and {@code OnlineFeatureStreamingJob} writes {@code u2vEmb:*}
 * and {@code topk:*} that every serving path reads.
 *
 * <p>Delegates to the shared standalone and Sentinel URI builders. Jobs snapshot connection
 * settings on the driver and build the URI inside workers, so neither a non-serializable URI nor
 * worker-local environment defaults can change which primary receives writes.
 */
public final class StreamingRedisUri {

    /** Builds inside the worker from serialized settings; Sentinel never uses the static host. */
    public static RedisURI from(String host, int port, String username, String password,
                                boolean tls, String mode, String master, String nodes) {
        if ("sentinel".equalsIgnoreCase(mode)) {
            if (master == null || master.isBlank() || nodes == null || nodes.isBlank()) {
                throw new IllegalArgumentException("Sentinel mode requires redis sentinel master and nodes");
            }
            return LettuceClientFactory.sentinelUri(master, nodes,
                    username == null ? "" : username, password == null ? "" : password,
                    tls, LettuceClientFactory.DEFAULT_TIMEOUT_MS);
        }
        if (mode != null && !mode.isBlank() && !"standalone".equalsIgnoreCase(mode)) {
            throw new IllegalArgumentException("Unknown Redis mode: " + mode);
        }
        return from(host, port, username, password, tls);
    }

    private StreamingRedisUri() {
    }

    /**
     * @param username blank or null for a legacy default-user login; non-blank for a Redis 6+ ACL
     *                 login. A Flink parameter default and an unset environment variable arrive
     *                 differently, so both must mean the same thing here.
     * @param password blank or null for no {@code AUTH} at all when the username is also blank
     */
    public static RedisURI from(String host, int port, String username, String password,
                                boolean tls) {
        return LettuceClientFactory.standaloneUri(host, port,
                username == null ? "" : username,
                password == null ? "" : password,
                tls, LettuceClientFactory.DEFAULT_TIMEOUT_MS);
    }
}
