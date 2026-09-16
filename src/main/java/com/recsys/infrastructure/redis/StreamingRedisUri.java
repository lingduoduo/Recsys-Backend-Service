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
 * <p>Delegates to {@link LettuceClientFactory#standaloneUri} rather than reimplementing it. A job
 * and a service must authenticate identically against the same server; {@code StreamingRedisUriTest}
 * asserts equality with that method rather than against a hand-written URI, so the two cannot drift
 * apart in the same edit.
 */
public final class StreamingRedisUri {

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
