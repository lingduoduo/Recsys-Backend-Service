package com.recsys.infrastructure.redis;

import com.recsys.config.RedisProperties;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

/** Opt-in live test: requires redis-server on PATH; no shared Redis or Docker. */
@EnabledIfSystemProperty(named = "redis.failover.test", matches = "true")
class LettuceSentinelFailoverTest {
    @TempDir Path directory;
    private final List<Process> processes = new ArrayList<>();

    @Test
    void establishedSharedAndPooledConnectionsFollowSentinelPromotion() throws Exception {
        int primaryPort = port(), replicaPort = port(), sentinelPort = port();
        String password = "failover-test-password";
        io.lettuce.core.RedisClient streamingClient = null;
        io.lettuce.core.api.StatefulRedisConnection<String, String> streamingConnection = null;
        try {
            Process primary = start("primary", "port " + primaryPort + "\nrequirepass " + password);
            Process replica = start("replica", "port " + replicaPort + "\nrequirepass " + password
                    + "\nmasterauth " + password + "\nreplicaof 127.0.0.1 " + primaryPort);
            start("sentinel", "port " + sentinelPort
                    + "\nsentinel monitor mymaster 127.0.0.1 " + primaryPort + " 1"
                    + "\nsentinel auth-pass mymaster " + password
                    + "\nsentinel down-after-milliseconds mymaster 1000"
                    + "\nsentinel failover-timeout mymaster 10000");
            RedisProperties props = new RedisProperties();
            props.setMode("sentinel");
            props.setSentinelNodes("127.0.0.1:" + sentinelPort);
            props.setPassword(password);
            try (RedisExecutor executor = LettuceClientFactory.from(props);
                 RedisExecutor replicaClient = LettuceClientFactory.executor(
                         LettuceClientFactory.standaloneUri("127.0.0.1", replicaPort, "", password, false, 500),
                         new org.apache.commons.pool2.impl.GenericObjectPoolConfig<>())) {
                await(() -> "OK".equals(executor.execute(c -> c.set("canary", "before"))));
                await(() -> "before".equals(replicaClient.execute(c -> c.get("canary"))));
                // Warm the dedicated pool before failure as well as the shared connection.
                var pooledCommands = executor.executePrimaryRead(c -> c, Duration.ofSeconds(2));
                assertEquals("before", pooledCommands.get("canary"));
                // Same raw RedisClient/URI path used by Flink and Spark workers.
                streamingClient = io.lettuce.core.RedisClient.create(StreamingRedisUri.from(
                        "", 6379, "", password, false, "sentinel", "mymaster", "127.0.0.1:" + sentinelPort));
                streamingConnection = streamingClient.connect();
                var streamingCommands = streamingConnection.sync();
                assertEquals("OK", streamingCommands.set("streaming-canary", "before"));
                primary.destroyForcibly().waitFor();
                await(() -> "OK".equals(executor.execute(c -> c.set("canary", "after"))));
                await(() -> "after".equals(executor.executePrimaryRead(c -> c.get("canary"), Duration.ofSeconds(2))));
                assertSame(pooledCommands, executor.executePrimaryRead(c -> c, Duration.ofSeconds(2)),
                        "The established pooled connection must recover rather than be replaced");
                await(() -> "OK".equals(streamingCommands.set("streaming-canary", "after")));
                assertEquals("after", replicaClient.execute(c -> c.get("streaming-canary")));
                assertTrue(replica.isAlive());
                assertTrue(replicaClient.execute(c -> c.info("replication")).contains("role:master"));
                assertEquals("after", replicaClient.execute(c -> c.get("canary")));
                start("primary", "port " + primaryPort + "\nrequirepass " + password
                        + "\nmasterauth " + password);
                try (RedisExecutor returningPrimary = LettuceClientFactory.executor(
                        LettuceClientFactory.standaloneUri("127.0.0.1", primaryPort, "", password, false, 500),
                        new org.apache.commons.pool2.impl.GenericObjectPoolConfig<>())) {
                    await(() -> returningPrimary.execute(c -> c.info("replication")).contains("role:slave"));
                    await(() -> "after".equals(returningPrimary.execute(c -> c.get("canary"))));
                    assertEquals("OK", executor.execute(c -> c.set("canary", "recovered")));
                    await(() -> "recovered".equals(returningPrimary.execute(c -> c.get("canary"))));
                }
            }
        } finally {
            if (streamingConnection != null) streamingConnection.close();
            if (streamingClient != null) streamingClient.shutdown();
            for (Process process : processes) process.destroyForcibly();
            for (Process process : processes) process.waitFor();
        }
    }

    private Process start(String name, String configuration) throws Exception {
        Path config = directory.resolve(name + ".conf");
        Files.writeString(config, "bind 127.0.0.1\nsave \"\"\nappendonly no\ndir " + directory
                + "\n" + configuration + "\n");
        List<String> command = new ArrayList<>(List.of("redis-server", config.toString()));
        if (name.equals("sentinel")) command.add("--sentinel");
        Process process = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(directory.resolve(name + ".log").toFile()).start();
        processes.add(process);
        return process;
    }

    private static int port() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) { return socket.getLocalPort(); }
    }

    private static void await(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(45).toNanos();
        RuntimeException last = null;
        while (System.nanoTime() < deadline) {
            try { if (condition.getAsBoolean()) return; }
            catch (RuntimeException transientFailure) { last = transientFailure; }
            Thread.sleep(100);
        }
        throw new AssertionError("Redis did not converge within 45 seconds", last);
    }
}
