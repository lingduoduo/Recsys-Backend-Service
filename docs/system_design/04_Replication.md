# Replication in Recsys-Backend-Service

An investigation of how the system replicates data for availability and read scaling:
a single-primary Redis with AZ-aware read replicas and Sentinel failover, a continuous
replica-lag probe, and async cross-region replication for disaster recovery. The
recurring shape is **one write leader, many read followers** — writes are linearizable
against the primary, reads fan out to replicas and tolerate bounded lag.

## The big picture

Replication here is deliberately **single-leader**:

- **Writes → one primary.** Every Redis write goes to a single primary pool; there is
  no multi-primary / last-write-wins path, because record ordering and sequence
  uniqueness need one leader (the CP-write side of [05_CAP](05_CAP.md#1-writes-are-cp)).
- **Reads → replicas, AZ-aware.** Reads prefer a same-AZ replica, spreading read QPS and
  avoiding cross-AZ data-transfer cost, and fall back gracefully when replicas are
  absent or unreachable (the AP-read side, [05_CAP](05_CAP.md#2-reads-are-ap-by-default)).
- **Failover is Sentinel's job.** The read router handles read *fan-out*; Redis Sentinel
  handles *leader election* when the primary dies — two orthogonal concerns.
- **Cross-region is async, for DR only.** Redis (ElastiCache Global Datastore) and MySQL
  (Aurora Global) replicate to a standby region asynchronously, with an accepted RPO;
  the streaming tier is *not* cross-region replicated.

## 1. Redis read replicas — AZ-aware read routing

[`RedisReadReplicaRouter`](../../src/main/java/com/recsys/infrastructure/redis/RedisReadReplicaRouter.java)
splits Redis traffic:

- **Writes** always go to the primary pool (`writablePool()`) — the single write leader.
- **Reads** prefer the replica in the **same Availability Zone** as the calling instance
  (`AWS_AZ`), fall back to a random replica, and fall back again to the primary when no
  replicas are configured. A separate `probeReadable()` path (used by the lag probe, §3)
  deliberately does **not** fall back to the primary, so it measures a real replica.

[`RoutingRedisExecutor`](../../src/main/java/com/recsys/infrastructure/redis/RoutingRedisExecutor.java)
is the adapter callers use: `execute(...)` (writes) targets the primary, `executeRead(...)`
targets a replica, and it collapses to single-endpoint behavior when no replicas are
configured — so callers pick read-vs-write intent and the routing is transparent.
Replicas are declared by
[`ReplicaConfig`](../../src/main/java/com/recsys/infrastructure/redis/ReplicaConfig.java) from
`REDIS_REPLICA_NODES` (comma-separated `host:port@az`; port defaults to 6379, az to
`"unknown"` when omitted), and the pools are built by
[`LettuceClientFactory`](../../src/main/java/com/recsys/infrastructure/redis/LettuceClientFactory.java)
(a primary pool plus one per replica, with a latency-capped routing variant for the
recall path).

```bash
# host:port@az (port → 6379 and az → "unknown" when omitted)
export AWS_AZ=us-east-1b
export REDIS_REPLICA_NODES="redis-b.internal:6379@us-east-1b,redis-c.internal:6379@us-east-1c"
```

When `REDIS_REPLICA_NODES` is unset the router routes every read to the primary, so
local dev needs no extra config. This is the system's core CAP dial — see
[05_CAP §4](05_CAP.md#4-the-tunable-dial).

## 2. Primary failover — Sentinel

The read router does not do failover; **Redis Sentinel** does. In Sentinel mode
(`LettuceClientFactory`: `REDIS_MODE=sentinel`, `REDIS_SENTINEL_MASTER` default
`mymaster`, `REDIS_SENTINEL_NODES`), Sentinel monitors the primary and **re-elects** a
new primary from the replicas when it fails, and the Lettuce client follows the new
leader. The division of labor is clean: the router spreads *reads* across replicas for
scale and AZ-locality; Sentinel keeps a *single write leader* alive across failures. The
resilience framing (how failover composes with single-flight and fail-open stores) is in
[18_Fault_Tolerance](18_Fault_Tolerance.md#redis-resilience).

## 3. Measuring replication lag

Replica reads are only safe because the lag is **measured, not assumed**.
[`RedisReplicaLagProbe`](../../src/main/java/com/recsys/infrastructure/redis/RedisReplicaLagProbe.java)
periodically writes a monotonic marker to the primary and reads it back **through replica
routing** (`probeReadable()`, no primary fallback), reporting the observed lag in
seconds. It runs on a schedule (`REDIS_REPLICA_LAG_PROBE_SECONDS`, default 10) and feeds
`ConsistencyMetrics` — publishing `redis_replica_lag_available` and
`redis_replica_lag_seconds` (a probe failure reports `available 0` rather than a
misleading zero). That signal is what makes replica staleness observable and bounds the
read-your-writes decision in [15_Eventual_Consistency §1](15_Eventual_Consistency.md).

## 4. Cross-region replication — for DR

Replication also spans regions, but only for disaster recovery and only asynchronously:

- **Redis** — ElastiCache **Global Datastore** replicates the primary region's Redis to
  the us-west-2 standby asynchronously (RPO ~seconds).
- **MySQL** — Aurora **Global Database** replicates the catalog/outbox to the standby
  (RPO ~seconds); writes fail over via a **manual** data-tier promotion.
- **Model artifacts** — ECR cross-region **digest replication**, so the standby pulls the
  identical image (no loss).
- **Streaming is *not* cross-region replicated.** There is no cross-region Kafka broker
  replication; the standby Flink runs its own offsets, so after failover online features
  are only as fresh as the standby's own consumer — the in-flight window is **accepted
  loss**. This is the region-boundary CAP choice in
  [05_CAP §5](05_CAP.md#5-partition-tolerance--the-p-is-real-and-bounded); designs:
  [multi-region DR failover](../superpowers/specs/2026-07-08-multi-region-dr-failover-design.md),
  [zonal failure hardening](../superpowers/specs/2026-07-08-zonal-failure-hardening-design.md).

## 5. What is *not* replicated in-app

Not everything is replicated, by design:

- **MySQL is a single read-only pool in-app.** The application opens one read-only
  HikariCP pool; replica/failover for MySQL is Aurora's job at the infra layer, not the
  app's.
- **`OnlineLearner` state is per-pod.** Learned biases live in each JVM and converge only
  via a 30 s Redis flush — they are *not* replicated across pods, so two pods can rank the
  same user differently until they flush.
- **Fire-and-forget events aren't replicated buffers.** `AsyncEventPublisher`'s in-memory
  queue is per-instance and at-most-once; durability comes from the outbox, not
  replication (see [07_Message_Queue](07_Message_Queue.md)).

## 6. Testing

- **Routing** — `RedisReadReplicaRouterTest` (write→primary, AZ-local preference, stable
  first-replica fallback, primary fallback when no replicas, `probeReadable`
  no-primary-fallback), `RoutingRedisExecutorTest` (read vs write routing,
  single-endpoint collapse).
- **Config** — `ReplicaConfigTest` (`host:port@az` parsing, port/az defaults).
- **Lag** — `RedisReplicaLagProbeTest` (marker round-trip, lag reporting,
  unavailable-on-failure).
- **Client build** — `LettuceClientFactoryTest` (pool construction, Sentinel URI).
- **Pipeline connection lifecycle** — `LettuceRedisExecutorPipelineTest` (a failed batch
  destroys its connection rather than returning it to the pool).
- **Sentinel tier, against a real cluster** — `scripts/k8s-sentinel-smoke-test.sh`. Every
  test above is a unit test; none of them starts a Sentinel. The script applies
  `k8s/base/redis-cluster.yaml` to a throwaway cluster (minikube/kind/k3d) and asserts the
  three things only a live cluster can answer: the Sentinel pods start, they reach quorum,
  and a client discovers a *reachable* primary through the `redis-sentinel` Service. Run it
  after any change to the sentinel template.

**Replica fallback is stable, not random.** When no same-AZ replica exists, `readable()`
returns the *first configured* replica, deliberately: `readable()` and `probeReadable()`
must resolve to the same node, or the lag probe's correlated sequence check reports the
lag of a replica no read actually used. Do not "fix" this to spread read load without
also reworking `RedisReplicaLagProbe`.

**A failed pipeline destroys its connection.** `executePipelined` borrows a dedicated
connection and disables auto-flush on it. If the callback throws after queueing but
before `flushCommands()`, those commands stay buffered — re-enabling auto-flush does not
flush them — so returning the connection would let the *next* borrower's flush execute a
failed batch's writes. On any failure the connection is invalidated instead, costing one
connection rather than risking a silent replay.

## Sharp edges — notes

1. **Replica reads are stale by design.** A same-AZ replica can lag the primary; the lag
   probe measures it and the read-your-writes token is the escape hatch, but the default
   read path accepts bounded staleness.
2. **Sentinel gives failover, not multi-primary.** There is still exactly one write
   leader; a partitioned minority can't write. Failover is seconds, not zero.
3. **Cross-region replication is async with an accepted RPO.** Redis/MySQL lag the
   primary region by ~seconds and the streaming tier isn't replicated at all — a regional
   failover accepts the in-flight window as loss.
4. **`AWS_AZ` must be set for locality to help.** Without the AZ tag every replica looks
   equally distant, so same-AZ cost savings evaporate (reads still work, just not
   AZ-optimally).
5. **No replicas configured = primary-only.** Unset `REDIS_REPLICA_NODES` is a valid
   (single-node) deployment, but then reads and writes share the primary and there is no
   read scaling or AZ-locality.
6. **The sentinel template must set `resolve-hostnames yes`.** It monitors the primary by
   hostname, and Redis Sentinel defaults `resolve-hostnames` to `no` — which makes a
   hostname a *fatal config file error*, not a lookup. Without the directive every Sentinel
   pod CrashLoopBackOffs at startup; this was the state of both
   `k8s/base/redis-cluster.yaml` and `docker/redis/sentinel.conf` until 2026-09-19, and
   nothing noticed, because the EKS overlays scale the Sentinel StatefulSet to zero in
   favour of ElastiCache. `scripts/k8s-sentinel-smoke-test.sh` is the check that would
   have caught it.
7. **Sentinel monitors the `redis-primary` Service, not a pod.** `sentinel monitor
   mymaster redis-primary 6379` resolves to that Service's ClusterIP — measured in
   minikube, Sentinel reported the master at a service-range address (`10.97.110.138`)
   while the replicas it discovered were pod IPs (`10.244.0.3`, `10.244.0.5`). A ClusterIP
   stays reachable for as long as the Service has any endpoint, so Sentinel's failure
   detector is watching a load-balancer VIP rather than the process it is supposed to
   supervise, and a promotion would not move the Service's `role: primary` selector
   anyway. Quorum and discovery work — verified — but **automatic failover in-cluster
   should not be assumed to work until it is exercised**, which the smoke test deliberately
   does not claim to do.
