# Make the Kafka/Flink partition contract tests runnable and truthful

## Intent and scope
`KafkaFlinkPartitionIntegrationTest` (`@Tag("docker")`) and `KafkaFlinkPartitionLoadTest`
(`@Tag("load")`, `@Tag("docker")`) are the only tests that drive `OnlineFeatureStreamingJob`'s
production graph through a real Kafka source, savepoint and rescale. Found while validating PR
#345: neither has been able to run. Goal: under `-Pstreaming-flink` with Docker running, the
integration test passes or fails only on the job's behaviour, never because of the classpath or
its own harness. No change to production code.

## Findings
1. **Classpath.** `flink-connector-kafka:3.2.0-1.18` declares `flink-connector-base` as `provided`,
   and the `streaming-flink` profile does not add it. Both tests die in setup with
   `NoClassDefFoundError: org/apache/flink/connector/base/source/reader/RecordEmitter`. The
   production job is unaffected: a Flink distribution ships connector-base on its classpath.
2. **Per-user ordering assertion fails 3/3 on main** once the class is present (random adjacent
   swaps, e.g. 1004/1005, 1006/1007). The cause is the test, not the job. `start()` creates a local
   environment with default parallelism 24 and attaches `TestSink` with no parallelism, while the
   dedup and watermark operators it reads from run at `operatorParallelism` (4, then 6 after the
   rescale). A parallelism change makes Flink insert a REBALANCE edge. Records then go round-robin
   to 24 sink subtasks that all append to one shared list, so per-user order is lost after the job
   has preserved it. In production every consumer of `events` re-keys by user (`keyBy(userId)` or
   `keyBy(userId|session)`), or by movie for the order-insensitive aggregations. Hash partitioning
   from a single upstream subtask preserves per-key order, so this edge never exists in production.
3. **Load test's acknowledged-rate floor (50k events/s) is not met under colima** (8–20k/s on this
   arm64 host). That floor measures the Kafka producer before the Flink graph is involved, and the
   class documents itself as a host capacity gate: "valid only for the host on which it runs". This
   is expected behaviour, not a defect.

## Design
- Add `org.apache.flink:flink-connector-base:1.18.1` with `test` scope to the `streaming-flink`
  profile, next to `flink-connector-kafka`. It is test scope because a Flink cluster provides it at
  runtime, and packaging it into the job jar would risk a version clash with the cluster's copy.
- In the integration test's `start()`, set both `TestSink`s to the run's `operatorParallelism`.
  That makes the events edge FORWARD, so the sink sees each dedup subtask's output in order. The
  snapshots sink uses the same parallelism for consistency; it is matched by window, not by order.
- Load test: no change. Record the observed rates in the PR so the next reader does not treat
  them as a regression.
- If fixing (1) and (2) exposes a later assertion failure (restore, dedup, exact Top-K), stop and
  report it as a finding with its evidence instead of loosening the assertion. A genuine job
  defect gets its own design.

## Alternatives
- Make the ordering assertion order-insensitive: hides exactly the property the test exists to
  pin (per-user order across partitions and rescale). Rejected.
- `keyBy(userId)` before the test sink: also preserves order, but adds a shuffle the production
  graph does not have at that point. FORWARD is the more faithful observation.
- Make the load target a `-D` property: no current need (YAGNI); revisit if CI hosts adopt it.

## Validation
- Failing first: with only the dependency added, the ordering assertion fails on the branch
  exactly as on main. With the parallelism fix, the test passes.
- Three runs of `KafkaFlinkPartitionIntegrationTest` on the branch, all passing, compared with the
  3/3 failures already measured on main. Command: `mvn test -Pstreaming-flink -Denforcer.skip=true
  -DexcludedGroups= -Dtest=KafkaFlinkPartitionIntegrationTest`, Docker via colima.
- The default build is unaffected: the Flink test sources are excluded outside the profile, and
  the profile's existing dependency-convergence errors (kryo, kafka-clients) predate this change.
  The command above skips the enforcer for that reason, and this PR does not fix it.
- CI: the PR gate runs only `-Presilience` and excludes `docker`-tagged tests, so these tests stay
  local-only. The PR says so plainly.
