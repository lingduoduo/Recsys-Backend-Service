# Kafka/Flink Contract Tests Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `KafkaFlinkPartitionIntegrationTest` runnable under `-Pstreaming-flink` and make its per-user ordering assertion observe the job, not a rebalance edge in the harness.

**Architecture:** Add one test-scoped dependency to the `streaming-flink` profile. Run the integration test's `TestSink`s at the operator parallelism, so the edge from the dedup and watermark operators is FORWARD. No production code changes.

**Tech Stack:** Java 17, Maven, Flink 1.18.1, flink-connector-kafka 3.2.0-1.18, Testcontainers (Kafka `confluentinc/cp-kafka`), JUnit 5, AssertJ.

**Spec:** docs/superpowers/specs/2026-09-30-flink-kafka-contract-tests-design.md

## Global Constraints
- No change to `src/main`. The production job jar must not bundle `flink-connector-base` (a Flink cluster provides it), so the dependency is `test` scope.
- Version: `org.apache.flink:flink-connector-base:1.18.1`, matching the profile's other Flink 1.18.1 artifacts.
- Do not loosen any assertion. A failure after the fix is a finding to report with evidence, not something to edit away.
- Load test (`KafkaFlinkPartitionLoadTest`): no code change.
- Environment for every run (colima Docker):
  ```bash
  export DOCKER_HOST=unix:///Users/linghuang/.colima/default/docker.sock \
         TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock \
         TESTCONTAINERS_RYUK_DISABLED=true \
         JAVA_HOME=$(/usr/libexec/java_home -v 17)
  ```
- Test command (the enforcer skip is for pre-existing convergence errors; `-DexcludedGroups=` is required or the `docker`-tagged test silently does not run):
  ```bash
  rm -rf target/surefire-reports
  mvn -q test -Pstreaming-flink -Denforcer.skip=true -DexcludedGroups= \
      -Dtest=KafkaFlinkPartitionIntegrationTest -Dsurefire.failIfNoSpecifiedTests=false
  grep -ho 'tests="[0-9]*" errors="[0-9]*" skipped="[0-9]*" failures="[0-9]*"' \
      target/surefire-reports/TEST-*.xml
  ```
  Read the XML counts, not Maven's exit code. A run that produced no report did not run.

## Review Focus
- **No report produced**: missing `-DexcludedGroups=` or Docker down means zero tests ran. Treat it as "not run", never as "pass".
- **Rescaled restore (parallelism 6)**: `start()` is called twice with different parallelism, so the sink fix must use the argument, not a constant 4.
- **Packaging**: `mvn -Pstreaming-flink dependency:tree` must show connector-base as `test`, never `compile`, or the fat job jar ships a second copy.
- **Restore-phase assertions** (dedup of `shared-event-id`, exact Top-K at window 7000): previously unreachable; they may fail for a genuine reason once ordering passes. Stop and report.
- **Flakiness**: one green run proves little. Run three times and quote all three.

---

### Task 1: Put `flink-connector-base` on the streaming-flink test classpath

**Files:**
- Modify: `pom.xml:873-877` (the `streaming-flink` profile's `<dependencies>`, directly before the `flink-connector-kafka` entry)

**Interfaces:**
- Consumes: nothing.
- Produces: `org.apache.flink.connector.base.*` on the test classpath of `-Pstreaming-flink`.

- [ ] **Step 1: Run the test to see the classpath failure**

Run the test command from Global Constraints.
Expected: `errors="1"`, and `target/surefire-reports/*.txt` contains `NoClassDefFoundError: org/apache/flink/connector/base/source/reader/RecordEmitter`.

- [ ] **Step 2: Add the dependency**

Insert before the `flink-connector-kafka` `<dependency>` in the `streaming-flink` profile:

```xml
        <dependency>
          <!-- flink-connector-kafka declares this `provided`: a Flink cluster supplies it at
               runtime, but the MiniCluster tests need it explicitly. Test scope keeps it out of
               the job jar. -->
          <groupId>org.apache.flink</groupId>
          <artifactId>flink-connector-base</artifactId>
          <version>1.18.1</version>
          <scope>test</scope>
        </dependency>
```

- [ ] **Step 3: Run the test; the classpath error is gone and the ordering failure appears**

Run the test command.
Expected: `errors="0"` with `failures="1"`; the `.txt` report shows `kafkaSourceProductionGraphAndRescaledRestorePreserveOrderingDedupAndTopK` failing on `containsExactly(1000L … 1007L)` with an adjacent swap. This is the failing-first state for Task 2.

- [ ] **Step 4: Verify the scope**

Run: `mvn -q -o -Pstreaming-flink -Denforcer.skip=true dependency:tree -Dincludes=org.apache.flink:flink-connector-base`
Expected: `org.apache.flink:flink-connector-base:jar:1.18.1:test`.

- [ ] **Step 5: Commit**

```bash
git add pom.xml
git commit -m "build(flink): add flink-connector-base to the streaming-flink test classpath"
```

### Task 2: Observe dedup output through a FORWARD edge

**Files:**
- Modify: `src/test/java/com/recsys/online/flink/KafkaFlinkPartitionIntegrationTest.java:175-176` (inside `start(...)`)

**Interfaces:**
- Consumes: Task 1's classpath; `start(String topic, String run, int operatorParallelism, String savepoint)`.
- Produces: nothing new; `TestSink` usage only.

- [ ] **Step 1: Confirm the failing test (from Task 1 Step 3)**

The ordering assertion fails with an adjacent swap. Don't proceed if it passes, because then the spec's diagnosis is wrong.

- [ ] **Step 2: Set the sink parallelism**

Replace:

```java
        graph.events().addSink(new TestSink<>(run, "events")).name("test-events-" + run);
        graph.snapshots().addSink(new TestSink<>(run, "snapshots")).name("test-snapshots-" + run);
```

with:

```java
        // The environment defaults to 24, but dedup runs at operatorParallelism; a mismatch inserts
        // a REBALANCE edge that scatters one user's records across sink subtasks and destroys the
        // per-user order this test asserts. Matching parallelism makes the edge FORWARD.
        graph.events().addSink(new TestSink<>(run, "events")).name("test-events-" + run)
                .setParallelism(operatorParallelism);
        graph.snapshots().addSink(new TestSink<>(run, "snapshots")).name("test-snapshots-" + run)
                .setParallelism(operatorParallelism);
```

- [ ] **Step 3: Run the test three times**

Run the test command three times.
Expected each time: `tests="2" errors="0" skipped="0" failures="0"`. If a later assertion fails (restore dedup, exact Top-K), stop: record the report text and raise it as a finding (spec, Design, last bullet).

- [ ] **Step 4: Prove the fix is what turned it green**

Temporarily revert only Step 2 (`git stash push src/test/java/com/recsys/online/flink/KafkaFlinkPartitionIntegrationTest.java`), run once, and expect the ordering failure. Then `git stash pop`.

- [ ] **Step 5: Run the focused Flink suite for regressions**

```bash
rm -rf target/surefire-reports
mvn -q test -Pstreaming-flink -Denforcer.skip=true \
    -Dtest='OnlineFeatureStreamingJobTest,MovieEventTest,KafkaTopicPartitionValidatorTest' \
    -Dsurefire.failIfNoSpecifiedTests=false
```
Expected: all pass, 0 skipped (Docker up).

- [ ] **Step 6: Commit**

```bash
git add src/test/java/com/recsys/online/flink/KafkaFlinkPartitionIntegrationTest.java
git commit -m "test(flink): observe dedup output through a forward edge"
```

### Task 3: Open the PR

- [ ] **Step 1: Push and open the PR** with:
  - the three run results on main vs. the branch;
  - the `dependency:tree` scope line;
  - the load-test rates observed under colima, with a note that it is a host capacity gate and unchanged;
  - an explicit statement that CI (`-Presilience`, `docker` excluded) still does not run these tests.
