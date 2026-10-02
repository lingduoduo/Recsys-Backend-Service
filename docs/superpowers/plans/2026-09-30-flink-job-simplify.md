# Flink Job Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.
>
> Retroactive record: the code in PR #345 was written before this plan. All steps are checked, and each records the evidence observed.

**Goal:** Cut per-event CPU and per-record Redis bytes in `OnlineFeatureStreamingJob`, and remove duplicated wiring, without changing any persisted contract.

**Architecture:** Faster byte-identical `%.6f` formatting, `EVALSHA` with an `EVAL` fallback in the shared sink base, key-derived Final Top-K timers, and shared graph builders used by both `main()` and the `buildPartitionGraph` test seam.

**Tech Stack:** Java 17, Flink 1.18.1, Lettuce 6.3.2, Maven, JUnit 5, AssertJ, Testcontainers (Redis).

**Spec:** docs/superpowers/specs/2026-09-30-flink-job-simplify-design.md

## Global Constraints
- Operator UIDs, keyed-state descriptor names and state types are unchanged, except that the `topk-final-emit-timer` and `topk-final-cleanup-timer` ValueStates are removed.
- Redis keys and values, and stored embedding state, are byte-identical.
- No new dependencies; Java 17.
- Test command: `mvn test -Pstreaming-flink -Denforcer.skip=true -Dtest='OnlineFeatureStreamingJobTest,MovieEventTest,KafkaTopicPartitionValidatorTest'`, with colima Docker (`DOCKER_HOST=unix:///Users/linghuang/.colima/default/docker.sock`, `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`, `TESTCONTAINERS_RYUK_DISABLED=true`).

## Review Focus
- Values whose `%.6f` form depends on rounding the shortest decimal (5e-7, 2.5e-6) must match `String.format` exactly.
- A negative value that rounds to zero must keep its sign (`-0.000000`).
- A sink must keep writing after Redis forgets its scripts (restart, failover, `SCRIPT FLUSH`).
- A savepoint taken under one `top-k-allowed-lateness-ms` and restored under another must not leak window state.
- A blank Kafka record must be skipped, not crash the job.

---

### Task 1: Byte-identical faster embedding encoding

**Files:** Modify `OnlineFeatureStreamingJob.java` (`UserEmbeddingFunction`: `appendFixed6`, `WHITESPACE`); Test `OnlineFeatureStreamingJobTest#fixedSixFormattingMatchesStringFormatByteForByte`.

- [x] **Step 1:** Microbenchmark plus 5M-value fuzz on JDK 17. Result: `String.format` ~700 ns/value, `BigDecimal.valueOf(v).setScale(6, HALF_UP).toPlainString()` ~237 ns/value, 0 mismatches.
- [x] **Step 2:** Test over 200k seeded values plus edge values (0.0, -0.0, ±1, 5e-7, 1.5e-6, 2.5e-6, 2.5e-7, -1e-9, `Double.MIN_VALUE`/`MAX_VALUE`, NaN, ±∞), asserting equality with `String.format(Locale.ROOT, "%.6f", v)`.
- [x] **Step 3:** Fast path only for `0 < v < +∞`; `+0.0` (raw bits 0) appends `"0.000000"`; everything else uses `String.format`. Precompile the split pattern.
- [x] **Step 4:** Mutation check: with the fast path widened to `Double.isFinite(v)`, the test fails (on `-1e-9`).

### Task 2: EVALSHA with NOSCRIPT fallback

**Files:** Modify `AbstractRedisSink.eval(...)`, its three callers, and `RedisTopKSink.apply`; Test `OnlineFeatureStreamingJobTest#sinkRecoversWhenRedisForgetsTheCachedScript`.

- [x] **Step 1:** Test (Docker): write, `SCRIPT FLUSH`, write again, then assert the second value and that both event ids are in the history.
- [x] **Step 2:** `eval` caches `cmd.digest(script)` per script, calls `evalsha`, and catches `RedisNoScriptException` to fall back to `eval`.
- [x] **Step 3:** Mutation check: with the catch narrowed to an unrelated exception, the test errors.

### Task 3: Key-derived Final Top-K timers

**Files:** Modify `FinalTopKWindowFunction`; Test `OnlineFeatureStreamingJobTest#finalTopKCleansUpTimersRestoredFromADifferentAllowedLateness`.

- [x] **Step 1:** Harness test: emit at lateness 10, snapshot, restore at lateness 50, advance the watermark to 110, and expect 0 keyed-state entries and no new snapshot.
- [x] **Step 2:** Register both timers on each partial; `onTimer` emits on `timestamp == windowEnd` and clears on anything else. Remove the two timer ValueStates.
- [x] **Step 3:** Mutation check: with cleanup requiring `timestamp == cleanupTimestamp(windowEnd, allowedLatenessMs)`, the test fails. The existing multi-channel watermark harness test still passes.

### Task 4: Shared builders and type consolidation

**Files:** Modify `OnlineFeatureStreamingJob.java` (`configure`, `deduplicatedEvents`, `topKSnapshots`, `parseEvents`, `RedisEndpoint`; remove `RedisRecentMoviesSink` and `UserRecentMoviesUpdate`); Modify `OnlineFeatureStreamingJobTest` (sink constructors take `RedisEndpoint`).

- [x] **Step 1:** Route `main()` and `buildPartitionGraph` through `deduplicatedEvents` and `topKSnapshots`; keep every name and UID literal unchanged.
- [x] **Step 2:** `RedisEndpoint` record (`Serializable`, password-free `toString`) replaces the five sink arguments.
- [x] **Step 3:** Recent movies emit `StringFeatureUpdate` into `RedisStringFeatureSink` under UID `redis-user-history-sink-v1`.
- [x] **Step 4:** Focused suite: 37/37 `OnlineFeatureStreamingJobTest`, 6/6 `MovieEventTest`, 4/4 `KafkaTopicPartitionValidatorTest`, 0 skipped.

### Task 5: PR

- [x] **Step 1:** PR #345 opened. Its description records the measurements, the compatibility notes (two dropped timer states, blank Kafka records now skipped silently) and the pre-existing Kafka test breakage, which is now specified separately in `2026-09-30-flink-kafka-contract-tests-design.md`.
