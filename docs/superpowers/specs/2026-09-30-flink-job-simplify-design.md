# Flink job: cheaper encoding, EVALSHA sinks, shared graph builders

## Intent and scope
Follow-up to the Top-K optimization (`2026-09-30-flink-topk-design.md`, PR #344). Optimize and
simplify the rest of `OnlineFeatureStreamingJob` while preserving every persisted contract:
operator UIDs, keyed-state names and serializers, Redis keys and values, and Kafka behaviour.
Optimizations are accepted only with a measurement; simplifications only if they remove duplication
without changing the job graph's stateful topology.

## Current behavior
- `UserEmbeddingFunction` keeps raw counts as a string in state. Each event parses 16 values
  (`split("\\s+")`, recompiling the regex) and formats 32 with `String.format("%.6f")`: 16 raw values
  for state and 16 normalized ones for Redis.
- Every Redis sink write is `EVAL <full Lua body>`, shipping the script (up to ~500 bytes) per record.
- `FinalTopKWindowFunction` stores its emit and cleanup timer timestamps in two `ValueState<Long>`s,
  although the key *is* the window end and both timestamps are functions of it. A cleanup timer is
  honoured only if it equals the stored value.
- `main()` and the `buildPartitionGraph` test seam each build dedup and the two Top-K stages
  separately, so the Kafka contract tests exercise a copy of the production wiring.
- Seven operators repeat the same name/uid/parallelism/maxParallelism chain; every sink takes five
  connection arguments; `RedisRecentMoviesSink`/`UserRecentMoviesUpdate` duplicate
  `RedisStringFeatureSink`/`StringFeatureUpdate` exactly; the Kafka and file sources duplicate
  their JSON parse lambda.

## Design
1. **Embedding encoding.** `appendFixed6` uses `BigDecimal.valueOf(v).setScale(6, HALF_UP)
   .toPlainString()`. `String.format` and this expression both round the shortest decimal
   representation half-up, so their output is byte-identical. BigDecimal loses the sign when a
   negative rounds to zero, so only positive finite values take the fast path. `+0.0` (most of a
   sparse vector) appends a constant, and anything else falls back to `String.format`. The split
   uses a precompiled `Pattern`.
2. **EVALSHA.** `AbstractRedisSink.eval` computes the script SHA locally (`digest`, no round trip)
   once per script per sink instance, runs `EVALSHA`, and on `RedisNoScriptException` falls back to
   `EVAL`, which runs the script and caches it again. That covers a restart, a failover to a fresh
   primary, or `SCRIPT FLUSH`. `EVALSHA` is in `@scripting`, the ACL category `EVAL` already needs.
3. **Key-derived Top-K timers.** Each partial registers `windowEnd` and
   `cleanupTimestamp(windowEnd)`; Flink deduplicates re-registration. `onTimer` emits on
   `timestamp == windowEnd` (unless `emitted`) and treats any other timestamp as cleanup. Treating
   any other timestamp as cleanup also clears windows whose timers were restored from a run with a
   different `top-k-allowed-lateness-ms`; the exact-match version would leak them.
4. **Shared builders.** `deduplicatedEvents(...)` and `topKSnapshots(...)` are used by both
   `main()` and `buildPartitionGraph`; `configure(...)` replaces the repeated operator chain;
   `parseEvents(...)` serves both sources.
5. **Type consolidation.** A serializable `RedisEndpoint` record replaces the five sink arguments;
   its `toString` omits the password. The recent-movies path emits `StringFeatureUpdate` into
   `RedisStringFeatureSink`, keeping the `redis-user-history-sink-v1` UID.

## Compatibility constraints
- Operator UIDs, keyed-state descriptor names, and state types are unchanged except for the two
  removed timer `ValueState`s. Their entries in an existing savepoint are never read, so restore
  does not fail.
- Redis output and stored embedding state are byte-identical (item 1).
- One deliberate behaviour change: blank Kafka records are skipped silently instead of reaching
  Jackson and logging a "malformed JSON" WARN. The file source already skipped them.
- No new dependencies; Java 17.

## Alternatives
- Store the embedding as `double[]` in state: removes parsing entirely but changes the state
  serializer and needs a savepoint migration. Rejected for this scope.
- `SCRIPT LOAD` in `open()`: one extra round trip and still needs the NOSCRIPT fallback after a
  failover, so local `digest` plus a fallback is strictly simpler.
- Key the movie-metric window by a tuple instead of a `"movieId|kind"` string: avoids a split per
  window fire but changes the window state's key serializer. Rejected.

## Validation
- Microbenchmark on JDK 17: `String.format` vs BigDecimal over 3.2M formats, and a 5M-value fuzz
  for byte equality. Measured ~700 vs ~237 ns per value, 0 mismatches.
- New tests, each shown to fail against the bug it guards:
  - a 200k-value formatting equivalence test including negatives, `-0.0`, NaN and infinities
    (fails if BigDecimal handles every finite value);
  - a sink that keeps writing after `SCRIPT FLUSH` (errors without the NOSCRIPT fallback);
  - a Final Top-K harness restored at lateness 50 from a snapshot taken at lateness 10 (fails with
    exact-match cleanup).
- Focused Flink suite under `-Pstreaming-flink` with Docker running, so the Redis tests do not skip.
