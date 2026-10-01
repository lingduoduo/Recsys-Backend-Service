# Flink Top-K Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce Top-K selection work and remove duplicated ranking logic without changing job behavior.

**Architecture:** Share a bounded-heap selector between partial and final ranking. Consume final ListState directly during synchronous merging.

**Tech Stack:** Java 17, Flink, Maven, JUnit 5, AssertJ.

**Spec:** docs/superpowers/specs/2026-09-30-flink-topk-design.md

## Global Constraints
- Preserve descending score and ascending movie-ID tie ordering.
- Preserve duplicate-movie score summation before selection.
- K=0 returns an empty list; negative K throws IllegalArgumentException, including empty input.
- Preserve operator UIDs, state descriptor names, state types, timers, window boundaries, Kafka and Redis behavior.
- Add no dependencies; retain Java 17 compatibility.
- Do not change raw window buffering or introduce a savepoint migration.

## Review Focus
- Ties at the heap cutoff must favor lower movie IDs.
- Long.MIN_VALUE/Long.MAX_VALUE scores must compare without overflow.
- Empty input and zero K must return empty output.
- Negative K must throw even with empty input.
- K larger than cardinality, including Integer.MAX_VALUE, must not cause eager huge allocation.

### Task 1: Shared bounded Top-K selection

**Files:**
- Modify: `src/main/java/com/recsys/online/flink/OnlineFeatureStreamingJob.java`
- Test: `src/test/java/com/recsys/online/flink/OnlineFeatureStreamingJobTest.java`

**Interfaces:**
- Consumes: existing score maps and `Iterable<PartialTopK>`.
- Produces: `static List<ScoredMovie> selectTopK(Map<Integer, Long> scores, int topK)`.

- [ ] Add tests for all Review Focus cases and an independent full-sort oracle. Core literal fixture:
```java
var scores = Map.of(9, Long.MIN_VALUE, 7, Long.MAX_VALUE, 2, Long.MAX_VALUE, 4, 0L);
assertThat(OnlineFeatureStreamingJob.selectTopK(scores, 2))
        .extracting(movie -> movie.movieId).containsExactly(2, 7);
assertThat(OnlineFeatureStreamingJob.selectTopK(scores, 0)).isEmpty();
assertThatThrownBy(() -> OnlineFeatureStreamingJob.selectTopK(Map.of(), -1))
        .isInstanceOf(IllegalArgumentException.class);
```
- [ ] Run `mvn -Pstreaming-flink test -Dtest=OnlineFeatureStreamingJobTest`. Expected: compilation fails because selectTopK does not exist yet.
- [ ] Implement the shared selector with this algorithm; use PriorityQueue import and a descending-score/ascending-ID comparator:
```java
if (topK < 0) throw new IllegalArgumentException("topK must not be negative");
if (topK == 0 || scores.isEmpty()) return new ArrayList<>();
Comparator<ScoredMovie> ranking = Comparator.comparingLong((ScoredMovie movie) -> movie.score)
        .reversed().thenComparingInt(movie -> movie.movieId);
var candidates = new PriorityQueue<ScoredMovie>(ranking.reversed());
for (var entry : scores.entrySet()) {
    var movie = new ScoredMovie(entry.getKey(), entry.getValue());
    if (candidates.size() < topK) candidates.add(movie);
    else if (ranking.compare(movie, candidates.peek()) < 0) {
        candidates.poll();
        candidates.add(movie);
    }
}
var ranked = new ArrayList<>(candidates);
ranked.sort(ranking);
return ranked;
```
- [ ] Replace both full-sort pipelines with `selectTopK(scores, topK)`. Replace the final temporary partial list with `mergeTopK(partials.get(), topK)`; retain state clearing after collection.
- [ ] Run `mvn -Pstreaming-flink test -Dtest=KafkaTopicPartitionValidatorTest,OnlineFeatureStreamingJobTest,MovieEventTest`. Expected: success; report infrastructure-dependent skips.
- [ ] Run `git diff --check`, inspect the diff for state/graph changes, and commit source plus tests as `perf(flink): bound top-k selection and simplify final merge`.
- [ ] Obtain a whole-branch code review, address material findings, and open a PR with the validation results and performance limits.
