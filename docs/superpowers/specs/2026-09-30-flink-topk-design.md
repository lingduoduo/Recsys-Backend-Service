# Flink Top-K optimization

## Intent and scope
Optimize and simplify the existing Java Flink job's ranking path. Preserve outputs and persisted-state contracts. The user requested a spec, implementation plan, code, and a pull request after reviewing the proposed bounded optimization.

## Current behavior
Both partial-window ranking and final merging aggregate movie scores into a map, sort every map entry by descending score then ascending movie ID, and retain K movies. Final emission also copies ListState into an ArrayList before immediately iterating it.

## Design
Introduce a package-private static `selectTopK(Map<Integer, Long> scores, int topK)` helper in OnlineFeatureStreamingJob. Use a PriorityQueue with the worst retained movie at its head. Compare scores with Comparator.comparingLong and IDs with thenComparingInt, avoiding arithmetic overflow. Retain at most K candidates, then sort only those candidates into output order. Both ranking stages delegate to this helper after their existing aggregation. Final emission passes `partials.get()` directly into mergeTopK and clears state after the synchronous merge and collection.

For N distinct movies and M=min(N,K), selection takes O(N log(M+1) + M log(M+1)) time and O(M) auxiliary space. Existing O(N) score aggregation remains. Do not preallocate the queue from an unbounded user-supplied K. No throughput claim is made without a deployment benchmark.

## Compatibility constraints
- Preserve descending score and ascending movie-ID tie ordering.
- Preserve duplicate-movie score summation before selection.
- K=0 returns an empty list; negative K throws IllegalArgumentException, including empty input.
- Preserve operator UIDs, state descriptor names, state types, timers, window boundaries, Kafka and Redis behavior.
- Add no dependencies; retain Java 17 compatibility.
- Do not change raw window buffering or introduce a savepoint migration.

## Alternatives
Keeping full sorting is simplest but repeats ranking code and sorts discarded candidates. Incremental window aggregation could reduce buffered events but changes state contracts and exceeds this scope. The bounded heap targets selection without changing the job graph.

## Validation
Compare helper output against an independent full-sort oracle across seeded inputs and multiple K values; cover ties, long-score extremes, empty input, zero/negative/oversized K. Existing partial/final and watermark tests exercise integration. Run the focused Flink suite and report Docker-dependent skips explicitly. Review the complete branch before opening the PR.
