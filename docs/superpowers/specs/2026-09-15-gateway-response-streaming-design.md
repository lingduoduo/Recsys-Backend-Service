# Gateway response streaming

## Approved design

Optimize latency and throughput by forwarding ordinary upstream responses as streams instead of aggregating the full body. The user approved this design and the changed failure contract: before final response headers are emitted, return the existing 502/503 JSON error; after final headers, terminate the failed stream. Request aggregation remains necessary for authorization and canonical recommendation request translation. The LLM proxy is outside this change.

## Implementation contract

Use Armeria stream operators with backpressure and cancellation propagation. Observe final response status without retaining body chunks. Settle each circuit-breaker permit exactly once: a completed non-5xx response succeeds; 5xx or stream failure fails; downstream cancellation and abort-before-subscription settle **neutrally**, releasing a half-open probe slot without counting for or against the route. (Amended during implementation. The contract originally said cancellation fails; that was measured to let an unauthenticated caller open a public route's circuit with five hang-ups, which the buffered implementation could not do. The user approved the change.) Do not mark success on headers alone. Preserve headers, trailers, credentials handling, rate limits, retries, and endpoint selection. No new dependencies. Use Java 17.

## Verification

A real upstream must deliver an initial chunk while its final chunk is gated by the test. Verify failure before headers, failure after headers, 5xx, trailers, cancellation, and circuit settlement. Run existing gateway and full default tests sequentially. Compare first-byte and completion latency with buffered versus streamed responses under concurrent requests; report local measurements without claiming production throughput gains.
