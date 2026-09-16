# Gateway Response Streaming Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Forward ordinary gateway responses without whole-body buffering.

**Architecture:** A package-private response wrapper observes stream status and terminal outcomes and maps pre-response failures. GatewayRequestForwarder delegates upstream responses to it.

**Tech Stack:** Java 17, Armeria 1.28.4, JUnit 5, Maven.

**Spec:** `docs/superpowers/specs/2026-09-15-gateway-response-streaming-design.md`

## Global constraints

Preserve request authorization, rate limits, retry policy, and upstream selection. Preserve headers and trailers. Settle circuit permits once, on termination. Use Java 17 and add no dependencies. Do not change LLM proxy behavior.

## Task 1: Streaming and regression coverage

- [x] Add `GatewayResponseStreamingTest.java`: use a real upstream that sends headers and an initial chunk, and hold completion until the test observes that chunk. Run this test against the buffered implementation and verify it fails.
- [x] Add `GatewayUpstreamResponse.java` with `static HttpResponse relay(HttpResponse upstream, RouteCircuitBreaker breaker, RouteCircuitBreaker.Permit permit, String routeName)`. Observe response objects through `FilteredHttpResponse`; return each unchanged. Use terminal callbacks to settle the permit once and `recover` to map failures before emission.
- [x] Replace `upstream.aggregate()` in `GatewayRequestForwarder.forward` with `GatewayUpstreamResponse.relay(upstream, cb, permit, route.name())`; move no-healthy-endpoint error classification into the helper.
- [x] Cover trailers, status accounting, pre-header failure, partial failure, and cancellation in `GatewayUpstreamResponseTest.java`.
- [x] Run `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Dtest=GatewayResponseStreamingTest,GatewayUpstreamResponseTest test`, then existing gateway tests and the full default suite, sequentially.

## Task 2: Evidence and delivery

- [x] Measure a controlled concurrent streaming fixture against the buffered and streaming paths; record first-byte and completion latency, explaining limitations.
- [x] Update `docs/system_design/09_API_Gateway.md` with the failure contract and memory implications.
- [x] Obtain independent code review, resolve findings, run `git diff --check`, commit, push, and create a PR against main.

## Amendment during execution

Independent review measured that the approved contract's "cancellation fails" rule
let an unauthenticated caller open a public route's circuit with five hang-ups —
a regression, since `aggregate()` had made the buffered path immune. Reproduced
locally, then fixed with a third settle outcome (`CircuitBreaker.releasePermit`)
after the user approved amending the spec. Also added from review: both new tests
to the `-Presilience` PR gate, a pooled-buffer test, a null-permit guard, and a
streaming test through the assembled decorator stack.

## Measured result

Local fixture, 32 concurrent requests, 256 KiB body whose tail is gated 100 ms
behind the first chunk. Two runs:

| | first-byte p50 | first-byte p95 | complete p50 | complete p95 |
|---|---|---|---|---|
| buffered | 166 / 156 ms | 294 / 284 ms | 169 / 184 ms | 304 / 300 ms |
| streaming | 36 / 49 ms | 129 / 75 ms | 164 / 161 ms | 225 / 212 ms |

First-byte latency improves ~3-4x; completion latency and throughput are
essentially unchanged. This is a local loopback fixture with a synthetic gating
delay, not a production capacity measurement.

