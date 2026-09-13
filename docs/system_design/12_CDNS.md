# CDN Edge in Recsys-Backend-Service

An investigation of the optional CloudFront edge that fronts the API Gateway ALB:
what it caches (narrowly) and what it deliberately doesn't, how the two shared
read routes are made cacheable without leaking private data, how the origin is
locked to *our* distribution, how the distribution is provisioned out-of-band, and
a local nginx stand-in that reproduces the cache semantics with no AWS account.

## The big picture

The CDN does four things, only one of which is caching:

- **Edge TLS termination** — viewer TLS terminates at the edge (ACM cert in
  us-east-1, `TLSv1.2_2021`, SNI-only).
- **Attack drop** — a `CLOUDFRONT`-scope WAF WebACL filters traffic before it
  reaches the region.
- **Backbone acceleration** — even uncacheable requests ride the AWS backbone from
  the edge POP to the origin instead of the public internet.
- **Narrow caching** — exactly two catalog read routes are cached; everything else
  is default-deny.

**The primary route earns nothing from caching — by design.** `POST /api/recommend`
is POST-only and personalized per `userId`, so CloudFront forwards 100% of those
requests to the origin (cache hit ratio zero). Its edge value is TLS, WAF, and
backbone acceleration, not caching. The recurring tension the rest of this doc
resolves: caching a route requires *not* varying on `Authorization`, which is only
safe for genuinely world-readable data — so caching is real but deliberately
limited to the two catalog reads.

The whole thing is created **out-of-band by an idempotent script**, matching how
the WAF WebACL and Route53 records are already managed — this repo has no IaC
toolchain. Design specs:
[cdn-edge-acceleration](../superpowers/specs/2026-07-14-cdn-edge-acceleration-design.md)
and
[local-cdn-and-origin-secret-hardening](../superpowers/specs/2026-07-14-local-cdn-and-origin-secret-hardening-design.md).

## 1. What is cached (and what isn't)

The distribution is **default-deny**: `DefaultCacheBehavior` uses the managed
`CachingDisabled` policy, so every route is uncacheable unless it matches one of
four explicit cache behaviors. Path patterns are exact: CloudFront evaluates them against the
path only, never the query string, so no wildcard is needed and one would make the edge's scope
wider than the exact `GATEWAY_PUBLIC_PATHS` entries the gateway authorizes.

| Path pattern | Policy | `DefaultTTL` | `MaxTTL` | Cache key (query whitelist) |
|---|---|---:|---:|---|
| `/api/catalog/item` | `recsys-item` (Cache) | 3600s (1h) | 86400s (24h) | `id` |
| `/api/v1/catalog/item` | `recsys-item` (Cache) | 3600s (1h) | 86400s (24h) | `id` |
| `/api/catalog/similar` | `recsys-similar` (Cache) | 300s (5min) | 3600s (1h) | `movieId`, `k` |
| `/api/v1/catalog/similar` | `recsys-similar` (Cache) | 300s (5min) | 3600s (1h) | `movieId`, `k` |
| everything else, incl. `POST /api/recommend`, `/api/catalog/user` | `CachingDisabled` | — | — | — |

All four cached behaviors are **GET/HEAD only**, `redirect-to-https`, `Compress: true`
(gzip + brotli), and set three cache-key behaviors that matter:

- **`QueryStringBehavior: whitelist`** — only the listed params form the cache key.
  Forwarding the full query string would let `?id=1&cachebuster=<n>` fragment the
  cache arbitrarily and act as an origin-DoS amplifier.
  The whitelist bounds *which* parameters can fragment the cache, not which **values** — so a
  whitelisted parameter that the origin clamps or alias-accepts is itself an unbounded
  cache-buster. `k` was exactly that until 2026-07-29: `k=201`…`k=2147483647` all clamped to
  200 and each was a distinct cache key over one identical body, as were `id=007` and
  `?id=1&id=<n>`. The origin now accepts one canonical spelling per value
  (`BaseApiService.cacheKeyIntParam`) and rejects everything else with a `no-store` 400,
  closing repeats, leading zeros, sign, whitespace, `-0`, out-of-range, and present-but-empty
  as cache-buster channels. One bounded alias remains and is accepted deliberately: an
  absent `k` and an explicit `k=10` are two keys for the same body, which cannot be removed
  without rejecting the default spelling. One channel is not closed here, because
  `cacheKeyIntParam` validates the **decoded** query value while the cache key is built from the
  **raw** query string: a percent-encoded value (`?id=%37`) is a second cache key for the same
  body. It is closed at the edge instead, by the viewer-request function in
  `scripts/cdn/normalize-catalog-query.js`. An encoded parameter *name* is not a channel at all —
  it is neither keyed nor forwarded. See sharp edge 9.
- **`HeaderBehavior: none`** — `Authorization` is **not** part of the cache key on
  cached routes (a JWT-keyed cache would fragment per user and never hit). This is
  what forces the "these routes must be public" decision in §2. It also means **no
  request header can vary a cached response**: a header-based API version
  (`Accept-Version`, `X-API-Version`) would be invisible to the cache key, so a v1
  and a v2 client would collide on one cached object and whichever missed first would
  pin its body for every other version. The repo has no header versioning today
  ([09_API_Gateway §1](09_API_Gateway.md#1-routing-and-prefix-strip)), and this cache
  behavior is the concrete reason a version belongs in the path — a distinct URL is a
  distinct cache key for free.
- **`CookieBehavior: none`.**

The origin **proposes** freshness via `Cache-Control: s-maxage`
([`HttpCaching.publicCache`](../../src/main/java/com/recsys/api/serving/HttpCaching.java)) and
the cache policy sets the **bounds** that proposal is clamped into. The three policy TTLs play
different roles, and only one of them is inert:

| Field | Role |
|---|---|
| `MinTTL: 0` | Load-bearing. Above zero CloudFront ignores `Cache-Control: no-store` outright, and every `no-store` on these routes — the 404s, the rejections, the gateway's 5xx — would stop working. |
| `DefaultTTL` | Inert for cached 200s. It applies only when the origin sends no `max-age`/`s-maxage`, and both cacheable routes always send one. |
| `MaxTTL` | Binding. CloudFront caches for the lesser of `s-maxage` and `MaxTTL`, serves stale content for the lesser of the stale directive and `MaxTTL`, and drops the object entirely after `MaxTTL`. |

Because `MaxTTL` sits *exactly at* each stale window (86400 for item, 3600 for similar), the
edge ceiling — not the origin directive — is what currently bounds outage tolerance. Raising
`stale-if-error` without raising `MaxTTL` changes nothing.

`stale-while-revalidate` / `stale-if-error` share the same window — so a total
origin outage still serves cached `/item` for up to 24h and `/similar` for up to
1h. Conversely, `GET /getuser`, `GET /api/v1/token`, and not-found responses are
`Cache-Control: no-store`, so a miss or a single-use token can never be pinned at the edge.
Which of those `no-store` headers is load-bearing depends on the status: CloudFront caches
404, 414, 500, 501, 502, 503 and 504 unconditionally for the Error Caching Minimum TTL (10 s
by default), and caches 400, 403, 405, 412 and 415 *only* if the origin sends
`max-age`/`s-maxage`. So the 404 branches and the gateway's 5xx genuinely need it, while the
400 branches are belt-and-braces — the routes apply it uniformly so there is no per-status
reasoning to get wrong. Behaviors on `CachingDisabled` cache no error responses at all, which
is why this only matters on the four cached catalog behaviors. All of it assumes
`MinTTL: 0`: above zero CloudFront ignores `no-store`.

On uncached routes the origin request policy is `AllViewerExceptHostHeader`, so
`Authorization` *is* forwarded to the origin — `POST /api/recommend` earns nothing
from caching both because it is default-deny and because its method isn't cached,
but the gateway still authenticates it normally.

## 2. Making the two reads cacheable — `GATEWAY_PUBLIC_PATHS`

For the edge to hit at a useful ratio, the two catalog reads must not vary on
`Authorization` — so they are marked public and CloudFront drops `Authorization`
on those behaviors. That is an explicit trust decision: movie catalog metadata and
item-to-item similarity are treated as non-sensitive and world-readable.

Public paths are configured via `GATEWAY_PUBLIC_PATHS` (default `/health`) in
[`GatewayAuthenticator`](../../src/main/java/com/recsys/application/gateway/GatewayAuthenticator.java);
the production value is:

```
GATEWAY_PUBLIC_PATHS=/health,/api/catalog/item,/api/catalog/similar
```

Two safety mechanisms make this hard to get wrong:

- **Prefix-with-boundary matching** — `matchesPrefix` accepts a path only if it
  `equals(prefix)` or `startsWith(prefix + "/")`. So `/api/catalog` would match
  `/api/catalog/user?userId=1` (leaking a private route) — which is exactly why the
  two reads **must be listed as exact paths**, never the bare `/api/catalog`
  prefix. The k8s configmap carries a loud DANGER comment to that effect.
- **Never-public guard** — `PROTECTED_PREFIXES = {/api/catalog/user, /api/users}`
  is checked *first*, so even if an operator mistakenly lists a prefix that covers
  a protected route, `isPublic` still returns false and `warnOnProtectedOverlap`
  logs the misconfiguration. Public-path handling is shared with edge auth — see
  the [API Gateway investigation](09_API_Gateway.md#3-authentication).

## 3. Origin lockdown — proving a request came from *our* distribution

The ALB security group is pinned to CloudFront's origin-facing managed prefix list,
but that list covers **every** AWS account's distributions — so it alone does not
prove a request came from *our* distribution.
[`GatewayOriginSecret`](../../src/main/java/com/recsys/application/gateway/GatewayOriginSecret.java)
closes that gap: CloudFront injects a custom `x-origin-secret` header on every
origin request, and the gateway rejects anything missing or mismatched with `403`
"direct origin access is not permitted", counting it in
`gateway_origin_secret_rejected_total`.

```bash
curl http://localhost:8010/metrics | grep gateway_origin_secret_rejected_total
```

Design details that make it safe to operate:

- **Off by default** — `GATEWAY_ORIGIN_SECRET` unset/blank → a `DISABLED` singleton,
  so local dev and the test suite are unaffected. Wired in
  [`MicroserviceGatewayServer`](../../src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java)
  only when enabled, and bound from the `recsys-gateway-origin-secret` Secret
  (`optional: true`, so pods stay schedulable pre-rollout / in no-CDN deploys).
- **Comma-separated secret SET for zero-downtime rotation** — the gateway accepts
  *any* secret in the set, so rotation has no 403 window:

  ```bash
  GATEWAY_ORIGIN_SECRET="old-secret,new-secret"   # 1. pods accept either
  # ... flip the distribution's custom header to new-secret ...
  GATEWAY_ORIGIN_SECRET="new-secret"              # 2. old retired
  ```

- **Constant-time compare** — validation uses `MessageDigest.isEqual` and
  deliberately does **not** early-exit on a match (accumulates with `matched |=`),
  so neither the secret value nor the set size/position leaks via timing.
- **`/health` + `/metrics` exempt** — boundary-matched (`/healthcheck` does *not*
  inherit `/health`'s exemption), since the ALB health check, kubelet probes, and
  the Prometheus scrape reach the pod directly and can't carry the secret. The
  first rejection logs one WARN; subsequent ones are counted, not logged, to avoid
  flooding under a scan or a botched rotation.
- **Encrypted hop** — `create-cdn-distribution.sh` defaults `ORIGIN_PROTOCOL_POLICY`
  to `https-only`, which encrypts the secret across the CloudFront→ALB hop
  (requires an ALB `:443` listener and a regional ACM cert). `http-only` remains a
  loudly-warned opt-out — under it the secret crosses the hop in cleartext and is
  replayable if observed.

## 4. Provisioning the distribution

[`scripts/create-cdn-distribution.sh`](../../scripts/create-cdn-distribution.sh) is an
**idempotent create-or-update** keyed on `Comment="recsys-edge"`: re-running finds
the existing distribution and issues `update-distribution --if-match <etag>`
instead of creating a duplicate. Required env vars all fail-fast:

| Env var | Purpose / validation |
|---|---|
| `ORIGIN_DOMAIN` | Route53 failover hostname (not the ALB directly) |
| `ALIAS_DOMAIN` | Public hostname (CloudFront alias) |
| `ACM_CERT_ARN` | Viewer TLS cert — **hard-validated to be in us-east-1**, else exit 1 |
| `WEB_ACL_ARN` | Must be scope=`CLOUDFRONT` (global) |
| `ORIGIN_SECRET` | Injected as the `x-origin-secret` custom origin header; must equal `GATEWAY_ORIGIN_SECRET` |
| `ORIGIN_PROTOCOL_POLICY` | `https-only` (default) or `http-only` (warned opt-out) |

Fixed settings: `HttpVersion: http2and3`, `PriceClass_All`, origin
`OriginReadTimeout: 30` / `OriginKeepaliveTimeout: 5`, `OriginSslProtocols:
[TLSv1.2]`. The two custom cache policies (`recsys-item`, `recsys-similar`) are
themselves created idempotently by name. Companion scripts: `invalidate-cdn.sh`
(bulk wildcard invalidation, respects the 1,000-free-path quota) and
`invalidate-local-cdn.sh` (local stand-in purge).

For LLM SSE traffic, the 30-second origin read timeout is tighter than the
ALB's 60-second idle timeout. The gateway defaults `LLM_SSE_KEEPALIVE_MS` to
10000 ms and rejects larger values, allowing margin for an idle check that
can defer a comment for nearly two periods after upstream data. Non-positive
values disable comments. Protection requires upstream SSE headers and a
complete frame boundary; see the [SSE guide](16_SSE_Streaming.md) and
[CDN troubleshooting steps](../runbooks/cdn-operations.md#llm-streams-close-during-a-quiet-gap).

The full rollout is out-of-band and **order-sensitive** — validate on the raw
`*.cloudfront.net` name → flip DNS → create the origin Secret → narrow the ALB SG
to the prefix list → retire the REGIONAL WebACL (reversing the last two steps locks
all traffic out of the origin):

```bash
ORIGIN_DOMAIN=origin.recsys.example.com \
ALIAS_DOMAIN=app.recsys.example.com \
ACM_CERT_ARN=arn:aws:acm:us-east-1:<acct>:certificate/<id> \
WEB_ACL_ARN=arn:aws:wafv2:us-east-1:<acct>:global/webacl/recsys-edge/<id> \
ORIGIN_SECRET="$(openssl rand -hex 32)" \
./scripts/create-cdn-distribution.sh
```

Ordered rollout, rollback, and the SG/prefix-list commands are in
[cdn-operations.md](../runbooks/cdn-operations.md) and
[cdn-rollback.md](../runbooks/cdn-rollback.md).

## 5. Local CDN stand-in

[`docker-compose.cdn.yml`](../../docker-compose.cdn.yml) runs an `nginx:1.27-alpine`
container on **`:8090`** that mirrors the five CloudFront behaviors one-for-one (the
default-deny behavior plus the four cached catalog behaviors), so the caching
semantics can be run and observed with no AWS account. The mirror extends to path scope:
nginx's `location =` blocks are exact matches, and the CloudFront `PathPattern`s are
wildcard-free for the same reason. `LocalCdnCacheTest` pins this by driving a glob-adjacent
path (`/api/catalog/items?id=99`) that the origin marks cacheable and asserting it is
`BYPASS` — scope is decided by the behavior, not by the response.

```bash
# 1. Gateway + backends on the host (gateway listens on :8010)
sh scripts/run-microservices-local.sh
# 2. The CDN in front of it, on :8090
docker compose -f docker-compose.cdn.yml up
# 3. See it work
curl -sI 'localhost:8090/api/catalog/item?id=1' | grep -i x-cache                # MISS
curl -sI 'localhost:8090/api/catalog/item?id=1' | grep -i x-cache                # HIT
curl -sI 'localhost:8090/api/catalog/item?id=1&cachebuster=99' | grep -i x-cache # HIT — key whitelist holds
curl -sI -X POST localhost:8090/api/recommend | grep -i x-cache                  # BYPASS — default-deny holds
```

The [nginx template](../../docker/cdn/default.conf.template) reproduces the CloudFront
semantics deliberately: `location /` is a default-deny mirror
(`proxy_cache_bypass 1; proxy_no_cache 1;` → `X-Cache: BYPASS`); the four catalog
locations (versioned and unversioned item, versioned and unversioned similar) set
`proxy_cache_key "$uri|$arg_id"` for both item spellings and
`"$uri|$arg_movieId|$arg_k"` for both similar spellings (only the whitelisted params;
`$uri` keeps the versioned and unversioned spellings in separate cache entries) and
deliberately omit `proxy_cache_valid` so lifetime comes only from the origin's
`s-maxage`; every location injects the
`x-origin-secret` header (`CDN_ORIGIN_SECRET` must match `GATEWAY_ORIGIN_SECRET` or
every request 403s). Purge with `./scripts/invalidate-local-cdn.sh` (whole cache —
nginx OSS has no path-scoped invalidation).

It is a **semantics harness, not a CloudFront emulator**: caching behavior only —
no WAF, no Shield, no edge TLS, no geographic distribution (one container, not 400+
POPs), and coarser whole-cache invalidation. See
[cdn-local.md](../runbooks/cdn-local.md).

## 6. Testing the edge

- **Origin secret** — `GatewayOriginSecretTest` (14 cases: disabled when unset,
  allow/reject matching/wrong/missing, `/health`+`/metrics` boundary exemption,
  rotation-set accept-either, CSV trimming) and `GatewayOriginSecretMetricsTest`
  (the `gateway_origin_secret_rejected_total` counter; null-registry support).
- **Public paths** — `GatewayAuthenticatorTest` proves the production set allows the
  two catalog reads but rejects `/api/catalog/user` and the bare prefix, and that
  protected prefixes stay auth-required even if explicitly listed public.
- **Cache behavior** — `LocalCdnCacheTest` (Testcontainers nginx on 8090):
  `s-maxage` alone caches (miss→hit), the cache-key whitelist means a cachebuster
  can't fragment the cache, default behavior never caches (BYPASS×2),
  `If-None-Match` 304 passthrough, the origin secret is injected on every forwarded
  request, and a different `similar?k=` is a fresh miss.

## Sharp edges — notes

1. **Caching is narrow on purpose.** Only two GET routes are cacheable; the
   dominant `POST /api/recommend` is uncacheable by design. The CDN's payoff on the
   hot path is TLS/WAF/backbone, not hit ratio.
2. **Public = world-readable, and prefix mistakes are dangerous.** `item`/`similar`
   are cached only because they carry no private data and don't vary on
   `Authorization`; listing the `/api/catalog` *prefix* instead of the exact paths
   would expose `/api/catalog/user`. The never-public guard is a backstop, not a
   license to be sloppy.
3. **The origin secret is the real lock; the prefix list isn't enough.** The
   managed prefix list admits every AWS account's CloudFront, so `x-origin-secret`
   is what authenticates *our* edge — and under the `http-only` opt-out it crosses
   the hop in cleartext.
4. **Rollout order is one-way-dangerous.** Narrowing the ALB SG before DNS/edge are
   verified locks traffic out of the origin; follow the runbook order and use
   `cdn-rollback.md` to back out.
5. **Everything is out-of-band.** No IaC — the distribution, WebACL, prefix-list
   pinning, and DNS are script/console-managed, so drift is possible and the
   runbooks are the source of truth.
6. **A versioned path is a separate cache key, and a separate behavior.** The versioned and
   unversioned catalog reads are four distinct CloudFront behaviors over the same two cache
   policies, so they occupy separate cache entries and warm independently. Adding a future
   `/api/v2/...` means new behaviors again — the gateway normalizes the version away, but
   the edge does not. Deploy order is one-way-dangerous: **gateway first, distribution
   second.** Adding a cache behavior before the gateway can normalize that path makes
   CloudFront drop `Authorization` on a path the origin still treats as private, turning
   every request into a `401` on a cached behavior.
7. **A whitelisted cache-key parameter is a cache-buster unless the origin canonicalizes it.**
   The query-string whitelist is necessary but not sufficient — it constrains parameter names,
   while the fragmentation budget is set by the number of accepted *spellings* per value. Any
   new cacheable route must parse its cache-key parameters with `cacheKeyIntParam`, not
   `optionalIntParam`; clamping is only safe off the cached behaviors. `cacheKeyIntParam` closes
   the decoded-value spellings (leading zeros, sign, whitespace, repeats, out-of-range) — it is
   not on its own sufficient to make one cache key map to one body; the percent-encoded value
   spelling it cannot see is closed at the edge instead — see sharp edge 9.
8. **The cache policies were create-once; the distribution is create-or-update.** They are
   different AWS resources with different update semantics, and the cache key plus the TTL
   ceilings live in the *policy*. Until 2026-07-29 `ensure_cache_policy` returned the existing
   id unconditionally, so a TTL or whitelist edit in the script never reached AWS — the exact
   inverse of the drift hazard the runbook warns about for the distribution. It now diffs the
   fields it manages and updates with `--if-match`, which propagates a cache-behavior change
   to every edge: not a step to take casually mid-incident.
9. **Two of the three percent-encoding channels never existed; the third is closed at the edge.**
   This entry claimed until 2026-08-08 that `?%69d=7` "collides with `?%69%64=8` and with a bare
   parameterless request — many bodies under one key". It does not. The four cached behaviors
   carry no `OriginRequestPolicyId`, and AWS forwards to the origin only what is in the cache key
   ("Other information from the viewer request, such as URL query strings, HTTP headers, and
   cookies, is not included in the origin request by default"). So a non-whitelisted `%69d` never
   reaches the origin: it 400s on the missing `id`, `no-store`, uncached, and no bodies collide.
   The same reasoning kills the `k`-default variant — `?movieId=1&%6b=200` forwards no `k`, the
   origin uses its default, and the body matches its key. **One channel is real:** `?id=%37` is a
   whitelisted name, so the encoded value is both keyed and forwarded, and the origin decodes it
   to `7` — a second cache key over one body, and an unbounded attacker-controlled cache-buster
   of exactly the kind sharp edge 7's ceiling exists to prevent. It is closed by
   `scripts/cdn/normalize-catalog-query.js`, a viewer-request function that rebuilds each cached
   route's query string from the whitelist alone, rejecting a percent-encoded or repeated value
   with a `no-store` 400 and emitting the rest in a fixed order. The cache key is computed from
   the function's output, so that is what fragments the cache.
   **The `ALLOWED[request.uri]` lookup fails closed.** Any URI that isn't an exact key in the
   whitelist gets the same `no-store` 400, not a pass-through. The function is associated only
   with the four exact-match cached behaviors, so a lookup miss cannot mean "some other,
   unprotected route" — every URI legitimately reaching this handler is one of the four keys
   above. A miss instead means the URI CloudFront handed the function differs from the URI
   CloudFront matched the behavior on, and AWS documents exactly that gap: "CloudFront normalizes
   URI paths consistent with RFC 3986 and then matches the path with the correct cache behavior.
   Once the cache behavior is matched, CloudFront sends the raw URI path to the origin." Confirmed
   against the real CloudFront runtime: `//api/catalog/item`, `/api/x/../catalog/item`, and
   `/api/catalog/%69tem` all miss the map. Under the original pass-through branch, any of those —
   including a bare leading slash — would have skipped the entire fix. Failing closed removes the
   dependence on the raw-vs-normalized question below instead of betting on it, and matches
   `GatewayProxyService.rejectNonCanonicalPath`, which already refuses a non-canonical spelling of
   a public route rather than serving it.
   **The origin cannot help here, and must not be asked to.** Armeria's codecs overwrite `:path`
   with the normalized request target before any service runs (`Http1RequestDecoder:177` into
   `ArmeriaHttpUtil:679`; `Http2RequestDecoder:128` into `:614`), and
   `QUERY_MUST_PRESERVE_ENCODING` excludes alphanumerics — so `?id=%37` arrives at the gateway as
   `id=7`. Measured on a real server over a raw socket. A gateway-side guard was written, passed
   its unit tests, and was inert in production, because `ServiceRequestContext.of(request)` is the
   one place that preserves the caller's request verbatim.
   **Still unverified:** three things, all of the same shape — what CloudFront puts *into* the
   event object, which `test-function` can never show because it consumes an event somebody else
   already built.
   1. Whether CloudFront percent-decodes parameter *names* before whitelist matching. The function
      is built not to depend on the answer: raw `%69d` is unlisted and dropped, decoded `id` is
      whitelisted and passes through as `id=7`, which was already correct.
   2. Whether a CloudFront Function receives the raw or the normalized `request.uri`. AWS states
      that the raw path reaches the origin *after* the behavior match, but says nothing about what
      a Function sees *before* it. The fail-closed lookup above exists because this question has no
      answer here.
   3. **Whether query-string *values* arrive raw or percent-decoded — and this one is not neutral.**
      The whole mechanism is `param.value.indexOf('%') >= 0`, which assumes the raw wire value is in
      the event. AWS's event-structure documentation does not say, and the harness supplies the
      literal `%37` itself — the identical shape of the Armeria failure above, where a guard passed
      its tests on an encoding production had already decoded away. *Cache-key soundness survives
      either way*: under decoding, `%37` is already `7` when the rebuild runs, so `?id=%37` and
      `?id=7` collapse onto one key and the channel still closes. What would be wrong is the
      **stated** behaviour — `?id=%37` would return 200 from the `id=7` entry rather than a 400,
      i.e. the edge *decoding* the alias, which this design explicitly refuses ("decoding `%37` to
      `7` here would CREATE a second working spelling"). The guard would have degraded from
      reject-the-alias to normalize-the-alias. Conditional on the same branch, a decoded `%26`
      would arrive as a bare `&` the check cannot see.

   That last point has a second edge. The function concatenates `name + '=' + param.value`
   **unencoded**, which is safe only because a value containing `%` was rejected first: on the raw
   branch nothing reaching the join can carry an encoded `&`, `=` or `#` and split itself into
   extra parameters. The `%` check is an injection guard as well as a cache-key measure, and
   relaxing it into a decode means adding encoding at the join.

   No distribution exists in this account, so the association wiring is unexercised too;
   `scripts/test-cdn-function.sh` verifies the function's *logic* against the real CloudFront
   runtime and nothing beyond that.
