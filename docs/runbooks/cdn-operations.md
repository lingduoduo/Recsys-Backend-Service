# CDN Operations

CloudFront fronts the API gateway. Design:
`docs/superpowers/specs/2026-07-14-cdn-edge-acceleration-design.md`. For how the
edge fits together — cache behaviors, origin lockdown, public paths — see the
[CDN Edge investigation](../system_design/12_CDNS.md).

Like the WAF WebACL and the Route53 records, the distribution is created out-of-band — this
repo has no IaC. There is no state file and no drift detection.

> **The `create-cdn-distribution.sh` payload is the sole source of truth for the distribution
> config.** `aws cloudfront update-distribution` REPLACES the entire configuration — any field
> not present in the payload is silently reset to its default. The script's JSON only sets
> `Comment`, `Enabled`, `HttpVersion`, `Aliases`, `Origins`, `DefaultCacheBehavior`,
> `CacheBehaviors`, `ViewerCertificate`, `WebACLId`, and `PriceClass` — it omits `Logging`,
> `CustomErrorResponses`, `Restrictions`, `DefaultRootObject`, `IsIPV6Enabled`, and
> `OriginGroups`. If an operator turns on access logging or adds a geo restriction via the
> console, the **next routine re-run of the script** (e.g. to rotate the origin secret) silently
> reverts it, with no error and no warning. Any console change that isn't also added to the
> script's `jq` payload does not survive the next run.
>
> **This applies to the distribution config only.** The script provisions three other AWS
> resources, across two resource types with their own update semantics: two cache policies, and
> one CloudFront **Function** (see "The viewer-request normalization function" below). The two
> cache policies (`recsys-item`,
> `recsys-similar`) hold the cache key and the TTL ceilings.
> `ensure_cache_policy` diffs the fields the script manages and issues
> `update-cache-policy --if-match` when they differ, printing the deployed-versus-desired diff
> first. Before 2026-07-29 it had no update path at all, so a policy edit in the script was a
> silent no-op — if you are debugging a TTL or cache-key change that "did not take", check
> `aws cloudfront get-cache-policy --id <id>` against the script rather than assuming the
> script won.

## What is and is not cached

Cached: `GET /api/catalog/item` and `GET /api/v1/catalog/item` (1 h fresh, 24 h
`stale-while-revalidate` + `stale-if-error`); `GET /api/catalog/similar` and
`GET /api/v1/catalog/similar` (5 min fresh, 1 h `stale-while-revalidate` +
`stale-if-error`). Four `CacheBehaviors` in total (`create-cdn-distribution.sh`) — the
versioned and unversioned spellings of each route are cached identically, since the
gateway strips the version segment before the request is otherwise distinguishable. The
two directives cover different failure modes and both matter: `stale-while-revalidate`
serves the stale copy while refreshing it in the background against a *healthy* origin —
it says nothing about an unhealthy one. `stale-if-error` is what covers an origin
outage: it lets the edge keep serving the cached object when the origin is
unreachable or returns a 5xx, for the same window. `HttpCaching.publicCache`
emits both with the same value, so a total origin outage still serves cached
`/item` (either spelling) for up to 24 h and `/similar` (either spelling) for up to 1 h.
Those windows are ceilings set by the cache policies' `MaxTTL`, which sits exactly at each
stale directive — so they are the *smaller* of the two limits, not the origin's number
winning. See the TTL table in [12_CDNS §1](../system_design/12_CDNS.md#1-what-is-cached-and-what-isnt).
See "Freshness" / "Availability" in the design doc for the full breakdown.

Everything else, including `POST /api/recommend`, is `CachingDisabled` by default and always
reaches the origin. **The hit ratio on the primary recommendation route is zero by design** —
that route is POST-only and personalized. The CDN earns its keep here through edge TLS
termination, WAF, and backbone acceleration, not caching.

## The viewer-request normalization function

The four cached behaviors each run a CloudFront Function, `recsys-normalize-catalog-query`
(source: `scripts/cdn/normalize-catalog-query.js`). It rebuilds the query string from the route's
whitelist so that `?id=%37` cannot be a second cache key for the `?id=7` body. Background:
[12_CDNS sharp edge 9](../system_design/12_CDNS.md).

**The provisioning script owns it.** `create-cdn-distribution.sh` creates the function on its first
run and updates it on every run after that, then calls `publish-function` **unconditionally** —
even when the `.js` is byte-identical to what is already LIVE. That is deliberate (a publish is
cheap and this is an operator-invoked tool, not a poll loop), but it means *any* re-run of the
script for an unrelated reason — an origin-secret rotation, say — also republishes whatever
`scripts/cdn/normalize-catalog-query.js` says at that moment. Check the working tree before
re-running the script mid-incident.

**Publishing propagates to every association at once, with no distribution update.** AWS states it
directly (`aws cloudfront publish-function help`): publishing "copies the function code from the
DEVELOPMENT stage to LIVE. This **automatically updates all cache behaviors that are using this
function** to use the newly published copy in the LIVE stage." Associations name the function's
ARN, which does not change, so `publish-function` alone pushes new code to every associated
behavior in every distribution — no `update-distribution`, no invalidation. There is no staged or
per-behavior rollout: update + publish is a fleet-wide code change to the request path of all four
cached routes.

**This function fails closed**, which sets the blast radius. Any URI that is not an exact key in
its `ALLOWED` map gets a `no-store` 400 rather than a pass-through. So an `ALLOWED` map that drifts
from the `PathPattern` list, or an association added to `DefaultCacheBehavior`, takes routes down
rather than degrading them. `CdnQueryNormalizationConformanceTest` fails the build on both, so the
realistic way to hit this in production is a console edit or a hand-run `update-function`.

**Verify before you publish.** `./scripts/test-cdn-function.sh` runs the committed `.js` against
the real CloudFront runtime via `create-function` + `test-function`. It uses a separate probe name
(`recsys-cdn-normalize-probe`) in the `DEVELOPMENT` stage, associated with nothing, and deletes it
on exit — it never touches `recsys-normalize-catalog-query` or any live traffic. It verifies logic
only; see its header for what it cannot show.

**Rollback is forward-only.** The CloudFront Functions API has exactly two stages, `DEVELOPMENT`
and `LIVE`, and no way to re-publish an earlier version — `aws cloudfront` offers
`create/update/publish/describe/get/list/test/delete-function` and nothing else (`list-functions`
enumerates by stage; it cannot recover a superseded version either). There is no "roll back to the
previous function". To revert:

```bash
# 1. What is live right now, byte for byte. Do this BEFORE changing anything.
aws cloudfront get-function --name recsys-normalize-catalog-query --stage LIVE /tmp/live.js
diff /tmp/live.js scripts/cdn/normalize-catalog-query.js

# 2. Restore the known-good source in git, then verify it against the real runtime.
git checkout <good-sha> -- scripts/cdn/normalize-catalog-query.js
./scripts/test-cdn-function.sh

# 3. Update + publish. Propagates to every association within minutes; no invalidation needed
#    (the function runs on the viewer request, before the cache lookup).
etag="$(aws cloudfront describe-function --name recsys-normalize-catalog-query \
  --query 'ETag' --output text)"
aws cloudfront update-function --name recsys-normalize-catalog-query --if-match "$etag" \
  --function-config '{"Comment":"Normalize catalog cache-key query strings","Runtime":"cloudfront-js-2.0"}' \
  --function-code fileb://scripts/cdn/normalize-catalog-query.js
etag="$(aws cloudfront describe-function --name recsys-normalize-catalog-query \
  --query 'ETag' --output text)"
aws cloudfront publish-function --name recsys-normalize-catalog-query --if-match "$etag"
```

Step 1 is the one that is easy to skip and impossible to redo: once you have published over the
live code, the only copy of what *was* live is whatever you saved.

The emergency escape hatch, if the function itself is the outage and no good source is at hand, is
to remove `FunctionAssociations` from the four cached behaviors and re-run
`create-cdn-distribution.sh`. That restores pre-2026-08-08 behavior — the routes serve again, and
the percent-encoded cache-buster is open again until the function is fixed. Removing the
associations is also a prerequisite for deleting the function at all: "You cannot delete a function
if it's associated with a cache behavior. First, update your distributions to remove the function
association from all cache behaviors, then delete the function"
(`aws cloudfront delete-function help`).

## Rollout

Order matters. Reversing steps 5 and 6 locks all traffic out of the origin.

1. Deploy the app (cache headers, ETag/304, origin-secret validation **disabled**). Not a pure
   no-op: this deploy also carries the `k8s/base` configmap value that flips
   `GATEWAY_PUBLIC_PATHS` from `/health` alone to `/health,/api/catalog/item,/api/catalog/similar`
   — a live authorization change (those two routes stop requiring auth), immaterial only because
   gateway auth is currently fail-open (no `GATEWAY_API_KEYS` / Cognito configured), not because
   the change itself does nothing. Once either is configured, this step's public-path flip
   becomes real and should be sequenced deliberately, not treated as invisible. Everything else
   in this step (headers, ETag, origin-secret validation) is genuinely inert until later steps.
2. Create the ACM cert (**us-east-1** — CloudFront ignores certs anywhere else), the
   `CLOUDFRONT`-scope WebACL, and the distribution:
   ```bash
   ORIGIN_DOMAIN=origin.recsys.example.com \
   ALIAS_DOMAIN=app.recsys.example.com \
   ACM_CERT_ARN=arn:aws:acm:us-east-1:<acct>:certificate/<id> \
   WEB_ACL_ARN=arn:aws:wafv2:us-east-1:<acct>:global/webacl/recsys-edge/<id> \
   ORIGIN_SECRET="$(openssl rand -hex 32)" \
   ./scripts/create-cdn-distribution.sh
   ```
   Save the `ORIGIN_SECRET` value — step 4 needs it.

   This same invocation also creates and publishes the `recsys-normalize-catalog-query` function
   and associates it with the four cached behaviors. It fails closed, so if those four routes
   return 400 immediately after this step, read "The viewer-request normalization function" above
   before touching anything else.

   **Origin protocol.** `ORIGIN_PROTOCOL_POLICY` defaults to `https-only`, which requires the ALB
   to have a `:443` listener and a **regional** ACM certificate (separate from the us-east-1 cert
   CloudFront uses for viewers). The ALB today listens on `:80` only
   (`k8s/eks/waf-api-gateway-ingress.yaml`), so that listener must exist before this default works.

   `ORIGIN_PROTOCOL_POLICY=http-only` still works and warns. It is a real weakening: the origin
   secret then crosses the public CloudFront→ALB hop in cleartext, where it is observable and
   replayable by exactly the attacker the header exists to stop. Prefer fixing the listener.
3. Validate against the raw distribution domain. Real traffic is still on the old path:
   ```bash
   D=dXXXXXXXXXXXXX.cloudfront.net
   curl -sI "https://$D/api/catalog/item?id=1" | grep -i 'x-cache\|cache-control\|etag'
   curl -sI "https://$D/api/catalog/item?id=1" | grep -i x-cache   # 2nd call: expect Hit
   curl -sI -X POST "https://$D/api/recommend" | grep -i x-cache   # expect Miss, always
   ```
   Expected: `X-Cache: Hit from cloudfront` on the repeated catalog GET;
   `X-Cache: Miss from cloudfront` on the POST.
4. Create the Secret so the gateway enforces the origin check. **`GATEWAY_ORIGIN_SECRET` lives in
   `k8s/base`, so it applies to the us-west-2 standby too** — CloudFront's origin is the
   `origin.*` Route53 failover record, which can resolve to either region's ALB, and it sends the
   same header value regardless of which one answers. Create it in **both** contexts, matching
   the two-context deploy pattern in `docs/runbooks/dr-regional-failover.md`:
   ```bash
   kubectl --context <us-east-1-ctx> -n recsys create secret generic recsys-gateway-origin-secret \
     --from-literal=secret='<the ORIGIN_SECRET from step 2>'
   kubectl --context <us-east-1-ctx> -n recsys rollout restart deployment/recsys-api-gateway
   kubectl --context <us-east-1-ctx> -n recsys rollout status deployment/recsys-api-gateway

   kubectl --context <us-west-2-ctx> -n recsys create secret generic recsys-gateway-origin-secret \
     --from-literal=secret='<the same ORIGIN_SECRET>'
   kubectl --context <us-west-2-ctx> -n recsys rollout restart deployment/recsys-api-gateway
   kubectl --context <us-west-2-ctx> -n recsys rollout status deployment/recsys-api-gateway
   ```
   Verify probes still pass in **both** contexts — `/health` and `/metrics` are exempt from the
   secret check. If the pods go NotReady here, that exemption is broken; roll back the Secret
   immediately.

   Skipping the us-west-2 half is not a no-op: `GatewayOriginSecret.fromEnvironment` returns
   `disabled()` when the env var is absent, so the standby gateway silently fails open — a
   failover would then serve traffic with the origin lockdown quietly not enforced at all. See
   `docs/runbooks/dr-regional-failover.md` for the standby-state implication.

   That exemption is deliberate, not incidental: the ALB health check, all three kubelet probes,
   and the Prometheus scrape all reach the pod directly, bypassing CloudFront entirely, and none
   of them can carry the origin secret. The consequence is that `/health` and `/metrics` remain
   reachable by **any** AWS account's CloudFront distribution once the SG is opened to the shared
   prefix list in step 6 — the prefix list, unlike the secret header, does not prove the request
   came from *our* distribution. This is not a new exposure (the ALB is already internet-facing
   today), but `/health` returns per-route circuit-breaker state, upstream reachability, and (when
   `SERVICE_REGISTRY_ENABLED`) registry resolution/topology detail, which is more than a bare
   liveness check would leak. If that becomes a concern, the options are: stop routing `/health`
   and `/metrics` through the distribution (serve them only from the ALB's own DNS name, never
   `app.*`), or move them to a separate management port that the WebACL/SG never expose publicly.
5. Point `app.*` at the distribution (Route53 alias A record). The failover records move to
   `origin.*` and keep working unchanged.
6. **Only now** narrow the ALB security group to the CloudFront prefix list, and retire the
   REGIONAL WebACL. **The port MUST match the origin protocol the distribution was created
   with** (`ORIGIN_PROTOCOL_POLICY` in step 2) — opening the wrong port here blackholes all edge
   traffic, at the exact step this runbook already flags as the lock-yourself-out step:
   - `ORIGIN_PROTOCOL_POLICY=https-only` (the script's default) → open **443**:
     ```bash
     aws ec2 authorize-security-group-ingress --group-id <alb-sg> --ip-permissions \
       'IpProtocol=tcp,FromPort=443,ToPort=443,PrefixListIds=[{PrefixListId=<pl-id>}]'
     ```
   - `ORIGIN_PROTOCOL_POLICY=http-only` → open **80**:
     ```bash
     aws ec2 authorize-security-group-ingress --group-id <alb-sg> --ip-permissions \
       'IpProtocol=tcp,FromPort=80,ToPort=80,PrefixListIds=[{PrefixListId=<pl-id>}]'
     ```
   An operator who provisions the ALB `:443` listener (as the default now implies) and then
   opens port 80 out of habit — or leaves an old port-80 rule as the only one — blackholes 100%
   of traffic: CloudFront connects to the origin on the port `ORIGIN_PROTOCOL_POLICY` selects,
   not the one this runbook happened to open.

   Find the prefix list id with:
   ```bash
   aws ec2 describe-managed-prefix-lists --region <region> \
     --filters Name=prefix-list-name,Values=com.amazonaws.global.cloudfront.origin-facing \
     --query 'PrefixLists[0].PrefixListId' --output text
   ```
   Then remove the old 0.0.0.0/0 rule.

Steps 1-4 are invisible to users **when gateway auth is unconfigured**, which is the case today.
If `GATEWAY_API_KEYS` or Cognito is ever configured, step 1's `GATEWAY_PUBLIC_PATHS` flip (see
above) stops being invisible — it opens `/api/catalog/item` and `/api/catalog/similar` to
anonymous access from that deploy onward.

## Cleartext exposure: only when http-only is an explicit opt-out

By default (`ORIGIN_PROTOCOL_POLICY=https-only`, see the "Origin protocol" note in step 2 above),
`HTTPSPort: 443` and `OriginSslProtocols: ["TLSv1.2"]` in `scripts/create-cdn-distribution.sh`'s
`CustomOriginConfig` are live, not inert: the `x-origin-secret` header CloudFront injects on the
POP-to-origin leg is encrypted over that hop. That default requires the ALB to have a `:443`
listener and a regional ACM certificate, which the ALB does not have today (it listens on `:80`
only — `k8s/eks/waf-api-gateway-ingress.yaml`), so that listener must exist before the default
actually takes effect end to end.

The cleartext exposure only applies when an operator explicitly opts out with
`ORIGIN_PROTOCOL_POLICY=http-only` (the script warns loudly when this is set). Under that opt-out,
the `x-origin-secret` header travels in cleartext over the POP-to-origin hop: observable to
anything positioned on that path and, if observed, replayable — the header check in
`GatewayOriginSecret` is a constant-value comparison, not a nonce or timestamped signature, so a
captured value keeps working until rotated. This partially weakens the origin-lockdown control the
header is supposed to provide.

This is no longer an unconditional accepted limitation — it is a consequence of an explicit
opt-out for the case where the ALB `:443` listener and regional ACM certificate aren't in place
yet. Once that infrastructure exists and the default `https-only` policy is in effect, the
cleartext exposure does not apply; use `http-only` only as a deliberate, temporary fallback.

## Rotating the origin secret

`GATEWAY_ORIGIN_SECRET` accepts a comma-separated **set** of secrets, so rotation has no 403
window. Both the old and the new secret are accepted while the distribution catches up.

> **Rotate in BOTH regions.** `GATEWAY_ORIGIN_SECRET` lives in `k8s/base` and is deployed to both
> the us-east-1 primary and the us-west-2 standby (rollout step 4 above). CloudFront's origin
> resolves to whichever region `origin.*` currently points at, and sends it the same header
> value either way. Rotating in us-east-1 only leaves the standby holding the old secret; if a
> failover lands after step 2 below completes in us-east-1, the distribution sends `new-secret`
> to a us-west-2 gateway that only accepts `old-secret` — 100% 403 in DR, during an outage. Run
> every step below in **both** contexts before moving to the next step.

> **`ORIGIN_PROTOCOL_POLICY` MUST be passed explicitly on every re-run, including rotation.**
> `aws cloudfront update-distribution` REPLACES the entire distribution config (see the note at
> the top of this doc), and `scripts/create-cdn-distribution.sh` defaults
> `ORIGIN_PROTOCOL_POLICY` to `https-only` (`scripts/create-cdn-distribution.sh:35`) if the
> variable is not set. If the distribution was created with `http-only` (because the ALB has no
> `:443` listener yet — still the case today), omitting `ORIGIN_PROTOCOL_POLICY` on a later
> rotation silently flips the origin back to `https-only`, and CloudFront starts sending traffic
> to a port nothing listens on: 502 on 100% of traffic. Always pass the same
> `ORIGIN_PROTOCOL_POLICY` the distribution currently uses — check with `aws cloudfront
> get-distribution-config` if you're not sure which one that is.

```bash
# 1. Accept both, in BOTH regions. Pods now take either value.
kubectl --context <us-east-1-ctx> -n recsys create secret generic recsys-gateway-origin-secret \
  --from-literal=secret='old-secret,new-secret' --dry-run=client -o yaml | kubectl --context <us-east-1-ctx> apply -f -
kubectl --context <us-east-1-ctx> -n recsys rollout status deployment/recsys-api-gateway

kubectl --context <us-west-2-ctx> -n recsys create secret generic recsys-gateway-origin-secret \
  --from-literal=secret='old-secret,new-secret' --dry-run=client -o yaml | kubectl --context <us-west-2-ctx> apply -f -
kubectl --context <us-west-2-ctx> -n recsys rollout status deployment/recsys-api-gateway

# 2. Flip the distribution to the new secret (CloudFront propagation takes minutes).
#    ORIGIN_PROTOCOL_POLICY is REQUIRED here — must match what the distribution was created
#    with (http-only until the ALB has a :443 listener; https-only after). Leaving it unset
#    lets the script's https-only default silently replace whatever policy is live today.
ORIGIN_SECRET='new-secret' ORIGIN_PROTOCOL_POLICY='http-only' ... ./scripts/create-cdn-distribution.sh

# 3. Once propagated, retire the old one, in BOTH regions.
kubectl --context <us-east-1-ctx> -n recsys create secret generic recsys-gateway-origin-secret \
  --from-literal=secret='new-secret' --dry-run=client -o yaml | kubectl --context <us-east-1-ctx> apply -f -
kubectl --context <us-east-1-ctx> -n recsys rollout status deployment/recsys-api-gateway

kubectl --context <us-west-2-ctx> -n recsys create secret generic recsys-gateway-origin-secret \
  --from-literal=secret='new-secret' --dry-run=client -o yaml | kubectl --context <us-west-2-ctx> apply -f -
kubectl --context <us-west-2-ctx> -n recsys rollout status deployment/recsys-api-gateway
```

Do not skip step 1, in either region. Going straight to step 2 reintroduces the window this
ordering exists to avoid: the distribution sends a secret no pod accepts, and 100% of non-exempt
traffic 403s until step 3 completes — and skipping a region entirely leaves that region's pods
never accepting the new secret at all.

**Watch the rejections.** `gateway_origin_secret_rejected_total` is exposed on the gateway's
`/metrics`. It should stay flat throughout a correct rotation. A rise means the distribution and
the pods disagree — the most likely cause is step 1 being skipped or not yet rolled out. The
first rejection also emits one WARN log (only the first, to avoid flooding).

## Freshness after a bulk embedding reload

`POST /setembedding` rewrites the vectors behind `/similar`. After a **bulk** reload, invalidate
once:

```bash
./scripts/invalidate-cdn.sh
```

Do not invalidate per write. A bulk load would issue thousands of calls and exhaust the
1,000-free-path monthly quota.

Single-item edits need no action: the 300 s fresh window bounds the staleness.

## LLM streams close during a quiet gap

The distribution creation script sets `OriginReadTimeout: 30`. SSE responses
can therefore be cut off during silence even when the gateway's LLM request
deadline has not expired. Increasing the ALB idle timeout alone does not change
this CloudFront limit.

1. Check the gateway's `LLM_SSE_KEEPALIVE_MS` override. The default and maximum
   are `10000` ms; larger values now fail startup. Remove an old override or
   reduce it before deploying. Non-positive values explicitly disable comments.
2. Confirm the upstream returns `text/event-stream` and finishes frames with
   a blank line. The gateway cannot insert a comment before a complete frame
   boundary without risking corruption; NDJSON is passed through without SSE
   comments.
3. Compare a quiet stream through the gateway with the same request through
   the CDN. Keep request authentication and the origin-secret requirement in
   place. Inspect whether silence occurs before headers, within a frame, or
   between complete frames; only the last case is covered by the heartbeat.
4. Check for gateway event-loop stalls and verify the deployed distribution's
   origin read timeout. A comment can arrive nearly two scheduler periods
   after upstream data; the 10-second ceiling leaves margin below the scripted
   30-second timeout, but cannot guarantee delivery during a stall.

HTTP/2 PINGs configured by `LLM_PING_INTERVAL_MS` operate on the
gateway-to-upstream connection; they do not replace client-facing SSE comments.
See the [configuration reference](../../CONFIG_GUIDE.md) and
[SSE guide](../system_design/16_SSE_Streaming.md) for the full contract.

## Monitoring

`CacheHitRate` is an **additional** CloudFront metric, off by default.
`create-cdn-distribution.sh` turns it on via `create-monitoring-subscription`, but an empty
`Datapoints` array below means "not enabled", not "no traffic" — check the subscription first.
Enabling additional metrics is not free — CloudWatch bills each of the (up to 8) additional
metrics at a small, fixed monthly rate per distribution, independent of request volume — but it
is non-zero, so don't enable it reflexively on distributions that don't need the hit-ratio query:

```bash
aws cloudfront get-monitoring-subscription --distribution-id <id>
```

```bash
# Hit ratio (expect high on catalog, ~0 overall — most traffic is uncacheable POSTs)
aws cloudwatch get-metric-statistics --namespace AWS/CloudFront \
  --metric-name CacheHitRate --dimensions Name=DistributionId,Value=<id> \
  Name=Region,Value=Global --start-time "$(date -u -v-1H +%Y-%m-%dT%H:%M:%SZ)" \
  --end-time "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --period 300 --statistics Average
```

A sudden 403 spike at the origin after step 4 means the origin secret does not match — compare
the Secret value against the distribution's `CustomHeaders`.

### If "no distribution found" appears unexpectedly

`invalidate-cdn.sh` looks up the distribution by `Comment=='recsys-edge'` with
`aws cloudfront list-distributions ... 2>/dev/null || true` — it swallows the AWS CLI's stderr and
treats *any* lookup failure (including an auth failure) the same as "the distribution doesn't
exist yet." An expired SSO session or missing/misconfigured credentials therefore surfaces as:

```
ERROR: no distribution found with Comment='recsys-edge'.
Run ./scripts/create-cdn-distribution.sh first.
```

which reads like an infrastructure gap when the real problem is authentication. If you see this
message and you're confident the distribution was already created, do not re-run the create
script — run `aws sts get-caller-identity` first. If that fails or returns unexpectedly, refresh
SSO / credentials.

`create-cdn-distribution.sh` does the same swallowed-stderr lookup (both for the distribution and
for each cache policy) but does **not** print the message above. On a lookup failure it falls
into its "not found" branch and takes the *create* path instead: it echoes "Creating distribution"
and attempts `aws cloudfront create-distribution` (similarly, a swallowed cache-policy lookup
falls into `aws cloudfront create-cache-policy`). So an expired SSO session here does not surface
as "no distribution found" — it surfaces later, as whatever error the first *unsuppressed* AWS
call throws at the create-cache-policy or create-distribution step. Separately, the script
hardcodes `CallerReference: "recsys-edge-1"`, so if the distribution actually already exists (the
lookup just couldn't confirm it), the attempted create is rejected by AWS with
`DistributionAlreadyExists` instead of silently provisioning a duplicate — a loud failure, but one
that names the wrong problem when the underlying cause was an auth error the lookup masked. Same
remediation applies: run `aws sts get-caller-identity` first whenever either script behaves
unexpectedly.
