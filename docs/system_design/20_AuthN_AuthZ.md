# AuthN / AuthZ in Recsys-Backend-Service

An investigation of who proves their identity, where that proof is checked, and what
the proof entitles them to. The short answer: **authentication happens once, at the
gateway**, and everything behind it trusts the gateway. Authorization is deliberately
coarse — two narrow privilege tiers sit above "authenticated caller": one is the
operator token, checked directly by online serving's own introspection surfaces (§5)
and, since §11, also enforced by the gateway itself for every route it classifies
`OPERATOR` across all three backends; the other scopes a JWT caller to their own
`userId` (§10).

## The big picture

Seven distinct credentials exist. Only the first two authenticate an external caller;
the rest are narrower gates, receipts, or — in the last row — how this system proves
its own identity to its data tier.

| Credential | Header | Checked by | Failure | Default |
|---|---|---|---|---|
| Gateway API key | `X-API-Key` / `Authorization: Bearer` | `GatewayAuthenticator` | `401` | unset → fail closed |
| Cognito JWT (RS256) | `Authorization: Bearer` | `CognitoJwtVerifier` | `401` / `403` | unset |
| CloudFront origin secret | `x-origin-secret` | `GatewayOriginSecret` | `403` | unset → disabled |
| Operator token | `X-Admin-Token` | `AdminTokenGuard` | `403` | unset → fail closed |
| Submit token (one-time CSRF) | `X-Submit-Token` | `SubmitTokenService` | `403` | disabled |
| Login session token | `Authorization: Bearer` | `LoginInterceptor` | envelope error | disabled |
| Redis password (`REDIS_PASSWORD`) | — (AUTH on connect) | Redis `requirepass` | startup refusal | unset → fail closed |

Three structural facts shape everything below:

- **The gateway is the only front door.** `k8s/base/network-policy.yaml` restricts
  ingress on 6010/7010/8080 to the `recsys-api-gateway` pod selector; catalog and
  model serving additionally admit Prometheus from the `monitoring` namespace for
  their `/metrics` scrape, and online serving admits nothing else. The backends'
  lack of their own authentication is a deliberate consequence, not an oversight.
- **There is no authorization *model*.** No roles, no scopes, no per-resource
  ownership checks. A caller is authenticated or not; the two exceptions are the
  operator token (§5, and, at the gateway, §11) and the user-scope check in §10.
- **No security framework is used.** There is no `spring-boot-starter-security`, no
  `jjwt`, no `nimbus-jose-jwt` in [`pom.xml`](../../pom.xml). JWT verification is
  hand-rolled on the JDK plus Jackson (§1), which is a deliberate dependency
  trade-off documented in the
  [Cognito JWT design](../superpowers/specs/2026-07-02-gateway-cognito-jwt-auth-design.md).

## 1. Edge authentication

[`GatewayAuthenticator.check`](../../src/main/java/com/recsys/application/gateway/GatewayAuthenticator.java)
accepts **either** credential and tries them in a fixed order: the value of
`X-API-Key` (falling back to the bearer token) is compared against every configured
`GATEWAY_API_KEYS` entry, and only if that fails is the bearer token verified as a
JWT. Matching neither yields `401` with `WWW-Authenticate: Bearer` and
`{"error":"missing or invalid gateway API key"}`.

The API-key comparison uses `MessageDigest.isEqual` and — importantly —
**accumulates with `|=` instead of breaking on the first match**, so the number of
comparisons does not depend on which key matched. `GatewayOriginSecret` repeats the
pattern for the same reason.

`CognitoJwtVerifier` is a dependency-free RS256 verifier:

| Concern | Behavior |
|---|---|
| Algorithm | `RS256` only; any other `alg` → `401`. `kid` is required |
| Claims | `iss` must equal the configured issuer; `exp`/`nbf` honored with **60 s** clock skew; `aud` matched against the array, the scalar, or `client_id` |
| `token_use` | Checked against `GATEWAY_COGNITO_TOKEN_USE` (default `access`) — **only when the claim is present** |
| JWKS cache | 5 min TTL; 2 s fetch timeout |
| Unknown `kid` | Triggers a refetch at most once per 30 s, behind a CAS single-flight that does **not** hold a lock across the I/O — so a flood of tokens with random `kid`s cannot amplify into a JWKS stampede |
| JWKS outage | **Serve-stale**: a transient fetch failure keeps the last-good keys and retries after 30 s, rather than rejecting tokens whose signing key is already cached. `exp` is still validated independently |

### Fail-closed startup

`GatewayAuthenticator.fromEnvironment` **refuses to construct** when there are no API
keys and no Cognito issuer, because such a gateway authenticates nobody and collapses
every caller to `anonymous` — turning any downstream authorization gap into an
unauthenticated one. Running wide open must be explicit:

```
GATEWAY_ALLOW_ANONYMOUS=true   # logs a loud WARN; dev/local only
```

`CognitoConfig.fromEnvironment` fails fast in the same spirit: setting
`GATEWAY_COGNITO_ISSUER` without `GATEWAY_COGNITO_AUDIENCE` throws at startup rather
than silently skipping audience validation.

`k8s/base` sets `GATEWAY_ALLOW_ANONYMOUS: "true"` (the base is dev-shaped); the
`k8s/eks-shared` component flips it to `false` and injects `GATEWAY_API_KEYS` from the
`recsys-gateway-auth` Secret. Operational setup is in
[gateway-auth.md](../runbooks/gateway-auth.md).

## 2. The trust boundary — stripping in, injecting out

Authentication is worth nothing if the credential or a forged identity reaches a
backend. [`buildUpstreamHeaders`](../../src/main/java/com/recsys/application/gateway/GatewayRequestForwarder.java)
enforces both directions on every proxied request:

- **Credentials are consumed, never forwarded** — `Authorization`, `x-api-key`, and
  `x-origin-secret` are stripped, along with hop-by-hop headers.
- **Client-supplied `x-authenticated-*` headers are stripped** by a case-insensitive
  16-character prefix match. This is the anti-spoofing half: backends trust those
  headers *only* because the gateway is provably their sole source.
- **The gateway injects the identity it derived** —
  [`GatewayPrincipal.identityHeaders()`](../../src/main/java/com/recsys/application/gateway/GatewayPrincipal.java)
  emits `x-authenticated-subject` / `-client-id` / `-token-use` (non-blank only), plus
  `x-gateway-service`.

The principal never carries the raw credential. An API-key caller becomes
`clientId="service"` with a rate-limit key of `apikey:<first 6 bytes of SHA-256, hex>`
— non-reversible, so logs and rate-limit keys cannot leak the key. A JWT caller keys
on `user:<sub>`, falling back to `client:<client_id>`.

Designs: [credential stripping](../superpowers/specs/2026-07-02-gateway-credential-stripping-design.md),
[principal propagation](../superpowers/specs/2026-07-02-gateway-principal-propagation-design.md).

## 3. Public paths and the never-public guard

`GATEWAY_PUBLIC_PATHS` (default `/health`) lists paths that skip authentication
entirely. Matching is **prefix-with-boundary** — `path.equals(prefix)` or
`path.startsWith(prefix + "/")` — so `/api/usersettings` does not match `/api/users`.

That boundary rule is also the trap: a bare `/api/catalog` entry *would* match
`/api/catalog/user`. Two mechanisms make the mistake survivable:

- **`PROTECTED_PREFIXES`** is consulted **first** in `isPublic`, so those paths are never
  anonymous regardless of configuration. It is two hand-listed prefixes (`/api/catalog/user`,
  `/api/users`) **plus every user-scoped route derived from `BackendRoutePolicy`** —
  `route.prefix() + backendPath` for each gateway prefix that reaches a declared handler, so
  `/api/catalog/getrecommendation`, `/api/movies/getuser` and `/api/online/online/features` are
  all covered. Deriving matters because making a user-scoped route public does more than skip
  authentication: an anonymous caller is SERVICE tier, and service tier is exempt from §10, so a
  public entry would silently switch that route's user-scope check off. A second hand-maintained
  list beside `BackendRoutePolicy` would drift; this one cannot.
- **`warnOnProtectedOverlap`** logs at startup when a configured public path would
  have exposed a protected prefix — the guard silently saving you is itself a
  misconfiguration worth fixing.

`BackendRoutePolicy.userScopedGatewayPaths` — the thing `PROTECTED_PREFIXES` derives from — used
to walk only the `EXACT` table. That was invisible as long as every `USER_SCOPED` entry was
declared exactly. The retrieval merge added this repo's first `USER_SCOPED` entries in the
`PREFIX` table (`/api/v1/retrieval/recommend`, `.../predict`, `.../users`, all
`userScoped(UserIdSource.PATH)`), because those three are path templates, not paths, for the same
reason `/api/v1/knowledge-bases` is a prefix entry below. An `EXACT`-only
`userScopedGatewayPaths` would not have seen them, so `noUserScopedRouteCanBeMadePublic` — the test
guarding exactly this — would have stayed green even if `GATEWAY_PUBLIC_PATHS` listed one of them:
the route would have gone anonymous, hence service-tier, hence exempt from §10's check, with the
guard reporting nothing wrong. `userScopedGatewayPaths` now walks both tables, and a second test,
`userScopedGatewayPathsIncludesPrefixDeclaredRoutes`, pins that it continues to.

Version segments cannot be used to evade either mechanism. The gateway normalizes
`/api/v1/users` to `/api/users` *before* `authenticator.check` runs (see
[09_API_Gateway §1](09_API_Gateway.md#api-versioning-and-deprecation)), so `PROTECTED_PREFIXES`
and `GATEWAY_PUBLIC_PATHS` are matched against version-free paths and need no versioned
entries. Had versioned prefixes instead been registered in the route table, every entry in
both lists would need a twin, and a missing one would be a silent auth bypass.

The production value lists the two catalog reads as **exact paths**, because
CloudFront drops `Authorization` on exactly those cache behaviors. That coupling is
covered from the caching side in
[12_CDNS §2](12_CDNS.md#2-making-the-two-reads-cacheable--gateway_public_paths).
Design: [never-public user paths](../superpowers/specs/2026-07-19-gateway-never-public-user-paths-design.md).

## 4. Origin authentication — is this request really from our CDN?

The ALB security group is pinned to CloudFront's managed prefix list, but that list
covers *every* AWS account's distributions.
[`GatewayOriginSecret`](../../src/main/java/com/recsys/application/gateway/GatewayOriginSecret.java)
is what proves the request came from *our* distribution: a server-wide decorator that
rejects any non-exempt request lacking a matching `x-origin-secret` with `403`.

Two design details matter operationally:

- **`GATEWAY_ORIGIN_SECRET` is a comma-separated *set*, not a single value** — both
  the old and new secret are accepted during a rotation, so there is no 403 window.
- **`/health` and `/metrics` are exempt**, because the ALB health check, the kubelet
  probes, and the Prometheus scrape reach the pod directly and would otherwise all
  fail, leaving the pod permanently unready.

Rejections increment `gateway_origin_secret_rejected_total` and log **once** — a
scan or a botched rotation would otherwise flood the log with a per-request warning.

## 5. Operator authorization — a privilege tier

Once edge auth is on, the `/api/online` passthrough lets **any authenticated client**
reach online serving's introspection surfaces. Those are operator tools, not client
API, so [`AdminTokenGuard`](../../src/main/java/com/recsys/application/auth/AdminTokenGuard.java)
adds a second factor: a constant-time `X-Admin-Token` match against
`SHARD_ADMIN_TOKEN`, wired from the `recsys-online-admin` Secret (`optional: true`).

| Route | Gate |
|---|---|
| `GET /online/ops` | Operator token (Armeria decorator) |
| `GET /shards/shard` | Operator token |
| `POST /shards/topology` (reshard) | Operator token; `403 "reshard disabled"` when unset |
| `GET /shards/device` | Gateway auth only — per-entity read |
| `POST /shards/records` | Gateway auth only — data-plane write |
| `/v2/recommend`, `/online/*` serving | Gateway auth only |

It **fails closed**: `isAuthorized` returns false when the token is unconfigured, so
an unprovisioned deployment returns `403` rather than exposing the surface. The
per-device read is deliberately *not* gated — under the established trust model,
backends legitimately read any device.
Design: [operator surfaces authz](../superpowers/specs/2026-07-19-online-operator-surfaces-authz-design.md).

## 6. Service-local tokens on model serving (8080)

Two Spring-side mechanisms exist that are **not** caller authentication:

**Submit token — one-time CSRF.** `GET /api/v1/token` issues a UUID stored in Redis
with a 300 s TTL; `POST /api/v1/recommend` consumes it via a Lua `GET`-then-`DEL` so
the check-and-consume is atomic and a token cannot be replayed. The issuing response
is `Cache-Control: no-store` — a shared cache handing the same token to two clients
would break single-use semantics. Disabled by default
(`RECSYS_SUBMIT_TOKEN_ENABLED=false`).

**Login session — an end-user session layer that is essentially dormant.**
`POST /api/v1/auth/login` constant-time-matches an API key from `recsys.login.api-keys`
and returns a UUID session token stored at `login:<token>` in Redis for 24 h;
`LoginInterceptor` parses the bearer token on every request and populates the
request-scoped `RequestScopeData`; `NeedLoginAspect` enforces `@NeedLogin`. In
practice this guards **exactly one endpoint** — `POST /api/v1/auth/logout` — and
`recsys.login.api-keys` is absent from `application.yml`, so `isEnabled()` is false
and login returns `401 "login is disabled on this server"` out of the box.

## 7. Signed tokens that are not credentials

[`ConsistencyTokenCodec`](../../src/main/java/com/recsys/application/consistency/ConsistencyTokenCodec.java)
issues an HMAC-SHA256 receipt (`X-Consistency-Token`) proving an online event was
durably accepted — a read-your-writes proof, not an identity. It is worth reading as
the repo's best-shaped token: a **versioned** header (`{"alg":"HS256","typ":"ECT","v":1}`),
**subject-bound** to the `userId`, a fixed 24 h lifetime, a 2048-character ceiling,
and a constructor that rejects a secret under 32 UTF-8 bytes. Semantics live in
[15_Eventual_Consistency](15_Eventual_Consistency.md).

[`RecommendationCursorCodec`](../../src/main/java/com/recsys/application/pagination/RecommendationCursorCodec.java)
is the same shape applied to pagination: an HMAC-SHA256 signature over a versioned
(`VERSION = "3"`), user- and query-bound payload, with constant-time comparison, an
issued-at expiry, a 2048-character ceiling, and an active/previous key pair for
rotation. Unsigned `v2:` tokens are accepted only behind an explicit compatibility
flag and upgrade on the next page. Mechanics are in
[19_Pagination §4](19_Pagination.md#4-implemented-recommendation-optimization);
key rotation is in
[recommendation-cursor-key-rotation.md](../runbooks/recommendation-cursor-key-rotation.md).

Neither token authenticates a caller — both bind data to a subject that the gateway
already authenticated. They are the two places in the repo where a signed wire format
is done properly, and worth copying from.

## 8. The L3/L4 access-control list

Everything above is L7. The control that makes it *sufficient* is
[`network-policy.yaml`](../../k8s/base/network-policy.yaml): the backends authenticate nobody
because the policy is what proves the gateway is their only reachable caller.

Each of the four services declares `policyTypes: [Ingress, Egress]` and permits an explicit
destination set. The relay is the deliberate exception — `policyTypes: [Ingress]`, leaving egress
unrestricted, because its destinations (MySQL, Kafka, and in EKS ElastiCache) are partly external
and partly per-overlay, so an allow-list there would black-hole delivery the moment it drifted.

The egress half is the half that rots. Ingress rules are stated once and stay true; egress rules
encode the *addresses of dependencies*, and those move — a new upstream in the ConfigMap, a flag
that opens a connection, an overlay that relocates Redis outside the cluster. Six such gaps had
accumulated by 2026-08. `NetworkPolicyEgressManifestTest` is the response: it derives every
upstream address from the union of `recsys-config` and every base Deployment's inline `env:`, and
requires a matching rule, so the two sets can no longer diverge without failing a PR. The
Deployment half was added after `SPLUNK_HEC_URL` — a literal env value on all four serving
Deployments, not a ConfigMap key — turned out to be dialing an unpermitted `splunk:8088`.
Ownership is declared rather than derived — `recsys-config` is one
ConfigMap `envFrom`'d into all five workloads, so possession of an env var proves nothing about
who dials it. A Deployment env var is the opposite case: it names its own dialer.

Two rules in that file are worth reading directly. Egress to `app: splunk` on 8088 exists because
every serving workload ships structured logs there, and the appender's at-most-once, drop-on-full
delivery means a blocked connection loses events with no error anywhere — the failure is
indistinguishable from a service that logged nothing. The DNS rule is scoped to
`namespaceSelector: kubernetes.io/metadata.name=kube-system` rather than left destination-free: a
NetworkPolicy rule with no `to[]` permits its port to *every* address, so the bare `ports: [53]`
this file used to carry was an unrestricted outbound channel from every workload. It stops at the
namespace rather than adding `podSelector: k8s-app=kube-dns` because CoreDNS's own labels vary
across distributions, so a podSelector pinned to one would break silently on another. Note that
neither selector matches NodeLocal DNSCache, which is host-networked and therefore carries the
node's IP: a cluster running it needs an additional `ipBlock` peer for `169.254.20.10/32` in the
same rule.

Design: [NetworkPolicy egress conformance](../superpowers/specs/2026-08-05-networkpolicy-egress-conformance-design.md).

## 9. Testing

- **Edge auth** — `GatewayAuthenticatorTest` (key match, bearer-as-key, public paths,
  never-public guard, fail-closed startup), `CognitoJwtVerifierTest` (alg, kid, iss,
  aud, exp/nbf skew, token_use), `CognitoJwtVerifierJwksTest` (serves from cache
  within TTL, serves stale after a failed refetch, backoff during an outage, unknown
  `kid` with nothing cached).
- **Trust boundary** — `GatewayRequestForwarderTest` (strips spoofed `x-authenticated-*`,
  strips consumed credentials and the origin secret, injects the principal),
  `GatewayPrincipalTest` (JWT and API-key principals, anonymous).
- **Origin secret** — `GatewayOriginSecretTest` (rotation set, exempt paths,
  constant-time), `GatewayOriginSecretMetricsTest` (counter).
- **Operator gate** — `AdminTokenGuardTest` (fail-closed unset, constant-time match,
  decorator `403`).
- **Service-local** — `SubmitTokenServiceTest` (single-use consume),
  `SubmitTokenCacheHeaderTest` (`no-store`), `ConsistencyTokenCodecTest`.
- **Data tier** — `LettuceClientFactoryTest` (TLS on/off, ACL username vs default-user
  AUTH, replica URIs inheriting both, the Spring-properties path, and the three guard
  cases: blank password refused, opt-out accepted, password needs no opt-out),
  `RedisAuthManifestTest` (every Redis client wired from the Secret, the relay
  deliberately not, `requirepass`/`masterauth`/`auth-pass` on the servers, no probe
  passing `-a`, and no manifest in any overlay setting `REDIS_ALLOW_NO_AUTH`).

## Data-tier authentication

Until 2026-08 every service connected to Redis as the unauthenticated `default` user over a
plaintext socket. `LettuceClientFactory` read `REDIS_PASSWORD` and no manifest set it — a
supported control that nothing switched on.

Redis now requires a password: `--requirepass` on the primary and replica, `--masterauth` so the
replica can authenticate to the primary, and `sentinel auth-pass` in the Sentinel template,
substituted from the environment by the init container because a ConfigMap cannot hold a secret.
Clients read `REDIS_PASSWORD` from the `redis-password` key of `recsys-secrets`.

`LettuceClientFactory` refuses to open a connection without a credential unless
`REDIS_ALLOW_NO_AUTH=true` — the same fail-closed shape as `GatewayAuthenticator.fromEnvironment`
in §1. Local development and the test suite set it; no overlay does.

The client also supports `REDIS_USERNAME` (Redis 6 ACL login) and `REDIS_TLS`. `REDIS_TLS` is
still unused: the in-cluster Redis has no certificates. `REDIS_USERNAME` is no longer inert —
authorization is covered below.

Two things this does not do. Traffic between the pods and Redis is still unencrypted, so the
NetworkPolicy remains the only control on who can read it in transit. And Sentinel's own port
26379 has no `requirepass` of its own: `sentinel auth-pass` is how a sentinel authenticates *to*
the primary, not how it authenticates callers. Anyone with network reach to 26379 can still issue
`SENTINEL FAILOVER mymaster` and move the write leader — exactly the "NetworkPolicy is the only
control" gap this closes for 6379, left open on the port that decides which node 6379 *is*.

Design: [Redis transport authentication](../superpowers/specs/2026-08-05-redis-transport-auth-design.md).

**Authorization is a separate concern from authentication, and was closed later.** The paragraph
above shipped every workload authenticating as `default`, and at the time this doc claimed the
five clients shared one credential "despite cleanly disjoint key ownership." A 2026-08-05 ACL
audit recorded that same claim, and it does not survive contact with the call graph: catalog,
model, and online serving all read the same `i2vEmb:`/`u2vEmb:` embedding keyspace and the same
`topk:` trending keyspace, so key ownership was never disjoint on the read side. Splitting reads
apart would require moving those reads behind a service boundary, not an ACL change, and is not
attempted. What the call graph does support is a write split: each key prefix has exactly one
service that writes it (catalog writes `i2vEmb:*`/`u2vEmb:*`; online writes `sr:*`/
`shard:topology`/`rate:online:*`; model writes `submit_token:*`/`login:*`; each service writes
only its own `svc:registry:*` key), even though several services read across those boundaries.

`k8s/base/redis-users.acl.template` now defines six ACL users — `default` plus one per workload
(`catalog`, `model`, `online`, `gateway`, `reconciliation`) — mounted via `--aclfile` on both the
primary and replica StatefulSets and wired to each workload's own `REDIS_USERNAME`. Each
non-default user is scoped to `-@all +@read +@write +@connection -@dangerous` over its own write
prefixes, `+@scripting` where `EVAL` is on the path (catalog, model, online — the trending read,
sharded record write, topology publish, rate limiting, and submit-token consume all run as Lua),
and read-only (`%R~`) grants onto the prefixes it reads but does not own. `topk:*` is the one
prefix granted full read-write (`~`, not `%R~`) to all three serving users regardless of logical
ownership, because `ShardedTopKStore` reads it through `EVAL` and Redis requires full read-write
permission on every key a script touches. `default` keeps `~* &* +@all`, so the original
`redis-password` credential — the one Flink authenticates as to write `u2vEmb:*` and `topk:*` — is
still a full-access credential, including `FLUSHALL`; the least-privilege boundary here is the
five service users, not the instance as a whole. See
[the ACL users section](../runbooks/redis-auth.md#acl-users) for the mechanics, including a
measured, load-bearing gotcha: an ACL file that omits `user default` does not leave `requirepass`
governing it, it silently disables authentication.

This still has two limits, both unchanged by the ACL work. The EKS overlays point every client at
ElastiCache instead of this Redis, and ElastiCache authorizes access through RBAC user groups
managed via the AWS API — a different mechanism that none of this ACL file touches. And nothing
here is enforced anywhere today: no EKS cluster exists in either region, so this is manifest
correctness plus a merge-blocking conformance test (`RedisAclManifestTest`), not a running
guarantee.

Design: [Redis per-service ACL users](../superpowers/specs/2026-08-09-redis-per-service-acl-design.md).

MySQL is the other data tier, and closes a gap Connector/J leaves open by default rather than one
this codebase left unset. `MySqlConnectionSettings`'s compact constructor refuses to build when
`MYSQL_ENABLED=true` and `MYSQL_URL` does not set `sslMode=VERIFY_IDENTITY`, and refuses a URL that
still carries the deprecated `useSSL` property (Connector/J 8 lets `sslMode` override it silently,
so the two together read as one thing and behave as another). MySQL client construction is
otherwise unconditional: `CatalogComponent.fromEnvironment()`, which `RecSysServer` (6010) calls on
every startup, builds `MySqlConnectionSettings` regardless of whether MySQL is enabled, so the
guard is live the moment `MYSQL_ENABLED=true` is set — a URL without a verified `sslMode` fails 6010
at pod start. `OnlinePredictionServer` (7010) builds it only when durable online event acceptance
is on (`ONLINE_DURABLE_EVENTS_ENABLED=true`), which itself requires `MYSQL_ENABLED=true` and throws
if it is unset — so the same guard applies to 7010 whenever that feature is enabled, not merely
whenever MySQL is. A third construction site is ungated in the same way 6010 is:
`ReconciliationCommand.main` builds `MySqlConnectionSettings` unconditionally, unlike
`OutboxRelayCommand.main`, which returns early when `ONLINE_DURABLE_EVENTS_ENABLED` is false. Its
CronJob `envFrom`s `recsys-config`, so a `MYSQL_URL` without a verified `sslMode` fails the
reconciliation job at every scheduled run — as a `CrashLoopBackOff` on a periodic pod rather than
at a rollout, which is the quieter of the two failures to notice.

`VERIFY_IDENTITY` rather than `REQUIRED`: `REQUIRED` encrypts the connection but verifies no
certificate, so it stops silent plaintext but not an active man-in-the-middle presenting any
certificate at all. Against RDS this costs no extra provisioning: per `MySqlConnectionSettings`'s
javadoc, Amazon's CAs are already in the JVM truststore, so an RDS certificate verifies with no
extra truststore work. `k8s/eks/redis-elasticache-patch.yaml` documents the same shape for the
other data tier's TLS — that ElastiCache's certificate verifies against the JVM truststore because
it already trusts the Amazon Root CA that ElastiCache's certificates chain to — though that file
only speaks to ElastiCache, not RDS.

Loopback hosts (`localhost`, `127.0.0.1`, `[::1]`) are exempt from the whole requirement, including
the `useSSL` rejection. This is a host test on the URL, not an opt-out flag: the host `k8s/base`
sets is `mysql`, and any RDS endpoint is a DNS name, so no manifest value can satisfy the loopback
test and disable the guard the way a manifest can set `REDIS_ALLOW_NO_AUTH=true` above. That is the
shape the Redis finding lacked — there, the opt-out is a flag a manifest could in principle carry;
here, exemption is unreachable from any deployed configuration.

The consequence is real but bounded: `MYSQL_ENABLED` defaults to `"false"` in every manifest, so
today this guard is dormant everywhere, and nothing in this repository deploys a MySQL server for
it to run against — the requirement is unverified against a real TLS handshake. `k8s/base/configmap.yaml`
now sets `MYSQL_URL` to a value that satisfies the guard (`?sslMode=VERIFY_IDENTITY`), so enabling
MySQL by flipping `MYSQL_ENABLED` alone does not trip it; a manifest change that touches `MYSQL_URL`
without preserving that parameter would.

Design: [MySQL transport TLS](../superpowers/specs/2026-08-05-mysql-transport-tls-design.md).

## 10. User-scope authorization — is this caller allowed to name this user?

§1–§4 answer "is this caller authenticated". This section answers "may this caller act on *this*
user", which is a separate question and was, until now, unasked: `userId` is an ordinary request
field, so any authenticated caller could name any user.

The rule is one comparison, applied at the gateway:

- **Credential type decides the tier.** `GatewayPrincipal.Tier` is `USER` for a JWT caller and
  `SERVICE` for an API key or (dev-only) anonymous. Service-tier callers are exempt — the trust
  model is that they are backends legitimately acting for many users, which is what
  `ModelRateLimiter` already assumes when it keys on the served userId rather than the caller.
- **User-tier callers may act only on their own id.** `GATEWAY_COGNITO_USER_ID_CLAIM` (default
  `sub`) names the JWT claim carrying the application userId; the gateway compares it, as an exact
  string, to the `userId` in the request.
- **Anything indeterminate is a denial.** A blank claim, a missing `userId`, an unparseable body
  — all 403. Tiering by credential type rather than by claim presence is what makes this hold: a
  JWT whose claim did not resolve stays user-tier and is denied, instead of falling through to
  service-tier freedom.

`BackendRoutePolicy` declares which backend routes take a `userId` and how it arrives, keyed on
`(backend service, backend path)` — not on the gateway path, because `/api/users`, `/api/movies`,
and `/api/catalog` all resolve to 6010 and `MicroserviceRoute.rewrite` forwards the suffix
verbatim, so one handler is reachable under three prefixes. (This is one access class among four —
`NO_PROXY`, `OPERATOR`, `USER_SCOPED`, `AUTHENTICATED` — that `BackendRoutePolicy` assigns to every
backend route; §11 covers the table as a whole. This section covers only the `USER_SCOPED` class,
which absorbed the entire remit of the former, single-purpose `UserScopedRoutes` class.) Five
source kinds cover every user-scoped route: `QUERY` (a query-string parameter), `BODY` (a
top-level `userId` field), `BODY_INSTANCES` (every element of a TF-Serving-shaped
`instances[]` array must name the same user), and — added by the retrieval merge, see §11 —
`PATH` (a path segment) and `BODY_USER` (a top-level `user` field). The third kind exists because
`POST /v1/models/recmodel:predict` turned out to be user-scoped: `PredictInstance` carries a
caller-supplied `userId`, and `PairPredictionService` loads `u2vEmb:<userId>` and returns scores
derived from it — a fact the route had first been excused past, and only the conformance test
below caught. `BackendRouteCoverageTest` requires every gateway-reachable backend route to be
classified in `BackendRoutePolicy` as one of the four access classes — not merely user-scoped or
not — so the enforcement cannot silently develop holes as routes are added; a second test in the
same class sweeps the whole `src/main/java` tree for route registrations and Spring controllers
outside the locations the first test scans, so the coverage claim cannot be undermined by a route
registered somewhere the scanner never looks.

Enforcement lives in `GatewayRequestForwarder.forward`, beside the credential stripping and
identity injection of §2. It runs after rate limiting (so a probing caller spends their own
tokens) and before the circuit-breaker permit is acquired (so a denial cannot leak one). Denials
return `403` with a fixed body (`forbidden: request is not scoped to the authenticated user`),
increment `gateway_user_scope_rejected_total`, and log once.

`forward` is the choke point for every *proxied* route — `GatewayProxyService` and
`RecommendationGatewayService` both reach it, and since §11 it also enforces the `NO_PROXY` and
`OPERATOR` classes there — but it is **not** the gateway's only forwarding path. `LlmProxyService`
forwards to the LLM upstream itself, duplicating §2's stripping and injection, and does not
consult `BackendRoutePolicy` for the request path at all — no user-scope check, no operator-token
check, nothing. That is sound only because no LLM route can reach a backend `BackendRoutePolicy`
classifies, and that premise is enforced rather than asserted: `LlmProxyService`'s constructor
refuses a route that resolves to *any* known backend, naming this section in the failure —
regardless of which access class that backend happens to declare, so a future `NO_PROXY` or
`OPERATOR` backend closes the same gap a `USER_SCOPED` one would. Adding an LLM route that reaches
a classified backend therefore fails at startup instead of forwarding unchecked. Two forwarding
paths is the real shape here; treating it as one was the mistake.

The guard resolves the route's **target**, not its label, and so does `forward`'s own lookup —
both go through `BackendRoutePolicy.effectiveServiceName`, which falls back to the `serviceName`
of whichever known route shares the same authority. A guard on the declared name alone could never
have fired: `MicroserviceRoute.fromEnvOptional`, the only thing that builds an LLM route in
production, always passes `serviceName = null`, and the 5-arg constructor defaults it to null too.
So the realistic misconfiguration — `LLM_SERVICE_URL` pointed at 8080 — produced a route both the
guard and the lookup waved through, and `/api/llm/api/v1/recommend` forwarded with no check at all.
A route cannot opt out of this enforcement by declining to name itself.

**The path the check sees must be the path the backend sees.** Armeria preserves a `.` segment
(it rejects only `..`); Tomcat, which serves 8080, collapses it. So
`/api/model/api/v1/./recommend` missed the `BackendRoutePolicy` table — matching there is exact, by
design — while still reaching the Spring handler registered for `/api/v1/recommend`, which made
every 8080 user-scoped route bypassable by respelling the path. `GatewayProxyService.serve`
rejects any path carrying a `.` or `..` segment with `400`, immediately after `ApiVersion.parse`
and before routing. Rejecting at the edge rather than canonicalizing at the lookup is the point:
route matching, `GATEWAY_PUBLIC_PATHS`, rate-limit keying, and the CloudFront cache key all key on
the same string, and a fix inside the lookup would have left the other four on whatever spelling
the client chose. This is the one control in §10 that also applies to service-tier callers — it is
a malformed-request rejection, not an authorization decision, and no published route contains a
dot segment. `RecommendationGatewayService` needs no equivalent guard: it is registered at two
exact paths and builds a constant `targetPath` of `/v2/recommend`, so no client-supplied path
segment reaches an upstream through it.

A percent-encoded separator is the same disagreement reached by a different spelling, and is
rejected alongside it. Armeria decodes unreserved characters but deliberately leaves `%2F`
encoded, so `/api/model/api/v1/.%2Frecommend` is *one* segment to every control here and two to
any backend that decodes it. Tomcat rejects an encoded solidus by default, which made this inert —
but that is a setting (`encodedSolidusHandling`), not a guarantee, and no route the gateway
publishes takes a path segment containing a literal `/`. Depending on a downstream default to hold
a security boundary is how the `.` case got missed in the first place.

**What this does not yet prove.** No environment sets `GATEWAY_COGNITO_ISSUER`, so every caller
today is service-tier and this section describes a path that is never taken in production. Tests
construct verified claims directly; the extraction of a real claim from a real user pool is
untested until the first deployment that enables Cognito. Its failure mode is a 403 on every
user-scoped route, not an opening.

Design: [user-scope authorization](../superpowers/specs/2026-08-05-gateway-user-scope-authorization-design.md).

## Sharp edges — notes

1. **The operator gate on control-plane writes is a gateway property, not a system property.**
   §11's `BackendRoutePolicy` now requires `X-Admin-Token` at the gateway for every `OPERATOR`
   route across all three backends — `/api/catalog/setembedding` (overwrite item embeddings on
   6010), `/api/model/api/v1/model/versions/activate`/`/rollback`/`/preload` (swap the serving
   model on 8080), and `/api/online/online/ops`. §10 remains the other check: a JWT (user-tier)
   caller may act only on its own `userId`, on the routes `BackendRoutePolicy` marks `USER_SCOPED`.
   Neither check does anything for a caller that reaches a backend directly instead of through the
   gateway — sharp edge 6 is unchanged: 6010, 7010, and 8080 authenticate nobody. For
   `/api/catalog/setembedding` and the model version endpoints, the gateway's `X-Admin-Token` check
   is the *only* control-plane authorization that exists anywhere in the system; a direct pod
   connection reaches them with no credential at all, operator or otherwise.
   `/shards/topology`, `GET /shards/shard`, and `GET /online/ops` are the exception — 7010 already
   guards those three with its own `AdminTokenGuard` (§5), so the gateway's check there is
   redundant defense-in-depth, not the only line. So: control-plane writes require the operator
   token when reached **through the gateway**. Whether they are also reachable without one depends
   on which backend they live on, and for two of the three routes above, they are.
2. **`k8s/base` runs wide open.** `GATEWAY_ALLOW_ANONYMOUS: "true"` lives in the base
   configmap; only the `eks-shared` component flips it to `false`. A new overlay that
   composes `../base` without `../eks-shared` inherits anonymous access silently —
   the fail-closed guard cannot help, because the opt-in is present.
3. **The session layer's `userId` is the API key.** `LoginController` calls
   `loginTokenService.create(request.getApiKey())`, so the Redis value at
   `login:<token>` is the API key in plaintext and `RequestScopeData.getUserId()`
   returns a credential. Anything that logs or keys on that "user id" is handling a
   secret.
4. **`@NeedLogin` denial is a `200`.** `NeedLoginAspect` returns
   `ApiResponseUtil.error("user not logged in")` — an error envelope with a success
   status code, not `401`. It currently guards only logout, so the blast radius is
   nil, but the pattern would mislead any client that checks status codes.
5. **A JWT with no `token_use` claim passes the `token_use` check.** The validation is
   skipped when the claim is blank, so the restriction only tightens tokens that
   declare a use.
6. **Backend safety depends on the NetworkPolicy, not on the backends.** 6010, 7010,
   and 8080 authenticate nobody. Reaching a backend pod directly — a misapplied
   policy, a debug port-forward, a service exposed by a new manifest — bypasses every
   control in §1–§4 at once.
7. **The NetworkPolicy is base-only.** No overlay patched it until the ElastiCache egress patch
   in `k8s/eks-shared`, yet both EKS overlays relocate Redis outside the cluster. An overlay that
   changes *where* a dependency lives changes it out from under the ACL, and nothing in `k8s/base`
   can notice — the conformance test reads base, so overlay coverage stops at asserting the patch
   exists.
8. **Enforcement is CNI-dependent.** EKS's default VPC CNI does not enforce NetworkPolicy unless
   policy support is explicitly enabled. "The NetworkPolicy protects the backends" is therefore a
   claim about cluster configuration, not about anything in this repo. If it is not enforced, §8's
   rules are documentation and the backends — which authenticate nobody — are reachable by any pod
   in the cluster.
9. **Nothing permits egress on 443.** The conformance test covers what the ConfigMap names, and
   the ConfigMap names no HTTPS endpoint. The Cognito JWKS fetch (once `GATEWAY_COGNITO_ISSUER` is
   set), IRSA/STS, Cloud Map, SQS, and PostHog feature flags (`PostHogFeatureFlagProvider`, which
   POSTs `distinct_id` and `person_properties` to an external SaaS) all leave the cluster on 443
   against no rule. On an enforcing CNI, turning on Cognito JWT auth would fail every verification
   — and §8's test would still be green, because a destination named in neither `recsys-config`
   nor a base Deployment's inline `env:` is a destination it cannot know about. Overlay-patched
   addresses are outside its reach for the same reason.

## 11. The gateway proxy policy — what the gateway is willing to forward

[`BackendRoutePolicy`](../../src/main/java/com/recsys/application/gateway/BackendRoutePolicy.java)
classifies every backend route the gateway can reach into one of four access classes:

| Class | Meaning |
|---|---|
| `NO_PROXY` | Never proxied — telemetry and diagnostics, reachable only on the pod |
| `OPERATOR` | Requires `X-Admin-Token`, for every caller including service-tier ones |
| `USER_SCOPED` | Requires a user-tier caller to name its own `userId` — §10 |
| `AUTHENTICATED` | Proxied to any authenticated caller — ordinary data paths |

Classification is an allow-list: an exact-path match is tried first, then a three-entry prefix
table (`/actuator` → `NO_PROXY`, `/shards` → `AUTHENTICATED`, `/api/v1/knowledge-bases` →
`AUTHENTICATED`). A path matching neither is denied exactly like one explicitly marked
`NO_PROXY`:
[`GatewayRequestForwarder.enforceRoutePolicy`](../../src/main/java/com/recsys/application/gateway/GatewayRequestForwarder.java)
returns `404 {"error":"no route found"}` for both, byte-identical to a path that was never routed
at all — a caller cannot distinguish "this route exists but is withheld" from "this route was
never registered." `BackendRouteCoverageTest` enforces the allow-list property at build time: it
scans all three backend mains, and fails if any route it finds has no entry in
`BackendRoutePolicy`, so an unclassified route added tomorrow is unreachable through the gateway
rather than silently exposed by it.

**Prefix entries exist for paths that cannot be written as exact strings** — three of them, for
three different reasons. `/actuator`'s membership is configuration, not source (below).
`/shards` is a single Armeria `pathPrefix` whose sub-paths are dispatched inside the handler.
`/api/v1/knowledge-bases` is there because two of `KnowledgeBaseController`'s four handlers are
declared with a Spring path *template* — `/knowledge-bases/{knowledgeBaseId}` — and a template is
not a path: declared as an exact entry it matches only the literal brace-bearing string that the
coverage scanner emits and no client ever sends, so every concrete id would 404 while both
coverage tests stayed green. A prefix covers the collection path and every id in one entry. The
corollary is that a prefix's own path must not *also* be declared exactly — exact wins the lookup,
which would make the prefix's own branch dead code — and
`BackendRouteCoverageTest#noPrefixEntryShadowsADeclaredExactPath` fails the build on it.

**Telemetry is no longer proxied, and nothing legitimate depended on it going through the
gateway.** `/metrics` (Armeria, 6010 and 7010), `/actuator/*` (Spring, 8080), 8080's diagnostic
`/health/*` surfaces (`/health/jvm`, `/health/gc`, `/health/metrics`, `/health/cache`,
`/health/ab-tests`, `/health/load`, `/health/live`, `/health/ready`), and each backend's plain
liveness/readiness paths (6010's `/health`, `/health/ready`, `/health/load`; 7010's `/health`,
`/health/live`, `/health/ready`) are all `NO_PROXY` — the complete `NO_PROXY` set, not a sample.
The gateway's own `/metrics` on 8010 is a **different surface** and is not in this table at all:
it is served directly by `MicroserviceGatewayServer` (`sb.service("/metrics", …)`), never routed
through `GatewayRequestForwarder`, so no access class applies to it and it stays reachable — by
design, since §4 deliberately exempts it from `GATEWAY_ORIGIN_SECRET` so the Prometheus scrape
works. Nothing in this table withholds it, and a claim that it is `NO_PROXY` would be asserting a
control that does not exist. The `ServiceMonitor`s in
[`k8s/base/servicemonitor.yaml`](../../k8s/base/servicemonitor.yaml) scrape each backend's own
Service directly, not through the gateway; the k8s startup/liveness/readiness probes hit the pod
directly; and `UpstreamEndpointGroups`' upstream health checking dials `baseUri + healthPath` on
the backend directly rather than through the proxy path. `/actuator`'s membership in `NO_PROXY` is
declared, not scanned — Spring's endpoint exposure is a configuration property
(`management.endpoints.web.exposure.include`, `application.yml`), invisible from a source scan, so
`BackendRouteCoverageTest` cannot cross-check that declaration the way it does the enumerable
routes. Whether `/actuator/*` is reachable on 8080 at all is therefore a fact about
`MANAGEMENT_ENDPOINTS_EXPOSURE`, not about this table.

**`OPERATOR` requires `X-Admin-Token` from `SHARD_ADMIN_TOKEN`, and binds every caller including
service-tier ones.** `GatewayRequestForwarder` demands a valid token — matched via
[`AdminTokenGuard`](../../src/main/java/com/recsys/application/auth/AdminTokenGuard.java) — for
`/api/catalog/setembedding`, the model version `activate`/`rollback`/`preload` endpoints, and
`/api/online/online/ops`. An API key is what every real caller already holds, so if it were
sufficient here the class would mean nothing: a denial returns
`403 {"error":"operator token required"}`, deliberately distinguishable from the 404 above — the
caller is being told to present a credential, not that the path is absent. The gateway now reads
`SHARD_ADMIN_TOKEN` from the `recsys-online-admin` Secret (`k8s/base/api-gateway.yaml`, mirroring
`k8s/base/online-serving.yaml`'s existing wiring for 7010) and warns at startup when it is unset;
unset means the guard authorizes nobody, so every `OPERATOR` route fails closed rather than open.

**Two more `OPERATOR` routes arrived with the retrieval service merge, and both were previously
unauthenticated.** The retrieval code was copied into 8080 as a foreign island with no gateway
awareness at all — `POST /api/v1/retrieval/model/reload` (reloads the live ONNX session, the same
class of mutation as `/api/v1/model/versions/activate`) and `/api/v1/retrieval/profile-audit`
(walks every user's profile and preferences) both landed with zero guard. Both are now classified
`OPERATOR` in `BackendRoutePolicy`, so they inherit the same `X-Admin-Token` requirement — which
means the standing constraint above is now load-bearing for retrieval too: **`SHARD_ADMIN_TOKEN`
must be deployed before this image ships**, or these two routes, along with the pre-existing four,
reject every caller with 403.

The reload route surfaces a subtlety worth stating explicitly, because it is the kind of thing a
future reader would otherwise only discover by tracing reachability by hand: in the incoming
service it lived at `POST /actuator/model-reload`, and `/actuator` is already declared `NO_PROXY`
for `recsys-model-serving` — the gateway refuses to proxy it today. Moving the route out from under
`/actuator` to `/api/v1/retrieval/model/reload` therefore *increases* its reachability rather than
decreasing it: an unclassified path outside `/actuator` is not silently protected the way an
unclassified path inside it would be, it is simply denied by the allow-list default until
classified, then reachable once classified. That is precisely why the move and the `OPERATOR`
classification had to land in the same commit — landing the move first, even briefly, would have
made the route reachable by any authenticated caller with no operator check at all.

**`UserIdSource` gained a fourth member, `PATH`, because three retrieval routes carry the userId in
a path segment** — `/api/v1/retrieval/recommend/{user}`, `/api/v1/retrieval/predict/{user}/{item}`,
and `/api/v1/retrieval/users/{user}/profile` — and the existing three sources (`QUERY`, `BODY`,
`BODY_INSTANCES`) have no way to read one. The alternative considered was classifying these routes
`AUTHENTICATED` instead of `USER_SCOPED`: it would have compiled and every existing test would have
stayed green, because nothing in the suite exercises cross-user path substitution. It would also
have let any authenticated caller read another user's recommendations or profile simply by editing
the `{user}` segment of an otherwise-ordinary request. `UserIdSource.PATH` closes that gap by giving
these three routes a real `USER_SCOPED` classification instead of a permissive one chosen for
convenience.

**A fifth member, `BODY_USER`, followed — the feedback route's body spells the subject `user`, not
`userId`.** `POST /api/v1/retrieval/feedback` deserializes into `FeedbackRequest`, whose field is
`user`, and `HybridRecommendationService.recordFeedback` acts on `request.user()`. Classified
`userScoped(UserIdSource.BODY)` it read a `userId` field that body never carries, which failed in
both directions at once: every honest call extracted `""` and was denied 403, and a body naming
*both* keys — `{"userId":"<caller>","user":"<victim>"}` — passed the gateway on the key the backend
ignores. Spring's Jackson disables `FAIL_ON_UNKNOWN_PROPERTIES` (a bare `ObjectMapper` does not, so
the difference is invisible in a unit test that builds its own mapper), so `userId` was dropped
silently and the bandit counters, Q-values, reward totals and replay buffer were written as the
victim. The fix reads the field the handler reads. The general lesson is that a source's field name
is a property of the *route's request type*, not a repo-wide constant: `UserIdSource.PARAM`'s
javadoc used to assert `userId` was universal, and it no longer is. `UserIdSourceTest` now
serializes a real `FeedbackRequest` with Jackson and runs those bytes through the source, because
what let this ship green was that no test had ever fed a real request body through the policy.

**`/api/v1/retrieval/predict/id` and `/predict/metadata` are `NO_PROXY`, not `AUTHENTICATED`,
because the first is an unscoped alias for a user-scoped route.**
`RetrievalRecommendationController.predictById` reaches the same
`predictionService.predict(userId, itemId)` as the templated `/predict/{user}/{item}` that the
`PATH` prefix above scopes — addressed by raw model indices instead of account ids. As
`AUTHENTICATED` it handed a user-tier caller the exact score they had just been 403'd for on the
templated spelling, and the indices are dense and bounded by `userLookup.size()`, so they enumerate
rather than having to be guessed; `/predict/metadata` publishes that bound. Both are debug/index
surfaces with no client need through the gateway, and the whole retrieval surface is new to it, so
refusing to proxy them regresses nothing. They can be promoted later, deliberately, with a
user-scope story — which is a different thing from having been classified permissively by default.

**The model-reload route is now guarded twice, like 7010's operator surfaces.**
`BackendRoutePolicy` classifying `POST /api/v1/retrieval/model/reload` as `OPERATOR` binds only
callers who arrive through the gateway; anything with in-cluster reach to 8080 skipped it entirely
and could swap the live ONNX session. `ModelReloadController` now performs its own `AdminTokenGuard`
check on the same `SHARD_ADMIN_TOKEN`, matching the precedent of `POST /shards/topology` and
`GET /online/ops`, which `ShardedRecordService` guards in-process *and* the table classifies
`OPERATOR`. The guard's fail-closed semantics carry over: unset token authorizes nobody, so the
route returns 403 rather than staying open. `k8s/base/model-serving.yaml` therefore now reads
`SHARD_ADMIN_TOKEN` from the same `recsys-online-admin` Secret (`optional: true`) that the gateway
and online serving already read.

**`/shards` stays `AUTHENTICATED`, not `OPERATOR`, because the prefix mixes tiers.**
[`ShardedRecordService`](../../src/main/java/com/recsys/infrastructure/store/ShardedRecordService.java)
already calls `adminGuard.isAuthorized` itself on `POST /shards/topology` and `GET /shards/shard` —
the two operator sub-paths — but not on `GET /shards/device` or `POST /shards/records`, which are
ordinary per-entity data paths. Classifying the whole prefix `OPERATOR` at the gateway would break
those two reads and writes; classifying it `AUTHENTICATED` leaves the backend's own two-way split
as the actual enforcement, and the gateway's contribution is the same as for any other data-plane
route — requiring an authenticated caller, nothing more.

`LlmProxyService` remains the second forwarding path noted in §10: it never consults
`BackendRoutePolicy` for the request path, for any of the four classes. Its construction guard
refuses any LLM route that resolves to a known backend at all — `NO_PROXY` and `OPERATOR`
backends included, not merely ones declaring a `USER_SCOPED` route — so a misrouted
`LLM_SERVICE_URL` fails at startup rather than forwarding an operator or telemetry path unchecked.

Design: [gateway proxy route policy](../superpowers/specs/2026-08-05-gateway-proxy-route-policy-design.md).

## 12. The third-party identifier boundary

Everything above governs callers reaching *in*. This section covers the one place an identifier
goes *out*: PostHog feature flags.

`RecommendationService` gates cold-start recommendations on a dynamic flag, keyed per user so a
rollout can be gradual. Resolving that flag POSTs to `https://us.i.posthog.com/decide/?v=3`, and the
key it carries is the request's `userId`. That is the only path in the system that sends an
application user identifier to an external service — the exception to §2's rule that identity stays
inside the trust boundary.

**What PostHog receives is a pseudonym, not the userId.**
[`PostHogFeatureFlagProvider`](../../src/main/java/com/recsys/infrastructure/featureflags/providers/PostHogFeatureFlagProvider.java)
sends `sha256(salt + ":" + userId)` as `distinct_id` — the full digest. Deterministic and stable
across pods and restarts, because the salt is shared configuration, which is what lets percentage
rollouts bucket a user consistently. One-way, so PostHog holds nothing that identifies a user here.

**`POSTHOG_DISTINCT_ID_SALT` is required when PostHog is enabled**, with no default. A blank salt
fails provider construction, the same fail-closed shape as the blank-API-key check beside it.
Requiring it is not ceremony: userIds in this system are small integers, so an *unsalted* digest
over that key space is a rainbow table anyone can build in seconds. An unsalted hash would look
like a control and be none. The two alternatives are both worse — falling back to the raw id
defeats the point, and a per-process random salt makes bucketing differ per pod and per restart, so
gradual rollout breaks silently instead of loudly.

**Rotating the salt re-buckets every user.** A rollout in flight reshuffles who is inside it. Treat
the salt as long-lived configuration.

**What it costs.** Nobody can look a specific user up in PostHog any more, because PostHog no
longer knows about that user. That is the intended effect, and it removes a debugging path anyone
enabling the flag might expect to have.

**How dormant this is.** PostHog is disabled by default (`POSTHOG_FEATURE_FLAGS_ENABLED=false`),
additionally requires a non-blank API key before the provider is constructed at all, is set in no
manifest or overlay, and is permitted by no egress rule — `k8s/base/network-policy.yaml` allows no
egress to PostHog, an instance of sharp edge 9 above. On an enforcing CNI the request would fail at
the network layer before disclosing anything. The risk this section closes was never "we are
leaking user ids"; it was that two environment variables could turn a recommendation path into a
disclosure path with nothing in the code marking that as a decision.

Scope note: `person_properties` is empty at the one call site. If it ever carries user attributes,
that is a fresh disclosure decision and this section does not authorize it.

Design: [PostHog pseudonymous distinct_id](../superpowers/specs/2026-08-05-posthog-pseudonymous-distinct-id-design.md).

## 13. Data-leakage posture — what these controls enforce as deployed

Sections 1–12 describe controls as *designed*. This one records what they **enforce as
rendered** (`kubectl kustomize k8s/base`, `k8s/eks`, `k8s/eks-us-west-2` at `123977a`),
because overlays override and a patch read in isolation says nothing about what a cluster
receives. Eleven PRs (#274–#284) shipped against four threat models — exfiltration by a
compromised workload, cross-user leakage, data reaching third parties, and accidental
disclosure through responses/metrics/logs — and every one is closed. A list of merged PRs is
not a posture. This is an audit of manifests and code, not a penetration test; nothing here
was executed against a live cluster.

### 13.1 The short answer

**Three controls would actually stop a leak if these manifests were applied today, and all
three are properties of code rather than configuration:** the Redis client refuses to start
without a password, the recommendation cursor is HMAC-signed with a key the pod cannot start
without, and the gateway strips caller credentials before proxying (§2). Everything else is
dormant, placeholder-shaped, or dependent on a Secret or AWS resource this repository does
not create.

**The most consequential finding was not on the PR list — and is now fixed.** `k8s/base`
used to publish the gateway as an `internet-facing` NLB *and* set
`GATEWAY_ALLOW_ANONYMOUS: "true"`. Those composed: any overlay building on `../base` without
`../eks-shared` got an internet-reachable gateway that authenticated nobody, and because
`GatewayAuthenticator` short-circuits when disabled, the `PROTECTED_PREFIXES` guard (§3) was
never consulted either — so `/api/catalog/user`, `/api/users/**`, `/api/features/**`,
`/api/online/**` and every user-scoped route were open with an arbitrary `userId`. Base's
Service is now `ClusterIP` with the eleven `aws-load-balancer-*` annotations removed;
`GATEWAY_ALLOW_ANONYMOUS` stays `"true"` there deliberately (flipping it was never the fix —
`fromEnvironment` refuses to start without a credential source and base has none), but
reaching it now requires `kubectl port-forward`. `GatewayExposureManifestTest` pins the
pairing.

### 13.2 Control inventory

Twenty controls. **ON** = enforcing as rendered. **FAIL-CLOSED** = configured to deny, which
denies legitimate operators too. **OFF** = present but not enforcing. **N/A** = the resource
does not exist in that configuration.

| # | Control | What it does | `k8s/base` | `k8s/eks` + `k8s/eks-us-west-2` | What makes it inert |
|---|---|---|---|---|---|
| 1 | Redis client auth guard (#274) | `LettuceClientFactory` throws at startup rather than connecting to Redis as the unauthenticated `default` user | ON (guard active; `REDIS_ALLOW_NO_AUTH` set in no manifest) | ON | Nothing disables it in-cluster. But `REDIS_PASSWORD` comes from an `optional: true` `secretKeyRef` on `recsys-secrets`, and **no `kind: Secret` exists anywhere under `k8s/`** — without an out-of-band Secret every serving pod CrashLoops. The control is real; its failure mode is outage, not silent plaintext |
| 2 | Redis transport TLS (#274) | `REDIS_TLS=true` wraps the Lettuce connection in TLS (Lettuce's default `SslVerifyMode.FULL` — chain + hostname) | OFF (unset → code default `false`) | **OFF (explicitly `REDIS_TLS: "false"` in both overlays)** | Redis traffic — embeddings, device history, the login-token→API-key mapping — is plaintext on the wire in every rendered configuration. The overlay comment states the overlay must not be applied until ElastiCache has transit encryption |
| 3 | Redis server-side `requirepass` (#274) | In-cluster Redis StatefulSets pass `--requirepass $(REDIS_PASSWORD)` | ON, conditional on the Secret | **N/A** — `k8s/eks-shared` scales `redis-primary`/`redis-replica`/`redis-sentinel` to `replicas: 0`; every client dials ElastiCache instead | In EKS the flag governs pods that do not run. ElastiCache AUTH is out-of-band and unverifiable from here |
| 4 | Redis per-workload ACL users (#284) | Six users (`default`, `catalog`, `model`, `online`, `gateway`, `reconciliation`) with per-key-prefix grants, loaded via `--aclfile`; each workload sends `REDIS_USERNAME` | Conditional | **INERT** — `--aclfile` is an argument to StatefulSets scaled to zero | `k8s/base/redis-users.acl.template` is a *template* with `__X_PASSWORD__` placeholders. It is mounted from a **non-optional** Secret volume (`recsys-secrets`, key `redis-users.acl`), so without that Secret the Redis pods block in `ContainerCreating`. In EKS the equivalent is ElastiCache RBAC user groups, which nothing in this repo creates — yet the workloads still send `REDIS_USERNAME`, so an ElastiCache without those users fails AUTH outright |
| 5 | Gateway authentication | API key (`x-api-key`/bearer) or Cognito JWT, else 401 | **OFF — `GATEWAY_ALLOW_ANONYMOUS: "true"`, no `GATEWAY_API_KEYS`** | ON — `GATEWAY_ALLOW_ANONYMOUS: "false"` and `GATEWAY_API_KEYS` from `recsys-gateway-auth` with `optional: false` (pod will not start without it) | In base, `GatewayAuthenticator.fromEnvironment` returns the `DISABLED` instance; `check` then allows every path unconditionally |
| 6 | Never-public prefix guard | `PROTECTED_PREFIXES` overrides `GATEWAY_PUBLIC_PATHS` so user-data routes can never be listed public | OFF (only consulted when authentication is enabled) | ON | Rides entirely on #5 |
| 7 | User-scope authorization (#275) | A JWT caller may only name its own `userId`; 403 + `gateway_user_scope_rejected_total` otherwise | **INERT** | **INERT** | `GATEWAY_COGNITO_ISSUER: ""` in all three renders, and blank is treated as unset. With no verifier, no principal can ever be `Tier.USER`; API-key and anonymous callers are `Tier.SERVICE` and the check returns immediately. Confirmed dormant, exactly as recorded |
| 8 | What authorizes user-scoped routes instead | — | **Nothing.** `userId` is client-supplied in the query string or body on every user-scoped route | Same | This is the honest answer to "what guards user data in the meantime": an authenticated API-key caller may name any `userId`, and in base an unauthenticated one may too |
| 9 | Gateway operator token (#276, #277) | `BackendRoutePolicy` classifies four backend paths `OPERATOR`; `GatewayRequestForwarder` requires a matching `X-Admin-Token`, tier-independent | FAIL-CLOSED | FAIL-CLOSED | `SHARD_ADMIN_TOKEN` comes from `recsys-online-admin` with `optional: true`, and that Secret is not in the repo. Unset ⇒ 403 for **every** caller on `/api/catalog/setembedding`, the three model version endpoints and `/api/online/online/ops`. The gateway logs a startup warning. Correctly fail-closed; also currently unusable |
| 10 | Online-serving operator token | `AdminTokenGuard` on `/online/ops`, `POST /shards/topology`, `GET /shards/shard` | FAIL-CLOSED | FAIL-CLOSED | Same Secret. Note the coverage is narrower than the gateway's: `GET /shards/device` and the record write path are unguarded, and 7010 logs **no** startup warning |
| 11 | Origin secret | `x-origin-secret` on a server-wide decorator; 403 + counter otherwise; `/health` and `/metrics` exempt | **OFF** | **OFF** | `GATEWAY_ORIGIN_SECRET` is an `optional: true` `secretKeyRef` on `recsys-gateway-origin-secret`, absent from the repo. When blank the decorator is not registered at all. This is the only server-wide gate on the gateway, and it is off |
| 12 | Credential stripping | The gateway removes `authorization`, `x-api-key` and `x-origin-secret` before forwarding upstream | **ON** | **ON** | Unconditional in code. One of the few controls with no configuration dependency |
| 13 | PostHog pseudonymization (#278) | Sends `sha256(salt + ":" + userId)` as `distinct_id`; blank salt fails construction | **INERT** | **INERT** | `POSTHOG_FEATURE_FLAGS_ENABLED` defaults `false` and is set in no manifest, compose file, or overlay, so the provider is never constructed. The code path *is* wired (model serving → `RecommendationService` → cold-start flag), so it is not dead code — but no NetworkPolicy egress rule permits reaching PostHog either. Confirmed dormant and unrouted |
| 14 | Pagination cursor signing (#279) | HMAC-SHA256 over `(version, issuedAt, userId, queryFingerprint, score, itemId)`; unsigned/tampered/expired/mismatched ⇒ 400 | **ON** | **ON** | `RECOMMENDATION_CURSOR_SIGNING_KEY` is a **non-optional** `secretKeyRef` on all three serving workloads and the config rejects a key under 32 bytes, so a pod cannot start with signing off. `RECOMMENDATION_CURSOR_ACCEPT_LEGACY: "false"` closes the unsigned legacy format. Genuinely enforced — but see §3 for what it is and is not worth |
| 15 | MySQL transport TLS (#280) | `MySqlConnectionSettings` refuses a `MYSQL_URL` without `sslMode=VERIFY_IDENTITY`, loopback exempt | **INERT** | **INERT** | `MYSQL_ENABLED: "false"` in all three renders, and the validation sits entirely behind that flag. No `mysql` Service or StatefulSet exists in `k8s/` at all. There is also a documented residual bypass: the guard does not percent-decode parameter names, so `?sslMode=VERIFY_IDENTITY&%73slMode=DISABLED` passes the guard and resolves to `DISABLED` in Connector/J |
| 16 | NetworkPolicy ingress lockdown | Restricts 6010/7010/8080 to the `recsys-api-gateway` pod selector (+ Prometheus) | Present | Present | **Enforcement is CNI-dependent and nothing in this repo establishes it.** There is no IaC of any kind — no Terraform, CDK, eksctl config, no Calico/Cilium install, no `ENABLE_NETWORK_POLICY`. EKS's default VPC CNI does not enforce NetworkPolicy unless explicitly enabled. Also: the gateway's own ingress rule on 8010 has no `from`, so any pod in any namespace may reach it |
| 17 | NetworkPolicy egress (#273, #283) | Denies unlisted egress on the four serving workloads; DNS scoped to `kube-system` | Present | Present, plus a `10.0.0.0/16` ElastiCache `ipBlock` marked `REPLACE_ME` | Same CNI dependency. Gaps that survive: there is **no default-deny**, the `recsys-outbox-reconciliation` CronJob is selected by no policy at all (unrestricted both directions), and `recsys-outbox-relay`, `redis` and `redis-sentinel` declare `policyTypes: [Ingress]` only. The relay's unrestricted egress is *pinned by a test as intentional* |
| 18 | CDN viewer-request normalization (#282) | Whitelists four URIs and their parameters, rejects multi-value and any `%`-containing value with 400, canonicalizes parameter order | **N/A** | **N/A** | `scripts/cdn/normalize-catalog-query.js` is deployed only by the manual `scripts/create-cdn-distribution.sh`; no workflow invokes it and the repo states no distribution exists in the account. Neither base nor the overlays reference CloudFront |
| 19 | CDN WAF WebACL | ALB Ingress annotation attaching a regional WebACL | **N/A** — base has no Ingress | Present but placeholder: `...:123456789012:regional/webacl/recsys-api-gateway/REPLACE_ME` | No WebACL rules exist anywhere in the repo. The repo's comments claim the ALB Controller rejects an invalid ARN at apply time; nothing here verifies that claim, and if it is wrong the failure mode is an internet-facing ALB with no WAF |
| 20 | Splunk log shipping | Ships structured JSON log events to HEC; bounded, drop-on-full, at-most-once | **OFF** | **OFF** | `SPLUNK_HEC_TOKEN` is an `optional: true` `secretKeyRef` on `recsys-splunk`, which is not in the repo, and no `splunk` Service is deployed either. Unset ⇒ the appender installs and starts but allocates no queue and no drain thread. Because delivery is at-most-once by design, a Splunk search is a lower bound on what was logged even when it *is* on |

Two further controls are worth naming because a reader may assume they are load-bearing:

- **Submit-token CSRF** (`RECSYS_SUBMIT_TOKEN_ENABLED`) defaults `false` and is set in no
  manifest. Even enabled it is not an authorization control — the token is obtainable
  anonymously from `GET /api/v1/token`.
- **Log PII redaction does not exist.** Nothing masks `userId` in log lines. The three
  redaction helpers in the tree scrub HEC tokens from error text, JDBC credentials from a
  URL, and feature-map keys in the Kafka path — none touches log events, and the Splunk
  serializer copies every MDC entry verbatim. Concrete userIds do reach log lines in the
  A/B exposure path.

The two EKS overlays **do not differ on any control here** — `k8s/eks-us-west-2` differs only
in region and HPA minimums, and both compose the same `k8s/eks-shared`.

### 13.3 Enforced, inert, and who could reach what

**Genuinely enforced.** Redis *client* authentication (`requireAuthentication` throws unless a
password is present or `REDIS_ALLOW_NO_AUTH` is explicitly set, which no manifest sets) —
but it protects the client's posture, not the server's, and the Flink and Spark jobs are
separate clients: they can now authenticate via `StreamingRedisUri`, yet **nothing in this
repository submits either job**, so no credential is configured anywhere here and neither
call site is compiled by the default build. Cursor signing (non-optional `secretKeyRef` on
all three serving workloads, key under 32 bytes rejected, so no configuration runs with it
off) — though it is a pagination-integrity control, not a cross-user one: the cursor is bound
to a `userId` the client already supplies, so what signing prevents is arbitrary keyset
repositioning and replay, not reading another user's data. Credential stripping
(unconditional). Operator-token fail-closure (denies everyone with the token unset, which is
correct and also means the tier cannot be used).

**Inert:** `REDIS_TLS` (explicitly `"false"` in both overlays). Redis ACL users in EKS (an
argument to StatefulSets scaled to zero). User-scope authorization (§10 — no Cognito issuer
in any render). PostHog pseudonymization (§12 — flag off, provider never constructed, egress
not permitted). MySQL TLS (`MYSQL_ENABLED=false`). Origin secret (§4 — Secret absent, so the
decorator is never registered). Splunk. CDN function and WAF. Gateway authentication *in
base*.

**Concretely, who could reach what:**

- **`k8s/base` applied** — the gateway is `ClusterIP`, so nothing on the internet reaches it.
  Whoever can (another pod, or `kubectl port-forward`) hits a gateway that authenticates
  nobody: every proxied route including `/api/catalog/user` and `/api/users/**` with an
  arbitrary `userId`, plus the unauthenticated `/metrics`. Only the four `OPERATOR` routes
  refuse, and they refuse everyone. Redis traffic is plaintext.
- **Either EKS overlay applied** — which its own comments say must not happen until
  ElastiCache has transit encryption: an API key is required, and an API-key caller is
  `Tier.SERVICE` and unrestricted, so **any valid API key can read any user's data by naming
  their `userId`**.
- **From inside the cluster, in any configuration** — the backends authenticate nobody. 6010,
  7010 and 8080 apply no authentication to serving routes, validate no header the gateway
  sets, and Spring Security is not on the classpath. `/setembedding` on 6010 — which the
  gateway classifies `OPERATOR` — is open to any pod that can reach the port. Only the
  NetworkPolicy separates them, and its enforcement is unestablished (§13.5).

### 13.4 Conformance tests that pass green while the control is off

Every gating security test here is a **file-shape test**. The PR gate runs `-Presilience` and
strips `@Tag("docker")` regardless of includes, so every test exercising a real Redis, MySQL,
Splunk or CDN is structurally incapable of blocking a merge.

- **`MySqlTlsManifestTest`** asserts `sslMode=VERIFY_IDENTITY` in `k8s/base` and never reads
  `MYSQL_ENABLED`, which is `"false"` — green, control unreachable. Its own javadoc notes the
  parser is a hand-copy of the production tokenizer, so a flaw in the rule is invisible to both.
- **`OperatorTokenManifestTest`** requires `optional: true` deliberately, so it asserts the
  *reference* and never the Secret. No `kind: Secret` exists under `k8s/`.
- **`RedisAclManifestTest`** asserts the template's passwords are *placeholders* — i.e. that
  the real credential is absent.
- **`RedisAuthManifestTest`** asserts `--requirepass` on StatefulSets the overlays scale to zero.
- **`NetworkPolicyEgressManifestTest`** is thorough about rule shape and cannot assert
  enforcement; its own messages say "under an enforcing CNI".
- **`SplunkLogbackWiringTest`** contains an assertion that *requires the control to be off* —
  it `assumeTrue`s the token is unset, then asserts the counters are zero.
- **`CdnQueryNormalizationConformanceTest`** compares two committed files to each other.
- **`GatewayOriginSecretTest`** is not in the `resilience` allow-list at all.

`GatewayExposureManifestTest` now pins the pairing behind §13.1's finding, with a stated
limit: **it reads files, it does not render a kustomization**, so the overlay half is a
coupling between texts. Both of that week's overlay defects reached `main` through exactly
that hole — an exposure check that only recognized the Service-type NLB annotation and missed
the ALB-Ingress annotation the overlays actually use, and a per-directory scan that could not
see `GATEWAY_ALLOW_ANONYMOUS: "false"` because it lives only in the `eks-shared` component
pulled in via `components:`.

### 13.5 What a reader should not assume, and what could not be established

Nine inferences the PR list invites and the manifests refuse: Redis transport auth shipping
does **not** mean traffic is encrypted; ACL users shipping does **not** mean least privilege
is in force where it matters; gateway fail-closed authentication is a property of the code,
not of `k8s/base`; user-scope authorization shipping does **not** mean user data is scoped
(nothing constrains which `userId` a caller names); the operator token "enforced" currently
means every operator route rejects every caller; NetworkPolicy conformance proves rule shape,
not enforcement — **and the entire argument for why the backends need no authentication of
their own rests on that unverified assumption**; the CDN controls describe a distribution
that does not exist, behind a WAF ARN that is the literal string `REPLACE_ME`; a green PR gate
says nothing about any of it; and Splunk being shipped does not mean there is an audit trail —
delivery is at-most-once and nothing redacts userIds.

Not establishable from this repository: whether any target cluster's CNI enforces
NetworkPolicy (no IaC of any kind exists — the single assumption the largest number of other
claims depend on); whether the required out-of-band Secrets exist (`recsys-secrets`,
`recsys-gateway-auth`, `recsys-online-admin`, `recsys-gateway-origin-secret`, `recsys-splunk`)
— note the asymmetry, an absent `recsys-gateway-auth` blocks the EKS gateway from starting
while an absent `recsys-online-admin` silently disables the operator tier; whether ElastiCache
has transit encryption, an AUTH token or RBAC groups; whether the ALB Controller really
rejects a nonexistent WAF ARN; how the Flink and Spark jobs are deployed, and therefore
whether anyone passes them the credentials they can now accept; and whether the CloudFront
viewer-request function receives raw or normalized percent encoding.
