# Redis authentication

## Provisioning

`REDIS_PASSWORD` comes from the `redis-password` key of the `recsys-secrets` Secret, consumed by
the four serving workloads, the reconciliation CronJob, and the Redis and Sentinel StatefulSets
themselves. The outbox relay does not use Redis and is deliberately not given the credential.

```bash
kubectl -n recsys patch secret recsys-secrets \
  -p "{\"stringData\":{\"redis-password\":\"$(openssl rand -base64 32)\"}}"
```

`patch`, not the `create --dry-run=client -o yaml | apply -f -` idiom the other runbooks use.
`recsys-secrets` is shared — it also holds `MYSQL_PASSWORD`, `ONLINE_CONSISTENCY_TOKEN_SECRET`, and
the recommendation-cursor signing keys — and `apply` replaces the Secret's whole data map with what
the applied document contains. A `create --from-literal=redis-password=...` document contains only
that one key, so applying it deletes the others. The single-key Secrets those other runbooks manage
(`recsys-gateway-origin-secret`, `recsys-online-admin`) have nothing to lose that way; this one does.

The password must avoid `|`, `&`, `\` and `$(`. The Sentinel config is produced by
`sed "s|__REDIS_PASSWORD__|$REDIS_PASSWORD|"`, so `|` closes the substitution early, `&` expands to
the whole match, and `\` starts an escape — each yields a Sentinel that authenticates with something
other than the password you set. `$(` is unsafe for two separate reasons: every command here embeds
the password in a double-quoted shell string, where `$(...)` is command substitution your shell runs
before `kubectl` ever sees it; and the primary and replica take theirs through Kubernetes `$(VAR)`
substitution in `args`, so a password written in that same syntax is unreadable to anyone auditing
those args against the Secret. `openssl rand -base64` emits only `+`, `/` and `=`, so the generator
above is safe — the constraint bites when someone substitutes a hand-chosen password.

The secret reference is `optional: true`, so pods stay schedulable before it exists — but the
missing-Secret state is worse than "nothing runs", and it fails two different ways. The primary and
replica set `--requirepass`/`--masterauth` via Kubernetes' `args`-level `$(REDIS_PASSWORD)`
substitution, which `optional: true` does not touch: an unresolvable reference is left as the
*literal string* `$(REDIS_PASSWORD)`, so both run reachable with a real, manifest-visible password.
Sentinel instead gets its password from an init container's `sed` substitution inside `sh -c` —
ordinary shell parameter expansion, not Kubernetes substitution — so an absent env var renders the
malformed line `sentinel auth-pass mymaster ` with no argument. All five client workloads,
meanwhile, have no `REDIS_PASSWORD` at all and crash-loop on the startup guard.

## Local development

`scripts/run-microservices-local.sh` exports `REDIS_ALLOW_NO_AUTH=true`, and Surefire sets it for
the test suite. Set it yourself if you run a service by hand against a passwordless Redis:

```bash
export REDIS_ALLOW_NO_AUTH=true
```

## ACL users

`k8s/base/redis-users.acl.template` is the source of the ACL file the primary and replica load.
It defines six users, one line each: `default` and five per-workload users — `catalog`, `model`,
`online`, `gateway`, `reconciliation`. Each line has the form
`user <name> on ><placeholder> <rules...>`; the operator substitutes each of the six
`__*_PASSWORD__` placeholders (`__REDIS_PASSWORD__`, `__CATALOG_PASSWORD__`, `__MODEL_PASSWORD__`,
`__ONLINE_PASSWORD__`, `__GATEWAY_PASSWORD__`, `__RECONCILIATION_PASSWORD__`) with a distinct
generated password — same character constraints as `REDIS_PASSWORD` above — and stores the
rendered result as the `redis-users.acl` key of `recsys-secrets`, alongside the existing
`redis-password` key. `__REDIS_PASSWORD__` must be rendered with the *same* value already stored
under `redis-password`: `--requirepass`, the replica's `--masterauth`, and Sentinel's
`auth-pass` all still read that one key, and they must agree with what the ACL file's own
`user default` line grants. The primary and replica StatefulSets mount the rendered result
read-only at `/etc/redis/users.acl` via `--aclfile /etc/redis/users.acl`; a read-only mount is
fine because nothing calls `ACL SAVE`. ACLs are per-node state, not replicated, so both
StatefulSets need the mount — a replica missing it rejects every authenticated read, which is the
normal AZ-aware read path in this system, not a rare failure mode.

The template carries no comments. Redis aborts startup on any non-blank ACL-file line that does
not begin with `user`, so the usual practice of documenting rationale inline is unavailable here;
that reasoning lives in this runbook and beside the `--aclfile` argument in `redis-cluster.yaml`
instead.

**`user default on >__REDIS_PASSWORD__ ~* &* +@all` must never be dropped from the file.** It is
tempting to assume an ACL file that says nothing about `default` leaves `requirepass` governing
it, the way an absent option normally falls back to a default. Measured against a real
`redis-server` on this branch, that assumption is false: `ACLLoadFromFile` *rebuilds* a missing
default user as `on nopass ~* &* +@all`, which silently re-enables unauthenticated access
(`ACL LIST` shows `nopass`, the log carries `# WARNING: Redis does not require authentication`, and
an unauthenticated `redis-cli ping` returns `PONG`), breaks replication
(`Unable to AUTH to MASTER … without any password configured for the default user`), and breaks
the Flink job — while both the readiness and liveness `redis-cli ping` probes stay green
throughout, since neither probe authenticates. This is the single worst failure mode an edit to
the template can introduce, and it produces no error anywhere in the deploy path.

Because `default` keeps `+@all`, the `redis-password` credential — the one the streaming jobs and
every other non-ACL-aware caller uses — can still run `FLUSHALL` against the whole instance. That is
deliberate, not an oversight: the Flink job writes `u2vEmb:*` and `topk:*` as `default`, and nothing
in this repository deploys it to exercise the risk. But it means the least-privilege boundary below
covers the five service users, not the instance as a whole — `default` remains a full-access
credential. See [Streaming jobs](#streaming-jobs) for what giving those two their own user would
take.

The five service users are each `-@all +@read +@write +@connection -@dangerous`, plus
`+@scripting` for `catalog`, `model`, and `online` (their trending reads, sharded record writes,
topology publish, rate limiting, and submit-token consume all run as Lua `EVAL`, which
`-@dangerous` does not strip and which is covered by none of `@read`/`@write`/`@connection`).
`topk:*` is granted with `~` (full read-write) rather than `%R~` (read-only) for those same three
users, even though their access to it is logically read-only: Redis requires full read-write
permission on every key an `EVAL` script touches, even a script whose shebang declares
`#!lua flags=no-writes`, and `ShardedTopKStore` passes `topk:` keys through `EVAL`. Granting
`%R~` there fails every trending read with `NOPERM`.

All of the above — the default-rebuild behavior, the comment restriction, and the `@scripting`/
`topk:` requirements — was measured against `redis-server 8.6.2` on this branch. The manifests
pin `redis:7-alpine`, and no Docker daemon was available on these machines to exercise Redis 7
itself.

Two limits worth stating plainly. The EKS overlays do not use this Redis at all — they point every
client at ElastiCache and scale the in-cluster StatefulSet to zero, and ElastiCache authorizes
access through RBAC user groups managed via the AWS API, a wholly different mechanism that none of
this ACL file touches. And nothing here is enforced anywhere today: no EKS cluster exists in
either region, so this is manifest correctness plus a merge-blocking conformance test
(`RedisAclManifestTest`), not a running guarantee.

## Rotation

Five ACL users means five distinct credentials to rotate, plus `default`'s `redis-password`. An
ACL `user` line accepts several passwords at once, so rotating one *service* user is an overlap
window rather than a coordinated restart:

1. Add the new password to that user's line in the rendered ACL file — e.g.
   `user catalog on >OLD_CATALOG_PASSWORD >NEW_CATALOG_PASSWORD ...` — so the user authenticates
   with either for the duration of the rotation.
2. Re-render the `redis-users.acl` key of `recsys-secrets` with that line and apply it.
3. Restart Redis (primary and replica) so the new aclfile content is loaded — the file is read at
   startup, not watched.
4. Roll that one service's Deployment onto the new password.
5. Once it is confirmed running on the new password, drop the old password from the user's line,
   re-render the Secret again, and restart Redis a second time to retire it.

`default` does not get this treatment. Its password is the single `redis-password` value, and
that same value also drives `--requirepass`, the replica's `--masterauth`, and Sentinel's
`auth-pass` — none of which support a second, overlapping password the way an ACL user line does.
`default` therefore still rotates the old way, by coordinated restart, exactly as described
below. One difference from before the ACL file existed: `redis-password` now backs two Secret
keys, not one — patching `redis-password` alone changes `--requirepass`/`--masterauth`/
`auth-pass` but leaves the ACL file's `user default` line on the old password, so step 1 below
must also re-render `redis-users.acl`'s `user default` line to the same new value before the
restart in step 2, or `default` ends up with two disagreeing passwords across the two keys.

Expect Redis to be unreachable for the duration. Everything degrades to its no-Redis path, which is
not a full outage but is a visible one.

> **Both regions.** `k8s/eks-us-west-2` is a warm standby dialing its *own* Redis with its own copy
> of the `redis-password` key, so the credential is per-region state. Rotating only us-east-1 leaves
> the standby holding a password its Redis no longer accepts, and nothing reports it until a
> failover — at which point every promoted pod crash-loops on a credential you rotated weeks
> earlier. Run every step below in **both** contexts before moving to the next step, the same
> two-context discipline as [cdn-operations.md](cdn-operations.md).

1. Update the Secret. Generate the value once and apply the *same* value in both contexts — the two
   regions must not drift apart, and a second `openssl rand` would give them different passwords:

   ```bash
   NEW_REDIS_PASSWORD="$(openssl rand -base64 32)"

   kubectl --context <us-east-1-ctx> -n recsys patch secret recsys-secrets \
     -p "{\"stringData\":{\"redis-password\":\"$NEW_REDIS_PASSWORD\"}}"
   kubectl --context <us-west-2-ctx> -n recsys patch secret recsys-secrets \
     -p "{\"stringData\":{\"redis-password\":\"$NEW_REDIS_PASSWORD\"}}"
   ```

2. Restart Redis and Sentinel first — clients cannot authenticate until the server has the new
   password:

   ```bash
   for ctx in <us-east-1-ctx> <us-west-2-ctx>; do
     kubectl --context "$ctx" -n recsys rollout restart statefulset/redis-primary
     kubectl --context "$ctx" -n recsys rollout restart statefulset/redis-replica
     kubectl --context "$ctx" -n recsys rollout restart statefulset/redis-sentinel
   done
   ```

   On an EKS overlay these StatefulSets are scaled to 0 and the step is a no-op: the server there is
   ElastiCache, and its side of the rotation is
   `aws elasticache modify-replication-group --auth-token "$NEW_REDIS_PASSWORD"
   --auth-token-update-strategy ROTATE` against that region's replication group, run before step 3.

3. Then the clients:

   ```bash
   for ctx in <us-east-1-ctx> <us-west-2-ctx>; do
     for d in recsys-api-gateway recsys-catalog-serving recsys-model-serving recsys-online-serving; do
       kubectl --context "$ctx" -n recsys rollout restart deployment/"$d"
       kubectl --context "$ctx" -n recsys rollout status deployment/"$d"
     done
   done
   ```

   The reconciliation CronJob picks the new value up on its next scheduled run.

4. Confirm the standby actually came back. A warm standby serves no traffic, so a failed rollout
   there is invisible in every dashboard that watches request rates:

   ```bash
   kubectl --context <us-west-2-ctx> -n recsys get pods
   ```

   Any pod in `CrashLoopBackOff` is the startup guard reporting that this region's Redis did not get
   the same password its clients did.

   **The two streaming jobs are not in this list and cannot be.** Nothing in this repository submits
   them, so a rotation here does not reach them — see [Streaming jobs](#streaming-jobs) below. If
   they are running, whoever submits them must be rotated in the same window, or they start failing
   `NOAUTH` against the keys the serving path reads.

## Streaming jobs

`OnlineFeatureStreamingJob` (Flink) and `ItemEmbeddingJob` (Spark) are built from this source but
submitted from outside it: there is no manifest, compose service, or submit script for either one
here. They do not go through `LettuceClientFactory`, so the startup guard above never applies to
them — a job with no credentials does not refuse to start, it connects and then fails `NOAUTH` on
the first command once `requirepass` is in force.

Both now accept credentials. They build their `RedisURI` through
`com.recsys.infrastructure.redis.StreamingRedisUri`, which delegates to the same
`LettuceClientFactory.standaloneUri` every service uses, so a job and a service authenticate
identically against the same server.

| Job | Parameters | Falls back to |
|---|---|---|
| `OnlineFeatureStreamingJob` | `--redis.username`, `--redis.password`, `--redis.tls` (beside the existing `--redis.host` / `--redis.port`) | `REDIS_USERNAME`, `REDIS_PASSWORD`, `REDIS_TLS` on the submitting process |
| `ItemEmbeddingJob` | `--redis-username=`, `--redis-password=`, `--redis-tls=` (beside `--redis-host=` / `--redis-port=`) | the same three variables |

A blank or absent username means legacy default-user `AUTH`; a non-blank one means a Redis 6+ ACL
login. Blank and absent are treated identically, because a parameter default and an unset
environment variable arrive differently. `--redis.tls` / `--redis-tls` must be `true` against a
server with encryption-in-transit — ElastiCache with an AUTH token always is.

Prefer the environment variables to the command-line form: a password passed as a job argument is
visible in `ps` on the submitting host and in the Flink Web UI's job configuration.

**Supplying the credentials is not something this repository can do.** Both jobs ship able to
authenticate; whether they *do* depends entirely on the submitting configuration, which lives
elsewhere. Until that is updated, deploying `requirepass` still breaks them — and
`OnlineFeatureStreamingJob` writes `u2vEmb:*` and `topk:*`, which every serving path reads, while
`ItemEmbeddingJob` writes `i2vEmb:*`.

### Why they authenticate as `default`

Both send whatever `REDIS_USERNAME` holds, and nothing sets it for them, so in practice they log in
as `default`. That is why the ACL work (#284) left `user default` at `~* &* +@all` rather than
narrowing it: a job authenticating as `default` needs write access to the key prefixes it owns, and
`default` is also the credential `--requirepass`, `--masterauth`, and Sentinel's `auth-pass` use.
The cost is that `default` remains a full-access credential — it can `FLUSHALL` the instance — so
the least-privilege boundary in [ACL users](#acl-users) covers the five service users, not the
instance as a whole.

Giving the jobs their own ACL user would mean: adding two `user` lines to
`k8s/base/redis-users.acl.template` (a Flink user granted `~u2vEmb:* ~topk:* ~feature:* ~lineage:*`
plus `+@scripting`, since the sinks write through `EVAL`; a Spark user granted `~i2vEmb:*`),
rendering their passwords into `recsys-secrets`, and setting `REDIS_USERNAME`/`REDIS_PASSWORD` on
whatever submits each job. The last step is the blocker, and it is the same unknown as everything
else in this section: nothing here knows where these jobs run. Until that is known, narrowing
`default` would break them silently.

Neither call site is exercised by any build in this repository — `online/flink/` and
`training/embedding/` are excluded from the Maven compile, so `mvn package` and the `resilience`
gate never compile them. `StreamingRedisUri` and its tests are in the compiled tree and do gate;
the two lines that call it do not. They compile only under the opt-in `-Pstreaming-flink` and
`-Poffline-embedding` profiles, which CI does not run.

## ElastiCache

**Neither EKS overlay is applyable as it stands.** This is a prerequisite, not future work.
`k8s/eks-shared` scales the in-cluster Redis to 0, so on EKS every client dials ElastiCache — and
both overlays still carry a `<placeholder>` `REDIS_HOST` and `REDIS_TLS: "false"`. ElastiCache
cannot hold an AUTH token without encryption-in-transit, so with the guard now requiring a
password there is no combination of those settings that works:

| What you set | What you get |
|---|---|
| No `redis-password` key | Every pod `CrashLoopBackOff`s on the startup guard, with the guard's message naming `REDIS_PASSWORD` in the logs. |
| A password, cluster has no AUTH token | `ERR Client sent AUTH, but no password is set` on the first command. The pod starts, then fails every Redis call. |
| A password, cluster has an AUTH token | That cluster necessarily has transit encryption on, so `REDIS_TLS: "false"` fails the TLS handshake — a connection-level error, not an auth error, which is the one that gets misdiagnosed. |

Before applying either overlay, create the region's ElastiCache cluster **with encryption-in-transit
enabled and an AUTH token**, then land all three of these in the same change:

1. `REDIS_HOST` (and `REDIS_REPLICA_NODES` in us-east-1) set to the real endpoints.
2. `REDIS_TLS: "true"` in that overlay's `redis-elasticache-patch.yaml`.
3. The AUTH token in the `redis-password` key of `recsys-secrets`, in that region's context.

They are only valid as a set. Landing any one of them alone produces one of the three failures
above. Each region has its own cluster and its own token — see the both-regions note under
[Rotation](#rotation).

The client verifies the certificate against the JVM truststore, which already trusts the Amazon Root
CA that ElastiCache certificates chain to — there is no truststore to configure and no verification
bypass to enable.
