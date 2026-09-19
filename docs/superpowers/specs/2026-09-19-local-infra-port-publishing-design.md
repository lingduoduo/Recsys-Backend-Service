# Publishing the Local Infrastructure Ports

## Problem

`docker-compose.streaming.yml` runs nine containers. Four publish a host port; five do not.
README's "Infrastructure — run in Docker" table records the gap honestly — three rows read
`Not published` — but recording it is not the same as it being right.

| Container | Container port | Host port today |
|---|---:|---|
| `zookeeper` | 2181 | none |
| `redis-replica` | 6379 | none |
| `redis-sentinel-1` / `-2` / `-3` | 26379 each | none |

The application services run **on the host**, not in the compose network
(`scripts/run-microservices-local.sh` starts four JVMs). Anything without a published port is
therefore invisible to a developer and to every host-run process. Three consequences follow.

**`REDIS_SENTINEL_NODES` defaults to a port nothing listens on.** `LettuceClientFactory`
defaults the node list to `localhost:26379` (`CONFIG_GUIDE.md`), which is the shape of a
setting written for local use. The stack publishes no Sentinel, so the default has never been
reachable. A developer setting `REDIS_MODE=sentinel` locally gets a connection refusal against
a default that reads as if it were meant to work.

**Replication cannot be observed.** `redis-replica` is the only replica in the repo's local
topology and the only way to exercise `REDIS_REPLICA_NODES` (`host:port@az`) outside EKS.
Without a host port there is no way to ask it `INFO replication`, so "is replication actually
up?" is answerable only by `docker exec`, which is a different question — it bypasses the path
a service would take.

**Sentinel state cannot be inspected during a failover drill.** `docs/system_design/04_Replication.md`
documents Sentinel-driven re-election. Rehearsing that locally means watching the quorum agree,
which means talking to the Sentinels.

The fix is small and its scope should stay small: publish the five ports.

## Goals

- Every container in `docker-compose.streaming.yml` publishes the port a host process would
  need to reach it.
- README's infrastructure table has no `Not published` row, and its host ports match the file.
- The one thing publishing does **not** buy — a working `REDIS_MODE=sentinel` from the host —
  is written down where a reader meets it, not left to be discovered.

## Non-goals

- **Making `REDIS_MODE=sentinel` work for host-run services.** See "The Sentinel caveat" below:
  it needs changes to what Sentinel announces, not just a published port, and those changes
  make the topology less like the deployed one. Out of scope by decision, not oversight.
  Measured, not assumed — see "Discovered during implementation".
- **Changing what Sentinel announces.** `docker/redis/sentinel.conf` gains
  `sentinel resolve-hostnames yes` (see "Discovered during implementation"), which is what
  makes the Sentinels *start*. It does not change what they announce, and no `announce-ip` /
  `announce-hostnames` is added.
- **Changing any service's default configuration.** `scripts/run-microservices-local.sh` keeps
  pointing at standalone `localhost:6379`. Publishing a port adds a path in; it does not
  re-route anything.
- **Publishing the Flink TaskManager.** It has no useful host-facing endpoint — the JobManager
  dashboard on 8081 is the operator surface, and it is already published.
- **Authentication on the newly published ports.** These containers are passwordless today and
  stay that way; `REDIS_ALLOW_NO_AUTH=true` is already the documented local posture
  (`docs/runbooks/redis-auth.md`). Binding them to the host does widen exposure from "the
  Docker bridge" to "this machine's loopback and, depending on the Docker runtime's default
  bind address, potentially its LAN interfaces" — the same exposure `redis-primary:6379`,
  Kafka `9092` and the Flink dashboard `8081` already carry in this file. This is a
  development stack; it should not be run on an untrusted network, and nothing here changes
  that calculus.

## Design

### Port assignment

| Container | Host port | Container port | Why this host port |
|---|---:|---:|---|
| `zookeeper` | 2181 | 2181 | Free; the conventional ZooKeeper client port. |
| `redis-replica` | **6380** | 6379 | 6379 belongs to `redis-primary`. 6380 is the conventional "second Redis". |
| `redis-sentinel-1` | **26379** | 26379 | The `REDIS_SENTINEL_NODES` default. Sentinel 1 gets it so the default resolves to a real listener. |
| `redis-sentinel-2` | **26380** | 26379 | Sequential from 26379. |
| `redis-sentinel-3` | **26381** | 26379 | Sequential from 26379. |

Nothing else in the file changes. No new environment variables, no config rewrite, no change
to `docker/redis/sentinel.conf`. Container-to-container traffic continues over the compose
bridge network exactly as before — a published port is an additional ingress, not a
redirection.

### The Sentinel caveat

Publishing 26379 makes Sentinel *reachable* from the host. It does not make Sentinel
*discovery* work from the host, and the distinction matters enough to document.

`docker/redis/sentinel.conf` says `sentinel monitor mymaster redis-primary 6379`. Sentinel
resolves that hostname once and thereafter reports the resolved **address** to clients. Inside
the compose network that address is `redis-primary`'s bridge IP. A host client that runs
`SENTINEL get-master-addr-by-name mymaster` is handed that bridge address and tries to connect
to it directly — and on macOS, where the Docker bridge network is inside a VM, it is not
routable from the host.

So from the host:

- `redis-cli -p 26379 sentinel masters` — **works.** Inspection is the point of this change.
- `REDIS_MODE=sentinel` on a host-run service — **still fails**, at the second hop, after
  Sentinel answers.

Making the second case work needs Sentinel to announce a host-routable address
(`sentinel resolve-hostnames` / `announce-hostnames` plus a host `/etc/hosts` entry, or
`announce-ip`), which bends the local topology away from the deployed one for the benefit of a
mode local runs do not use. README states the limitation instead. Local runs stay on
`REDIS_MODE=standalone` against `localhost:6379`.

### Documentation

`README.md` — the three `Not published` cells become the host ports above, and a short
paragraph under the table records the Sentinel caveat in the two-line form given above
(inspection works; discovery does not; stay on standalone).

## Verification

Assertion-by-YAML-reading is not verification. Bring the stack up and ask each newly published
port a question only a live server can answer:

| Check | Expected |
|---|---|
| `docker compose -f docker-compose.streaming.yml ps` | five new host-port mappings |
| `redis-cli -p 6380 info replication` | `role:slave`, `master_link_status:up` |
| `redis-cli -p 26379 sentinel masters` (and 26380, 26381) | `mymaster`, `num-other-sentinels:2` |
| `echo srvr \| nc localhost 2181` | ZooKeeper version banner |
| `redis-cli -p 26379 sentinel get-master-addr-by-name mymaster` | a bridge IP — confirms the caveat rather than assuming it |
| `mvn test -Dtest=RedisEvictionPolicyManifestTest` | green (the one test that parses this file) |

The fifth row is deliberate. The caveat is the one claim in this document that could be wrong,
so it is measured, not reasoned about.

## Risks

**Host port collision.** 2181, 6380, 26379-26381 are all previously unused by this repo's three
compose files (`streaming`, `splunk` → 8000/8088, `cdn` → 8090). A developer with an unrelated
ZooKeeper or Redis on those ports now gets a start-time bind failure instead of a silently
working stack. That failure is loud and README's "Port conflicts" section already covers it.

**No test enforces the mapping.** `RedisEvictionPolicyManifestTest` parses this file but only
for the eviction policy, and no test asserts port publication. A future edit could drop a
`ports:` block and only the README would disagree. Judged not worth a conformance test: unlike
the eviction policy — a silent correctness invariant — an unpublished port fails immediately
and visibly the first time someone uses it.

## Discovered during implementation

Two things the design did not anticipate, both found by running the stack rather than reading
it. Recorded here because the second one changes the scope of the change.

### The Sentinel containers had never started

Bringing the stack up showed all three Sentinels exiting 1 immediately:

```
# Failed to resolve hostname 'redis-primary'
*** FATAL CONFIG FILE ERROR (Redis 7.4.11) ***
Reading the configuration file, at line 1
>>> 'sentinel monitor mymaster redis-primary 6379 2'
Can't resolve instance hostname.
```

This is pre-existing and independent of port publishing — it reproduces on the unmodified
file. It is *not* a DNS problem; resolution on the compose network is fine:

```console
$ docker run --rm --network recsys-backend-service_default redis:7-alpine \
    sh -c 'getent hosts redis-primary; redis-cli -h redis-primary ping'
172.20.0.3        redis-primary
PONG
```

The cause is Redis Sentinel's own `resolve-hostnames`, which defaults to `no`. With that
default a hostname in `sentinel monitor` is a fatal config error rather than a lookup, and
Compose offers a DNS name with no stable IP to hardcode in its place. So
`docker/redis/sentinel.conf` gains one line, `sentinel resolve-hostnames yes`, without which
the three newly published Sentinel ports would have no listener behind them and this change
would not deliver its goal.

After the fix all three come up healthy and form a quorum — `num-other-sentinels 2` from each
of 26379, 26380, and 26381.

### The caveat is confirmed, from the real host

The design predicted that Sentinel would hand back an unroutable bridge address. Measured from
macOS, outside the Docker VM:

```console
$ redis-cli -p 26379 sentinel get-master-addr-by-name mymaster
172.20.0.3
6379
$ redis-cli -t 4 -h 172.20.0.3 -p 6379 ping
Could not connect to Redis at 172.20.0.3:6379: Operation timed out
```

Sentinel answers; the address it answers with does not. The caveat stands as written.

### One check not exercised

The literal `2181:2181` binding was not exercised: host 2181 was held by an unrelated local
ZooKeeper throughout, and stopping someone else's container is not this change's business. The
ZooKeeper container was instead verified on a throwaway `2182:2181` override, which confirms
it serves 2181 and that a `ports:` mapping on it works:

```console
$ echo srvr | nc -w 4 127.0.0.1 2182 | head -1
Zookeeper version: 3.8.4-9316c2a7a97e1666d8f4593f34dd6fc36ecc436c, built on 2024-02-12 22:16 UTC
```

What remains unverified is one integer. A wrong one fails loudly at container start, which is
the same class of failure the "Risks" section already accepts for host-port collisions.
