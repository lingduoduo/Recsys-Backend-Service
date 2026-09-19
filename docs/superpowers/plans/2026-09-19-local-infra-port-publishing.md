# Local Infrastructure Port Publishing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish the five `docker-compose.streaming.yml` container ports that currently have no host mapping, so a host-run process can reach ZooKeeper, the Redis replica, and all three Redis Sentinels.

**Architecture:** Purely additive compose configuration — a `ports:` block on five services, plus the README table rows and one caveat paragraph that describe them. No environment variables change, no service default is re-routed, and `docker/redis/sentinel.conf` is untouched. Container-to-container traffic keeps using the compose bridge network exactly as it does today; a published port is an additional ingress, not a redirection.

**Tech Stack:** Docker Compose v2, Redis 7 (`redis-server`, `redis-sentinel`), Confluent ZooKeeper 7.6.1, Markdown, JUnit 5 + AssertJ (one existing test parses the compose file).

**Spec:** `docs/superpowers/specs/2026-09-19-local-infra-port-publishing-design.md`

## Global Constraints

- Host port assignments are exact and non-negotiable: `zookeeper` → `2181:2181`, `redis-replica` → `6380:6379`, `redis-sentinel-1` → `26379:26379`, `redis-sentinel-2` → `26380:26379`, `redis-sentinel-3` → `26381:26379`.
- Sentinel 1 **must** get host port 26379, because that is the value `REDIS_SENTINEL_NODES` defaults to (`CONFIG_GUIDE.md`). Do not renumber.
- Do not modify `docker/redis/sentinel.conf`. Do not add `announce-ip`, `announce-hostnames`, or `resolve-hostnames`. Making `REDIS_MODE=sentinel` work from the host is an explicit non-goal of the spec.
- Do not modify `scripts/run-microservices-local.sh` or any service default. Local runs stay on `REDIS_MODE=standalone` against `localhost:6379`.
- Do not publish the Flink `taskmanager`. It has no host-facing endpoint; the JobManager dashboard on 8081 is already published.
- `ports:` entries are quoted strings in this file (`- "9092:9092"`). Match that style — an unquoted `6380:6379` is still valid YAML here, but the file is uniformly quoted and consistency is cheap.
- Docker must be running before Task 2. On this machine that is Colima: `colima status`, and `colima start` if it is not running.
- Requires JDK 17 for the Maven step: `JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn ...`.

---

### Task 1: Publish the five ports in the compose file

**Files:**
- Modify: `docker-compose.streaming.yml` (services `zookeeper`, `redis-replica`, `redis-sentinel-1`, `redis-sentinel-2`, `redis-sentinel-3`)

**Interfaces:**
- Consumes: nothing.
- Produces: host ports `2181`, `6380`, `26379`, `26380`, `26381`. Task 2 verifies these against live containers; Task 3 documents them in README.

- [ ] **Step 1: Confirm the starting state — these five services have no `ports:` block**

Run:

```bash
grep -n "^  [a-z-]*:\|ports:" docker-compose.streaming.yml
```

Expected: `ports:` appears exactly three times, under `kafka`, `redis-primary`, and `jobmanager`. The five services named above have none. If `ports:` already appears under any of them, stop — the file has diverged from the spec and the plan needs revisiting.

- [ ] **Step 2: Add `ports:` to `zookeeper`**

Insert immediately after the `image:` line, before `environment:`:

```yaml
  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.1
    ports:
      - "2181:2181"
    environment:
```

- [ ] **Step 3: Add `ports:` to `redis-replica`**

6379 belongs to `redis-primary`, so the replica takes 6380 on the host and keeps 6379 in the container. Insert after `container_name:`, before the `# See the primary above` comment:

```yaml
  redis-replica:
    image: redis:7-alpine
    container_name: redis-replica
    # 6379 on the host is the primary's; the replica takes the conventional second Redis port.
    ports:
      - "6380:6379"
    # See the primary above: eviction is confined to TTL-bearing keys.
```

- [ ] **Step 4: Add `ports:` to each of the three Sentinels**

All three listen on 26379 inside their own container; they differ only in host port. Insert after `container_name:`, before `volumes:`, in each service.

`redis-sentinel-1` — this one carries the `REDIS_SENTINEL_NODES` default, so it gets 26379:

```yaml
  redis-sentinel-1:
    image: redis:7-alpine
    container_name: redis-sentinel-1
    # 26379 is the REDIS_SENTINEL_NODES default, so sentinel 1 holds it.
    ports:
      - "26379:26379"
    volumes:
```

`redis-sentinel-2`:

```yaml
  redis-sentinel-2:
    image: redis:7-alpine
    container_name: redis-sentinel-2
    ports:
      - "26380:26379"
    volumes:
```

`redis-sentinel-3`:

```yaml
  redis-sentinel-3:
    image: redis:7-alpine
    container_name: redis-sentinel-3
    ports:
      - "26381:26379"
    volumes:
```

- [ ] **Step 5: Verify the file still parses and the mappings are what you intended**

Run:

```bash
docker compose -f docker-compose.streaming.yml config | grep -A2 "published:"
```

Expected: eight published ports — the three that already existed (9092, 6379, 8081) plus 2181, 6380, 26379, 26380, 26381. A YAML error here means an indentation slip in one of the insertions.

- [ ] **Step 6: Confirm the eviction-policy test still passes**

`RedisEvictionPolicyManifestTest` reads this file line by line and rejects any `allkeys-` setting. It is the only test that parses the compose file, and it must stay green.

Run:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=RedisEvictionPolicyManifestTest
```

Expected: `BUILD SUCCESS`, 2 tests run, 0 failures.

- [ ] **Step 7: Commit**

```bash
git add docker-compose.streaming.yml
git commit -m "fix(compose): publish the five unmapped local infrastructure ports

ZooKeeper, the Redis replica and all three Sentinels ran with no host
port, so no host-run process could reach them. REDIS_SENTINEL_NODES
defaults to localhost:26379 - a port nothing listened on.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Verify against live containers

**Files:**
- No source changes. This task produces evidence, and it is the task that decides whether the spec's Sentinel caveat is stated correctly.

**Interfaces:**
- Consumes: the five host ports published by Task 1.
- Produces: the measured Sentinel master address, which Task 3 describes in README. Do not write that paragraph from memory — write it from what Step 6 below actually prints.

- [ ] **Step 1: Make sure Docker is running**

Run:

```bash
docker version --format 'server {{.Server.Version}}'
```

Expected: a version string. If it fails with a socket error, run `colima start` and retry.

- [ ] **Step 2: Bring the stack up and wait for health**

Run:

```bash
docker compose -f docker-compose.streaming.yml up -d
docker compose -f docker-compose.streaming.yml ps
```

Expected: all nine containers `running`, and the `PORTS` column shows the five new mappings. `kafka` and `jobmanager` take up to 30 s to report healthy.

- [ ] **Step 3: Verify ZooKeeper answers on 2181**

Run:

```bash
echo srvr | nc localhost 2181
```

Expected: a ZooKeeper version banner and `Mode: standalone`. (If `nc` exits immediately with nothing, the container is up but not yet serving — retry after a few seconds.)

- [ ] **Step 4: Verify the replica answers on 6380 and is actually replicating**

A reachable port is not the same as a working replica; ask it for its role.

Run:

```bash
docker run --rm --network host redis:7-alpine redis-cli -p 6380 info replication | head -5
```

Expected: `role:slave`, `master_host:redis-primary`, `master_link_status:up`.

(Use the containerized `redis-cli` if the host has no `redis-cli` installed; `redis-cli -p 6380 info replication` is equivalent if it does.)

- [ ] **Step 5: Verify all three Sentinels answer and see each other**

Run:

```bash
for p in 26379 26380 26381; do
  echo "--- sentinel on $p ---"
  docker run --rm --network host redis:7-alpine redis-cli -p "$p" sentinel master mymaster \
    | paste - - | grep -E 'name|num-other-sentinels|num-slaves|flags'
done
```

Expected, from each of the three: `name mymaster`, `flags master`, `num-slaves 1`, `num-other-sentinels 2`. A `num-other-sentinels` below 2 means the quorum has not converged yet — wait ~10 s and retry before treating it as a failure.

- [ ] **Step 6: Measure the master address Sentinel hands back — this is the spec's one falsifiable claim**

The spec asserts that publishing 26379 makes Sentinel *reachable* but not *usable for discovery*, because the address it returns is a Docker bridge IP the host cannot route to. Confirm that rather than assuming it.

Run:

```bash
docker run --rm --network host redis:7-alpine redis-cli -p 26379 sentinel get-master-addr-by-name mymaster
```

Expected: two lines — an address and `6379`. Record the address literally.

- If it is a bridge IP (`172.x.x.x` or similar): the spec's caveat is correct. Carry that exact observation into Task 3.
- If it is `127.0.0.1` or a host-routable address: **the spec is wrong.** Stop, do not write the caveat paragraph, and report the finding — the design's "Non-goals" and "The Sentinel caveat" sections both need revising, and `REDIS_MODE=sentinel` may work locally after all.

- [ ] **Step 7: Leave the stack running or tear it down**

The quick start only needs `redis-primary`. To stop the rest while keeping volumes:

```bash
docker compose -f docker-compose.streaming.yml down
```

- [ ] **Step 8: No commit**

This task changes no files. Its output is the measured address from Step 6, which Task 3 consumes.

---

### Task 3: Update the README infrastructure table and record the caveat

**Files:**
- Modify: `README.md:117-126` (the "Infrastructure — run in Docker" table and the paragraph that follows it)

**Interfaces:**
- Consumes: the five host ports from Task 1 and the measured master address from Task 2 Step 6.
- Produces: nothing downstream. This is the last task.

- [ ] **Step 1: Replace the three `Not published` cells**

The table currently ends:

```markdown
| ZooKeeper | Not published | 2181 | No |
| Redis replica | Not published | 6379 | No |
| Redis Sentinels (three containers) | Not published | 26379 each | No |
```

Replace those three rows with:

```markdown
| ZooKeeper | 2181 | 2181 | No |
| Redis replica | 6380 | 6379 | No |
| Redis Sentinel 1 | 26379 | 26379 | No |
| Redis Sentinel 2 | 26380 | 26379 | No |
| Redis Sentinel 3 | 26381 | 26379 | No |

Every container in the streaming stack now publishes a host port, so
`redis-cli`, `nc`, and any host-run process can reach it directly.
```

The three Sentinels become three rows because they no longer share a host port — collapsing them would hide which one is 26379, and 26379 is the one that matters.

- [ ] **Step 2: Add the Sentinel caveat immediately after that paragraph**

Write this from the address Task 2 Step 6 actually printed. The wording below assumes it was a bridge IP, as the spec predicts:

```markdown
Publishing 26379 makes Sentinel reachable, not usable for discovery.
`redis-cli -p 26379 sentinel masters` works from the host, but
`sentinel.conf` monitors the primary as `redis-primary`, so the address
Sentinel hands back is that container's Docker-bridge IP — which the host
cannot route to. `REDIS_MODE=sentinel` therefore still fails from a
host-run service, at the second hop, after Sentinel answers. Local runs
stay on `REDIS_MODE=standalone` against `localhost:6379`.
```

- [ ] **Step 3: Check that nothing else in README still claims these ports are unpublished**

Run:

```bash
grep -n "Not published" README.md
```

Expected: no output.

- [ ] **Step 4: Confirm the documentation index test still passes**

`DocumentationIndexTest` asserts README's documentation map stays complete in both directions. This change adds no new doc under `docs/system_design/` or `docs/runbooks/` and `docs/superpowers/` is deliberately out of its scope, so it should be unaffected — confirm rather than assume.

Run:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=DocumentationIndexTest
```

Expected: `BUILD SUCCESS`, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add README.md
git commit -m "docs(readme): record the published infrastructure ports

Every streaming-stack container now publishes a host port. Also states
what publishing Sentinel does not buy: discovery still returns a
Docker-bridge address, so REDIS_MODE=sentinel does not work from the host.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>"
```

---

## Self-Review

**Spec coverage.** Every spec section maps to a task: the port-assignment table → Task 1 Steps 2-4; "The Sentinel caveat" → Task 2 Step 6 (measure) and Task 3 Step 2 (document); "Documentation" → Task 3; "Verification" → Task 2 plus Task 1 Step 6 and Task 3 Step 4. The spec's non-goals are restated as Global Constraints so an executor reading only one task cannot violate them.

**Placeholders.** None. Every step carries the literal YAML, command, or Markdown to apply.

**Consistency.** Host ports are written identically in the Global Constraints, Task 1, Task 2, and Task 3: 2181, 6380, 26379, 26380, 26381. The container port for all three Sentinels is 26379 in every place it appears.

**One deliberate branch.** Task 2 Step 6 can falsify the spec. It tells the executor to stop and report rather than write the caveat anyway — a plan that cannot be contradicted by measurement is not verifying anything.
