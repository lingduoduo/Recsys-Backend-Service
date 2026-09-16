# Retrieval Service Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Merge the 174-file `com.demo.retrieval` drop-in into `ModelApplication` on port 8080 as `com.recsys.retrieval`, restoring a compiling repository and closing an unauthenticated model-reload endpoint.

**Architecture:** Three stacked PRs. PR1 restores the build with zero behavioural change (rename, dependencies, dead-code pruning) and ends in a *measurement* of the 61 inherited tests. PR2 wires the code into `ModelApplication`'s component scan, bridging Spring Data Redis onto the existing `recsys.redis` configuration so the `REDIS_ALLOW_NO_AUTH` guard keeps covering it. PR3 namespaces the HTTP surface under `/api/v1/retrieval` and classifies the two mutating routes `OPERATOR`.

**Tech Stack:** Java 17, Maven, Spring Boot 3.3.4, Spring Data Redis (new), Lettuce 6.3.2, Apache Avro (new), ONNX Runtime 1.18.0, JUnit 5, Mockito, Testcontainers.

**Spec:** `docs/superpowers/specs/2026-09-16-retrieval-service-consolidation-design.md`

## Global Constraints

- **JDK 17 required.** Every Maven command must be prefixed `JAVA_HOME=$(/usr/libexec/java_home -v 17)`. On JDK 25 a clean compile of `LlmResponseCache.java` and `RecommendationCache.java` fails for unrelated pre-existing reasons.
- **Branch:** all work lands on `feat/retrieval-service-consolidation`, stacked. Never commit to `main`; every PR is reviewed.
- **Never commit `.claude/CLAUDE.md`.** Notes that belong there go in the PR body instead.
- **No new numbered markdown.** Fold documentation into `09_API_Gateway.md`, `10_MicroServices.md`, `20_AuthN_AuthZ.md`.
- **Spring Boot version is `${spring-boot.version}` = 3.3.4.** New Spring dependencies must use that property, never a literal.
- **Lettuce is pinned at 6.3.2.RELEASE.** Adding `spring-boot-starter-data-redis` must not move it.
- **Commit message trailer:** every commit ends with
  `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`
- **Attribution in PR bodies:** `🤖 Generated with [Claude Code](https://claude.com/claude-code)`

---

# PR1 — Restore the build

`ModelApplication` is not edited in this PR, and `com.recsys.retrieval` is added to no
component scan. 8080 therefore cannot regress. This is what makes the large mechanical diff
safe to review quickly.

### Task 1: Rename `com.demo.retrieval` → `com.recsys.retrieval`

**Files:**
- Modify: all 174 files under `src/main/java/com/recsys/retrieval/` and `src/test/java/com/recsys/retrieval/`
- Modify: `src/main/java/com/recsys/retrieval/service/LocalEmbeddingLoader.java` (package declaration disagrees with its directory)

**Interfaces:**
- Consumes: nothing.
- Produces: the package root `com.recsys.retrieval`, which every later task imports from.

- [ ] **Step 1: Confirm the build is red, and why**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q compile -DskipTests 2>&1 | grep -c ERROR
```

Expected: a non-zero count. The errors name missing `org.springframework.data.redis.*` and
`org.apache.avro.*` symbols inside `com.demo.retrieval.*` classes. Record the count — Task 2
reduces it and Task 4 drives it to zero.

- [ ] **Step 2: Rewrite every package and import reference**

```bash
cd /Users/linghuang/Git/Recsys-Backend-Service
grep -rl 'com\.demo\.retrieval' src/main/java/com/recsys/retrieval src/test/java/com/recsys/retrieval \
  | xargs sed -i '' 's/com\.demo\.retrieval/com.recsys.retrieval/g'
```

This covers `package` declarations, `import` statements, and the one static import of
`com.demo.retrieval.service.audit.UserAuditClassifierTest.profileJson`.

- [ ] **Step 3: Verify no reference survives**

```bash
grep -rn 'com\.demo' src/ || echo "CLEAN"
```

Expected: `CLEAN`.

- [ ] **Step 4: Fix the one file whose package disagrees with its directory**

`LocalEmbeddingLoader.java` sits in `service/` but now declares
`package com.recsys.retrieval.kafka;`. It is the only such file; every other file is a clean
prefix rename.

```bash
sed -i '' 's/^package com\.recsys\.retrieval\.kafka;/package com.recsys.retrieval.service;/' \
  src/main/java/com/recsys/retrieval/service/LocalEmbeddingLoader.java
```

- [ ] **Step 5: Verify every package declaration now matches its directory**

```bash
for f in $(find src/main/java/com/recsys/retrieval src/test/java/com/recsys/retrieval -name '*.java'); do
  pkg=$(grep -m1 '^package ' "$f" | sed 's/package //;s/;//')
  dir=$(dirname "$f" | sed 's|src/main/java/||;s|src/test/java/||;s|/|.|g')
  [ "$pkg" = "$dir" ] || echo "MISMATCH $f ($pkg vs $dir)"
done; echo "checked"
```

Expected: no `MISMATCH` lines.

- [ ] **Step 6: Commit**

```bash
git add -A src/main/java/com/recsys/retrieval src/test/java/com/recsys/retrieval
git commit -m "$(cat <<'EOF'
refactor(retrieval): rename com.demo.retrieval to com.recsys.retrieval

The tree was copied in with its directory renamed but not its package, so
all 174 files declared com.demo.retrieval while living under
src/main/java/com/recsys/retrieval. LocalEmbeddingLoader was doubly wrong:
it declared ...retrieval.kafka while sitting in service/.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 2: Add the two missing dependencies

**Files:**
- Modify: `pom.xml` (dependency list)

**Interfaces:**
- Consumes: nothing.
- Produces: `org.springframework.data.redis.core.StringRedisTemplate` (used by 28 retrieval
  files and by `RetrievalRedisConfig` in Task 6); `org.apache.avro.Schema`,
  `GenericRecordBuilder`, `SchemaNormalization` (used by `RecsysEventAvroCodec`).

- [ ] **Step 1: Add both dependencies**

Insert alongside the other `spring-boot-starter-*` entries in `pom.xml`:

```xml
    <!-- The retrieval code reaches Redis through Spring Data's StringRedisTemplate.
         RedisAutoConfiguration is excluded in ModelApplication and the template is built
         by RetrievalRedisConfig from recsys.redis, so LettuceClientFactory's
         REDIS_ALLOW_NO_AUTH guard still covers these call sites. -->
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-data-redis</artifactId>
      <version>${spring-boot.version}</version>
    </dependency>
    <!-- RecsysEventAvroCodec encodes GRPO impression events against
         src/main/resources/schemas/recsys-event-v3.avsc. -->
    <dependency>
      <groupId>org.apache.avro</groupId>
      <artifactId>avro</artifactId>
      <version>1.11.3</version>
    </dependency>
```

- [ ] **Step 2: Verify Lettuce did not move off its pin**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q dependency:tree -Dincludes=io.lettuce 2>&1 | grep lettuce
```

Expected: `io.lettuce:lettuce-core:jar:6.3.2.RELEASE`. If the starter dragged in a different
version, add an explicit `<dependencyManagement>` pin rather than accepting the drift — the
existing raw-Lettuce code is built against 6.3.2.

- [ ] **Step 3: Confirm the error count dropped**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q compile -DskipTests 2>&1 | grep -c ERROR
```

Expected: strictly lower than Task 1 Step 1's count. It may not yet be zero; Task 3 handles
the remainder.

- [ ] **Step 4: Commit**

```bash
git add pom.xml
git commit -m "$(cat <<'EOF'
build: add spring-boot-starter-data-redis and avro

The retrieval code needs StringRedisTemplate in 28 files and Avro in
RecsysEventAvroCodec; neither was in the pom, which is one of the three
reasons the repository did not compile.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: Prune the unreachable Kafka consumer half

The retained Kafka path exists only to serve
`HybridRecommendationService.createGrpoSender`, which is gated by
`recsys.retrieval.grpo.emit-events` (default **false**). A reference-closure analysis over the
24-file `kafka/` package found the consumer side is reachable *only* through four unused public
helper methods on `KafkaUtils`. Removing those methods frees seven further classes.

The minimal producer closure is clean and verified: `KafkaProducer` → `KafkaProducerConfig`;
`KafkaConfig`, `SslConfig`, `WilyConfig` have no internal dependencies at all.

**Files:**
- Delete (11, nothing references them): `kafka/core/KafkaEventSerializer.java`,
  `kafka/core/KafkaTopics.java`, `kafka/core/Metrics.java`,
  `kafka/core/MovieLensDeserializer.java`, `kafka/core/MovieLensEventProcessor.java`,
  `kafka/event/MovieEvent.java`, `kafka/event/MovieInteractionEvent.java`,
  `kafka/event/MovieLensEvent.java`, `kafka/event/RatingEvent.java`,
  `kafka/event/RecSysEvent.java`, `kafka/event/UserEvent.java`
- Delete (7, freed by trimming `KafkaUtils`): `kafka/core/KafkaConsumer.java`,
  `kafka/config/KafkaConsumerConfig.java`, `kafka/core/MessageSource.java`,
  `kafka/core/LocalFileMessageSource.java`, `kafka/core/PartitionLag.java`,
  `kafka/event/KafkaMessage.java`, `kafka/config/Args.java`
- Modify: `kafka/core/KafkaUtils.java`
- Keep (6): `kafka/config/KafkaConfig.java`, `kafka/config/KafkaProducerConfig.java`,
  `kafka/config/SslConfig.java`, `kafka/config/WilyConfig.java`,
  `kafka/core/KafkaProducer.java`, `kafka/core/KafkaUtils.java`

**Interfaces:**
- Consumes: Task 1's renamed packages.
- Produces: `KafkaUtils.kafkaConfig(String dest, String topic, SslConfig ssl) -> KafkaConfig`
  and `KafkaUtils.producerConfig(KafkaConfig base) -> KafkaProducerConfig` — the only two
  methods `HybridRecommendationService` calls (lines 179-180).

- [ ] **Step 1: Trim `KafkaUtils` to the producer path**

Replace the body of `src/main/java/com/recsys/retrieval/kafka/core/KafkaUtils.java` with:

```java
package com.recsys.retrieval.kafka.core;

import com.recsys.retrieval.kafka.config.KafkaConfig;
import com.recsys.retrieval.kafka.config.KafkaProducerConfig;
import com.recsys.retrieval.kafka.config.SslConfig;
import com.recsys.retrieval.kafka.config.WilyConfig;

// Shared factory methods for Kafka producer config objects.
//
// The consumer-side helpers (consumerSsl, consumerConfig, createKafkaConsumer,
// localMessageSource, deserializeKafkaMessages) were removed when this tree was consolidated
// into model serving: nothing outside kafka/ ever called them, and they were the only thing
// keeping KafkaConsumer, KafkaConsumerConfig, MessageSource, LocalFileMessageSource,
// PartitionLag, KafkaMessage and Args reachable. Consuming in this system is
// infrastructure/messaging's job.
public final class KafkaUtils {

    private KafkaUtils() {}

    public static KafkaConfig kafkaConfig(String dest, String topic, SslConfig ssl) {
        return KafkaConfig.builder()
            .dest(dest)
            .topic(topic)
            .wilyConfig(WilyConfig.defaultConfig())
            .ssl(ssl)
            .build();
    }

    public static KafkaProducerConfig producerConfig(KafkaConfig base) {
        return KafkaProducerConfig.builder()
            .baseConfig(base)
            .build();
    }
}
```

- [ ] **Step 2: Delete the 18 unreachable files**

```bash
cd /Users/linghuang/Git/Recsys-Backend-Service/src/main/java/com/recsys/retrieval/kafka
git rm -f --ignore-unmatch \
  core/KafkaEventSerializer.java core/KafkaTopics.java core/Metrics.java \
  core/MovieLensDeserializer.java core/MovieLensEventProcessor.java \
  event/MovieEvent.java event/MovieInteractionEvent.java event/MovieLensEvent.java \
  event/RatingEvent.java event/RecSysEvent.java event/UserEvent.java \
  core/KafkaConsumer.java config/KafkaConsumerConfig.java core/MessageSource.java \
  core/LocalFileMessageSource.java core/PartitionLag.java event/KafkaMessage.java \
  config/Args.java 2>/dev/null
rm -f core/KafkaEventSerializer.java core/KafkaTopics.java core/Metrics.java \
  core/MovieLensDeserializer.java core/MovieLensEventProcessor.java \
  event/MovieEvent.java event/MovieInteractionEvent.java event/MovieLensEvent.java \
  event/RatingEvent.java event/RecSysEvent.java event/UserEvent.java \
  core/KafkaConsumer.java config/KafkaConsumerConfig.java core/MessageSource.java \
  core/LocalFileMessageSource.java core/PartitionLag.java event/KafkaMessage.java \
  config/Args.java
```

(The files are untracked at this point, so `git rm` may not match; the plain `rm` is the one
that does the work.)

- [ ] **Step 3: Verify the deletions broke nothing**

```bash
cd /Users/linghuang/Git/Recsys-Backend-Service
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q compile -DskipTests 2>&1 | grep ERROR | head -20 || echo "COMPILE CLEAN"
```

Expected: `COMPILE CLEAN`. If a deleted class is still referenced, the compiler names it — that
is the authoritative reachability oracle. Restore only the named file from
`git stash`/the original copy, and record why the closure analysis missed it.

- [ ] **Step 4: Confirm `LocalEmbeddingLoader` survived**

It lives in `service/`, not `kafka/`, despite its original package declaration. Nothing in
Step 2 should have touched it.

```bash
test -f src/main/java/com/recsys/retrieval/service/LocalEmbeddingLoader.java && echo "PRESENT"
```

Expected: `PRESENT`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(retrieval): drop the unreachable Kafka consumer half

kafka/ arrived as 24 files. Only the producer path is reachable, from
HybridRecommendationService.createGrpoSender behind a default-false flag.
The consumer side was held alive solely by four unused KafkaUtils helpers;
removing them frees seven more classes. Consuming is
infrastructure/messaging's role in this repo.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: Run the inherited suite and report — **STOP AND REPORT**

This task produces a measurement, not a code change. Its output decides whether PR2 proceeds
and what the CI-gating answer is.

**Files:** none modified.

- [ ] **Step 1: Run the full suite**

```bash
cd /Users/linghuang/Git/Recsys-Backend-Service
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test 2>&1 | tee /tmp/retrieval-suite.log | tail -40
```

`@Tag("load")` and `@Tag("docker")` are excluded by the `excludedGroups` default, so
`UserProfileIntegrationTest` will not run here — it is Testcontainers-based and documented in
its own header as unrunnable against Docker 25+ because docker-java pins API version 1.32
through every reachable configuration surface. Do not spend time trying to make it run; five
approaches were already tried and recorded on 2026-08-16.

- [ ] **Step 2: Extract the per-class result for the 61 inherited files**

```bash
grep -E "Tests run:.*(Failures: [1-9]|Errors: [1-9])" /tmp/retrieval-suite.log
grep -E "^\[ERROR\].*com\.recsys\.retrieval" /tmp/retrieval-suite.log | head -40
grep -E "Tests run:.*Time elapsed" /tmp/retrieval-suite.log | grep -c "recsys.retrieval"
```

- [ ] **Step 3: Separate pre-existing failures from new ones**

Two Spark-fixture errors are known pre-existing in the full suite and are **not** caused by
this work. `OutboxRelayTest` has a known timing flake. Confirm any failure outside
`com.recsys.retrieval` against those before attributing it to the merge.

- [ ] **Step 3a: Expect one specific existing test to fail, and confirm it does**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=BackendRouteCoverageTest
```

Expected: **FAIL** on `everyRouteRegistrationLivesWhereAScannerLooks`, naming the three
controllers under `src/main/java/com/recsys/retrieval/controller/`.

That test walks all of `src/main/java` and fails on any `@RestController` outside
`SPRING_SCAN_ROOT` (`src/main/java/com/recsys/api/rest`). It exists precisely so a controller in
a new package cannot ship unclassified by the gateway's route policy. The drop-in put three
controllers outside that root, so this fires as soon as the code compiles — regardless of
component scanning, and therefore already in PR1.

This is **not** an inherited-test failure. It is an existing repo test correctly reporting that
the drop-in introduced unclassified routes. Report it as such, distinctly from the 61. PR3
Task 11 is what fixes it.

If it does *not* fail, stop: the scanner's premise has changed and the classification work in
PR3 needs rethinking before it is written.

- [ ] **Step 4: Report and stop**

Report to the user: how many of the 61 inherited test files pass, fail, and error; the failure
reasons grouped by cause; and whether the pre-existing failures are the only non-retrieval ones.

**Do not patch failing inherited tests to green.** Per the spec, a substantial failure count is
evidence the drop-in is incomplete — missing config or missing fixtures — and the correct
response is to return to the user with the evidence. Await direction on (a) whether to proceed
to PR2 and (b) which tests to add to the `-Presilience` include-list.

- [ ] **Step 5: Open PR1**

```bash
git push -u origin feat/retrieval-service-consolidation
gh pr create --title "build(retrieval): restore the build after the retrieval drop-in" --body "$(cat <<'EOF'
The retrieval service was copied in untracked as `com.demo.retrieval` and left the repository
unable to compile. This PR restores the build and changes nothing about how the application
runs: `ModelApplication` is untouched and `com.recsys.retrieval` is added to no component scan.

- Renamed `com.demo.retrieval` → `com.recsys.retrieval` across 174 files, and fixed
  `LocalEmbeddingLoader`, whose package declaration named `kafka` while it sat in `service/`.
- Added `spring-boot-starter-data-redis` and `org.apache.avro:avro`. Verified Lettuce stays
  pinned at 6.3.2.RELEASE.
- Dropped 18 of 24 `kafka/` files. Only the producer path is reachable (behind a default-false
  flag); the consumer half was held alive solely by four unused `KafkaUtils` helpers.

Test results for the 61 inherited files are in the thread below. Wiring into 8080, the Redis
bridge and the route/authz work follow in stacked PRs.

Design: `docs/superpowers/specs/2026-09-16-retrieval-service-consolidation-design.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

# PR2 — Wire into `ModelApplication`

Branch from PR1's head. This PR is where 8080's behaviour changes.

### Task 5: Public authentication-guard entry point

`LettuceClientFactory.requireAuthentication(String, Map)` is package-private and
`com.recsys.config.RetrievalRedisConfig` cannot call it. Rather than move the config class into
the infrastructure package, expose a narrow public overload that takes the properties object.

**Files:**
- Modify: `src/main/java/com/recsys/infrastructure/redis/LettuceClientFactory.java`
- Test: `src/test/java/com/recsys/infrastructure/redis/LettuceClientFactoryAuthGuardTest.java` (create)

**Interfaces:**
- Consumes: existing package-private `requireAuthentication(String password, Map<String,String> env)`.
- Produces: `public static void LettuceClientFactory.requireAuthentication(RedisProperties props, Map<String,String> env)` — throws `IllegalStateException` when the password is blank and `REDIS_ALLOW_NO_AUTH` is not `true`. Task 6 calls it.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/recsys/infrastructure/redis/LettuceClientFactoryAuthGuardTest.java`:

```java
package com.recsys.infrastructure.redis;

import com.recsys.config.RedisProperties;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/**
 * The properties-taking overload exists so RetrievalRedisConfig, which lives in com.recsys.config,
 * can run the same guard the raw-Lettuce path runs. An env map is passed explicitly rather than
 * read from System.getenv() because Surefire sets REDIS_ALLOW_NO_AUTH=true for the whole suite,
 * which would make the refusal case unobservable.
 */
class LettuceClientFactoryAuthGuardTest {

    private static RedisProperties propsWithPassword(String password) {
        RedisProperties props = new RedisProperties();
        props.setPassword(password);
        return props;
    }

    @Test
    void refusesABlankPasswordWithoutAnExplicitOptIn() {
        assertThatThrownBy(() ->
                LettuceClientFactory.requireAuthentication(propsWithPassword(""), Map.of()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("REDIS_PASSWORD");
    }

    @Test
    void allowsABlankPasswordWhenTheOptInIsSet() {
        assertDoesNotThrow(() -> LettuceClientFactory.requireAuthentication(
            propsWithPassword(""), Map.of("REDIS_ALLOW_NO_AUTH", "true")));
    }

    @Test
    void allowsAConfiguredPassword() {
        assertDoesNotThrow(() -> LettuceClientFactory.requireAuthentication(
            propsWithPassword("s3cret"), Map.of()));
        assertThat(propsWithPassword("s3cret").getPassword()).isEqualTo("s3cret");
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=LettuceClientFactoryAuthGuardTest
```

Expected: compilation failure — `requireAuthentication(RedisProperties, Map)` does not exist.

- [ ] **Step 3: Add the overload**

In `LettuceClientFactory.java`, directly above the existing package-private
`requireAuthentication(String, Map)`:

```java
    /**
     * Public entry point for callers outside this package that build their own client from
     * {@link RedisProperties} — today, {@code RetrievalRedisConfig}, which hands Spring Data
     * Redis a connection factory. Without this, the Spring Data path would be the one way into
     * Redis that the credential guard does not cover.
     */
    public static void requireAuthentication(RedisProperties props, Map<String, String> env) {
        requireAuthentication(props.getPassword(), env);
    }
```

- [ ] **Step 4: Run it and watch it pass**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=LettuceClientFactoryAuthGuardTest
```

Expected: 3 tests, 0 failures.

- [ ] **Step 5: Prove the test can actually fail**

Temporarily change the overload body to `return;` and re-run. The first test must fail. Restore
the body. This step exists because a guard test that passes whether or not the guard runs is
worse than no test — it is the repo's standing lesson on conformance-test blind spots.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/recsys/infrastructure/redis/LettuceClientFactory.java \
        src/test/java/com/recsys/infrastructure/redis/LettuceClientFactoryAuthGuardTest.java
git commit -m "$(cat <<'EOF'
feat(redis): expose the credential guard to properties-based callers

RetrievalRedisConfig lives in com.recsys.config and cannot reach the
package-private guard. Without a public entry point the Spring Data Redis
path would be the one way into Redis that REDIS_ALLOW_NO_AUTH does not
cover.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: The Redis bridge

**Files:**
- Create: `src/main/java/com/recsys/config/RetrievalRedisConfig.java`
- Test: `src/test/java/com/recsys/config/RetrievalRedisConfigTest.java` (create)

**Interfaces:**
- Consumes: `LettuceClientFactory.requireAuthentication(RedisProperties, Map)` from Task 5;
  `com.recsys.config.RedisProperties` (prefix `recsys.redis`; getters `getMode()`, `getHost()`,
  `getPort()`, `getUsername()`, `getPassword()`, `isTls()`, `getTimeoutMs()`,
  `getSentinelMaster()`, `getSentinelNodes()`).
- Produces: beans `LettuceConnectionFactory retrievalRedisConnectionFactory` and
  `StringRedisTemplate stringRedisTemplate`, consumed by the 28 retrieval classes that
  `@Autowired` a `StringRedisTemplate`. Also the package-private static
  `RetrievalRedisConfig.connectionFactory(RedisProperties, Map<String,String>)` for testing.

- [ ] **Step 1: Write the failing test**

Create `src/test/java/com/recsys/config/RetrievalRedisConfigTest.java`:

```java
package com.recsys.config;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins that the Spring Data Redis connection factory is built from recsys.redis and runs the
 * same credential guard as the raw-Lettuce path. If this drifts back to Spring Boot's
 * spring.data.redis.* autoconfiguration, an unauthenticated Redis connection becomes reachable
 * on 8080 with no refusal anywhere.
 */
class RetrievalRedisConfigTest {

    private static RedisProperties standalone() {
        RedisProperties props = new RedisProperties();
        props.setHost("redis.example.internal");
        props.setPort(6380);
        props.setPassword("s3cret");
        props.setTimeoutMs(1500);
        return props;
    }

    @Test
    void buildsTheFactoryFromRecsysRedisNotSpringDataRedis() {
        LettuceConnectionFactory factory =
            RetrievalRedisConfig.connectionFactory(standalone(), Map.of());

        RedisStandaloneConfiguration cfg = factory.getStandaloneConfiguration();
        assertThat(cfg).isNotNull();
        assertThat(cfg.getHostName()).isEqualTo("redis.example.internal");
        assertThat(cfg.getPort()).isEqualTo(6380);
        assertThat(new String(cfg.getPassword().get())).isEqualTo("s3cret");
    }

    @Test
    void refusesAnUnauthenticatedConnection() {
        RedisProperties props = standalone();
        props.setPassword("");

        assertThatThrownBy(() -> RetrievalRedisConfig.connectionFactory(props, Map.of()))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("REDIS_PASSWORD");
    }

    @Test
    void honoursTheSentinelMode() {
        RedisProperties props = standalone();
        props.setMode("sentinel");
        props.setSentinelMaster("mymaster");
        props.setSentinelNodes("sentinel-a:26379,sentinel-b:26379");

        LettuceConnectionFactory factory = RetrievalRedisConfig.connectionFactory(props, Map.of());

        assertThat(factory.getSentinelConfiguration()).isNotNull();
        assertThat(factory.getSentinelConfiguration().getMaster().getName()).isEqualTo("mymaster");
    }
}
```

- [ ] **Step 2: Run it and watch it fail**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=RetrievalRedisConfigTest
```

Expected: compilation failure — `RetrievalRedisConfig` does not exist.

- [ ] **Step 3: Write the configuration**

Create `src/main/java/com/recsys/config/RetrievalRedisConfig.java`:

```java
package com.recsys.config;

import com.recsys.infrastructure.redis.LettuceClientFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisSentinelConfiguration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.RedisConfiguration.WithPassword;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Bridges the retrieval code's Spring Data Redis access onto this service's own Redis
 * configuration.
 *
 * <p>The retrieval tree reaches Redis through {@link StringRedisTemplate} in 28 classes. Letting
 * Spring Boot autoconfigure that template would create a second connection pool configured by
 * {@code spring.data.redis.*}, independent of {@code recsys.redis} and — the point — invisible to
 * {@link LettuceClientFactory}'s credential guard. An existing security control would become
 * bypassable on 8080 simply by merging this code. So {@code RedisAutoConfiguration} is excluded
 * on {@code ModelApplication} and the factory is built here, from the same properties and behind
 * the same guard as the raw-Lettuce executors in {@link RedisConfig}.
 *
 * <p>Consequence worth knowing: {@code spring.data.redis.*} is inert in this application. Tests
 * that point at an ephemeral Redis must set {@code recsys.redis.host} / {@code recsys.redis.port}.
 */
@Configuration
public class RetrievalRedisConfig {

    static LettuceConnectionFactory connectionFactory(RedisProperties props, Map<String, String> env) {
        LettuceClientFactory.requireAuthentication(props, env);

        LettuceClientConfiguration.LettuceClientConfigurationBuilder client =
            LettuceClientConfiguration.builder()
                .commandTimeout(Duration.ofMillis(props.getTimeoutMs()));
        if (props.isTls()) {
            client.useSsl();
        }

        if ("sentinel".equalsIgnoreCase(props.getMode())) {
            Set<String> nodes = Arrays.stream(props.getSentinelNodes().split(","))
                .map(String::strip)
                .filter(node -> !node.isEmpty())
                .collect(Collectors.toCollection(LinkedHashSet::new));
            RedisSentinelConfiguration sentinel =
                new RedisSentinelConfiguration(props.getSentinelMaster(), nodes);
            applyCredentials(sentinel, props);
            return new LettuceConnectionFactory(sentinel, client.build());
        }

        RedisStandaloneConfiguration standalone =
            new RedisStandaloneConfiguration(props.getHost(), props.getPort());
        applyCredentials(standalone, props);
        return new LettuceConnectionFactory(standalone, client.build());
    }

    private static void applyCredentials(WithPassword config, RedisProperties props) {
        if (props.getUsername() != null && !props.getUsername().isBlank()) {
            config.setUsername(props.getUsername());
        }
        if (props.getPassword() != null && !props.getPassword().isBlank()) {
            config.setPassword(props.getPassword());
        }
    }

    @Bean
    public LettuceConnectionFactory retrievalRedisConnectionFactory(RedisProperties props) {
        return connectionFactory(props, System.getenv());
    }

    @Bean
    public StringRedisTemplate stringRedisTemplate(LettuceConnectionFactory factory) {
        return new StringRedisTemplate(factory);
    }
}
```

Note on the `WithPassword` helper: both `RedisStandaloneConfiguration` and
`RedisSentinelConfiguration` implement `RedisConfiguration.WithPassword`, which declares
`setUsername`/`setPassword`. If the compiler rejects the shared parameter type, fall back to two
small overloads rather than duplicating the null/blank logic.

- [ ] **Step 4: Run it and watch it pass**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=RetrievalRedisConfigTest
```

Expected: 3 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/recsys/config/RetrievalRedisConfig.java \
        src/test/java/com/recsys/config/RetrievalRedisConfigTest.java
git commit -m "$(cat <<'EOF'
feat(retrieval): build StringRedisTemplate from recsys.redis

Autoconfiguring the template from spring.data.redis.* would open a second
connection pool that LettuceClientFactory's credential guard never sees,
making REDIS_ALLOW_NO_AUTH bypassable on 8080 as a side effect of merging
this code. The factory is built here instead, behind the same guard.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 7: Re-prefix `RecommendationProperties` to `recsys.retrieval`

No existing class owns the bare `recsys` prefix, so this is ownership hygiene rather than a
collision fix: it puts the new class alongside the eight existing `recsys.<name>` owners
(`recsys.ab-test`, `recsys.feature-flags`, `recsys.health`, `recsys.login`, `recsys.model`,
`recsys.recommendation-cache`, `recsys.redis`, `recsys.submit-token`) instead of above them.

**Files:**
- Modify: `src/main/java/com/recsys/retrieval/config/RecommendationProperties.java:21`
- Modify: `src/test/java/com/recsys/retrieval/UserProfileIntegrationTest.java` (8 `recsys.catalog.*` keys)

**Interfaces:**
- Consumes: nothing new.
- Produces: the config prefix `recsys.retrieval`, which Task 10 populates in `application.yml`.

- [ ] **Step 1: Change the prefix**

```bash
sed -i '' 's/@ConfigurationProperties(prefix = "recsys")/@ConfigurationProperties(prefix = "recsys.retrieval")/' \
  src/main/java/com/recsys/retrieval/config/RecommendationProperties.java
```

- [ ] **Step 2: Update every property key that names the old prefix**

```bash
grep -rn '"recsys\.' src/test/java/com/recsys/retrieval/
```

Rewrite each hit from `recsys.<leaf>` to `recsys.retrieval.<leaf>`. As of writing, all eight are
`recsys.catalog.*` keys in `UserProfileIntegrationTest`'s `@SpringBootTest(properties = {...})`
block. Leave `spring.data.redis.port=1` alone for now — Task 9 handles it.

- [ ] **Step 3: Verify no stale key remains**

```bash
grep -rn '"recsys\.\(catalog\|cache\|embeddings\|candidate-generation\|filtering\|bandit\|replay-buffer\|reward-model\|sequence\|grpo\|profile-audit\|measurements\)' src/ \
  && echo "STALE KEYS ABOVE" || echo "CLEAN"
```

Expected: `CLEAN`.

- [ ] **Step 4: Compile**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q compile -DskipTests && echo OK
```

Expected: `OK`.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(retrieval): move config under recsys.retrieval

RecommendationProperties owned the bare recsys prefix, sitting above the
eight existing recsys.<name> owners rather than beside them. No collision
today; this keeps prefix ownership legible as the merged service grows.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 8: Rename the colliding controller

`com.recsys.retrieval.controller.RecommendationController` and
`com.recsys.api.rest.RecommendationController` both decapitalize to the bean name
`recommendationController`. Scanning both raises `ConflictingBeanDefinitionException` at startup.
Renaming the type is preferred over `@RestController("someName")`: it fixes the bean collision
*and* removes a duplicated class name that would confuse every future import.

**Files:**
- Rename: `src/main/java/com/recsys/retrieval/controller/RecommendationController.java` →
  `RetrievalRecommendationController.java`
- Rename: `src/test/java/com/recsys/retrieval/controller/RecommendationControllerTest.java` →
  `RetrievalRecommendationControllerTest.java`

**Interfaces:**
- Consumes: nothing.
- Produces: bean name `retrievalRecommendationController`, distinct from the existing
  `recommendationController`.

- [ ] **Step 1: Rename both files and the type**

```bash
cd /Users/linghuang/Git/Recsys-Backend-Service
git mv src/main/java/com/recsys/retrieval/controller/RecommendationController.java \
       src/main/java/com/recsys/retrieval/controller/RetrievalRecommendationController.java \
  2>/dev/null || mv src/main/java/com/recsys/retrieval/controller/RecommendationController.java \
       src/main/java/com/recsys/retrieval/controller/RetrievalRecommendationController.java
mv src/test/java/com/recsys/retrieval/controller/RecommendationControllerTest.java \
   src/test/java/com/recsys/retrieval/controller/RetrievalRecommendationControllerTest.java
sed -i '' 's/\bRecommendationControllerTest\b/RetrievalRecommendationControllerTest/g; s/\bRecommendationController\b/RetrievalRecommendationController/g' \
  src/main/java/com/recsys/retrieval/controller/RetrievalRecommendationController.java \
  src/test/java/com/recsys/retrieval/controller/RetrievalRecommendationControllerTest.java
```

The `RecommendationControllerTest` substitution runs first so the longer name is not mangled by
the shorter pattern.

- [ ] **Step 2: Verify no other retrieval file referenced the old name**

```bash
grep -rn '\bRecommendationController\b' src/main/java/com/recsys/retrieval src/test/java/com/recsys/retrieval \
  && echo "STALE REFERENCES ABOVE" || echo "CLEAN"
```

Expected: `CLEAN`. (Hits under `src/main/java/com/recsys/api/rest/` are the *existing*
controller and must be left alone.)

- [ ] **Step 3: Run the renamed test**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=RetrievalRecommendationControllerTest
```

Expected: the same result this file produced in Task 4. It is still a `@WebMvcTest` slice
against `RetrievalServiceApplication` at this point; Task 9 repoints it.

- [ ] **Step 4: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(retrieval): rename RecommendationController to avoid a bean clash

Both this class and com.recsys.api.rest.RecommendationController
decapitalize to recommendationController, so scanning them together raises
ConflictingBeanDefinitionException. Renaming the type also removes a
duplicated class name.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 9: Delete `RetrievalServiceApplication` and wire the component scan

**Files:**
- Delete: `src/main/java/com/recsys/retrieval/RetrievalServiceApplication.java`
- Modify: `src/main/java/com/recsys/api/rest/ModelApplication.java`
- Modify: `src/test/java/com/recsys/retrieval/config/MeasurementPropertiesTest.java`
- Modify: `src/test/java/com/recsys/retrieval/config/ProfileAuditPropertiesTest.java`
- Modify: `src/test/java/com/recsys/retrieval/controller/ModelReloadControllerTest.java`
- Modify: `src/test/java/com/recsys/retrieval/controller/ProfileAuditControllerTest.java`
- Modify: `src/test/java/com/recsys/retrieval/controller/RetrievalRecommendationControllerTest.java`
- Modify: `src/test/java/com/recsys/retrieval/UserProfileIntegrationTest.java`

**Interfaces:**
- Consumes: `RetrievalRedisConfig` (Task 6), the `recsys.retrieval` prefix (Task 7), the renamed
  controller (Task 8).
- Produces: a single Spring application on 8080 that includes the retrieval beans.

- [ ] **Step 1: Extend `ModelApplication`'s scan and exclude Redis autoconfiguration**

In `ModelApplication.java`, add `"com.recsys.retrieval"` to `scanBasePackages`, add
`RecommendationProperties.class` to `@EnableConfigurationProperties`, and exclude
`RedisAutoConfiguration`:

```java
@SpringBootApplication(
        scanBasePackages = {"com.recsys.api", "com.recsys.config", "com.recsys.exception",
                "com.recsys.metrics", "com.recsys.jvm", "com.recsys.tracing",
                "com.recsys.ratelimit", "com.recsys.loadshed", "com.recsys.resilience",
                "com.recsys.health", "com.recsys.application", "com.recsys.retrieval"},
        // RetrievalRedisConfig builds StringRedisTemplate from recsys.redis so the
        // REDIS_ALLOW_NO_AUTH guard covers it. Autoconfiguration would build a second,
        // unguarded pool from spring.data.redis.*.
        exclude = {org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration.class})
```

and add to the `@EnableConfigurationProperties` list:

```java
        com.recsys.retrieval.config.RecommendationProperties.class,
```

- [ ] **Step 2: Delete the second application class**

```bash
rm src/main/java/com/recsys/retrieval/RetrievalServiceApplication.java
```

- [ ] **Step 3: Repoint every Spring-context test at `ModelApplication`**

Five files name or resolve `RetrievalServiceApplication`. Two name it explicitly:

```bash
sed -i '' 's/import com\.recsys\.retrieval\.RetrievalServiceApplication;/import com.recsys.api.rest.ModelApplication;/; s/@SpringBootTest(classes = RetrievalServiceApplication\.class)/@SpringBootTest(classes = ModelApplication.class)/' \
  src/test/java/com/recsys/retrieval/config/MeasurementPropertiesTest.java \
  src/test/java/com/recsys/retrieval/config/ProfileAuditPropertiesTest.java
```

Three `@WebMvcTest` slices and one bare `@SpringBootTest` resolve their configuration by
searching *ancestor* packages for `@SpringBootApplication`. `com.recsys.api.rest.ModelApplication`
is **not** an ancestor of `com.recsys.retrieval`, so each needs an explicit pointer. Add to each
of `ModelReloadControllerTest`, `ProfileAuditControllerTest`,
`RetrievalRecommendationControllerTest`:

```java
@ContextConfiguration(classes = ModelApplication.class)
```

with `import com.recsys.api.rest.ModelApplication;` and
`import org.springframework.test.context.ContextConfiguration;`.

For `UserProfileIntegrationTest`, change the bare annotation to
`@SpringBootTest(classes = ModelApplication.class, properties = { ... })`, keeping the existing
properties block.

- [ ] **Step 4: Repoint the integration test's Redis properties**

`RedisAutoConfiguration` is now excluded, so `spring.data.redis.*` is inert and the test would
silently talk to `localhost:6379` instead of its container. In `UserProfileIntegrationTest`:

```java
    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("recsys.redis.host", REDIS::getHost);
        registry.add("recsys.redis.port", () -> REDIS.getMappedPort(6379));
    }
```

and change the static `"spring.data.redis.port=1"` entry in the `properties` block to
`"recsys.redis.port=1"`.

- [ ] **Step 5: Verify the context starts**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=MeasurementPropertiesTest
```

Expected: PASS. A `ConflictingBeanDefinitionException` here means Task 8's rename did not take;
a `UnsatisfiedDependencyException` on `StringRedisTemplate` means Task 6's beans are not being
scanned (`com.recsys.config` is already in `scanBasePackages`, so check the class compiled).

- [ ] **Step 6: Run the whole suite and compare against Task 4's baseline**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test 2>&1 | tee /tmp/retrieval-suite-pr2.log | tail -30
diff <(grep -oE "Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+.*-- in .*" /tmp/retrieval-suite.log | sort) \
     <(grep -oE "Tests run: [0-9]+, Failures: [0-9]+, Errors: [0-9]+.*-- in .*" /tmp/retrieval-suite-pr2.log | sort)
```

Expected: no *new* failures relative to Task 4. The `@WebMvcTest` slices now boot against the
fully assembled 8080 context, which is the most likely source of a new failure and the specific
risk this PR was separated to isolate. If one appears, report it rather than working around it.

- [ ] **Step 7: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(retrieval): merge the retrieval beans into model serving

Deletes the second @SpringBootApplication and scans com.recsys.retrieval
from ModelApplication, excluding RedisAutoConfiguration so the Spring Data
template comes from RetrievalRedisConfig. The six Spring-context tests are
repointed: ModelApplication is not an ancestor package of
com.recsys.retrieval, so bare @SpringBootTest and @WebMvcTest could not
resolve it by search.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 10: Author the missing `recsys.retrieval` configuration

The service's own `application.yml` never came across. Every key falls back to a Java default,
and the catalog's default is an **empty map** that 16 call sites read — so without this the
content-based retrieval path starts cleanly and serves nothing.

**Files:**
- Modify: `src/main/resources/application.yml`
- Test: `src/test/java/com/recsys/retrieval/config/CatalogConfigurationTest.java` (create)

**Interfaces:**
- Consumes: the `recsys.retrieval` prefix (Task 7).
- Produces: a non-empty `recsys.retrieval.catalog` at runtime.

- [ ] **Step 1: Inspect the available seed data**

```bash
python3 -c "import json;d=json.load(open('src/main/resources/mlp_embedding_lookups.json'));print(type(d));print(list(d)[:10] if isinstance(d,dict) else d[:5])"
```

Compare the item ids against `RecommendationProperties.MovieProfile`'s fields (`title`,
`genres`, `tags`, `newRelease`). If the ids correspond, seed the catalog from this file. **If
they do not correspond, stop and say so** — write a minimal catalog sufficient for the tests and
report to the user that a real catalog is still needed. Do not invent movie metadata and present
it as data.

- [ ] **Step 2: Write the failing test**

Create `src/test/java/com/recsys/retrieval/config/CatalogConfigurationTest.java`:

```java
package com.recsys.retrieval.config;

import com.recsys.api.rest.ModelApplication;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The retrieval service's own application.yml never came across with its Java sources, so every
 * recsys.retrieval key fell back to a Java default. The catalog's default is an empty map that 16
 * call sites read, which means the content path starts cleanly and serves nothing — a failure
 * mode with no error anywhere to announce it. This asserts the shipped configuration is present.
 */
@SpringBootTest(classes = ModelApplication.class)
class CatalogConfigurationTest {

    @Autowired
    private RecommendationProperties properties;

    @Test
    void shipsANonEmptyCatalog() {
        assertThat(properties.getCatalog()).isNotEmpty();
    }

    @Test
    void everyCatalogEntryHasATitleAndAtLeastOneGenre() {
        assertThat(properties.getCatalog().values()).allSatisfy(profile -> {
            assertThat(profile.getTitle()).isNotBlank();
            assertThat(profile.getGenres()).isNotEmpty();
        });
    }
}
```

- [ ] **Step 3: Run it and watch it fail**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=CatalogConfigurationTest
```

Expected: FAIL on `shipsANonEmptyCatalog` — the catalog is empty. This failure is the evidence
that the missing config is real and not theoretical.

- [ ] **Step 4: Add the configuration block**

Append to the existing `recsys:` block in `src/main/resources/application.yml`, indented as a
sibling of `model:` and `redis:`:

```yaml
  # Configuration for the retrieval code merged in from the standalone retrieval service.
  # Its own application.yml did not come across with its Java sources; these are its Java
  # defaults made explicit, plus the catalog, whose default is empty and which 16 call sites
  # read. GRPO event emission stays off: enabling it opens a Kafka producer.
  retrieval:
    catalog-path: ${RECSYS_RETRIEVAL_CATALOG_PATH:}
    grpo:
      emit-events: ${RECSYS_RETRIEVAL_GRPO_EMIT_EVENTS:false}
    catalog:
      # Seeded in Step 1. Each entry: title, genres, tags, new-release.
```

followed by the catalog entries derived in Step 1.

- [ ] **Step 5: Run it and watch it pass**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=CatalogConfigurationTest
```

Expected: 2 tests, 0 failures.

- [ ] **Step 6: Commit and open PR2**

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(retrieval): ship the recsys.retrieval configuration

The catalog default is an empty map read by 16 call sites, so without this
the content path starts cleanly and serves nothing, with no error to say
so. CatalogConfigurationTest fails against the empty default.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
git push
gh pr create --title "feat(retrieval): merge retrieval into model serving on 8080" --body "$(cat <<'EOF'
Stacked on the build-restoration PR. This is where 8080's behaviour changes.

- Deletes the second `@SpringBootApplication` and scans `com.recsys.retrieval` from
  `ModelApplication`.
- Renames the incoming `RecommendationController`, which collided on the bean name
  `recommendationController` with the existing one.
- Builds `StringRedisTemplate` from `recsys.redis` via `RetrievalRedisConfig`, with
  `RedisAutoConfiguration` excluded. Autoconfiguring it would have opened a second connection
  pool that `LettuceClientFactory`'s credential guard never sees — merging this code would have
  made `REDIS_ALLOW_NO_AUTH` bypassable on 8080 as a side effect.
- Moves the retrieval config from the bare `recsys` prefix to `recsys.retrieval`, and ships the
  configuration block that never came across with the Java sources.

**`spring.data.redis.*` is now inert in this application.** Tests pointing at an ephemeral Redis
must set `recsys.redis.host` / `recsys.redis.port`; `UserProfileIntegrationTest` is updated
accordingly.

Routes and authorization follow in the next PR — `POST /actuator/model-reload` is still
unauthenticated at this commit.

Design: `docs/superpowers/specs/2026-09-16-retrieval-service-consolidation-design.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

# PR3 — Routes, authorization, documentation

Branch from PR2's head.

**Two corrections to the spec's sketch of this PR, found while planning it.** Both make PR3
larger than the spec implied, and both were discovered by reading `BackendRoutePolicy` and
`BackendRouteCoverageTest` rather than assuming:

1. **`BackendRoutePolicy` is keyed by backend service name and uses the backend's own path
   spelling** — `recsys-model-serving` already declares `/api/v1/model/versions` *with* the
   version segment. The version-stripping described in CLAUDE.md applies to the gateway's
   inbound path for public-path checks, which is a different path space from the backend path
   `BackendRoutePolicy.lookup(serviceName, backendPath)` receives. Classify using the versioned
   spelling.
2. **`/actuator` is already classified `NO_PROXY` for `recsys-model-serving`.** So moving the two
   operator routes out from under `/actuator` *increases* their reachability: today the gateway
   refuses to proxy them at all. Classifying them `OPERATOR` is therefore not a tidy-up — it is
   what makes the move safe, and the move and the classification must land in the same commit.

### Task 11: Relocate the controllers into the scanned root

`BackendRouteCoverageTest.everyRouteRegistrationLivesWhereAScannerLooks` fails on any
`@RestController` outside `src/main/java/com/recsys/api/rest`. Moving the three controllers
there is also what the repo's package map asks for: `api/rest` is where Spring Boot controllers
belong, and transport classes are the one part of this tree that has an obvious home in the
layer scheme.

**Files:**
- Move: `src/main/java/com/recsys/retrieval/controller/RetrievalRecommendationController.java` →
  `src/main/java/com/recsys/api/rest/retrieval/RetrievalRecommendationController.java`
- Move: `src/main/java/com/recsys/retrieval/controller/ModelReloadController.java` →
  `src/main/java/com/recsys/api/rest/retrieval/ModelReloadController.java`
- Move: `src/main/java/com/recsys/retrieval/controller/ProfileAuditController.java` →
  `src/main/java/com/recsys/api/rest/retrieval/ProfileAuditController.java`
- Move the three matching test files to `src/test/java/com/recsys/api/rest/retrieval/`

**Interfaces:**
- Consumes: the controllers from Tasks 8-9.
- Produces: package `com.recsys.api.rest.retrieval`, inside both `SPRING_SCAN_ROOT` and
  `ModelApplication`'s existing `com.recsys.api` scan.

- [ ] **Step 1: Confirm the test fails before the move**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=BackendRouteCoverageTest
```

Expected: FAIL on `everyRouteRegistrationLivesWhereAScannerLooks`, naming the three files.

- [ ] **Step 2: Move the controllers and their tests**

```bash
cd /Users/linghuang/Git/Recsys-Backend-Service
mkdir -p src/main/java/com/recsys/api/rest/retrieval src/test/java/com/recsys/api/rest/retrieval
git mv src/main/java/com/recsys/retrieval/controller/RetrievalRecommendationController.java        src/main/java/com/recsys/retrieval/controller/ModelReloadController.java        src/main/java/com/recsys/retrieval/controller/ProfileAuditController.java        src/main/java/com/recsys/api/rest/retrieval/
git mv src/test/java/com/recsys/retrieval/controller/RetrievalRecommendationControllerTest.java        src/test/java/com/recsys/retrieval/controller/ModelReloadControllerTest.java        src/test/java/com/recsys/retrieval/controller/ProfileAuditControllerTest.java        src/test/java/com/recsys/api/rest/retrieval/
rmdir src/main/java/com/recsys/retrieval/controller src/test/java/com/recsys/retrieval/controller 2>/dev/null
sed -i '' 's/^package com\.recsys\.retrieval\.controller;/package com.recsys.api.rest.retrieval;/'   src/main/java/com/recsys/api/rest/retrieval/*.java src/test/java/com/recsys/api/rest/retrieval/*.java
```

- [ ] **Step 3: Add the imports the move breaks**

The controllers previously sat in the same package tree as the services they inject. Each now
needs explicit imports for the `com.recsys.retrieval.*` types it references. Let the compiler
enumerate them:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q compile -DskipTests 2>&1 | grep -E "cannot find symbol|symbol:" | head -30
```

Add one import per reported symbol. Do not widen to a wildcard import; the repo imports
explicitly throughout.

- [ ] **Step 4: Verify the relocation satisfied the scanner**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=BackendRouteCoverageTest
```

Expected: `everyRouteRegistrationLivesWhereAScannerLooks` now PASSES, and
`everyBackendRouteIsClassified` now **FAILS** — the scanner can see the nine new routes and none
is classified. That swap is the proof the move worked; Task 14 clears the second failure.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
refactor(retrieval): move controllers into the scanned api/rest root

BackendRouteCoverageTest fails on any @RestController outside
src/main/java/com/recsys/api/rest, which is what stops a controller in a
new package shipping unclassified by the gateway route policy. The drop-in
put three controllers outside it. api/rest is also where the package map
says Spring controllers belong.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 12: Namespace the serving routes under `/api/v1/retrieval`

**Files:**
- Modify: `src/main/java/com/recsys/api/rest/retrieval/RetrievalRecommendationController.java`
- Modify: `src/test/java/com/recsys/api/rest/retrieval/RetrievalRecommendationControllerTest.java`

**Interfaces:**
- Consumes: the relocated controller from Task 11.
- Produces: the seven serving paths under `/api/v1/retrieval`, classified in Task 14 and routed
  in Task 15.

- [ ] **Step 1: Update the test first**

Prefix every `get(...)` / `post(...)` path in `RetrievalRecommendationControllerTest` with
`/api/v1/retrieval`, keeping whatever user ids the existing assertions use:

```java
        mockMvc.perform(get("/api/v1/retrieval/recommend/123"))
            .andExpect(status().isOk());
```

- [ ] **Step 2: Run and watch it fail**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=RetrievalRecommendationControllerTest
```

Expected: FAIL with 404s — the controller still answers at the root.

- [ ] **Step 3: Add the class-level mapping**

On `RetrievalRecommendationController`, above the class declaration:

```java
@RequestMapping("/api/v1/retrieval")
```

with `import org.springframework.web.bind.annotation.RequestMapping;`. The seven method mappings
(`/recommend/{user}`, `/predict/{user}/{item}`, `/predict/id`, `/predict/metadata`,
`/embedding/{item}`, `/users/{user}/profile`, `/feedback`) and the root `/metrics` mapping stay
as written and inherit the prefix.

- [ ] **Step 4: Run and watch it pass**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=RetrievalRecommendationControllerTest
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(retrieval): namespace serving routes under /api/v1/retrieval

The routes arrived root-level and unversioned, contradicting the ApiVersion
contract every other 8080 route follows. The root GET /metrics moves under
the prefix too, so it stops sitting beside the actuator metrics.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 13: Add `UserIdSource.PATH`

Three retrieval routes carry the application userId in a **path segment**:
`/recommend/{user}`, `/predict/{user}/{item}`, `/users/{user}/profile`. `UserIdSource` today has
only `QUERY`, `BODY` and `BODY_INSTANCES`, so there is no way to classify these `USER_SCOPED`.

Classifying them `AUTHENTICATED` instead would compile, pass every test, and let any
authenticated caller read any other user's profile and recommendations by editing the path —
`/users/{user}/profile` in particular. That is the leak this enum exists to prevent, so the
enum gains a case rather than the routes being downgraded.

**Files:**
- Modify: `src/main/java/com/recsys/application/gateway/UserIdSource.java`
- Test: `src/test/java/com/recsys/application/gateway/UserIdSourceTest.java` (locate the existing
  file with `find src/test -name 'UserIdSource*'`; create it if absent)

**Interfaces:**
- Consumes: the existing `UserIdSource` enum — each constant implements
  `String extract(String targetPath, AggregatedHttpRequest request)` returning the userId or
  `null`.
- Produces: `UserIdSource.PATH`, consumed by Task 14's policy entries.

- [ ] **Step 1: Write the failing test**

Read the existing `UserIdSource` cases first and match their style. `PATH` must extract the
segment following a fixed marker segment, since the userId is not always last
(`/predict/{user}/{item}`):

```java
    @Test
    void extractsTheUserIdFromThePathSegment() {
        assertEquals("123", UserIdSource.PATH.extract(
            "/api/v1/retrieval/recommend/123", request));
    }

    @Test
    void extractsTheUserIdWhenMoreSegmentsFollow() {
        assertEquals("123", UserIdSource.PATH.extract(
            "/api/v1/retrieval/predict/123/456", request));
    }

    @Test
    void extractsTheUserIdFromTheUsersRoute() {
        assertEquals("123", UserIdSource.PATH.extract(
            "/api/v1/retrieval/users/123/profile", request));
    }

    @Test
    void returnsNullWhenNoSegmentFollowsTheMarker() {
        assertNull(UserIdSource.PATH.extract("/api/v1/retrieval/recommend", request));
    }

    @Test
    void ignoresTheQueryStringWhenReadingTheSegment() {
        assertEquals("123", UserIdSource.PATH.extract(
            "/api/v1/retrieval/recommend/123?limit=5", request));
    }
```

- [ ] **Step 2: Run and watch it fail**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='UserIdSource*'
```

Expected: compilation failure — `PATH` does not exist.

- [ ] **Step 3: Implement `PATH`**

Add the constant to `UserIdSource`, following the file's existing structure. It must strip any
query string before splitting (the other cases already handle their own query concerns), take
the segment after `recommend`, `predict` or `users`, and return `null` rather than throwing when
the path is shorter than expected — an unclassifiable path must fail closed upstream, not raise
here.

- [ ] **Step 4: Run and watch it pass**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='UserIdSource*'
```

Expected: 5 tests, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(gateway): extract the userId from a path segment

The retrieval routes carry the userId in the path, which no existing
UserIdSource could read. Without this they could only be classified
AUTHENTICATED, letting any authenticated caller read another user's profile
by editing the path — the exact leak USER_SCOPED exists to prevent.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 14: Classify all nine routes

`BackendRouteCoverageTest.everyBackendRouteIsClassified` is failing from Task 11 Step 4 and is
the acceptance test for this task.

**Files:**
- Modify: `src/main/java/com/recsys/application/gateway/BackendRoutePolicy.java`
- Modify: `src/main/java/com/recsys/api/rest/retrieval/ModelReloadController.java`
- Modify: `src/main/java/com/recsys/api/rest/retrieval/ProfileAuditController.java`
- Modify: `src/test/java/com/recsys/api/rest/retrieval/ModelReloadControllerTest.java`
- Modify: `src/test/java/com/recsys/api/rest/retrieval/ProfileAuditControllerTest.java`
- Modify: `src/test/java/com/recsys/application/gateway/BackendRoutePolicyTest.java`

**Interfaces:**
- Consumes: `BackendRoutePolicy.lookup(String serviceName, String backendPath)` returning a
  `Policy` record or `null`; `Access.{OPERATOR, AUTHENTICATED, USER_SCOPED, NO_PROXY}`;
  `UserIdSource.PATH` from Task 13; the private helpers `of(Access)` and
  `userScoped(UserIdSource)`.
- Produces: a classification for every route the Spring scanner finds under
  `com/recsys/api/rest/retrieval`.

- [ ] **Step 1: Move the two operator routes**

In `ModelReloadController`, change `@PostMapping("/actuator/model-reload")` to
`@PostMapping("/api/v1/retrieval/model/reload")`. In `ProfileAuditController`, change
`@GetMapping("/actuator/profile-audit")` to `@GetMapping("/api/v1/retrieval/profile-audit")` and
`@GetMapping("/actuator/profile-audit/{user}")` to
`@GetMapping("/api/v1/retrieval/profile-audit/{user}")`.

This move and Step 3's classification must land in the same commit: `/actuator` is currently
`NO_PROXY` for `recsys-model-serving`, so until the classification exists the move makes these
routes *more* reachable, not less.

- [ ] **Step 2: Write the failing policy tests**

In `BackendRoutePolicyTest`, matching its existing `assertEquals(new
BackendRoutePolicy.Policy(...), BackendRoutePolicy.lookup(...))` style:

```java
    @Test
    void classifiesRetrievalModelReloadAsOperator() {
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.OPERATOR, null),
            BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/model/reload"));
    }

    @Test
    void classifiesProfileAuditAndItsPerUserPathAsOperator() {
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.OPERATOR, null),
            BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/profile-audit"));
        assertEquals(new BackendRoutePolicy.Policy(BackendRoutePolicy.Access.OPERATOR, null),
            BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/profile-audit/123"));
    }

    @Test
    void scopesTheRetrievalUserRoutesToTheCallersOwnUserId() {
        assertEquals(new BackendRoutePolicy.Policy(
                BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.PATH),
            BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/recommend/123"));
        assertEquals(new BackendRoutePolicy.Policy(
                BackendRoutePolicy.Access.USER_SCOPED, UserIdSource.PATH),
            BackendRoutePolicy.lookup("recsys-model-serving", "/api/v1/retrieval/users/123/profile"));
    }
```

Confirm the `Policy` record's second component and its no-source spelling against the file
before writing — the existing `of(Access)` helper shows what it uses.

- [ ] **Step 3: Add the entries**

Under `"recsys-model-serving"`, add to `EXACT`:

```java
                    Map.entry("/api/v1/retrieval/predict/id", of(Access.AUTHENTICATED)),
                    Map.entry("/api/v1/retrieval/predict/metadata", of(Access.AUTHENTICATED)),
                    Map.entry("/api/v1/retrieval/feedback", userScoped(UserIdSource.BODY)),
                    Map.entry("/api/v1/retrieval/metrics", of(Access.NO_PROXY)),
                    // Reloads the live ONNX session — the same class of mutation as
                    // /api/v1/model/versions/activate, and it arrived from the retrieval drop-in
                    // with no authorization at all.
                    Map.entry("/api/v1/retrieval/model/reload", of(Access.OPERATOR)),
```

and to `PREFIX`, for the four path-template routes that cannot be spelled exactly:

```java
                    // Path templates, not paths: declaring "/api/v1/retrieval/recommend/{user}"
                    // exactly would match only the literal the scanner emits and that no client
                    // sends, 404ing every real id while both coverage tests stayed green. Same
                    // trap as /api/v1/knowledge-bases. These must NOT also appear in EXACT —
                    // exact wins the lookup and would kill the prefix branch.
                    "/api/v1/retrieval/recommend", userScoped(UserIdSource.PATH),
                    "/api/v1/retrieval/predict", userScoped(UserIdSource.PATH),
                    "/api/v1/retrieval/users", userScoped(UserIdSource.PATH),
                    "/api/v1/retrieval/profile-audit", of(Access.OPERATOR),
```

`/api/v1/retrieval/predict` as a prefix would shadow the exact `/predict/id` and
`/predict/metadata` entries — but exact is tried first, so those two still resolve to
`AUTHENTICATED`. Confirm `noPrefixEntryShadowsADeclaredExactPath` agrees; if it rejects this
pairing, drop the `/predict` prefix and give `/predict/{user}/{item}` its own handling rather
than weakening the two exact entries.

Note `PREFIX` currently uses `Map.of`, which caps at ten pairs. Adding four entries to
`recsys-model-serving`'s inner map takes it to six — fine — but if it ever exceeds ten, switch
to `Map.ofEntries` as `EXACT` already does.

- [ ] **Step 4: Run both gateway tests**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='BackendRoutePolicyTest,BackendRouteCoverageTest'
```

Expected: both PASS, including `everyBackendRouteIsClassified`,
`noPrefixEntryShadowsADeclaredExactPath` and `noUserScopedRouteCanBeMadePublic`.

- [ ] **Step 5: Update the two controller slice tests**

Change the paths in `ModelReloadControllerTest` and `ProfileAuditControllerTest`, then:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='ModelReloadControllerTest,ProfileAuditControllerTest'
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
fix(retrieval): classify every retrieval route, guard model reload

POST /actuator/model-reload reloads the live ONNX session and arrived with
no authorization. It moves to /api/v1/retrieval/model/reload and is
classified OPERATOR in the same commit: /actuator is NO_PROXY today, so
moving it without classifying it would have increased its reachability.

The user routes carry their userId in the path and are USER_SCOPED via the
new UserIdSource.PATH, so a caller cannot read another user's profile by
editing the path.

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 15: Add the gateway route-table entry

**Files:**
- Modify: `src/main/java/com/recsys/api/gateway/MicroserviceGatewayServer.java`
- Modify: the gateway routing test covering the model-serving upstream (find it with
  `grep -rln 'recsys-model-serving' src/test/java/com/recsys/`)

**Interfaces:**
- Consumes: the `/api/v1/retrieval` prefix from Task 12 and its classifications from Task 14.
- Produces: gateway forwarding of the retrieval prefix to the model-serving upstream.

- [ ] **Step 1: Write the failing routing test**

Add a case asserting the retrieval prefix resolves to the model-serving upstream, matching the
existing tests' style and the spelling the route table actually uses.

- [ ] **Step 2: Run and watch it fail**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='*Gateway*Rout*'
```

Expected: FAIL — no route matches the prefix.

- [ ] **Step 3: Register the route**

Add the retrieval prefix against the model-serving upstream, following the file's established
registration pattern. Keep the registration inside `MicroserviceGatewayServer` — registering it
elsewhere fails `everyRouteRegistrationLivesWhereAScannerLooks`.

- [ ] **Step 4: Run and watch it pass**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest='*Gateway*Rout*'
```

Expected: PASS.

- [ ] **Step 5: Confirm no retrieval path became public**

`GATEWAY_PUBLIC_PATHS` must list exact paths and defaults to
`/health,/api/catalog/item,/api/catalog/similar`. The user-scoped routes must stay behind
authentication — and `noUserScopedRouteCanBeMadePublic` already asserts this, so it is a
double check rather than the only one.

```bash
grep -rn 'GATEWAY_PUBLIC_PATHS' src/main/java k8s/ | grep -i retrieval && echo "PROBLEM" || echo "CLEAN"
```

Expected: `CLEAN`.

- [ ] **Step 6: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
feat(gateway): route the retrieval prefix to model serving

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
```

---

### Task 16: Documentation

**Files:**
- Modify: `docs/system_design/20_AuthN_AuthZ.md` (§11, `BackendRoutePolicy`)
- Modify: `docs/system_design/09_API_Gateway.md` (route table)
- Modify: `docs/system_design/10_MicroServices.md` (service composition)

No new numbered markdown, and no README index row: `DocumentationIndexTest` is scoped to
`docs/system_design/` and `docs/runbooks/` and excludes `docs/superpowers/`; all three files are
already indexed, and this work creates no new document. Never renumber an existing `##` heading.

- [ ] **Step 1: Record the authorization changes in `20_AuthN_AuthZ.md`**

Weave into the existing §11 treatment of `BackendRoutePolicy`: the new `OPERATOR` routes and
that they arrived unauthenticated; the new `UserIdSource.PATH` and why the alternative
(`AUTHENTICATED`) would have been a cross-user read; and the standing constraint that
`SHARD_ADMIN_TOKEN` must be deployed **before** the image or every `OPERATOR` route rejects
every caller with 403.

Record the `/actuator` subtlety explicitly — it is the kind of thing a future reader will
otherwise rediscover the hard way: moving a route out from under a `NO_PROXY` prefix increases
its reachability, so the move and its classification belong in one commit.

- [ ] **Step 2: Record the route prefix in `09_API_Gateway.md`**

Add `/api/v1/retrieval` to the route table, noting it resolves to model serving.

- [ ] **Step 3: Record the composition change in `10_MicroServices.md`**

State that the retrieval code is merged into model serving rather than deployed as a fifth
service, and record the residuals a future reader will trip over: `com.recsys.retrieval.*` is
feature-shaped in a role-shaped package scheme (its controllers excepted, now in `api/rest`),
and two Redis client stacks coexist in the 8080 JVM, both configured from `recsys.redis` and
both behind the credential guard.

- [ ] **Step 4: Verify the documentation index still passes**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test -Dtest=DocumentationIndexTest
```

Expected: PASS.

- [ ] **Step 5: Full suite, commit, open PR3**

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn test 2>&1 | tail -30
```

Compare against Task 4's baseline: no new failures, and `BackendRouteCoverageTest` now green.

```bash
git add -A
git commit -m "$(cat <<'EOF'
docs(retrieval): record the merged service and its route policy

Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>
EOF
)"
git push
gh pr create --title "feat(retrieval): version the routes and guard model reload" --body "$(cat <<'EOF'
Stacked on the merge PR. Closes the authorization hole the drop-in introduced.

- Moves the three controllers into `com/recsys/api/rest/retrieval`. `BackendRouteCoverageTest`
  fails on any `@RestController` outside that root — which is how the repo stops a controller in
  a new package shipping unclassified by the gateway route policy. The drop-in tripped it.
- Serving routes move under `/api/v1/retrieval/**`.
- `POST /actuator/model-reload` → `/api/v1/retrieval/model/reload`, classified `OPERATOR`, so
  `GatewayRequestForwarder` requires `X-Admin-Token`. Profile audit moves with it. Note `/actuator`
  is `NO_PROXY` today, so the move and the classification had to land together.
- Adds `UserIdSource.PATH`. The retrieval routes carry the userId in the path, which no existing
  source could read; without it they could only be `AUTHENTICATED`, letting any authenticated
  caller read another user's profile by editing the path.
- Documentation folded into `09_API_Gateway.md`, `10_MicroServices.md`, `20_AuthN_AuthZ.md`.

**Deploy `SHARD_ADMIN_TOKEN` before this image.** Unset, the guard authorizes nobody and every
`OPERATOR`-class route rejects every caller with 403.

Design: `docs/superpowers/specs/2026-09-16-retrieval-service-consolidation-design.md`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Deferred, by decision

- **CI gating.** The PR gate runs only `-Presilience`, an include-list. Until entries are added,
  none of the 61 inherited tests blocks a merge. The decision waits on Task 4's measured
  results; a gated test must be non-docker *and* added to that profile.
- **Layer-scheme conformance.** `com.recsys.retrieval.*` is feature-shaped in a repo whose
  packages advertise role. Folding it into `api/ application/ domain/ infrastructure/` is a
  follow-up, to be done with working tests underneath it.
- **The duplicate Kafka producer path.** Six retained files overlap
  `infrastructure/messaging`, behind a default-false flag.
- **Porting the 28 Redis call sites onto `RedisExecutor`.** The bridge preserves the security
  property without the rewrite.
