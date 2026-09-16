# Consolidating the retrieval service into model serving

## Problem

A complete second Spring Boot application was copied into this repo as an untracked directory,
`src/main/java/com/recsys/retrieval/` plus `src/test/java/com/recsys/retrieval/`: 174 Java
files, roughly 17,000 lines, with its own controllers, Redis clients, Kafka layer, ONNX
prediction service, measurement subsystem, profile-audit subsystem and GRPO policy scorer. It
arrived alongside `src/main/resources/schemas/recsys-event-v3.avsc`,
`mlp_embedding_lookups.json` and `mlp_embedding_model.onnx`.

**The repository does not compile.** `mvn compile` fails today. Three independent causes:

1. **Package/directory mismatch.** Every one of the 174 files declares `package
   com.demo.retrieval.*` while living under `src/main/java/com/recsys/retrieval/`. The
   directory was renamed on copy; the package was not.
2. **Missing dependencies.** The code needs `spring-boot-starter-data-redis` (28 files use
   `StringRedisTemplate`) and `org.apache.avro` (`RecsysEventAvroCodec`). Neither is in
   `pom.xml`.
3. **No integration.** The tree has zero imports from the existing `com.recsys` codebase. It is
   a foreign island, not a merged service.

Two further collisions are latent — they cannot fire while the build is red, and both fire the
moment it is green and the code is component-scanned:

- **Bean name collision.** `com.demo.retrieval.controller.RecommendationController` and the
  existing `com.recsys.api.rest.RecommendationController` both decapitalize to the bean name
  `recommendationController`. Scanning both raises `ConflictingBeanDefinitionException` at
  startup. (Three further simple-name collisions — `MovieCandidate`, `MovieEvent`,
  `RecommendationResult` — are type-level only and harmless to the compiler.)
- **An unauthenticated model mutation.** `POST /actuator/model-reload` reloads the ONNX session.
  It is exactly the class of route `BackendRoutePolicy` classifies `OPERATOR` and guards with
  `X-Admin-Token`, and it currently has no guard at all.

Two absences shape the work as much as the defects do. The new service's own `application.yml`
never came across — only Java, the Avro schema and the two `mlp_embedding_*` files — so every
`recsys.catalog`, `recsys.grpo` and `recsys.bandit` key falls back to its Java default, and the
catalog's default is an empty map that 16 call sites read. And none of the 61 new test files has
ever been executed in this repo.

## Goals

- `mvn compile` and `mvn test` run green, with the retrieval code merged into `ModelApplication`
  on port 8080.
- The `REDIS_ALLOW_NO_AUTH` control in `LettuceClientFactory` covers the new code's Redis access,
  not just the existing code's.
- Every retrieval route is reachable under the repo's `/api/v1/...` versioning contract, and the
  two operator-class routes require `X-Admin-Token`.
- Existing 8080 behaviour is unchanged except by the deliberate additions above.
- The state of the 61 inherited tests is measured and reported, not assumed.

## Non-goals

- **Redistributing the code into the layer scheme.** CLAUDE.md's package map says a package
  advertises a class's *role*, not the service that uses it; `com.recsys.retrieval.*` is
  feature-shaped and does not conform. Folding it into `api/ application/ domain/
  infrastructure/` and unifying its Kafka layer with `infrastructure/messaging` is deliberately
  deferred: it touches every file twice and rewrites code whose tests have not yet been run
  once. It becomes a follow-up, filed with working tests underneath it.
- **Porting the 28 Redis call sites onto `RedisExecutor`.** The bridge below keeps the security
  property without the rewrite.
- **Exposing the retrieval routes through the CDN or adding cache behaviors.**
- **Wiring the GRPO Kafka publisher into a real broker.** It stays gated by
  `recsys.retrieval.grpo.emit-events`, default false.

## Decisions

Five decisions were taken during design and are settled:

| Question | Decision |
|---|---|
| End state | Merge into the 8080 model service; delete `RetrievalServiceApplication` |
| Redis stack | Bridge — hand-built `StringRedisTemplate` over the existing `recsys.redis` config |
| HTTP surface | Namespace under `/api/v1/retrieval/**`; classify the two mutating routes `OPERATOR` |
| Detachable code | Keep `evaluation/`; drop the unreachable Kafka consumer half |
| CI gating | Decide after the inherited tests are run and their real state is known |

Two coupling facts justify the fourth row. `evaluation/` (4 main + 4 test files, ~1,100 lines)
imports nothing from the rest of retrieval and nothing imports it; it is a self-contained
offline MovieLens policy-evaluation harness with its own `main()`, detachable and worth keeping.
`kafka/` (22 files) is reachable only from `HybridRecommendationService.createGrpoSender`, which
constructs a producer behind a default-false flag; the consumer half has no caller, and
`infrastructure/messaging/KafkaAsyncEventPublisher` already occupies that role.

## Design

Three stacked pull requests. The split is chosen so that the first is provably incapable of
changing 8080's behaviour, which is what makes the large mechanical diff safe to review quickly.

### PR1 — Compile

Restores a green build and changes nothing about how the application runs.

- Rename `com.demo.retrieval` → `com.recsys.retrieval` across all 174 files: `package`
  declarations, `import` statements, and the one static import of
  `UserAuditClassifierTest.profileJson`.
- Correct `LocalEmbeddingLoader.java`, which declares `com.demo.retrieval.kafka` while sitting
  in `service/`. It is the only file whose package disagrees with its directory beyond the
  uniform `com.demo` prefix; every other file is a clean rename.
- Add `spring-boot-starter-data-redis` (at `${spring-boot.version}`) and `org.apache.avro:avro`
  to `pom.xml`. The starter transitively supplies Lettuce, which the pom already pins at
  6.3.2.RELEASE; confirm via `mvn dependency:tree` that the pin holds.
- Delete the unreachable Kafka consumer classes. The set is derived by deleting and compiling,
  not by name-matching — a `grep -w` survey during design produced counts inflated by matches in
  comments and string literals, and is not trustworthy as a reachability oracle.
- `RetrievalServiceApplication` is retained and is added to **no** component scan.
  `ModelApplication` is not edited in this PR. 8080 therefore cannot regress.

Exit criteria: `mvn compile` green; `mvn test` executed in full; a written report of exactly
which of the 61 inherited test files pass, fail, or error. That report is the input to the CI
gating decision, which is deliberately deferred to it.

If a substantial number fail, that is evidence the drop-in is incomplete — missing config,
missing fixtures — and the right response is to return to the user, not to patch tests green.

### PR2 — Wire into `ModelApplication`

- Delete `RetrievalServiceApplication`; add `com.recsys.retrieval` to `ModelApplication`'s
  `scanBasePackages` list.
- Rename the incoming `RecommendationController` to `RetrievalRecommendationController`.
  Renaming the type is preferred over supplying an explicit bean name via
  `@RestController("...")`: it resolves the bean collision *and* removes a duplicated class name
  that would otherwise confuse every future import.
- **Redis bridge.** Exclude `RedisAutoConfiguration` from autoconfiguration and introduce
  `RetrievalRedisConfig`, which builds a `LettuceConnectionFactory` and `StringRedisTemplate`
  from the existing `recsys.redis` `RedisProperties` through `LettuceClientFactory`. This is the
  load-bearing step: it is what keeps `LettuceClientFactory.requireAuthentication` — and
  therefore the `REDIS_ALLOW_NO_AUTH` opt-in — covering the new code's 28 call sites. Allowing
  Spring Boot to autoconfigure `StringRedisTemplate` from `spring.data.redis.*` would create a
  second, independently configured connection pool that the guard does not see, making an
  existing security control bypassable on 8080.
  - Consequence: `UserProfileIntegrationTest` registers `spring.data.redis.host` and
    `spring.data.redis.port` through `@DynamicPropertySource`. Those must move to
    `recsys.redis.*` or the test points at the wrong Redis.
- **Config prefix.** Re-prefix `RecommendationProperties` from `recsys` to `recsys.retrieval`.
  No existing class owns the bare `recsys` prefix, so this is not a collision fix; it is
  ownership hygiene, putting the new class alongside the eight existing `recsys.<name>` owners
  rather than above them. Updates the 8 `recsys.catalog.*` keys used in tests.
- **Author the missing configuration.** Add a `recsys.retrieval.*` block to
  `src/main/resources/application.yml`. The catalog is the minimum: its default is empty and 16
  call sites read it, so without this the content-based retrieval path starts cleanly and serves
  nothing. Seed it from `mlp_embedding_lookups.json` if the item ids correspond; if they do not,
  say so explicitly rather than inventing a catalog.
- Repoint the six Spring-context tests at `ModelApplication.class`: two `@SpringBootTest(classes
  = RetrievalServiceApplication.class)`, three `@WebMvcTest` slices, and the bare
  `@SpringBootTest` in `UserProfileIntegrationTest` (bare resolution searches ancestor packages
  for `@SpringBootApplication`, and `com.recsys.api.rest.ModelApplication` is not an ancestor of
  `com.recsys.retrieval`).

### PR3 — Routes, authorization, documentation

- Move the serving routes under `/api/v1/retrieval/**`: `recommend/{user}`,
  `predict/{user}/{item}`, `predict/id`, `predict/metadata`, `embedding/{item}`,
  `users/{user}/profile`, `feedback`. The root-level `GET /metrics` folds into the same
  namespace so it no longer sits beside the actuator metrics story at the server root.
- Move `POST /actuator/model-reload` → `/api/v1/retrieval/model/reload` and
  `/actuator/profile-audit` → `/api/v1/retrieval/profile-audit`. Both are classified `OPERATOR`
  in `BackendRoutePolicy`, so `GatewayRequestForwarder` requires a matching `X-Admin-Token`.
  This closes the unauthenticated model-reload hole.
  - Note the standing deployment constraint: `SHARD_ADMIN_TOKEN` must be present before the
    image ships, or every `OPERATOR` route rejects every caller with 403.
- Add the `/api/v1/retrieval` prefix to the gateway route table.
- Update the three `@WebMvcTest` files whose expected paths change.

## Error handling

The merge inherits, and does not alter, the incoming code's failure posture. Two behaviours are
worth recording because they are easy to mistake for bugs later:

- `HybridRecommendationService.recommend` catches hydration failure and returns an empty
  `RecommendationResult` rather than propagating. A retrieval outage is a quiet empty response,
  not a 500.
- `createGrpoSender` catches producer-construction failure and returns `null`, logging a warning.
  An unreachable broker must not fail startup, because publishing is a logging side effect and
  never a serving dependency.

`CatalogLoader`'s `@PostConstruct` is a no-op when `catalog-path` is unset and logs-and-skips a
missing file, so it cannot block startup; only a malformed catalog file throws.

## Testing

- The 61 inherited test files are the primary safety net and are run in full at PR1.
- PR2's risk is concentrated in the six Spring-context tests: the `@WebMvcTest` slices will now
  boot against the fully assembled 8080 context and may surface failures unrelated to retrieval
  code. That is a genuine possibility, not a theoretical one, and it is the reason PR2 is
  separated from PR1.
- `UserProfileIntegrationTest` is Testcontainers-based and is documented in its own header as
  unrunnable against Docker 25+ because docker-java pins API version 1.32 through every
  configuration surface reachable from this project. It will not be verified locally, and that
  limit must be stated rather than worked around.

## Accepted residuals

1. **`com.recsys.retrieval.*` does not conform to the layer scheme.** It is feature-shaped in a
   repo whose packages are role-shaped. Accepted for now; filed as a follow-up.
2. **A duplicate Kafka producer path.** The retained producer half of `kafka/` overlaps
   `infrastructure/messaging`. It stays behind a default-false flag until the follow-up unifies
   them.
3. **Two Redis client stacks in one JVM.** The bridge means both are configured from
   `recsys.redis` and both pass the auth guard, but Spring Data Redis and raw Lettuce still
   coexist. Removing one is the `RedisExecutor` port, explicitly out of scope.
4. **CI gating is unresolved by design.** Per the repo's PR gate, only `-Presilience` runs, and
   it is an include-list. Until entries are added, none of the 61 inherited tests blocks a merge
   and they can rot silently. The decision waits on PR1's measured results.

## Documentation

Per the repo's standing convention, no new numbered markdown is created for this work. The
outcome is folded into the existing numbered investigations that own each topic:

- `docs/system_design/20_AuthN_AuthZ.md` — the two new `OPERATOR`-class routes and their
  `X-Admin-Token` requirement, extending the existing §11 treatment of `BackendRoutePolicy`.
- `docs/system_design/09_API_Gateway.md` — the `/api/v1/retrieval` prefix in the route table.
- `docs/system_design/10_MicroServices.md` — the service-composition change: four services, not
  five, with the retrieval code merged into model serving rather than deployed on its own port.

No README index row is required. `DocumentationIndexTest` is scoped to `docs/system_design/` and
`docs/runbooks/` and explicitly excludes `docs/superpowers/`; all three files above are already
indexed, and this work creates no new document. `.claude/CLAUDE.md` is not committed; anything
that belongs in it is noted in the PR body instead.
