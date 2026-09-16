# Embedding Training Package Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the existing Word2Vec item embedding job an accurate package name and deliver the code with a PR.

**Architecture:** Move the existing job to `training.embedding` and align build exclusions and current documentation. Preserve its learned behavioral similarity pipeline and runtime contracts.

**Tech Stack:** Java 17, Maven, Spark MLlib Word2Vec, Lettuce Redis.

**Spec:** `docs/superpowers/specs/2026-09-15-embedding-training-package-design.md`

## Global Constraints

- Use JDK 17 for Maven verification.
- Preserve Word2Vec training, rating filters, CLI flags, CSV output, Redis keys, and runtime behavior.
- Add no dependencies.
- The new entry point is `com.recsys.training.embedding.ItemEmbeddingJob`; external launch commands referencing the old fully qualified name must be updated.
- Keep historical specs and plans unchanged.

## Task 1: Rename and document the embedding job

**Files:**
- Move: `src/main/java/com/recsys/training/rulebased/ItemEmbeddingJob.java` to `src/main/java/com/recsys/training/embedding/ItemEmbeddingJob.java`
- Modify: `pom.xml`
- Modify: `src/main/java/com/recsys/infrastructure/redis/StreamingRedisUri.java`
- Modify: `src/test/java/com/recsys/infrastructure/k8s/IrsaPermissionSourceFactsTest.java` (comment only)
- Modify: `.claude/CLAUDE.md`
- Modify: `docs/runbooks/redis-auth.md`
- Modify: `docs/system_design/10_MicroServices.md`

**Interfaces:** Existing `public static void main(String[] args)` moves to the new package. Arguments and generated artifacts remain identical.

- [x] Move the job and use `package com.recsys.training.embedding;`.
- [x] Replace `training/rulebased` with `training/embedding` in the listed build and documentation files.
- [x] Add the following class documentation:

```java
/**
 * Learns item embeddings from movie interaction sequences for similarity retrieval.
 * Word2Vec captures behavioral co-occurrence rather than movie-text similarity.
 */
```

- [x] Verify using JDK 17:

```bash
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -DskipTests compile
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q -Poffline-embedding -DskipTests compile
JAVA_HOME=$(/usr/libexec/java_home -v 17) mvn -q test
git diff --check
rg -n 'rulebased' src pom.xml .claude docs/runbooks docs/system_design
```

Expected: Maven commands and whitespace check exit 0; the reference search returns no matches (exit 1). Inspect the rename diff to ensure training code is unchanged. Existing compilation and tests cover this refactor; do not add implementation-mirroring tests.

- [x] Request independent code review and resolve material findings.
- [ ] Commit the code, spec, and plan on `refactor/embedding-training-package`, push to origin, and create a PR against `main`. Include verification results and the entry-point migration note.

## Verification results

Default `mvn -q clean test` passed on JDK 17: {'tests': 2001, 'failures': 0, 'errors': 0, 'skipped': 0}; `mvn -q -Poffline-embedding -DskipTests compile` passed. Independent code review found no material issues. An initial overlapping Maven run failed test compilation; rerunning from clean sequentially passed without source changes.
