# Embedding Training Package Design

## Problem and approved scope

The user approved renaming `training.rulebased` to `training.embedding` and requested a spec, implementation plan, code, and PR. `ItemEmbeddingJob` trains Word2Vec on chronological movie interaction sequences. The old package name incorrectly suggests a hand-written recommendation algorithm.

## Design

Move `src/main/java/com/recsys/training/rulebased/ItemEmbeddingJob.java` to `src/main/java/com/recsys/training/embedding/ItemEmbeddingJob.java` and change its package to `com.recsys.training.embedding`. Describe its purpose as learning item embeddings for similarity retrieval. Similarity is learned from behavioral co-occurrence, not movie text or a language model.

Update Maven default and streaming-profile exclusions to `com/recsys/training/embedding/**`; retain the offline-embedding profile's ability to compile the job. Update current package documentation and source comments. Historical specs and plans retain their original paths as records of earlier changes.

## Alternatives

Keeping `rulebased` preserves the old entry point but misrepresents the algorithm. A compatibility forwarding class adds maintenance for an entry point with no in-repository callers; use the direct rename. Building a new text embedding pipeline would require a separate data/model design and is outside this change.

## Constraints and compatibility

- Use JDK 17 for Maven verification.
- Preserve Word2Vec training, rating filters, CLI flags, CSV output, Redis keys, and runtime behavior.
- Add no dependencies.
- The new entry point is `com.recsys.training.embedding.ItemEmbeddingJob`; external launch commands referencing the old fully qualified name must be updated.
- Keep historical specs and plans unchanged.

## Validation

Compile the default and `offline-embedding` profiles, run the default test suite, and check whitespace. Verify that current source, Maven configuration, and operational docs contain no stale package references. Inspect the source diff to confirm only the package declaration and explanatory class documentation change. No new behavioral tests are necessary for a package-only refactor.
