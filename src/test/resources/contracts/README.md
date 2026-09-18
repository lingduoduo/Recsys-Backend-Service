# Frozen producer contracts

These files are **copies**. They are owned by
[lingduoduo/Recsys-Streaming-Pipeline](https://github.com/lingduoduo/Recsys-Streaming-Pipeline),
whose jobs produce the events and profiles this service consumes. They are frozen here so the
service's tests run without a pipeline checkout — see `ContractFixtures`.

| File | Canonical source in Recsys-Streaming-Pipeline |
|---|---|
| `recsys-event-v3.avsc` | `recsys-pipeline/schemas/recsys-event-v3.avsc` |
| `serving-impression-v3.avro` | `recsys-pipeline/schemas/fixtures/serving-impression-v3.avro` |
| `user_profile_v1.json` | `recsys-pipeline/integration-tests/fixtures/user_profile_v1.json` |

One more shared contract lives outside this directory:
`src/test/resources/sequence-schema.json`, whose source is
`recsys-pipeline/services/spark-streaming-job/src/test/resources/sequence-schema.json`.

`RecsysEventSchemaDriftTest` checks the snapshot above against this service's own codec
fingerprint. The comparison against the canonical pipeline schema is owned by the pipeline, at
`recsys-pipeline/integration-tests/test_cross_repo_contracts.py`, and the pairing is recorded in
`recsys-pipeline/schemas/CONTRACTS.md`.

**Do not edit these files here alone.** Change the canonical copy first, then mirror it here, then
refresh the hash in the pipeline's `CONTRACTS.md`. Neither repository's CI can see the other, so
nothing will catch a one-sided edit automatically.
