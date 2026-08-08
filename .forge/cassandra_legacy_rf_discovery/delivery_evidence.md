# Legacy RF delivery evidence

Date: 2026-08-08

## Passed local gates

- source and focused boundary tests;
- full `make test`, including envtest;
- focused race tests;
- generation idempotence;
- `go vet ./...` and lint;
- Kustomize and Helm render parity;
- Hugo production build;
- `git diff --check`.

These gates prove the checked-in source and local harness behavior. They do not prove a production image or a live Cassandra lifecycle.

## Image boundary

The production Dockerfile build did not complete because Docker Hub base-image resolution timed out. Therefore the final production image and its embedded Apache Cassandra GoCQL driver NOTICE were not inspected. The validation-only derivative image hashes and scratch COPY check are harness-only evidence and must not be presented as production packaging proof.

## Cassandra lifecycle boundary

- **4.0:** five source datacenters reached Running. The first source `ALTER KEYSPACE` exited 1 before discovery began; the helper discarded `cqlsh` stderr, so the exact cause is unavailable. This is not 4.0 feature proof.
- **4.1:** the retained artifact is invalid/incomplete, reports `accepted endpoint must have an accepted attempt`, and contains no accepted snapshot. This is not 4.1 feature proof.
- **5.0:** the retained historical artifact proves only its recorded failed/accepted/skipped attempt trace, accepted snapshot/hash, and before/after RF rows. Its explicit `doesNotProve` limitations remain binding, including skipped-seed identity/state, ring health or membership, ownership, streaming, repair, and data availability.

No approval or release claim is implied by this evidence record.
