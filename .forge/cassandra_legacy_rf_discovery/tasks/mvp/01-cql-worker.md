---
phase: mvp
wave: B
todos: [MVP-B-01, MVP-B-02, MVP-B-03]
---

# Wave B — Read-Only CQL Worker

- [x] **MVP-B-01 — Implement the grounded endpoint-pinned CQL observer** → new: pkg/discovery/cql_observer.go
  - Requirements: use only the `PRE-G-01`/Phase 4.7-grounded driver/API; one fresh session for the currently attempted IP:9042; disable peer discovery/load balancing/redirection; enforce auth/TLS IP verification, connect/query and overall fallback deadlines, exact cluster-name checks, Cassandra release/version validation within that candidate, and read-only queries for `system.local`, `system.peers_v2`, and three `system_schema.keyspaces` rows.
  - Scope: only `pkg/discovery/cql_observer.go`; no DDL method, global session, mutable package state, or target superuser fallback.
  - Tests: injected driver/session fakes cover authn/authz/TLS/reachability, query routing, null/conflicting identity, cleanup, and error redaction.
  - Acceptance evidence: unit trace records each attempted seed as the actual coordinator, proves only `SELECT` statements are possible, and shows no session/query opened after success.
  - Depends: PoC complete and `PRE-G-01` (`dependencies.md` driver grounding).

- [x] **MVP-B-02 — Build the isolated worker executable** → new: cmd/legacy-rf-discovery/main.go
  - Requirements: parse bounded ordered attempt input; validate qualified Secret/TLS mounts; run lazy sequential fallback until first complete valid result or deterministic exhaustion; write one canonical signed result; set sanitized exit/error behavior; never log credentials, certificates, HMAC key, DSN, or raw result; reject mutable/missing worker digest binding.
  - Scope: only `cmd/legacy-rf-discovery/main.go`; dependency construction only, with logic retained in `pkg/discovery`.
  - Tests: subprocess/unit tests use fake observer and filesystem inputs; malformed/missing mounts and cancellation fail closed.
  - Acceptance evidence: captured stdout/stderr contains stable reason only and secret canaries are absent.
  - Depends: MVP-B-01 and POC-A-07/08.

- [x] **MVP-B-03 — Test worker authority, protocol, and redaction end to end in-process** → new: pkg/discovery/cql_observer_test.go
  - Requirements: verify lazy fresh sessions, no redirection, stable ordered fallback, first complete valid authoritative response, earlier failure/later success, deterministic exhaustion, no later contact after success, no cross-seed merge/comparison, before/after fingerprint rejection, sparse heterogeneous maps including `2/9/2/10/10`, Cassandra 4.0 minimum, and resource closure on every attempted endpoint.
  - Scope: `pkg/discovery/cql_observer_test.go` and new `cmd/legacy-rf-discovery/main_test.go` only.
  - Tests: `go test -race ./pkg/discovery ./cmd/legacy-rf-discovery` with deterministic fakes and secret canaries.
  - Acceptance evidence: coverage report maps every observer/public worker error path and records zero mutation queries.
  - Depends: MVP-B-01 and MVP-B-02.
