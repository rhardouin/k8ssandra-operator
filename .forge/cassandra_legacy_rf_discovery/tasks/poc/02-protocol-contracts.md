---
phase: poc
wave: A
todos: [POC-A-05, POC-A-06, POC-A-07, POC-A-08]
---

# Wave A — Canonical Source Protocol

- [x] **POC-A-05 — Define discovery domain types and typed boundary failures** → new: pkg/discovery/types.go
  - Requirements: define ordered attempts, one complete endpoint-attempt result/failure, authoritative candidate provenance, explicit three-keyspace observations, source version, qualified bindings, managed locations, immutable digest identity, and a minimal read-only `EndpointObserver`; do not retain the obsolete identity-only/all-seed split.
  - Scope: `pkg/discovery/types.go` and companion `pkg/discovery/errors.go` only; interfaces stay domain-facing and at most five methods.
  - Tests: compile assertions and typed-error mapping in POC-T-01.
  - Acceptance evidence: package API review shows no mutation method, secret payload, free-form keyspace map, `any`, or lifecycle-invalid Management API dependency.
  - Depends: POC-A-01 and POC-A-02.

- [x] **POC-A-06 — Implement canonical IP, RF, topology, and snapshot hashing** → new: pkg/discovery/canonical.go
  - Requirements: use `netip`; reject whitespace, zones, ports, FQDNs, invalid/mapped ambiguity; unmap IPv4-mapped IPv6; deduplicate by first occurrence while preserving spec order; compute an order-sensitive seed digest; parse ASCII RF `1..2147483647`; preserve sparse maps; canonicalize supported NTS aliases; encode qualified bindings and fingerprints deterministically.
  - Scope: only `pkg/discovery/canonical.go`; no network or Kubernetes I/O.
  - Tests: table/fuzz tests in POC-T-01 include IPv4/IPv6, mapped IPv6, duplicate first-occurrence retention, seed reorder changing the digest, Unicode digits, overflow/zero/negative RF, RF 9/10, map-order independence, and omission.
  - Acceptance evidence: repeat and randomized-order runs produce identical bytes/hash.
  - Depends: POC-A-05.

- [x] **POC-A-07 — Implement bounded signed result-envelope validation** → new: pkg/discovery/protocol.go
  - Requirements: canonical bounded JSON, HMAC-SHA-256, constant-time MAC check, schema/version/UID/generation/marker/attempt/ordered-seed/binding/worker-digest validation, size cap, canonical hash verification, deterministic exhausted-fallback failure precedence, and sanitized public mapping. A pre-acceptance seed reorder makes a result stale.
  - Scope: only `pkg/discovery/protocol.go`; raw result bodies and secrets must never enter public errors.
  - Tests: POC-T-01 covers forged, stale, oversize, reordered, truncated, wrong-digest/binding/generation payloads and valid round-trip.
  - Acceptance evidence: focused tests and race/fuzz runs demonstrate rejection before snapshot consumption and no secret/body leakage.
  - Depends: POC-A-05 and POC-A-06.

- [x] **POC-A-08 — Define ordered fallback seed authority state machine** → new: pkg/discovery/worker.go
  - Requirements: attempt canonical seeds sequentially in normalized spec order; open one fresh endpoint-pinned session lazily; validate one complete candidate; close on failure and continue; accept the first complete valid candidate verbatim; close and never open later sessions; block only after exhaustion using deterministic precedence. Never merge or compare endpoints. Accepted before/after fingerprints must match and source must be Cassandra `>=4.0.0`.
  - Scope: only `pkg/discovery/worker.go`; use injected observer/clock/bounds, close all sessions, issue no mutation.
  - Tests: POC-T-01 uses deterministic fake observers for first success, failure then success, multiple failures then success, all-fail exhaustion, wrong-cluster/auth/TLS/version/schema fallback, no-later-contact, cancellation, overall deadline, and close behavior.
  - Acceptance evidence: trace/assertions prove query for seed A cannot be served by B, A-fails/B-succeeds/C-is-unopened, and B's data is used verbatim.
  - Depends: POC-A-05 through POC-A-07; concrete driver adapter waits for grounding/MVP.
