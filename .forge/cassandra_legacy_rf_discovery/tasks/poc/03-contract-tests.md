---
phase: poc
wave: Test
todos: [POC-T-01, POC-T-02, POC-T-03]
---

# Test Wave — Contract Freeze

- [x] **POC-T-01 — Verify canonical protocol and ordered fallback authority** → new: pkg/discovery/discovery_test.go
  - Requirements: cover every POC-A-05..08 invariant, boundary error, endpoint-pinning proof, first seed success with later seeds untouched, first failure then second success, multiple failures then success, all-fail deterministic exhaustion, wrong-cluster/auth/TLS/version/schema failure followed by success, no merge/comparison, active-session-only cancellation/closure, before/after stability, Cassandra 3.x fallback/rejection, and Cassandra 4.0+ acceptance.
  - Scope: only `pkg/discovery/discovery_test.go`; no real external service or wall-clock ordering.
  - Tests: `go test -race ./pkg/discovery` and bounded fuzz targets for canonical/result parsing.
  - Acceptance evidence: deterministic repeated run log plus coverage of every public function/error return.
  - Depends: POC-A-05 through POC-A-08.

- [x] **POC-T-02 — Verify current-plan compatibility and historical safety** → new: controllers/k8ssandra/legacy_rf_discovery_plan_test.go
  - Requirements: prove reorder/removal/non-colliding addition are legal; external collision/identity/server/external-set changes block; accepted seeds survive spec edits; accepted/history locations never become desired topology; RF maps remain immutable.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery_plan_test.go`.
  - Tests: `go test ./controllers/k8ssandra -run TestValidateLegacyRFCurrentPlan`.
  - Acceptance evidence: named cases correspond to every current-plan contract and first-discovery-location reorder/removal requirement.
  - Depends: POC-A-03.

- [x] **POC-T-03 — Verify direct reads, acceptance ordering, and snapshot-loss boundary** → new: pkg/clientcache/cache_test.go
  - Requirements: test direct remote readers separately from cache; authoritative current+history survey; acceptance CAS then stop/read-back; pre-managed corrupt snapshot permits rediscovery only with false tombstone/full absence; post-managed/historical presence or any read error permanently blocks.
  - Scope: `pkg/clientcache/cache_test.go` plus existing controller status tests only if a controller fake is required; do not change production code.
  - Tests: `go test -race ./pkg/clientcache ./controllers/k8ssandra -run 'TestDirect|TestLegacyRFSnapshot'`.
  - Acceptance evidence: assertions capture call order and prove cache absence cannot authorize acceptance/creation.
  - Depends: POC-A-09 and POC-A-10.
