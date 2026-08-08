---
phase: poc
wave: A
todos: [POC-A-09, POC-A-10]
---

# Wave A — Authoritative Remote Reads

- [x] **POC-A-09 — Add cached/direct remote client-pair contract** → modify: pkg/clientcache/cache.go
  - Requirements: retain existing cached clients while exposing a direct API reader for every local/remote context; constructor rejects missing dependencies; direct absence reads must bypass informer cache and return errors rather than infer absence.
  - Scope: only `pkg/clientcache/cache.go`; preserve existing callers and multi-cluster behavior.
  - Tests: POC-T-03 proves cached staleness cannot authorize absence and unknown/unavailable contexts fail closed.
  - Acceptance evidence: focused package tests show separate cached/direct paths and backward-compatible existing methods.
  - Depends: POC-A-01 location representation.

- [x] **POC-A-10 — Define direct managed-state survey semantics** → new: controllers/k8ssandra/legacy_rf_discovery_status.go
  - Requirements: introduce explicit, lifecycle-accurate status/absence operations and `ManagedDatacenterState.AssertAbsent`; union current, accepted, and append-only history locations; check exact DC plus correlated Cassandra pods; any API error/presence blocks; encode acceptance CAS/read-back and tombstone/history monotonicity without hiding non-atomic ordering.
  - Scope: only `controllers/k8ssandra/legacy_rf_discovery_status.go`; no main controller wiring or resource creation.
  - Tests: POC-T-03 covers location union, stale cache, partial context outage, pod-only presence, tombstone/history no-prune, CAS conflicts, and hash read-back.
  - Acceptance evidence: tests prove removal/decommission does not erase the historical search domain and unavailable history never permits rediscovery.
  - Depends: POC-A-01, POC-A-03, and POC-A-09.
