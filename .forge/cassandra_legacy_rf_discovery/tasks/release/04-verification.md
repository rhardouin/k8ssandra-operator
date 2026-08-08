---
phase: release
wave: Test
todos: [REL-V-01, REL-V-02, REL-V-03]
---

# Final Test Wave — Release Verification

- [x] **REL-V-01 — Run full source, generation, lint, unit, race, envtest, render, and docs gates** → modify: Makefile
  - Requirements: use existing targets where possible; add only a narrowly named aggregate target if needed; require clean second generation, formatting/vet/lint, unit+race+envtest, manifest/Helm schema checks, and Hugo production build. Investigate every failure to root cause rather than accepting flaky reruns.
  - Scope: `Makefile` only if orchestration is missing; otherwise record commands without changing it.
  - Tests: execute all final commands from a clean-enough working tree while preserving unrelated changes.
  - Acceptance evidence: command/result manifest with versions, exit codes, coverage (>=60% or explicit justification), and RCA/fix evidence for any initial failure.
  - Delivered evidence: generation was idempotent; `make test` (including envtest), focused races, `go vet ./...`, lint, Kustomize/Helm render parity, Hugo production build, and `git diff --check` passed. This local evidence does not establish production-image or Cassandra lifecycle proof.
  - Depends: all implementation and docs tasks.

- [ ] **REL-V-02 — Run the complete finite packaged lifecycle and upgrade gates** → modify: test/e2e/suite_test.go
  - Requirements: wire exact selection of REL-T-01..08 into the release suite; fail when any of the three declared Cassandra 4.0.17, 4.1.9, or 5.0.6 scenarios is skipped, when RollingUpdate/leader-election/webhook safety is absent, or when artifact/redaction validation fails. Do not add a digest gate that the repository does not otherwise enforce.
  - Scope: only `test/e2e/suite_test.go`; do not broaden unrelated e2e selection.
  - Tests: execute old/new upgrade plus the three named Cassandra version-line scenarios through the existing per-test CI model.
  - Acceptance evidence: suite summary reports three declared/executed/passed Cassandra scenarios, package variants, artifact paths, and no skipped mandatory case.
  - Depends: REL-V-01 and all Release Test tasks.

- [ ] **REL-V-03 — Complete ensemble quality/security/documentation review with traceability** → new: .forge/cassandra_legacy_rf_discovery/verification_traceability.md
  - Requirements: map every PRD FR/AC, analysis invariant/edge/security boundary, design contract, task checkbox, and PLAN/IMPLEMENT/TEST/DOCUMENT quality gate to file:line and test/artifact evidence; resolve all CRITICAL/HIGH/MEDIUM findings; list only genuine LOW follow-ups.
  - Scope: only verification traceability artifact; reviewers remain read-only except targeted fix loop.
  - Tests: automated check for missing IDs plus four-reviewer cross-read agreement.
  - Acceptance evidence: complete matrix, zero unresolved blocking findings, full task/YAML/checkbox parity, and explicit statement separating local/rendered evidence from live lifecycle evidence and from unproved ring/repair/data health.
  - Depends: REL-V-01/02 and REL-D-04.
