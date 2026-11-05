# Contract Registry

This directory is the **authoritative index** of all contract artifacts in the
repository. The canonical registry file is
[`contract_registry.v1.yaml`](./contract_registry.v1.yaml).

## Purpose

`contract_registry.v1.yaml` is the single source of truth for locating,
versioning and resolving contract artifacts. It records each contract's
canonical path, version, scope/lane, supersession relationships, implementation
references, enforcement test references, authority notes and overlap notes.

## Status vocabulary

Each contract entry carries exactly one status from the following vocabulary:

| Status       | Meaning                                                              |
| ------------ | -------------------------------------------------------------------- |
| `active`     | Currently authoritative and in force.                                |
| `superseded` | Replaced by a newer contract; retained for history.                  |
| `deprecated` | No longer recommended; retained for history.                         |
| `historical` | No longer in force; retained for audit/history only.                 |

## Required fields

Every contract entry must define:

- `contract_id` — stable unique identifier
- `canonical_path` — repo-relative path to the authoritative artifact
- `version` — contract version (e.g. `v1`)
- `scope` — category/lane (architecture, datasets, runtime, experiments,
  features, instruments, registries, ...)
- `supersedes` — contract_id(s) this entry replaces (empty if none)
- `superseded_by` — contract_id(s) that replace this entry (empty if none)
- `implementation_refs` — repo-relative paths to implementation artifacts
- `enforcement_test_refs` — repo-relative paths to enforcement/validation tests
- `authority` — authority/runtime notes for this contract
- `duplicate_overlap_notes` — notes on YAML/MD overlap resolution

## Authority rules for YAML/MD overlaps

When a `.yaml` and a `.md` contract cover the same subject, the following rules
apply:

1. **YAML is authoritative by default.** A `.yaml` contract is the machine
   readable, canonical definition. Any `.md` narrative covering the same
   subject must defer to the YAML for schema, semantics and validation.
2. **MD is authoritative only when no YAML exists** for the subject, or when
   the registry explicitly marks the MD as the canonical artifact (e.g. an
   experiment semantics contract that is narrative by nature).
3. **Precedence** is resolved per entry via `canonical_path` and
   `duplicate_overlap_notes`. The entry with the authoritative `canonical_path`
   wins; the other is treated as a companion narrative.
4. **Supersession** is expressed via `supersedes` / `superseded_by`. A
   superseded contract is retained for history but is no longer authoritative.
5. **Conflicts** between a YAML and an MD on the same subject are resolved in
   favor of the YAML unless the registry explicitly designates the MD as
   canonical for that subject.

## Incremental population

This registry is a **skeleton**. It seeds representative entries across
architecture, datasets, runtime, experiments and registries using evidence from
the Phase 1 audit. Later Phase 2A batches will populate the remaining contract
artifacts incrementally. Do not treat the current entry set as exhaustive.
