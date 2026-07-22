# Route B Context Registry

Status: active SoT package
Project: MOEX Bot
Scope: Route B project-management context resolution

This package defines the GitHub-resolvable context references used by Route B orchestration.

## Purpose

Route B requests must pass compact dynamic handoffs plus context references. n8n workers and PM/sub-chat handoffs must resolve those references through this repo package instead of pasting full static or role context into each request.

## Registry

Machine-readable registry:

- `docs/sot/context/registry.route_b.v1.yaml`

The registry maps:

- `static_context_refs` to files under `docs/sot/context/static/`
- `role_context_refs` to files under `docs/sot/context/roles/`
- request/return schema docs to files under `docs/sot/context/schemas/`

## Resolution rule

A Route B resolver must:

1. Read `docs/sot/context/registry.route_b.v1.yaml` from the repo ref selected by the workflow run.
2. Resolve only repo-relative paths listed in the registry.
3. Reject absolute paths, server paths, implicit file discovery, and unresolved context refs.
4. Keep GitHub/repo as Source of Truth and Postgres as workflow state/evidence store.

## Route B chain

`PM L2 -> PM L3 -> Sub-chat -> PM L3 -> PM L2`

Sub-chat output returns to PM L3. PM L3 validation/evidence returns to PM L2.

## Boundary

This package does not define n8n workflow JSON, Postgres DDL, trading strategy logic, runtime/live behavior, broker integration, or server execution.

## Note

- The status of the package should be clearly defined as 'active'.

## GitHub Workflow

This section describes the workflow involving PM Lead and github_worker:

1. **PM Lead** initiates the workflow by defining the task and its scope.
2. **github_worker** executes the task as per the defined scope.
3. Any discrepancies or issues are reported back to the PM Lead for review.
4. Upon approval, the workflow continues as per the defined processes.