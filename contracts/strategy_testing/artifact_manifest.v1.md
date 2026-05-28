# Artifact Manifest Contract v1

contract_id: artifact_manifest.v1
schema_version: v1
artifact_class: artifact_manifest
producer: moex_research.publishers.artifact_manifest
consumer: moex_research.registry / PM review

## purpose

Documents the PR 3 artifact manifest skeleton used to declare produced research/backtest artifacts.

## required_fields

Manifest fields:

- artifact_manifest_id
- run_id
- schema_version
- created_ts
- producer_component
- repo_commit
- artifacts

Artifact item fields:

- artifact_id
- artifact_role
- artifact_class
- producer
- consumer
- format
- schema_version
- path
- required_for_canonical

## validation_rules

- artifact_manifest_id must be non-empty and unique for the run.
- run_id must match the corresponding experiment registry entry.
- schema_version must identify this manifest schema version.
- created_ts must be present.
- producer_component must identify the publisher that created the manifest.
- repo_commit must identify the source commit.
- artifacts must be non-empty.
- artifact_id, artifact_role, artifact_class, producer, consumer, format, schema_version, path, and required_for_canonical must be present for every item.
- path must be explicit and must not point to stdout, latest folders, glob patterns, or directories.
- required_for_canonical must mark artifacts needed for canonical status.

## forbidden_patterns

- Stdout-only artifact references.
- Globbed artifact paths.
- latest-folder artifact discovery.
- Directory paths standing in for explicit artifact files.
- Artifact manifests that omit required canonical artifacts for canonical registry results.
