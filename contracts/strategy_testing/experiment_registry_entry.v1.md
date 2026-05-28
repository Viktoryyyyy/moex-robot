# Experiment Registry Entry Contract v1

contract_id: experiment_registry_entry.v1
schema_version: v1
artifact_class: experiment_registry_entry
producer: moex_research.registry
consumer: PM review / future comparison tooling

## purpose

Documents the PR 3 experiment registry entry skeleton used for strategy test result registration.

## required_fields

- registry_entry_id
- run_id
- strategy_id
- strategy_version
- test_type
- instrument_scope
- timeframe_scope
- run_status
- result_status
- canonicality_status
- artifact_manifest_ref
- repo_commit
- created_ts
- metrics
- promotion_verdict_ref

## validation_rules

- registry_entry_id must be unique.
- run_id must match the artifact manifest run_id.
- strategy_id and strategy_version must match the tested strategy manifest.
- test_type must be explicit.
- instrument_scope and timeframe_scope must be non-empty.
- run_status must be one of planned, executed, blocked, failed, invalidated, or superseded.
- result_status must be one of supported_canonical, not_supported_canonical, supported_provisional, not_supported_provisional, blocked, or invalidated.
- canonicality_status must be one of canonical, provisional, non_canonical, or blocked.
- artifact_manifest_ref must match artifact_manifest_id.
- repo_commit and created_ts must be present.
- metrics may contain quantitative results but must not contain promotion verdicts.
- promotion_verdict_ref is optional and must be a separate reference, not embedded metrics.

## forbidden_patterns

- Promotion verdict embedded in metrics.
- Canonical result status without required canonical artifacts in the artifact manifest.
- Missing artifact_manifest_ref.
- Registry entries that rely on stdout-only results.
- Registry entries that infer artifact paths from latest folders.
