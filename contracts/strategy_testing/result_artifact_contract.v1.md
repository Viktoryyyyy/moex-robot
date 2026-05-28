# Result Artifact Contract v1

contract_id: result_artifact_contract.v1
schema_version: v1
artifact_class: result_artifact_contract
producer: moex_research.publishers / moex_backtest.reports
consumer: moex_research.registry / PM review

## purpose

Declares required outputs for a reproducible strategy test result package.

## required_fields

- result_artifact_contract_id
- schema_version
- artifact_class
- producer
- consumer
- run_metadata_artifact
- parameter_snapshot_artifact
- metrics_artifact
- primary_result_artifact
- diagnostics_artifacts
- artifact_manifest_ref
- registry_entry_ref
- repo_commit

## validation_rules

- run_metadata_artifact must identify run_id, strategy_id, strategy_version, test_type, and created_ts.
- parameter_snapshot_artifact must capture all configurable strategy and backtest parameters.
- metrics_artifact must be machine-readable and must not contain promotion verdict fields.
- primary_result_artifact must distinguish primary research labels from secondary execution-compatible labels.
- diagnostics_artifacts must be explicit and may include fragility, subperiod, cost, and quality diagnostics.
- artifact_manifest_ref must point to artifact_manifest.v1.
- registry_entry_ref must point to experiment_registry_entry.v1.
- repo_commit must identify the source commit that produced the result package.

## forbidden_patterns

- Stdout-only results standing in for formal artifacts.
- Report-only conclusions without machine-readable artifacts.
- Metrics artifacts that embed promotion verdicts.
- Undeclared charts, tables, or files consumed by PM review.
- Result paths inferred by latest-folder selection.
