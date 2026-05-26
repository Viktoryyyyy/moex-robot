# D1 TSMOM Research Result Contract

status: design_contract
project: MOEX Bot
artifact_class: external_pattern
contract_version: d1_tsmom_research_result_contract.v1

## Purpose

This contract defines the formal output artifact requirements for D1 TSMOM research results. It prevents stdout-only acceptance and requires reproducible, manifest-bound result artifacts before a result can be treated as canonical.

This contract does not claim any research result and does not authorize runtime or live execution.

## Artifact class policy

Research reports and run outputs are external artifacts unless a future repo contract explicitly stores a report under a repo-relative path.

Allowed artifact classes:

```yaml
allowed_artifact_classes:
  - external_pattern
  - repo_relative
```

Rules:

- `external_pattern` is the default for generated research run outputs.
- `repo_relative` is allowed only if the repo intentionally stores a static report or contract artifact.
- No guessed absolute server path is part of this contract.
- A concrete runner must bind external artifacts through a declared path pattern, CLI argument contract, or environment/config contract before execution.

## Formal output artifacts

Every D1 TSMOM research run must produce or reference the following formal artifacts:

```yaml
formal_output_artifacts:
  run_metadata_json:
    file_name: run_metadata.json
    format: json
    required: true
  input_manifest_snapshot_or_hash:
    allowed_file_name: input_manifest_snapshot.json
    allowed_hash_reference: manifest_sha256
    format: json_or_sha256_reference
    required: true
  parameter_snapshot_json:
    file_name: parameter_snapshot.json
    format: json
    required: true
  dataset_references_json:
    file_name: dataset_references.json
    format: json
    required: true
  metrics_table_parquet:
    file_name: metrics_table.parquet
    format: parquet
    required: true
  trades_or_positions_table_parquet:
    file_name: trades_or_positions_table.parquet
    format: parquet
    required: true
  daily_equity_table_parquet:
    file_name: daily_equity_table.parquet
    format: parquet
    required: true
  diagnostics_table_parquet:
    file_name: diagnostics_table.parquet
    format: parquet
    required: true
  quality_gate_summary_json:
    file_name: quality_gate_summary.json
    format: json
    required: true
  report_markdown:
    file_name: report.md
    format: markdown
    required: true
```

Stdout may summarize these artifacts, but stdout is not a result artifact.

## Required metadata

`run_metadata.json` must include:

```yaml
required_metadata:
  accepted_commit: eb7271f16b98fb31b6cbc74ade71604ab5c25bbc
  research_input_contract_version: d1_tsmom_research_input_contract.v1
  backtest_semantics_contract_version: d1_tsmom_backtest_semantics_contract.v1
  dataset_references: required
  manifest_path: string
  manifest_sha256: string
  run_id: string
  run_status: string
  result_status: canonical | provisional | blocked
```

`dataset_references.json` must include one entry for every input dataset:

```yaml
dataset_reference_required_fields:
  dataset_id: string
  schema_version: string
  family_or_symbol: string
  manifest_path: string
  manifest_sha256: string
  run_id: string
  date_range:
    from: YYYY-MM-DD
    till: YYYY-MM-DD
  lineage_fields_if_applicable:
    source_contracts: optional_required_for_continuous
    roll_map_id: optional_required_for_continuous
    roll_policy_id: optional_required_for_continuous
    adjustment_policy_id: optional_required_for_continuous
```

Rules:

- Every input must declare `dataset_id` and `schema_version`.
- Si continuous inputs must declare source-contract and roll-map lineage.
- USDRUBF perpetual inputs must not be represented as continuous-roll inputs.
- FUTOI, if used, must be declared as `futures_futoi_5m_raw.v1` and must include timestamp/publication safety metadata.

## Parameter snapshot requirements

`parameter_snapshot.json` must include:

```yaml
parameter_snapshot_required_fields:
  strategy_family: D1_TSMOM
  tsmom_lookback: number
  min_history: number
  position_formation_rule: object
  execution_delay_semantics: object
  cost_model: object
  slippage_model: object
  terminal_close_rule: object
  zero_cost_or_default_values_declared: boolean
```

Rules:

- TSMOM lookback and minimum history must be declared before execution.
- Zero/default cost or slippage values are allowed only if explicitly declared.
- Parameter snapshots must be sufficient to reproduce the metrics from declared artifacts.

## Quality gate summary requirements

`quality_gate_summary.json` must include:

```yaml
quality_gate_summary_required_fields:
  d1_quality_status: pass | fail
  duplicate_timestamps: number
  invalid_ohlc: number
  off_calendar_dates: number
  missing_expected_trading_days: explicit_summary_or_rows
  partial_si_chain_gaps: not_bridged | blocked | not_applicable
  continuous_quality_report_fail_rows: number_or_null
  source_lineage_check: pass | fail | not_applicable
  usdrubf_identity_check: pass | fail | not_applicable
  final_quality_gate_verdict: pass | fail
```

Any failed required quality gate must set `result_status=blocked`.

## Table artifact requirements

```yaml
table_artifact_requirements:
  metrics_table.parquet:
    required_role: primary_metrics
    reproducible_from_declared_artifacts: true
  trades_or_positions_table.parquet:
    required_role: position_or_trade_path
    no_hidden_state: true
  daily_equity_table.parquet:
    required_role: daily_equity_curve
    no_stdout_substitute: true
  diagnostics_table.parquet:
    required_role: diagnostics_and_gate_context
    includes_roll_and_gap_diagnostics_if_applicable: true
```

Rules:

- All result tables must be written as declared artifacts.
- Metrics must be reproducible from declared artifacts.
- Tables must not depend on undeclared mutable files.
- Diagnostics must include roll-boundary and gap context when Si continuous input is used.

## Report requirements

`report.md` must include:

```yaml
report_required_references:
  accepted_commit: required
  research_input_contract_version: required
  backtest_semantics_contract_version: required
  manifest_path: required
  manifest_sha256: required
  run_id: required
  dataset_id_schema_version_for_every_input: required
  result_status: canonical | provisional | blocked
```

Rules:

- `report.md` must reference exact dataset refs and manifest hash.
- `report.md` must state whether the result is canonical, provisional, or blocked.
- `report.md` must not promote blocked or provisional outputs as canonical conclusions.

## Validation rules

```yaml
validation_rules:
  stdout_only_acceptance: forbidden
  all_result_tables_written_as_declared_artifacts: required
  report_references_exact_dataset_refs_and_manifest_hash: required
  metrics_reproducible_from_declared_artifacts: required
  failed_quality_gate_sets_result_status_blocked: required
```

Rules:

- No stdout-only acceptance is allowed.
- All required artifacts must exist or the run is blocked.
- All result tables must be written as declared artifacts.
- `report.md` must reference exact dataset refs and manifest hash.
- Metrics must be reproducible from declared artifacts.
- Any failed quality gate must set `result_status=blocked`.

## Result status rule

```yaml
result_status_rule:
  canonical:
    allowed_when: all input, quality, semantics, and artifact gates pass
  provisional:
    allowed_when: no required gate fails, but semantics or data binding is incomplete
  blocked:
    required_when: any required input, quality, semantics, or artifact gate fails
```

A canonical result cannot exist without the formal artifacts declared in this contract.

## Acceptance boundary

This contract is satisfied only when a D1 TSMOM research run writes or references all declared artifacts and its report binds the result to exact inputs, parameters, quality gates, and semantics. It does not itself create research outputs.
