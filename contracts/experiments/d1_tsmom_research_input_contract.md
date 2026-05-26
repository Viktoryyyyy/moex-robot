# D1 TSMOM Research Input Contract

status: design_contract
project: MOEX Bot
artifact_class: repo_relative
contract_version: d1_tsmom_research_input_contract.v1

## Purpose

This contract defines the minimal pinned input requirements for the first canonical D1 TSMOM research design after the accepted futures data lake closeout. It binds any D1 TSMOM research run to an explicit data manifest reference and forbids implicit dataset discovery.

This contract does not implement a strategy, runner, backtest, data lake mutation, or research result.

## Accepted data lake baseline

accepted_data_lake_commit: eb7271f16b98fb31b6cbc74ade71604ab5c25bbc
accepted_data_lake_status: accepted_final

The latest accepted universal daily refresh manifest is an external artifact reference only and must not be hardcoded into code:

```text
futures/runs/universal_daily_refresh/run_date=2026-05-25/manifest.json
```

## Required input reference object

Every D1 TSMOM research run must declare a `data_manifest_ref` object before execution.

Required fields:

```yaml
data_manifest_ref:
  accepted_commit: eb7271f16b98fb31b6cbc74ade71604ab5c25bbc
  dataset_id: string
  schema_version: string
  run_date: YYYY-MM-DD
  snapshot_date: YYYY-MM-DD
  run_id: string
  manifest_path: string
  manifest_sha256: string
  family_or_symbol: string
  date_range:
    from: YYYY-MM-DD
    till: YYYY-MM-DD
```

Rules:

- `manifest_path` must be a repo-declared artifact contract reference or an explicit external artifact reference.
- `manifest_sha256` is mandatory for canonical result status.
- `family_or_symbol` must bind the input to the intended instrument or family.
- `date_range` must be explicit and must not be inferred from files present on disk.

## USDRUBF source rule

USDRUBF is a perpetual futures input and must be treated separately from expiring-contract continuous series.

```yaml
usdrubf_source_rule:
  dataset_id: futures_derived_d1_ohlcv
  schema_version: futures_derived_d1_ohlcv.v1
  instrument_type: perpetual_future
  continuous_fields_allowed: false
  roll_fields_allowed: false
```

Required behavior:

- USDRUBF D1 research input must use raw/perpetual-derived D1 OHLCV semantics.
- USDRUBF must not use continuous roll fields.
- USDRUBF must not be silently mapped through Si continuous-series rules.

## Si source rule

Si D1 TSMOM research input must use the accepted continuous D1 dataset and retain raw-lineage traceability.

```yaml
si_source_rule:
  dataset_id: futures_continuous_d1
  schema_version: futures_continuous_d1.v1
  roll_policy_id: expiration_minus_1_trading_session_v1
  adjustment_policy_id: unadjusted_v1
  required_run_gates:
    daily_refresh_result_verdict: pass
    artifact_validation_status: pass
    continuous_quality_report_fail_rows: 0
    source_lineage_check: pass
    usdrubf_identity_check: pass
    partial_chain_gap_summary: explicit
```

Required behavior:

- Si continuous D1 must remain traceable to raw contract inputs through `source_contracts` and `roll_map_id`.
- Roll and adjustment policy IDs must be captured in the run metadata and dataset references.
- Excluded SiH7/SiM7 gaps must not be silently bridged.
- Any partial-chain gap handling must be explicit in `partial_chain_gap_summary`.

## Raw contracts behind Si

The following lineage fields are mandatory for Si continuous D1 research inputs:

```yaml
raw_lineage_requirements:
  source_contracts: required
  roll_map_id: required
  roll_policy_id: required
  adjustment_policy_id: required
  excluded_gap_policy: explicit
```

Rules:

- Raw contracts remain the audit source behind the continuous series.
- Continuous data is not a replacement for raw data lineage.
- Missing or excluded raw-contract spans must produce explicit diagnostics.
- The SiH7/SiM7 excluded gaps must remain excluded or explicitly documented; they must not be filled by generic forward fill, backward fill, interpolation, or neighbor-contract bridging.

## FUTOI rule

FUTOI may be used only as optional enrichment.

```yaml
futoi_rule:
  usage: optional_enrichment_only
  hidden_input_allowed: false
  required_dataset_if_used: futures_futoi_5m_raw
  required_schema_version_if_used: futures_futoi_5m_raw.v1
  required_safety_fields_if_used:
    - source_timestamp
    - publication_timestamp
    - known_by_when
    - join_rule
```

Rules:

- FUTOI must not be used as a hidden input.
- If used, it must appear in `dataset_references.json`, `parameter_snapshot.json`, and `report.md`.
- If used for D1 features, timestamp/publication safety must prove the value was known before the declared signal timestamp.

## Forbidden dataset selection

The following dataset selection modes are forbidden:

```yaml
forbidden_dataset_selection:
  - latest
  - current
  - default run_date
  - directory scan
  - unpinned glob
```

A research run must fail closed if any input path, manifest, dataset version, run date, or instrument binding is selected through these modes.

## Warmup or minimum history

Before execution, the research design must declare:

```yaml
warmup_or_min_history:
  tsmom_lookback: required
  min_history: required
  declared_before_execution: true
  fail_closed_if_available_history_below_min_history: true
```

Rules:

- TSMOM lookback must be declared in the parameter snapshot before execution.
- Minimum usable D1 history after quality filters must be declared before execution.
- Research must fail closed if available D1 history after quality filters is below declared `min_history`.
- Warmup rows must not be included in performance metrics unless explicitly declared as eligible by the result contract.

## Required quality gates

The following gates are mandatory for canonical research result status:

```yaml
required_quality_gates:
  d1_quality_status: pass
  duplicate_timestamps: 0
  invalid_ohlc: 0
  off_calendar_dates: 0
  missing_expected_trading_days: explicit
  partial_si_chain_gaps: not_bridged
```

Rules:

- Missing expected trading days must be listed or summarized explicitly.
- Partial Si chain gaps must not be silently bridged.
- Any failed required gate must set the result status to `blocked` under the result contract.

## Acceptance boundary

This contract is satisfied only when a D1 TSMOM research design references explicit, pinned input artifacts and declares all required gates before execution. It does not authorize a research run or strategy conclusion by itself.
