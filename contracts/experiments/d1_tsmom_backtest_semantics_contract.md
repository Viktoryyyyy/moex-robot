# D1 TSMOM Backtest Semantics Contract

status: design_contract
project: MOEX Bot
timeframe: D1
contract_version: d1_tsmom_backtest_semantics_contract.v1

## Purpose

This contract defines the minimal anti-leakage-safe D1 backtest and execution semantics required before the first canonical D1 TSMOM research design. It defines timing, data validity, roll behavior, and result-status rules. It does not implement a strategy, runner, or backtest engine.

## Signal timestamp semantics

```yaml
signal_timestamp_semantics:
  timeframe: D1
  signal_row_date: D
  signal_inputs_allowed: data known no later than the finalized D close and declared publication-safety cutoffs
  signal_materialization_point: after D bar is final and all explicitly referenced inputs are known
  signal_for_same_day_pre_close_execution_allowed: false
```

Rules:

- A signal timestamped on D may use D close only after the D bar is final.
- A signal timestamped on D must not be used for D pre-close execution.
- Any external or enrichment input must declare `known_by_when` before it can affect the signal.

## Known-by-when

```yaml
known_by_when:
  d1_ohlcv: after_final_d1_bar_for_D
  si_roll_map: as_pinned_in_manifest_no_future_mutable_lookup
  quality_gates: as_pinned_in_manifest_no_future_mutable_lookup
  futoi_if_used: explicit_publication_timestamp_and_join_rule_required
```

Rules:

- `known_by_when` must be included in the parameter or run metadata snapshot.
- FUTOI is unavailable to the model unless explicitly referenced and timestamp-safe.
- Future updates to roll maps, quality reports, or manifests outside the pinned manifest must not alter the research result.

## Earliest executable point

```yaml
earliest_executable_point:
  default: next_trading_session_after_signal_day
  earliest_bar: next_session_open_or_declared_execution_bar
  same_day_pre_close_execution: forbidden
```

A D1 TSMOM signal using D close may first be executed no earlier than the next eligible trading session after D, unless a stricter delay is declared in the parameter snapshot.

## Execution delay semantics

```yaml
execution_delay_semantics:
  default_delay: one_trading_session_after_signal_materialization
  delay_unit: trading_session
  calendar_source: pinned_manifest_or_declared_calendar_contract
  non_trading_day_behavior: advance_to_next_eligible_trading_session
```

Rules:

- Execution delay must be measured in trading sessions, not naive calendar days.
- Non-trading days must not be counted as executable sessions.
- Any alternate delay must be declared in `parameter_snapshot.json`.

## Position formation rule placeholder

```yaml
position_formation_rule_placeholder:
  required_before_execution: true
  allowed_output: target_position_or_target_weight
  required_fields:
    - signal_value
    - lookback
    - threshold_or_ranking_rule
    - direction_rule
    - sizing_rule
    - max_position
    - rebalance_rule
```

Rules:

- The position formation rule is intentionally a placeholder in this contract.
- It must be made explicit in the concrete D1 TSMOM research design before execution.
- No default position formation rule may be inferred by the backtest runner.

## Missing bar behavior

```yaml
missing_bar_behavior:
  missing_signal_input_bar: block_signal_for_date
  missing_execution_bar: skip_or_delay_only_if_declared
  missing_expected_trading_day: quality_gate_explicit
```

Rules:

- Missing bars must not be forward-filled for signal construction unless explicitly declared and separately diagnosed.
- Missing execution bars must not be treated as zero-return bars unless declared.
- Missing expected trading days must be captured in the quality gate summary.

## Invalid data behavior

```yaml
invalid_data_behavior:
  invalid_ohlc: block
  duplicate_timestamp: block
  off_calendar_date: block
  non_monotonic_timestamp: block
  negative_volume: block
```

Any invalid required input data must set `result_status=blocked` unless the row is excluded by a predeclared, reproducible quality filter and the remaining history still satisfies declared `min_history`.

## Roll boundary behavior for Si continuous

```yaml
si_continuous_roll_boundary_behavior:
  dataset_id: futures_continuous_d1
  roll_policy_id: expiration_minus_1_trading_session_v1
  adjustment_policy_id: unadjusted_v1
  roll_boundary_field_required: true
  source_contract_field_required: true
  roll_map_id_required: true
  excluded_gap_bridging_allowed: false
```

Rules:

- Si continuous D1 must preserve roll-boundary visibility.
- Returns across roll boundaries are allowed only under the declared unadjusted continuous policy and must remain diagnosable.
- Excluded SiH7/SiM7 gaps must not be bridged silently.
- A run must be blocked if Si continuous input lacks source-contract or roll-map lineage required by the input contract.

## USDRUBF perpetual identity behavior

```yaml
usdrubf_perpetual_identity_behavior:
  dataset_id: futures_derived_d1_ohlcv
  schema_version: futures_derived_d1_ohlcv.v1
  instrument_type: perpetual_future
  continuous_roll_semantics: forbidden
  identity_mapping_required: true
```

Rules:

- USDRUBF is treated as a perpetual raw-derived D1 series.
- Roll boundary fields and roll adjustment fields must not be required for USDRUBF.
- If a research package includes both Si and USDRUBF, their dataset semantics must remain separated.

## Cost and slippage placeholders

```yaml
cost_slippage_placeholders:
  required_before_execution: true
  required_fields:
    commission_model_id: string
    commission_value: number
    slippage_model_id: string
    slippage_value: number
    spread_assumption: number_or_null
    execution_price_field: string
  zero_or_default_values_allowed_only_if_declared_in_parameter_snapshot: true
```

Rules:

- Cost and slippage values may be zero only if explicitly declared in `parameter_snapshot.json`.
- No implicit zero-cost default is allowed.
- Metrics must identify whether they are gross, net of costs, or both.

## Terminal close rule

```yaml
terminal_close_rule:
  required_before_execution: true
  allowed_modes:
    - force_close_on_last_available_bar
    - leave_open_and_mark_unrealized
  declaration_location: parameter_snapshot.json
```

The selected terminal close rule must be explicit before execution and must be referenced by `report.md`.

## No-lookahead rules

```yaml
no_lookahead_rules:
  d_day_close_for_d_day_pre_close_execution: forbidden
  future_returns_in_feature_construction: forbidden
  future_roll_quality_knowledge_beyond_pinned_manifest: forbidden
  hidden_futoi: forbidden
```

Mandatory rules:

- No D-day close may be used for D-day pre-close execution.
- No future returns may be used in feature construction.
- No future roll-map or quality knowledge beyond the pinned manifest may be used.
- No FUTOI may be used unless explicitly referenced and timestamp-safe.

## Result status rule

```yaml
result_status_rule:
  canonical: all input, quality, semantics, and artifact gates pass
  provisional: any semantics or data binding is incomplete but no required gate failed
  blocked: any required gate fails
```

Rules:

- `canonical` is allowed only if all input, quality, semantics, and artifact gates pass.
- `provisional` is required if any semantics or data binding is incomplete.
- `blocked` is required if any required gate fails.
- A blocked result must not be promoted to a research conclusion.

## Acceptance boundary

This contract is satisfied only when a concrete D1 TSMOM research design declares timing, position formation, costs, terminal handling, quality behavior, and no-lookahead controls before execution.
