# Phase 2.9 — D1 panel build plan interface

Status: implemented planning interface

Project: MOEX Bot
Lane: ema_3_19_ai
Task: ema_3_19_ai_market_phase_phase_2_9_d1_panel_build_plan_interface

## Purpose

Phase 2.9 adds a small pure-code, test, and documentation layer for a future PIT-safe D1 panel materialization plan.

This phase is a planning interface only. It does not build data and does not authorize data work.

## New files

Exact files added by this phase:

- `src/moex_data/futures/phase2_d1_panel_build_plan.py`
- `tests/ema_3_19_ai/test_phase2_d1_panel_build_plan.py`
- `docs/sot/strategies/ema_3_19_ai/phase2_d1_panel_build_plan_v1.md`

No existing files are changed by this phase.

## Relationship to Phase 2.8 materialization gate

The new module imports and calls `build_materialization_gate_report(repo_root)` from `src/moex_data/futures/phase2_materialization_gate.py`.

The Phase 2.8 gate remains the repository-file readiness gate. Phase 2.9 uses the gate result as one required precondition, but still keeps the panel plan blocked until explicit PM L2 data-build approval is present.

Required gate status in the plan:

```text
plan_status: blocked_pending_data_build_approval
materialization_gate.status: blocked_pending_data_build_approval
materialization_gate.gate_passed: true
```

## Future target panel

Future target panel id:

```text
usdrubf_phase2_d1_panel.v1
```

Source dataset:

```text
futures_raw_5m.v1
```

Target grain:

```text
one row per trade_date per canonical instrument
```

This phase does not define a data root, generated output path, or write target.

## PIT rules and forecast anchor

Forecast anchor:

```text
06:00 Europe/Moscow
```

PIT cutoff rule:

```text
availability_ts_utc <= forecast_anchor_ts
```

D1 availability rule:

```text
D1 trade_date T available no earlier than T+1 06:00 Europe/Moscow
```

The future panel must exclude or delay any source row whose `availability_ts_utc` is not known by the forecast anchor.

## Label and EMA roles

Label role:

```text
y_only_not_feature
```

Labels may be used only as y/target material for research validation. They must not enter feature columns.

EMA role:

```text
context_diagnostic_only_not_label_source
```

EMA 3/19 crossover context remains a diagnostic or context field. It is not a label source and is not allowed to leak future phase outcomes.

CBR official USD/RUB role:

```text
reference_only_not_causal_market_input
```

The official CBR USD/RUB rate may be treated as a reference indicator only. It must not be framed as a causal market input for same-day ruble movement.

## Blocked external sources

The following sources remain blocked for the D1 panel plan:

- FUTOI source contract
- oil
- DXY
- CNY/RUB proxy
- USD/RUB spot proxy
- news raw ingestion
- news LLM classification

FUTOI status:

```text
blocked_until_provider_timestamp_schema_revision_policy
```

## Conditions before any real data build

All of the following must be satisfied before a real D1 panel data build can be considered:

- materialization_gate_passed
- explicit_PM_L2_data_build_approval
- server_apply_window_if_needed
- data_root_defined_by_config_or_runtime_context
- no_label_leakage_validation
- PIT availability validation
- output_schema_validation

Passing the Phase 2.8 materialization gate is necessary but not sufficient. Explicit PM L2 approval is still required before any data build.

## Explicit non-authorization

This phase performs and authorizes none of the following:

- no ingestion
- no backfill
- no materialization run
- no feature computation
- no modeling
- no prediction
- no server apply
- no runtime commands
- no market data loading
- no network calls
- no output file writes
- no generated data paths

The module returns a plain dictionary only. It is deterministic, uses stdlib only, and reads only repository files from the supplied `repo_root`.
