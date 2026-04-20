# PHASE 8 Sixth Runtime Observability Slice Apply Report

## 1. verdict

Apply is complete.

The frozen phase-8 sixth runtime observability slice is now reflected in repo SoT:
- the existing orchestrator-owned portfolio runtime run report remains the only persisted artifact in scope
- portfolio-level `portfolio_run_schema_version` advanced from `5` to `6`
- top-level `delegated_outcome_summary` is now emitted on both success and delegated failure paths
- `execution_summary` remains unchanged
- `failure_summary` remains unchanged
- `run_timing_summary` remains unchanged
- `data_freshness_summary` remains unchanged

## 2. frozen spec being applied

Frozen authority:
- `docs/sot/PHASE_8_SIXTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- spec commit: `3d08146ee7caf7b38adb0e55c2d463f79011a4d6`

Frozen target from that spec:
- add one compact top-level `delegated_outcome_summary` inside the existing orchestrator-owned portfolio runtime run report
- persist it on both success and delegated failure paths
- persist exactly these fields:
  - `delegated_outcome_summary_schema_version`
  - `delegated_scope`
  - `enabled_strategy_count`
  - `delegated_success_count`
  - `delegated_failure_count`
  - `delegated_outcome_status`
- keep persisted artifact path unchanged
- keep `execution_summary` unchanged
- keep `failure_summary` unchanged
- keep `run_timing_summary` unchanged
- keep `data_freshness_summary` unchanged
- keep whole-run normalized aggregation only

## 3. exact repo mutation set

Code apply:
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`

Report file added in the same cycle:
- `docs/sot/PHASE_8_SIXTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

No other file scope is part of this apply.

## 4. exact code-apply commit on main

Code apply commit on `main`:
- `e2537339b51ba61601049f960fb80fffa7950615` — `Add delegated outcome summary to portfolio runtime report`

This report file is a separate report-only commit after the code apply commit.

## 5. exact repo proof of frozen behavior on main

### 5.1 orchestrator ownership and artifact path proof

`src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py` proves:
- orchestrator remains the single persisted portfolio runtime run report producer
- persisted artifact path remains exactly:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`
- no second persisted report artifact path is introduced

### 5.2 versioning proof

That same file proves:
- `portfolio_run_schema_version = 6` on both success and delegated failure paths
- `delegated_outcome_summary_schema_version = 1`
- `delegated_scope = "portfolio_registered_strategies"`
- delegated success result `runtime_result_schema_version` remains unchanged
- `failure_summary_schema_version` remains unchanged at `1`
- `run_timing_summary_schema_version` remains unchanged at `1`
- `data_freshness_summary_schema_version` remains unchanged at `1`

### 5.3 delegated-outcome proof

That same file proves:
- `delegated_outcome_summary` is top-level and present on both success and delegated failure paths
- `enabled_strategy_count`, `delegated_success_count`, and `delegated_failure_count` are emitted as normalized integer counts
- `delegated_success_count + delegated_failure_count = enabled_strategy_count`
- `delegated_outcome_status` is emitted only as `all_succeeded`, `partial_failure`, `all_failed`, or `none_enabled`
- no per-strategy delegated payload list is introduced inside `delegated_outcome_summary`
- no strategy-specific metric widening beyond the frozen compact counts is introduced
- no duplication of `execution_summary`, `failure_summary`, `run_timing_summary`, or `data_freshness_summary` semantics is introduced

### 5.4 non-goal preservation proof

The apply keeps all explicit non-goals intact:
- no second persisted artifact
- no `execution_summary` redesign
- no `failure_summary` redesign
- no `run_timing_summary` redesign
- no `data_freshness_summary` redesign
- no notifier/dashboard/registry widening
- no external telemetry coupling

## 6. contract compliance statement

The landed code matches the frozen sixth-slice contract exactly:
- one compact top-level `delegated_outcome_summary` only
- whole-run normalized aggregation only
- success and delegated failure deterministic presence
- exact frozen field set only
- portfolio-level schema advanced exactly `5 -> 6`
- existing orchestrator-owned persisted report remains the only artifact in scope
- `execution_summary` unchanged
- `failure_summary` unchanged
- `run_timing_summary` unchanged
- `data_freshness_summary` unchanged

## 7. GitHub SoT vs server distinction

GitHub/repo is the architectural SoT and proves the apply contract.
Server validation is applied-state proof only and remains separate from repo proof.

## 8. owner-run validation still required

Full slice completion still requires owner-run applied-state proof showing a real persisted portfolio runtime run report with:
- `portfolio_run_schema_version = 6`
- top-level `delegated_outcome_summary` present
- `delegated_outcome_summary_schema_version = 1`
- `delegated_scope = "portfolio_registered_strategies"`
- valid `enabled_strategy_count`, `delegated_success_count`, and `delegated_failure_count`
- valid `delegated_outcome_status`
- unchanged prior summary structure for failure/timing/freshness

## 9. final statement

Phase-8 sixth runtime observability slice is fully applied in repo SoT and fully documented in repo SoT.
