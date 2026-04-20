# PHASE 8 Fifth Runtime Observability Slice Apply Report

## 1. verdict

Apply is complete.

The frozen phase-8 fifth runtime observability slice is now reflected in repo SoT:
- the existing orchestrator-owned portfolio runtime run report remains the only persisted artifact in scope
- portfolio-level `portfolio_run_schema_version` advanced from `4` to `5`
- top-level `data_freshness_summary` is now emitted on both success and delegated failure paths
- `execution_summary` remains unchanged
- `failure_summary` remains unchanged
- `run_timing_summary` remains unchanged

## 2. frozen spec being applied

Frozen authority:
- `docs/sot/PHASE_8_FIFTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- spec commit: `03f21aafbdb3edc132431cf6cab3b13ca6addbee`

Frozen target from that spec:
- add one compact top-level `data_freshness_summary` inside the existing orchestrator-owned portfolio runtime run report
- persist it on both success and delegated failure paths
- persist exactly these fields:
  - `data_freshness_summary_schema_version`
  - `freshness_scope`
  - `freshness_coverage_status`
  - `oldest_latest_bar_end_utc`
  - `newest_latest_bar_end_utc`
  - `latest_bar_end_span_seconds`
- keep persisted artifact path unchanged
- keep `execution_summary` unchanged
- keep `failure_summary` unchanged
- keep `run_timing_summary` unchanged
- keep aggregation whole-run only

## 3. exact repo mutation set

Code apply:
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`

Report file added in the same cycle:
- `docs/sot/PHASE_8_FIFTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

No other file scope is part of this apply.

## 4. exact code-apply commit on main

Code apply commit on `main`:
- `3b234c2eec9d29714e624be39ccb856eaf393551` — `Add data freshness summary to portfolio runtime report`

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
- `portfolio_run_schema_version = 5` on both success and delegated failure paths
- delegated success result `runtime_result_schema_version` remains unchanged at `3`
- `failure_summary_schema_version` remains unchanged at `1`
- `run_timing_summary_schema_version` remains unchanged at `1`

### 5.3 data-freshness proof

That same file proves:
- `data_freshness_summary` is top-level and present on both success and delegated failure paths
- `data_freshness_summary_schema_version = 1`
- `freshness_scope = "portfolio_runtime_inputs"`
- freshness aggregation uses only delegated successful `result.latest_bar_end` values already present in completed delegated results
- `freshness_coverage_status` is emitted only as `complete`, `partial`, or `unavailable`
- `oldest_latest_bar_end_utc` and `newest_latest_bar_end_utc` are populated only when coverage is not `unavailable`
- `latest_bar_end_span_seconds` is a non-negative JSON numeric value only when coverage is not `unavailable`
- no per-strategy list duplication is introduced
- no per-step telemetry is introduced

### 5.4 non-goal preservation proof

The apply keeps all explicit non-goals intact:
- no second persisted artifact
- no `execution_summary` redesign
- no `failure_summary` redesign
- no `run_timing_summary` redesign
- no notifier/dashboard/registry widening
- no external telemetry coupling

## 6. contract compliance statement

The landed code matches the frozen fifth-slice contract exactly:
- one compact top-level `data_freshness_summary` only
- whole-run normalized aggregation only
- success and delegated failure deterministic presence
- exact frozen field set only
- portfolio-level schema advanced exactly `4 -> 5`
- existing orchestrator-owned persisted report remains the only artifact in scope
- `execution_summary` unchanged
- `failure_summary` unchanged
- `run_timing_summary` unchanged

## 7. GitHub SoT vs server distinction

GitHub/repo is the architectural SoT and proves the apply contract.
Server validation is applied-state proof only and remains separate from repo proof.

## 8. owner-run validation still required

Full slice completion still requires owner-run applied-state proof showing a real persisted portfolio runtime run report with:
- `portfolio_run_schema_version = 5`
- top-level `data_freshness_summary` present
- `freshness_scope = "portfolio_runtime_inputs"`
- populated `freshness_coverage_status`
- populated `oldest_latest_bar_end_utc`, `newest_latest_bar_end_utc`, and `latest_bar_end_span_seconds` when coverage is not `unavailable`
- unchanged execution/failure/timing summary structure

## 9. final statement

Phase-8 fifth runtime observability slice is fully applied in repo SoT and fully documented in repo SoT.
