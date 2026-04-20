# PHASE 8 Fourth Runtime Observability Slice Apply Report

## 1. verdict

Apply is complete.

The frozen phase-8 fourth runtime observability slice is now reflected in repo SoT:
- the existing orchestrator-owned portfolio runtime run report remains the only persisted artifact in scope
- portfolio-level `portfolio_run_schema_version` advanced from `3` to `4`
- top-level `run_timing_summary` is now emitted on both success and delegated failure paths
- `execution_summary` remains unchanged
- `failure_summary` remains unchanged

## 2. frozen spec being applied

Frozen authority:
- `docs/sot/PHASE_8_FOURTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- spec commit: `18bcdb5a8256addd9ce35228d0eb362b2b0786ff`

Frozen target from that spec:
- add one compact top-level `run_timing_summary` inside the existing orchestrator-owned portfolio runtime run report
- persist it on both success and delegated failure paths
- persist exactly these fields:
  - `run_timing_summary_schema_version`
  - `timing_scope`
  - `wall_clock_duration_seconds`
- keep persisted artifact path unchanged
- keep `execution_summary` unchanged
- keep `failure_summary` unchanged
- keep whole-run timing only

## 3. exact repo mutation set

Code apply:
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`

Report file added in the same cycle:
- `docs/sot/PHASE_8_FOURTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

No other file scope is part of this apply.

## 4. exact code-apply commit on main

Code apply commit on `main`:
- `a7ab4815cb71359c8baae820382a74f6d0836bbc` — `Add run timing summary to portfolio runtime report`

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
- `portfolio_run_schema_version = 4` on both success and delegated failure paths
- delegated success result payload ownership remains unchanged
- `failure_summary` ownership and schema version remain unchanged

### 5.3 run-timing proof

That same file proves:
- `run_timing_summary` is top-level and present on both success and delegated failure paths
- `run_timing_summary_schema_version = 1`
- `timing_scope = "portfolio_run_wall_clock"`
- `wall_clock_duration_seconds` is derived from top-level `started_at_utc` and `completed_at_utc`
- `wall_clock_duration_seconds` is emitted as a JSON numeric value
- `run_timing_summary` does not repeat `started_at_utc`
- `run_timing_summary` does not repeat `completed_at_utc`

### 5.4 non-goal preservation proof

The apply keeps all explicit non-goals intact:
- no second persisted artifact
- no `execution_summary` redesign
- no `failure_summary` redesign
- no step-level profiling
- no retry tracing
- no notifier/dashboard/registry widening

## 6. contract compliance statement

The landed code matches the frozen fourth-slice contract exactly:
- one compact top-level `run_timing_summary` only
- whole-run timing only
- success and delegated failure deterministic presence
- exact frozen field set only
- portfolio-level schema advanced exactly `3 -> 4`
- existing orchestrator-owned persisted report remains the only artifact in scope
- `execution_summary` unchanged
- `failure_summary` unchanged

## 7. GitHub SoT vs server distinction

GitHub/repo is the architectural SoT and proves the apply contract.
Server validation is applied-state proof only and remains separate from repo proof.

## 8. owner-run validation still required

Full slice completion still requires owner-run applied-state proof showing a real persisted portfolio runtime run report with:
- `portfolio_run_schema_version = 4`
- top-level `run_timing_summary` present
- `timing_scope = "portfolio_run_wall_clock"`
- populated numeric `wall_clock_duration_seconds`
- unchanged success/failure summary structure

## 9. final statement

Phase-8 fourth runtime observability slice is fully applied in repo SoT and fully documented in repo SoT.
