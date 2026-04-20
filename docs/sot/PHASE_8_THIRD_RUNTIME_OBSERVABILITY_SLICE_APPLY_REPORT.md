# PHASE 8 Third Runtime Observability Slice Apply Report

## 1. verdict

Apply is complete.

The frozen phase-8 third runtime observability slice is now reflected in repo SoT:
- the existing orchestrator-owned portfolio runtime run report remains the only persisted artifact in scope
- portfolio-level `portfolio_run_schema_version` advanced from `2` to `3`
- delegated success result `runtime_result_schema_version` remains `3`
- an optional top-level `failure_summary` is now emitted only on delegated runtime failure
- success path remains unchanged and does not emit `failure_summary`

## 2. frozen spec being applied

Frozen authority:
- `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- spec commit: `8ff407d2bebfb8942d32b036940b49e6a051aa4e`

Frozen target from that spec:
- add one optional top-level `failure_summary` inside the existing orchestrator-owned portfolio runtime run report
- emit it only on delegated runtime failure
- first terminal captured failure wins
- persist exactly these fields:
  - `failure_summary_schema_version`
  - `failure_scope`
  - `failed_strategy_id`
  - `failed_at_utc`
  - `error_type`
  - `error_message`
- keep success path unchanged
- keep persisted artifact path unchanged

## 3. exact repo mutation set

Code apply:
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`

Report file added in the same cycle:
- `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

No other file scope is part of this apply.

## 4. exact code-apply commit on main

Code apply commit on `main`:
- `cdd77b4e4083ab8ea2d52684ba94edb0918a32a5` — `Add delegated runtime failure summary to portfolio report`

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
- `portfolio_run_schema_version = 3` on both success and failure paths
- delegated success result contract is preserved because the orchestrator still embeds delegated `result` payloads unchanged
- `runtime_result_schema_version` ownership remains delegated and unchanged at `3`

### 5.3 failure-summary proof

That same file proves:
- `failure_summary` is top-level and optional
- `failure_summary` is emitted only in the delegated exception path
- `failure_summary_schema_version = 1`
- `failure_scope = "delegated_runtime"`
- `failed_strategy_id` is the exact delegated strategy that failed
- `failed_at_utc` is captured at the terminal delegated failure point
- `error_type` is the exception class name
- `error_message` is the compact string form of the exception
- first terminal captured failure wins because the orchestrator still stops fail-closed on first delegated runtime exception
- partial delegated results accumulated before stop remain persisted in `delegated_strategy_results`

### 5.4 non-goal preservation proof

The apply keeps all explicit non-goals intact:
- no second persisted artifact
- no stack trace persistence
- no retry ledger
- no strategy-specific payload persistence in `failure_summary`
- no notifier/dashboard/registry/runtime-boundary widening

## 6. contract compliance statement

The landed code matches the frozen third-slice contract exactly:
- one optional top-level `failure_summary` only
- failure-path only
- first terminal captured failure only
- exact frozen field set only
- existing orchestrator-owned persisted report remains the only artifact in scope
- success path unchanged

## 7. GitHub SoT vs server distinction

GitHub/repo is the architectural SoT and proves the apply contract.
Server validation is applied-state proof only and is outside this sub-chat scope.

## 8. final statement

Phase-8 third runtime observability slice is fully applied in repo SoT and fully documented in repo SoT.
