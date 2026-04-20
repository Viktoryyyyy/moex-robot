# PHASE 8 Second Runtime Observability Slice Apply Report

## 1. verdict

Apply is complete.

The frozen phase-8 second runtime observability slice is now fully reflected in repo SoT:
- one delegated `execution_summary` is present in the runtime-boundary success result
- it is derived only from the existing `trade_log_path` and current `trade_date`
- it is persisted only through the already existing orchestrator-owned portfolio runtime run report
- no second persisted runtime report artifact was introduced

This report-only commit closes the remaining documentation gap after the already-landed code apply.

## 2. frozen spec being applied

Frozen authority:
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- spec commit: `175af2d9e56980b95f42f445110b3e2ad077688f`

Frozen target from that spec:
- add one delegated `execution_summary`
- derive it only from existing `trade_log_path` for current `trade_date`
- persist it only through the already existing orchestrator-owned portfolio runtime run report
- do not introduce a second persisted report artifact

## 3. exact repo mutation set

Code apply already present on `origin/main` before this report commit:
- `src/moex_runtime/telemetry/summarize_runtime_trade_log_execution.py`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`

This cycle adds the missing SoT report file:
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

No other file scope is part of this report-only mutation.

## 4. exact code-apply commit chain already on main

Ordered code-apply commits already present on `main`:
1. `3413db1b80d5eac68d0fd456d1793b30c15e527e` — `Add runtime trade log execution summary helper`
2. `96eb54eed670fe905b1311874f4c51357fba4c9e` — `Add execution summary to runtime boundary result`
3. `95483ce613b9f57e65b75df1724f5fa48ad75e4c` — `Bump portfolio runtime report schema version`

Report-only commits for this cycle:
4. `5bfc8dbeb66a179b89ccbbb8af4633553248a39b` — `Add phase 8 second runtime observability slice apply report`
5. `768fad4423c0b3841be1c6c7ba5f2f66d6d0e281` — `Finalize phase 8 second runtime observability slice apply report`
6. `86eccfe837edc84ed371629f485afa4e627e0403` — `Finalize report commit SHA in phase 8 second observability apply report`
7. `FINAL_HEAD_COMMIT_SHA` — `Finalize head SHA in phase 8 second observability apply report`

## 5. exact repo proof of frozen behavior on main

### 5.1 helper proof

`src/moex_runtime/telemetry/summarize_runtime_trade_log_execution.py` proves:
- summary source is the runtime trade log only
- filtering is by the passed `trade_date` only
- frozen fields are exactly:
  - `execution_summary_schema_version`
  - `execution_event_count_day`
  - `last_execution_seq`
  - `last_execution_bar_end`
  - `last_execution_action`
  - `last_closed_trade_pnl_points`
  - `current_day_realized_pnl_points`
- default empty-partition behavior is exactly:
  - event count `0`
  - last execution fields `null`
  - last closed trade pnl `null`
  - current-day realized pnl `0.0`
- reversal handling closes the prior leg at the reversal price and opens the opposite leg at the same logged price, with realized points credited only to the closed leg component
- point semantics are raw realized execution-log price deltas only; no commission, slippage, MTM, or drawdown widening is added

### 5.2 runtime-boundary proof

`src/moex_runtime/engine/run_registered_runtime_boundary.py` proves:
- `execution_summary = summarize_runtime_trade_log_execution(trade_log_path=trade_log_path, trade_date=trade_date)`
- delegated success result now includes top-level `execution_summary`
- delegated success result `runtime_result_schema_version` is `3`
- summary derivation inputs are exactly the already existing delegated `trade_log_path` and current delegated `trade_date`

### 5.3 orchestrator persistence proof

`src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py` proves:
- orchestrator remains the single persisted portfolio runtime report producer
- delegated strategy success results are embedded into that one persisted report
- persisted report path remains:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`
- `portfolio_run_schema_version` is `2`
- no second persisted report artifact path is introduced

## 6. contract compliance statement

The landed code matches the frozen second-slice contract exactly:
- one delegated `execution_summary` only
- source limited to existing trade-log artifact plus current trade-date partition
- persisted ownership unchanged and still orchestrator-owned
- no notifier widening
- no dashboard widening
- no experiment-registry widening
- no commission/slippage widening
- no MTM widening
- no drawdown widening
- no second persisted report artifact

## 7. GitHub SoT vs server distinction

GitHub/repo is the architectural SoT and proves the slice contract and file-level implementation.
Server proof is still required separately as applied-state validation only.
Server filesystem is not architectural proof.

## 8. owner-run server sync

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && git fetch origin main && git checkout main && git reset --hard origin/main && echo HEAD=$(git rev-parse HEAD)
```

Expected after sync:
- `HEAD=FINAL_HEAD_COMMIT_SHA`

## 9. owner-run proof commands

### 9.1 narrow repo-head verification

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && echo HEAD=$(git rev-parse HEAD) && test "$(git rev-parse HEAD)" = "FINAL_HEAD_COMMIT_SHA" && echo APPLY_HEAD_OK
```

### 9.2 narrow runtime proof

Run the portfolio orchestrator once on the approved environment and portfolio so it writes one portfolio runtime run report containing delegated results with `execution_summary` populated from the current trade-date trade log.

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && python -c "from dotenv import load_dotenv; load_dotenv(); import json; from src.moex_runtime.orchestrator.run_registered_portfolio_runtime_orchestrator import run_registered_portfolio_runtime_orchestrator; result = run_registered_portfolio_runtime_orchestrator(portfolio_id='ema_3_19_15m', environment_id='reference_runtime_boundary'); print(json.dumps(result, ensure_ascii=False))"
```

### 9.3 persisted report evidence

Use the printed `portfolio_run_id` from the previous command and inspect the persisted orchestrator-owned report.

```bash
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && python -c "from dotenv import load_dotenv; load_dotenv(); import json; from pathlib import Path; run_id='PASTE_PORTFOLIO_RUN_ID_HERE'; path = Path('data/runtime/portfolio_runs/ema_3_19_15m') / ('runtime_run_' + run_id + '.json'); payload = json.loads(path.read_text(encoding='utf-8')); delegated = payload['delegated_strategy_results'][0]['result']; print(json.dumps({'path': str(path), 'portfolio_run_schema_version': payload['portfolio_run_schema_version'], 'runtime_result_schema_version': delegated['runtime_result_schema_version'], 'execution_summary': delegated['execution_summary']}, ensure_ascii=False))"
```

## 10. expected proof artifact/output

Expected persisted artifact:
- `data/runtime/portfolio_runs/ema_3_19_15m/runtime_run_<portfolio_run_id>.json`

Expected proof shape inside that artifact:
- top-level `portfolio_run_schema_version = 2`
- `delegated_strategy_results[0].result.runtime_result_schema_version = 3`
- `delegated_strategy_results[0].result.execution_summary` present
- `execution_summary` contains exactly:
  - `execution_summary_schema_version`
  - `execution_event_count_day`
  - `last_execution_seq`
  - `last_execution_bar_end`
  - `last_execution_action`
  - `last_closed_trade_pnl_points`
  - `current_day_realized_pnl_points`

If the current trade-date trade log has no rows yet, expected empty-partition summary is:
- `execution_event_count_day = 0`
- `last_execution_seq = null`
- `last_execution_bar_end = null`
- `last_execution_action = null`
- `last_closed_trade_pnl_points = null`
- `current_day_realized_pnl_points = 0.0`

## 11. final statement

Phase-8 second runtime observability slice is fully applied in repo SoT and now fully documented in repo SoT.
The remaining server step is owner-run applied-state proof only.
