# PHASE 7 Architecture-Only Second Strategy Validation Slice — Apply Report

## Verdict

Applied the frozen architecture-only second strategy validation slice in repo scope.

A synthetic second registered live strategy package now exists under `src/strategies/reference_flat_15m_validation/` and a new two-strategy validation portfolio now exists under `configs/portfolios/reference_ema_3_19_15m_with_flat_validation.json`.

The new strategy is architecture-only and synthetic:

- it reuses the existing `si` / `15m` / `si_fo_5m_intraday` / `si_15m_ohlc_from_5m` runtime family
- its `generate_signals(...)` is deterministic and returns an empty signal frame
- its `build_live_decision(...)` always returns a valid flat `LiveAdapterDecision`
- its fixed reason code is `architecture_validation_force_flat`
- it declares its own runtime `state` artifact path and runtime trade-log artifact path

No runtime-boundary redesign, orchestrator redesign, portfolio netting, capital allocation, scheduler work, lock expansion, notifier expansion, telemetry expansion, broker-routing redesign, feature/dataset contract redesign, registry-model redesign, or backtest-semantics widening was introduced.

## Exact file scope changed

- `src/strategies/reference_flat_15m_validation/manifest.py`
- `src/strategies/reference_flat_15m_validation/config.py`
- `src/strategies/reference_flat_15m_validation/signal_engine.py`
- `src/strategies/reference_flat_15m_validation/live_adapter.py`
- `src/strategies/reference_flat_15m_validation/artifact_contracts.py`
- `configs/strategies/reference_flat_15m_validation.json`
- `configs/strategies/reference_flat_15m_validation.default.json`
- `configs/portfolios/reference_ema_3_19_15m_with_flat_validation.json`
- `docs/sot/PHASE_7_ARCHITECTURE_ONLY_SECOND_STRATEGY_VALIDATION_SLICE_APPLY_REPORT.md`

## Exact file scope not changed

- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `src/moex_core/contracts/registry_loader.py`
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/strategies/ema_3_19_15m.default.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `configs/environments/reference_runtime_boundary.json`

## Code apply commit SHA

Code apply was completed as a GitHub-first multi-commit new-file batch because the frozen scope contains only new files. Exact code commit chain in order:

- `8d8c955cfc6a4e2fa012c342ec888e28f42a691b`
- `37ebeb05cdbfb451e814303b4a528898503bea7b`
- `fc1af59ef47226f431d6063b4e6acb6e520b1258`
- `2ebe4b877677da95e8c406aa299e37a7478424ea`
- `b5d6428b348407cfe28ede8c1adec1b9b8a55d8e`
- `934cd472dd557bedbceb92f7c063fe8e7d50b0cf`
- `4efebe6e1d00cbc20d45d58eea259aa146c36b00`

Final code tip before report commit:

- `4efebe6e1d00cbc20d45d58eea259aa146c36b00`

## Report commit SHA

- this report file commit

## Proof the second strategy is architecture-only and synthetic

`src/strategies/reference_flat_15m_validation/signal_engine.py` emits no trading signals.

`src/strategies/reference_flat_15m_validation/live_adapter.py` always returns:

- `desired_position = 0.0`
- fixed `reason_code = "architecture_validation_force_flat"`

So the package exists only to prove package registration, runtime-boundary reuse, orchestrator reuse, and artifact isolation for `enabled_strategy_ids > 1`.

## Proof no runtime/orchestrator widening occurred

No file under `src/moex_runtime/` changed in this slice.

The existing single-strategy runtime boundary remains callable unchanged through:

`run_registered_runtime_boundary(strategy_id=..., portfolio_id=..., environment_id=...)`

The existing sequential portfolio orchestrator remains reused unchanged through:

`run_registered_portfolio_runtime_orchestrator(portfolio_id=..., environment_id=...)`

## Proof strategy-specific artifact paths avoid collision

The validation strategy runtime state path is:

`data/state/reference_flat_15m_validation_signal_state_{trade_date}.json`

The validation strategy trade-log path is:

`data/signals/reference_flat_15m_validation_realtime_{trade_date}.csv`

These do not collide with EMA paths:

- `data/state/ema_3_19_15m_signal_state_{trade_date}.json`
- `data/signals/ema_3_19_15m_realtime_{trade_date}.csv`

## Proof current `ema_3_19_15m` package stayed untouched

No file inside `src/strategies/ema_3_19_15m/` changed.

The validation slice adds a second package and a second portfolio config only.

## Server sync proof

Direct server access was unavailable during this cycle.

Owner-run sync command:

`cd ~/moex_bot && source venv/bin/activate && cd moex-robot && git fetch origin main && echo PRE_PULL_HEAD=$(git rev-parse HEAD) && echo ORIGIN_MAIN=$(git rev-parse origin/main) && git pull --ff-only origin main && echo POST_PULL_HEAD=$(git rev-parse HEAD)`

Expected sync proof:

- `POST_PULL_HEAD` equals `origin/main`
- `POST_PULL_HEAD` includes this report commit at repo tip

## Server proof result

Direct server execution was unavailable during this cycle.

Owner-run safe proof command:

`cd ~/moex_bot && source venv/bin/activate && cd moex-robot && python -c "from dotenv import load_dotenv; load_dotenv(); from src.moex_runtime.orchestrator.run_registered_portfolio_runtime_orchestrator import run_registered_portfolio_runtime_orchestrator; result = run_registered_portfolio_runtime_orchestrator(portfolio_id='reference_ema_3_19_15m_with_flat_validation', environment_id='reference_runtime_boundary'); print(result)"`

Expected proof surface:

- `enabled_strategy_ids` equals `('ema_3_19_15m', 'reference_flat_15m_validation')`
- `delegated_strategy_results` contains exactly two per-strategy results in declared order
- `ema_3_19_15m` resolves through the unchanged runtime boundary
- `reference_flat_15m_validation` resolves through the unchanged runtime boundary
- validation strategy result contains its own `state_path`
- validation strategy result contains its own `trade_log_path`
- validation strategy paths do not equal EMA paths
- no widened runtime semantics are introduced

## Blockers

No repo-side blocker remains for the frozen architecture-only validation slice.

Only owner-run server sync and owner-run server proof remain because direct server access was unavailable in this cycle.
