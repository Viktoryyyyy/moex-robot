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
- `1ee1bc99dfdcc6413430ed2ae7753bce9e6cf728`
- `934cd472dd557bedbceb92f7c063fe8e7d50b0cf`
- `4efebe6e1d00cbc20d45d58eea259aa146c36b00`

Final code tip before original report commit:

- `4efebe6e1d00cbc20d45d58eea259aa146c36b00`

## Report commit SHA

Original apply report commit:

- `e1409a4d8bc3764d389c094c78b0d7948e824786`

## Proof the second strategy is architecture-only and synthetic

`src/strategies/reference_flat_15m_validation/signal_engine.py` emits no trading signals.

`src/strategies/reference_flat_15m_validation/live_adapter.py` always returns:

- `desired_position = 0.0`
- fixed `reason_code = \"architecture_validation_force_flat\"`

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

Owner-run server sync was completed after repo apply.

Observed sync result:

- `POST_PULL_HEAD = e1409a4d8bc3764d389c094c78b0d7948e824786`
- `ORIGIN_MAIN = e1409a4d8bc3764d389c094c78b0d7948e824786`

## Server proof result

Owner-run server proof was executed and passed.

Observed proof result:

- `portfolio_id = \"reference_ema_3_19_15m_with_flat_validation\"`
- `environment_id = \"reference_runtime_boundary\"`
- `status = \"ok\"`
- `ok = true`
- `enabled_strategy_ids = (\"ema_3_19_15m\", \"reference_flat_15m_validation\")`
- `delegated_strategy_results` contained exactly two ok per-strategy results in declared order
- `ema_3_19_15m` resolved through the unchanged runtime boundary with its existing runtime artifacts:
  - `state_path = \"/home/trader/moex_bot/moex-robot/data/state/ema_3_19_15m_signal_state_2026-04-02.json\"`
  - `trade_log_path = \"/home/trader/moex_bot/moex-robot/data/signals/ema_3_19_15m_realtime_2026-04-02.csv\"`
- `reference_flat_15m_validation` resolved through the unchanged runtime boundary with its own non-colliding runtime artifacts:
  - `state_path = \"/home/trader/moex_bot/moex-robot/data/state/reference_flat_15m_validation_signal_state_2026-04-02.json\"`
  - `trade_log_path = \"/home/trader/moex_bot/moex-robot/data/signals/reference_flat_15m_validation_realtime_2026-04-02.csv\"`
  - `signal_count = 0`
  - `current_position = 0.0`
  - `desired_position = 0.0`
  - `position_changed = false`
  - `action = null`

This is an observed PASS for the frozen architecture-only validation question: the existing sequential portfolio orchestrator and the unchanged registered runtime boundary ran successfully with `enabled_strategy_ids > 1`, while the synthetic validation strategy stayed flat and artifact-isolated.

## Blockers

No blocker remained for the frozen architecture-only validation slice after the owner-run sync and owner-run server proof passed.

This report-only cleanup corrects the SoT record to match accepted reality.
