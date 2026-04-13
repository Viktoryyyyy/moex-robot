# REFERENCE VERTICAL SLICE — EMA 3/19 15m — WAVE 3 APPLY REPORT

Status: APPLIED
Project: MOEX Bot
Reference strategy: `ema_3_19_15m`
Cycle type: narrow remediation

## VERDICT

Wave-3 applied exactly as frozen.

## PROOF

Frozen SoT used:
- `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_IMPLEMENTATION_WAVE_3.md`

Created:
- `configs/environments/reference_runtime_boundary.json`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`

Updated:
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `src/moex_core/contracts/registry_loader.py`

Intentionally untouched:
- `src/cli/loop_ema_3_19_15m_realtime.py`
- all `src/strategy/realtime/ema_3_19_15m/*`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/backtest_adapter.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/moex_features/intraday/si_15m_ohlc_from_5m.py`
- `src/moex_backtest/engine/run_registered_backtest.py`

Validation evidence:
- imports passed
- `supports_live = true` validated
- `is_live_allowed = true` validated
- `build_live_decision(...)` returned valid `LiveAdapterDecision`
- artifact contracts declare `ema_3_19_15m_signal_state`
- artifact contracts declare `ema_3_19_15m_trade_log`
- exact runtime path ran successfully for:
  - `strategy_id = ema_3_19_15m`
  - `portfolio_id = reference_ema_3_19_15m_single`
  - `environment_id = reference_runtime_boundary`
- explicit runtime state written:
  - `data/state/ema_3_19_15m_signal_state_2026-04-02.json`
- explicit runtime trade-log written:
  - `data/signals/ema_3_19_15m_realtime_2026-04-02.csv`
- no broker/network side effects

## BLOCKERS

None.
