# REFERENCE VERTICAL SLICE — EMA 3/19 15m — WAVE 2 APPLY REPORT

Status: APPLIED  
Project: MOEX Bot  
Reference strategy: `ema_3_19_15m`  
Cycle type: narrow remediation  
Authority: `MOEX_Bot_Target_Architecture_2026_All_In_One.md`, `STRATEGY_SDK_SPEC.md`, `BACKTEST_EXECUTION_SEMANTICS_SPEC.md`, `REGISTRY_CONFIG_MODEL_SPEC.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_SPEC.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_IMPLEMENTATION_WAVE_1.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_WAVE_1_APPLY_REPORT.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_IMPLEMENTATION_WAVE_2.md`

---

## VERDICT

Wave-2 applied exactly as frozen.

Implemented only:
- exact registry/config records for the reference slice
- exact dataset/feature contracts for the slice backtest path
- exact minimal platform registry loader
- exact platform-owned 15m materialization surface
- exact minimal registry-backed backtest runner

No wave-2 no-touch surface was modified.

---

## PROOF

### Frozen SoT docs used

- `docs/sot/MOEX_Bot_Target_Architecture_2026_All_In_One.md`
- `docs/sot/STRATEGY_SDK_SPEC.md`
- `docs/sot/BACKTEST_EXECUTION_SEMANTICS_SPEC.md`
- `docs/sot/REGISTRY_CONFIG_MODEL_SPEC.md`
- `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_SPEC.md`
- `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_IMPLEMENTATION_WAVE_1.md`
- `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_WAVE_1_APPLY_REPORT.md`
- `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_IMPLEMENTATION_WAVE_2.md`

### Exact repo files created

Artifact contracts:
- `contracts/datasets/si_fo_5m_intraday.json`
- `contracts/features/si_15m_ohlc_from_5m.json`

Registry/config records:
- `configs/instruments/si.json`
- `configs/datasets/si_fo_5m_intraday.json`
- `configs/features/si_15m_ohlc_from_5m.json`
- `configs/strategies/ema_3_19_15m.json`
- `configs/strategies/ema_3_19_15m.default.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `configs/environments/reference_backtest.json`

Minimal platform code:
- `src/moex_core/contracts/registry_loader.py`
- `src/moex_features/intraday/si_15m_ohlc_from_5m.py`
- `src/moex_backtest/engine/run_registered_backtest.py`

### Exact repo files intentionally untouched

- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/cli/loop_ema_3_19_15m_realtime.py`
- all `src/strategy/realtime/ema_3_19_15m/*`
- `src/research/ema/build_ema_pnl_multitimeframe.py`
- `src/research/ema/lib_ema_search.py`
- any `src/moex_runtime/*`
- existing strategy package files under `src/strategies/ema_3_19_15m/`

### Validation evidence

Validated against a local repo-faithful reconstruction of the exact reused strategy/SDK files plus the new wave-2 files:
- required imports succeed for:
  - `src.moex_core.contracts.registry_loader`
  - `src.moex_features.intraday.si_15m_ohlc_from_5m`
  - `src.moex_backtest.engine.run_registered_backtest`
  - `src.strategies.ema_3_19_15m.manifest`
  - `src.strategies.ema_3_19_15m.config`
  - `src.strategies.ema_3_19_15m.signal_engine`
  - `src.strategies.ema_3_19_15m.backtest_adapter`
  - `src.strategies.ema_3_19_15m.artifact_contracts`
- registry linkage fails closed on missing required artifact-root env var with `StrategyRegistrationError`
- one exact registry-backed start path runs end-to-end for:
  - `strategy_id = ema_3_19_15m`
  - `portfolio_id = reference_ema_3_19_15m_single`
  - `environment_id = reference_backtest`
- that run emits explicit output artifact:
  - `data/backtests/ema_3_19_15m_day_metrics_ema_3_19_15m__reference_ema_3_19_15m_single__reference_backtest.csv`
- emitted day-metrics columns:
  - `date`
  - `pnl_day`
  - `max_dd_day`
  - `num_trades_day`
  - `EMA_EDGE_DAY`

---

## GITHUB APPLY RESULT

Applied on `main` by create-only new-file commits.

---

## GITHUB SOT FILE

- exact path: `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_WAVE_2_APPLY_REPORT.md`
- saved as the wave-2 apply report

---

## BLOCKERS

None.
