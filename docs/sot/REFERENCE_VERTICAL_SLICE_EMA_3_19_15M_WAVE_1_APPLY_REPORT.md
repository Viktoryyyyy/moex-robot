# REFERENCE VERTICAL SLICE — EMA 3/19 15m — WAVE 1 APPLY REPORT

Status: APPLIED  
Project: MOEX Bot  
Reference strategy: `ema_3_19_15m`  
Cycle type: narrow remediation

---

## Scope applied

Applied exactly the frozen wave-1 create-only file scope:

SDK base:
- `src/moex_strategy_sdk/manifest.py`
- `src/moex_strategy_sdk/interfaces.py`
- `src/moex_strategy_sdk/config_schema.py`
- `src/moex_strategy_sdk/artifact_contracts.py`
- `src/moex_strategy_sdk/errors.py`

Reference strategy package:
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/backtest_adapter.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`

## No-touch respected

Untouched in this wave:
- legacy realtime loop
- legacy realtime strategy files
- research EMA files
- registry records
- `src/moex_runtime/*`
- `src/moex_backtest/*`
- `src/moex_features/*`

## Validation summary

Repo-side surface now exists for:
- minimal SDK types/errors/contracts
- importable target package `src/strategies/ema_3_19_15m/`
- manifest/config/signal/backtest/live/artifact modules

Wave-1 behavior encoded:
- `supports_backtest = True`
- `supports_live = False`
- `live_adapter.py` fails closed with `UnsupportedModeError`
- backtest adapter is normalized-boundary only
- signal engine is pure EMA crossover logic only

## Result

Wave-1 package-first remediation is applied without registry wiring, runtime wiring, CLI refactor, aggregation extraction, or engine migration.
