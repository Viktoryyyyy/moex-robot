# PHASE 7 Runtime Boundary Reuse Blocker Removal — Apply Report

## Verdict

Applied the frozen blocker-removal slice in repo scope only.

`run_registered_runtime_boundary(...)` is now strategy-registered instead of EMA-pinned while preserving the same single-strategy callable surface:

- `strategy_id`
- `portfolio_id`
- `environment_id`

No orchestration, scheduler, notifier, broker, risk, lock, or strategy-package redesign was introduced.

## Exact file scope changed

- `src/moex_core/contracts/registry_loader.py`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- `docs/sot/PHASE_7_RUNTIME_BOUNDARY_REUSE_BLOCKER_REMOVAL_APPLY_REPORT.md`

## Exact file scope not changed

- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `configs/environments/reference_runtime_boundary.json`

## Code apply summary

Applied exactly three narrow changes:

1. Portfolio validation was relaxed from exact equality to explicit membership:
   - before: `enabled_strategy_ids == [strategy_id]`
   - after: requested `strategy_id` must be explicitly present in `enabled_strategy_ids`

2. Runtime state / trade-log contract resolution was changed from EMA-only artifact ids to strategy-registered artifact-contract selection:
   - runtime state: unique strategy artifact contract with `artifact_role="state"` and `producer="moex_runtime"`
   - runtime trade log: unique strategy artifact contract with `artifact_role="output"` and `producer="moex_runtime"`

3. Runtime feature materialization, signal generation, and live decision building were changed from direct EMA imports to registration-based loading:
   - feature builder from feature registry `producer_ref`
   - signal builder from strategy registry `package_ref + ".signal_engine:generate_signals"`
   - live decision builder from strategy registry `package_ref + ".live_adapter:build_live_decision"`

## Proof that runtime boundary is now strategy-registered rather than EMA-pinned

Repo proof after apply:

- `src/moex_runtime/engine/run_registered_runtime_boundary.py` no longer imports:
  - `src.moex_features.intraday.si_15m_ohlc_from_5m:materialize_feature_frame`
  - `src.strategies.ema_3_19_15m.signal_engine:generate_signals`
  - `src.strategies.ema_3_19_15m.live_adapter:build_live_decision`

- `src/moex_core/contracts/registry_loader.py` now resolves runtime-facing hooks/contracts through registration:
  - feature builder via feature registry `producer_ref`
  - signal/live hooks via strategy registry `package_ref`
  - runtime state/trade-log via strategy artifact contracts instead of hardcoded EMA artifact ids

## Proof that current reference slice still works through the same callable surface

Callable surface was preserved exactly:
- `run_registered_runtime_boundary(strategy_id=..., portfolio_id=..., environment_id=...)`

The function remains single-strategy.
It does not iterate portfolios.
It does not iterate `enabled_strategy_ids`.
It delegates exactly one requested strategy through the existing runtime boundary.

## Proof that strategy package stayed untouched

No file inside `src/strategies/ema_3_19_15m/` was modified by this apply.

## Proof that artifact/env contracts for reference slice stayed unchanged

The reference slice still uses the same existing contracts:
- portfolio id: `reference_ema_3_19_15m_single`
- strategy id: `ema_3_19_15m`
- environment id: `reference_runtime_boundary`
- runtime state locator remains declared in `src/strategies/ema_3_19_15m/artifact_contracts.py`
- runtime trade-log locator remains declared in `src/strategies/ema_3_19_15m/artifact_contracts.py`
- environment contract file `configs/environments/reference_runtime_boundary.json` was not changed

## Shared phase-6 infra reuse preserved

Still reused without duplication:
- external artifact path resolver
- runtime session store
- runtime position transition execution

## Server sync proof

Direct server access was unavailable in this apply cycle.

Owner-run sync command prepared for exact commit-chain sync:
`cd ~/moex_bot && source venv/bin/activate && cd moex-robot && git fetch origin main && git rev-parse HEAD && git rev-parse origin/main && git pull --ff-only origin main && git rev-parse HEAD`

Expected sync proof:
- pre-pull `HEAD`
- `origin/main`
- post-pull `HEAD == origin/main`

## Server proof result

Direct server proof execution was owner-driven only in this cycle.

Prepared exact safe proof command for the frozen reference path:
`cd ~/moex_bot && source venv/bin/activate && cd moex-robot && python -c "from dotenv import load_dotenv; load_dotenv(); from src.moex_runtime.engine.run_registered_runtime_boundary import run_registered_runtime_boundary; result = run_registered_runtime_boundary(strategy_id='ema_3_19_15m', portfolio_id='reference_ema_3_19_15m_single', environment_id='reference_runtime_boundary'); print(result)"`

Expected safe proof result:
- call shape unchanged
- reference runtime boundary completes without caller-shape change
- returned payload contains the same three ids and resolved artifact paths

## Blockers

No repo-side blocker remained for the approved blocker-removal slice.

Only remaining proof dependency is owner-run server sync/proof because direct server access was unavailable in this cycle.
