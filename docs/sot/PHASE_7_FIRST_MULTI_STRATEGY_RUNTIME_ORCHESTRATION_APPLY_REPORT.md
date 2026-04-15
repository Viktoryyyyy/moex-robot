# PHASE 7 First Multi-Strategy Runtime Orchestration — Apply Report

## Verdict

Applied the frozen first orchestration unit exactly as specified.

A platform-owned portfolio-scoped sequential registered runtime orchestrator now exists under `src/moex_runtime/orchestrator/`.

The orchestrator:

- accepts one `portfolio_id`
- accepts one shared `environment_id`
- reads `enabled_strategy_ids` from the portfolio registry/config
- iterates those strategy ids sequentially in declared order
- delegates one call per strategy into the existing single-strategy runtime boundary
- collects one result record per delegated strategy run
- returns a fail-closed portfolio outcome when any delegated strategy run fails

No scheduler, daemon, concurrency, lock expansion, risk engine, broker routing, notifier framework, portfolio netting, or capital allocation work was introduced.

## Exact file scope changed

- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `docs/sot/PHASE_7_FIRST_MULTI_STRATEGY_RUNTIME_ORCHESTRATION_APPLY_REPORT.md`

## Exact file scope not changed

- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- `src/moex_core/contracts/registry_loader.py`
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `configs/environments/reference_runtime_boundary.json`

## Code apply commit SHA

ebe4ef42fea57b529d05f785f047ef76fec39919

## Report commit SHA

(see commit introducing this report file)

## Proof orchestration now lives under moex_runtime/orchestrator

New platform-owned file:

`src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`

This module is inside the runtime layer and outside any strategy package.

## Proof iteration is sequential only

The orchestrator uses a simple `for strategy_id in enabled_strategy_ids` loop.

There are:

- no async constructs
- no thread pools
- no multiprocessing
- no scheduler constructs

Therefore execution is strictly sequential.

## Proof single-strategy runtime boundary is reused

The orchestrator delegates execution through:

`run_registered_runtime_boundary(strategy_id=..., portfolio_id=..., environment_id=...)`

This proves that the phase-6 single-strategy runtime boundary is composed rather than duplicated.

## Proof strategy packages stayed untouched

No file inside `src/strategies/` was modified.

The orchestrator interacts only with the existing platform runtime boundary.

## Proof artifact/env contracts for reference slice stayed unchanged

The reference slice still uses:

- portfolio id: `reference_ema_3_19_15m_single`
- strategy id: `ema_3_19_15m`
- environment id: `reference_runtime_boundary`

No changes were made to:

- portfolio config
- environment config
- strategy artifact contracts

## Server sync proof

Direct server access was unavailable during this cycle.

Owner-run sync command:

`cd ~/moex_bot && source venv/bin/activate && cd moex-robot && git fetch origin main && git rev-parse HEAD && git rev-parse origin/main && git pull --ff-only origin main && git rev-parse HEAD`

Expected proof:

- pre-pull HEAD
- origin/main
- post-pull HEAD == origin/main

## Server proof result

Prepared safe proof command for the reference portfolio case:

`cd ~/moex_bot && source venv/bin/activate && cd moex-robot && python -c "from dotenv import load_dotenv; load_dotenv(); from src.moex_runtime.orchestrator.run_registered_portfolio_runtime_orchestrator import run_registered_portfolio_runtime_orchestrator; result = run_registered_portfolio_runtime_orchestrator(portfolio_id='reference_ema_3_19_15m_single', environment_id='reference_runtime_boundary'); print(result)"`

Expected result:

- orchestrator reads `enabled_strategy_ids = ['ema_3_19_15m']`
- delegates exactly one call into `run_registered_runtime_boundary(...)`
- returns a portfolio result containing one delegated strategy result

## Blockers

No repo-side blocker remains for the approved orchestration unit.

Only server execution confirmation depends on owner-run sync and proof because direct server access was unavailable in this cycle.
