## 1. verdict

The smallest correct blocker-removal unit is a narrow runtime-boundary reuse slice that makes `run_registered_runtime_boundary(...)` strategy-registered instead of EMA-pinned, without changing its current single-strategy id-only call shape and without introducing orchestration, scheduler, risk, broker, or broader runtime redesign.

## 2. repo proof

- `_load_common_registered_components(...)` in `src/moex_core/contracts/registry_loader.py` currently rejects any portfolio whose `enabled_strategy_ids` is not exactly `[strategy_id]`, so the current boundary cannot be reused against a portfolio record that contains more than the delegated strategy id being executed.
- `load_registered_runtime_boundary(...)` in `src/moex_core/contracts/registry_loader.py` currently resolves runtime state/trade-log contracts by the EMA-only artifact ids `ema_3_19_15m_signal_state` and `ema_3_19_15m_trade_log`, so runtime artifact resolution is not strategy-registered.
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` still imports `materialize_feature_frame` from `src.moex_features.intraday.si_15m_ohlc_from_5m`, `generate_signals` from `src.strategies.ema_3_19_15m.signal_engine`, and `build_live_decision` from `src.strategies.ema_3_19_15m.live_adapter`, so the runtime boundary still hardcodes the EMA reference slice instead of dispatching through strategy registration.
- The same runtime boundary already proves the reusable shared infra that must be preserved: registry load, external artifact path resolution, shared runtime session store, and shared position transition execution.
- `configs/portfolios/reference_ema_3_19_15m_single.json` proves that `enabled_strategy_ids` belongs to portfolio config/registry, while `configs/strategies/ema_3_19_15m.json` proves that strategy registry already owns manifest/config/artifact-contract references. Therefore the blocker is boundary reuse, not missing platform ownership of strategy registration.

## 3. chosen blocker-removal unit

Freeze one narrow pre-orchestration unit:

`registered single-strategy runtime boundary reuse slice`

Exact responsibility of this unit:
- keep `run_registered_runtime_boundary(strategy_id, portfolio_id, environment_id)` unchanged as the callable surface
- make runtime artifact resolution strategy-registered instead of EMA-id-pinned
- make runtime feature/signal/live-decision loading strategy-registered instead of direct EMA imports
- relax portfolio validation only enough to allow execution of one requested `strategy_id` when that id is present in `enabled_strategy_ids`, without turning the boundary itself into a portfolio orchestrator

## 4. why it is the smallest correct pre-orchestration unit

It is the smallest correct pre-orchestration unit because all currently proven blockers are inside the existing single-strategy boundary surfaces, not in orchestration control flow.

Freezing anything smaller would leave one of the three proven reuse blockers intact:
- registry loader would still reject multi-enabled portfolios
- runtime artifact resolution would still be EMA-pinned
- runtime feature/signal/live-decision dispatch would still be EMA-pinned

Freezing anything broader would widen into plugin/runtime redesign or orchestration apply before the current boundary is actually reusable.

## 5. exact current repo surfaces in scope

In scope only:
- `src/moex_core/contracts/registry_loader.py`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- strategy registration surfaces already referenced by the current registry path, limited to the minimum needed to load runtime-facing strategy hooks/contracts from registration rather than EMA-only imports:
  - `configs/strategies/<strategy_id>.json`
  - strategy package refs already declared there
- no other runtime, broker, scheduler, notifier, risk, or orchestration surfaces are in scope

## 6. exact target layer destination

Exact destination for the blocker-removal result:
- `src/moex_core/contracts/`
- `src/moex_runtime/engine/`

Meaning:
- registration-based runtime hook/artifact resolution belongs in the contract-loading layer
- the callable single-strategy boundary remains in the runtime engine layer
- no new orchestration layer is introduced by this blocker-removal spec

## 7. exact non-goals

- no first orchestration apply
- no portfolio loop or sequential orchestrator implementation
- no scheduler / cron / daemon design
- no parallel execution
- no locks expansion
- no risk allocation or netting
- no notifier expansion
- no broker routing redesign
- no artifact model redesign beyond the minimum needed to stop EMA-only runtime artifact selection
- no registry-model redesign beyond the minimum needed to load already-registered runtime-facing strategy hooks/contracts
- no strategy package redesign
- no backtest/research changes
- no environment contract changes for the reference slice

## 8. acceptance criteria for later apply

Later apply is acceptable only if all points below are true:

- `run_registered_runtime_boundary(...)` still accepts exactly `strategy_id`, `portfolio_id`, `environment_id`
- the function remains single-strategy and does not iterate portfolios or enabled strategy lists
- portfolio validation no longer requires `enabled_strategy_ids == [strategy_id]`, but still fails closed unless the requested `strategy_id` is explicitly present and the portfolio remains active/live-allowed
- runtime state and runtime trade-log contracts are resolved from strategy-registered contracts rather than hardcoded EMA artifact ids
- runtime feature materialization, signal generation, and live decision building are loaded through strategy registration/contracts rather than direct EMA imports
- current reference slice ids and artifact/env contracts continue to run through the same boundary without caller shape change
- shared phase-6 runtime infra remains reused rather than duplicated
- no new scheduler, orchestrator, risk, broker, notifier, or netting behavior is introduced

## 9. blockers if any

No additional blocker is proven beyond the three already frozen reuse blockers.

The blocker-removal unit is supportable from current repo proof.

## 10. one sentence final scope statement

Freeze phase-7 blocker removal as one narrow registered single-strategy runtime-boundary reuse slice that removes the current portfolio-validation, runtime-artifact, and runtime-hook EMA pinning while preserving the existing id-only single-strategy boundary surface for later orchestration composition.
