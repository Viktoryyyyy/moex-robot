## 1. verdict

The correct first phase-7 unit is a platform-owned portfolio-scoped sequential registered runtime orchestrator that reads enabled strategy ids from portfolio config/registry and calls the existing single-strategy runtime boundary one strategy at a time.

Current repo proof also shows a narrow apply blocker: the existing single-strategy runtime boundary is still pinned to the EMA reference slice and is not yet reusable across arbitrary enabled strategy ids.

So this spec freezes that orchestrator as the correct first phase-7 unit, with explicit blocker status for later apply until the proven single-strategy boundary constraints below are removed.

## 2. repo proof

- `src/moex_runtime/engine/run_registered_runtime_boundary.py` already provides the exact single-strategy callable shape the first phase-7 orchestrator should compose: `strategy_id`, `portfolio_id`, `environment_id`.
- `src/moex_core/contracts/registry_loader.py` already proves that strategy/portfolio/environment are the governing registry/config surfaces for registered runtime loading.
- `configs/portfolios/reference_ema_3_19_15m_single.json` already proves that portfolio config owns `enabled_strategy_ids`.
- `configs/environments/reference_runtime_boundary.json` already proves that runtime environment selection is a shared platform input and not strategy-local state.
- `src/moex_runtime/state_store/file_backed_runtime_session_store.py`, `src/moex_runtime/execution/runtime_position_transition.py`, and `src/moex_core/contracts/external_pattern_artifact_path_resolver.py` prove that the shared runtime boundary dependencies are already extracted and therefore ready to be composed by an orchestrator rather than duplicated.
- The target architecture already places runtime orchestration under `src/moex_runtime/orchestrator/` and explicitly requires runtime to support multiple strategies plus declarative enable/disable control from config/registry.
- The current blocker is also proven in repo:
  - `_load_common_registered_components(...)` in `src/moex_core/contracts/registry_loader.py` currently requires `enabled_strategy_ids == [strategy_id]`
  - `load_registered_runtime_boundary(...)` currently resolves runtime contracts by the EMA-only artifact ids `ema_3_19_15m_signal_state` and `ema_3_19_15m_trade_log`
  - `src/moex_runtime/engine/run_registered_runtime_boundary.py` still imports the EMA-only feature/signal/live-adapter surfaces directly

## 3. chosen orchestration unit or blocker

Chosen first phase-7 unit:

`portfolio-scoped sequential registered runtime orchestrator`

Exact responsibility of that unit:
- accept one `portfolio_id`
- accept one shared `environment_id`
- read `enabled_strategy_ids` from the portfolio registry/config
- iterate those strategy ids sequentially in declared order
- call the existing single-strategy runtime boundary once per strategy
- collect one result record per strategy
- return a fail-closed portfolio outcome when any delegated strategy run fails

Exact blocker status for later apply:
- the current single-strategy runtime boundary is not yet strategy-generic, so later apply must first or simultaneously remove the proven EMA-pinned reuse constraints without widening into broader redesign

## 4. why it is the correct first phase-7 unit

It is the correct first phase-7 unit because it is the smallest platform-owned step that adds multi-strategy runtime orchestration while staying fully inside the frozen scope.

It composes the phase-6 result instead of bypassing it:
- phase-6 produced a thin single-strategy runtime boundary
- phase-7 should compose that boundary at portfolio scope rather than reopen shared runtime infrastructure extraction

It also aligns exactly with the target architecture:
- orchestration belongs in `moex_runtime`
- strategy enablement belongs in portfolio config/registry
- strategy packages must not own cross-strategy control flow

## 5. exact current repo surfaces it builds on

Direct build-on surfaces:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- `src/moex_core/contracts/registry_loader.py`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `configs/environments/reference_runtime_boundary.json`

Already-extracted shared infra the orchestrator must reuse and not duplicate:
- `src/moex_core/contracts/external_pattern_artifact_path_resolver.py`
- `src/moex_runtime/state_store/file_backed_runtime_session_store.py`
- `src/moex_runtime/execution/runtime_position_transition.py`

Delegated-through reference strategy surfaces only:
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`

## 6. exact target layer destination

Exact target destination:
- `src/moex_runtime/orchestrator/`

Exact target boundary to freeze:
- one platform-owned orchestration surface under `src/moex_runtime/orchestrator/`
- portfolio-scoped sequential iteration only
- one shared `environment_id` input
- delegation into the existing single-strategy runtime boundary per enabled strategy id

## 7. exact non-goals

Non-goals for this phase-7 unit:
- no scheduler / cron / daemon design
- no concurrent execution
- no locks expansion
- no risk engine expansion
- no notifier expansion
- no broker routing redesign
- no monitoring stack redesign
- no portfolio netting
- no cross-strategy capital allocation
- no artifact model redesign
- no registry-model redesign beyond the minimum blocker-removal needed to delegate by enabled strategy id
- no backtest/research mixing
- no strategy signal math changes
- no strategy package redesign
- no legacy broad audit

## 8. acceptance criteria for later apply

The later apply is acceptable only if all points below are true:

- a new platform-owned orchestration surface exists under `src/moex_runtime/orchestrator/`
- it accepts exactly one `portfolio_id` and one shared `environment_id`
- it reads enabled strategy ids from portfolio config/registry rather than from hardcoded call sites
- it iterates strategies sequentially only
- it delegates one call per strategy into the single-strategy runtime boundary rather than inlining strategy runtime logic
- it collects one result record per delegated strategy run
- it returns a fail-closed portfolio outcome when any delegated strategy run fails
- it introduces no scheduler, daemon, lock, risk, broker, notifier, netting, or allocation work
- the single-strategy runtime boundary becomes reusable for each enabled strategy id by removing the currently proven EMA-only blockers, with no broader registry-model redesign
- existing phase-6 shared infra surfaces remain reused rather than duplicated
- the current reference single-strategy runtime path remains callable with the same ids and unchanged artifact/env contracts

## 9. blockers if any

Proven blockers are present.

Exact blockers:
- `src/moex_core/contracts/registry_loader.py` currently rejects any portfolio whose `enabled_strategy_ids` is not exactly `[strategy_id]`
- `load_registered_runtime_boundary(...)` currently resolves runtime state/trade-log contracts by EMA-only artifact ids
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` still imports the EMA reference feature/signal/live-adapter surfaces directly

These are narrow boundary-reuse blockers, not evidence against the orchestrator as the correct first phase-7 unit.

## 10. one sentence final scope statement

Freeze phase-7 first as one platform-owned portfolio-scoped sequential runtime orchestrator under `src/moex_runtime/orchestrator/` that delegates per enabled strategy into the single-strategy runtime boundary, while treating the current EMA-pinned boundary reuse constraints as explicit blocker work for later apply.
