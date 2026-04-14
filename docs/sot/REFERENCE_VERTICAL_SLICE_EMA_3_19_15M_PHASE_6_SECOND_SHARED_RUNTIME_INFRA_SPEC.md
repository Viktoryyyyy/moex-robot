## 1. verdict

The remaining runtime position-transition logic inside `src/moex_runtime/engine/run_registered_runtime_boundary.py` is already generic platform infra.

It is the correct next narrow phase-6 extraction unit.

This spec approves extraction of that unit into `src/moex_runtime/execution/` only.

## 2. repo proof

- `src/moex_runtime/engine/run_registered_runtime_boundary.py` still owns the candidate unit inline:
  - current-position reconciliation from persisted runtime state
  - fallback reconciliation from the last trade-log row
  - position-change detection
  - generic action classification via `_action(...)`
  - generic runtime state update assembly around `decision.state_patch`
- `src/strategies/ema_3_19_15m/live_adapter.py` already stops at strategy-local outputs:
  - `desired_position`
  - `reason_code`
  - `state_patch`
  It does not own current-position reconciliation, action classification, or runtime trade/state transition handling.
- `src/moex_strategy_sdk/interfaces.py` exposes `LiveAdapterDecision` as a strategy output contract, which confirms that reconcile-vs-current-state remains runtime-owned execution behavior.
- `src/strategies/ema_3_19_15m/artifact_contracts.py` declares both runtime artifacts with `producer="moex_runtime"`, proving that persisted runtime transition handling is platform-owned rather than strategy-owned.
- The target architecture already reserves `moex_runtime/execution/` for shared runtime execution responsibilities and forbids keeping generic runtime framework responsibilities inside strategy packages.
- The legacy realtime path also contains the same three-state position transition/action behavior in `src/strategy/realtime/ema_3_19_15m/executor_ema_3_19_15m.py`, which further proves that this is execution infra rather than EMA-specific signal math.

## 3. chosen extraction unit or blocker

Chosen extraction unit:

`generic runtime position-transition execution unit`

Exact responsibility of that unit:
- reconcile `current_position` from persisted runtime state and, when needed, the last trade-log row
- detect whether `desired_position` differs from `current_position`
- classify the generic runtime transition action for `-1 / 0 / 1`
- assemble the generic runtime-owned state update fields around strategy-provided `state_patch`

No blocker is present in current repo proof.

## 4. why it is platform-shared and not strategy-local

This unit is platform-shared because it operates only on generic runtime concepts:
- persisted current position
- desired position
- transition action
- runtime state metadata
- runtime trade-log continuity

It is not strategy-local because EMA-specific responsibility already ends before this boundary at signal generation and live decision production. The strategy returns a desired position and state patch; the runtime decides how that desired position is reconciled and recorded.

## 5. exact current file scope

Exact current source scope approved for later extraction:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`

Exact inline responsibility inside that file:
- `_coerce_position(...)`
- `_action(...)`
- `current_position` reconciliation block
- `position_changed` detection block
- action assignment block
- generic `updated_state` assembly block around `decision.state_patch`

Source-proof compatibility file only:
- `src/strategy/realtime/ema_3_19_15m/executor_ema_3_19_15m.py`

Explicit no-touch strategy-local files for this extraction:
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/manifest.py`

## 6. exact target layer destination

Exact target destination:
- `src/moex_runtime/execution/`

Exact target boundary to freeze:
- one platform-owned execution surface under `src/moex_runtime/execution/`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` remains the thin caller/orchestrating boundary for the reference path

This spec freezes the destination layer and responsibility boundary only.

## 7. exact non-goals

Non-goals for this extraction:
- no path resolution / locator redesign
- no state-store redesign
- no scheduler work
- no lock framework work
- no risk-gate work
- no notifier work
- no feed materialization migration
- no strategy signal generation changes
- no live adapter semantics change
- no registry/config redesign
- no artifact/env contract redesign
- no broker routing or execution venue work
- no backtest/research mixing
- no multi-strategy orchestration
- no legacy broad audit

## 8. acceptance criteria for later apply

The later apply is acceptable only if all points below are true:

- a new platform-owned execution surface exists under `src/moex_runtime/execution/`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` no longer owns inline position-transition execution helpers and blocks listed in this spec
- the reference runtime path still starts by the same ids only:
  - `strategy_id = ema_3_19_15m`
  - `portfolio_id = reference_ema_3_19_15m_single`
  - `environment_id = reference_runtime_boundary`
- artifact ids, locator refs, partition semantics, and env contracts remain unchanged
- strategy-local files under `src/strategies/ema_3_19_15m/` remain unchanged
- `decision.state_patch` remains strategy-provided and is only wrapped by generic runtime-owned state fields
- emitted action values and `current_position` semantics remain unchanged
- runtime trade-log continuity and runtime state continuity remain unchanged
- one exact end-to-end run of `run_registered_runtime_boundary(...)` still succeeds with no broker/network side effects

## 9. blockers if any

None.

Current repo state is sufficient to freeze this extraction unit without widening scope.

## 10. one sentence final scope statement

Freeze only the extraction of generic runtime position-transition execution logic out of `src/moex_runtime/engine/run_registered_runtime_boundary.py` into `src/moex_runtime/execution/`, with no behavior change and no contract redesign.
