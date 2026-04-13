## 1. verdict

The first phase-6 extraction unit is the file-backed runtime session store for the reference live path.

This unit must be extracted first from the current one-shot runtime boundary because it is already platform-owned infra and not strategy logic.

The extraction scope is exactly:
- load prior runtime state artifact
- read the last emitted trade-log row
- allocate the next runtime sequence number
- atomically persist updated runtime state
- idempotently append the runtime trade-log row

This spec does not approve any broader runtime redesign.

## 2. repo proof

- `src/strategies/ema_3_19_15m/live_adapter.py` is already strategy-local only: it maps normalized signal state into `LiveAdapterDecision` and emits only `state_patch`; it does not own file IO, path resolution, current-position reconciliation, or trade-log writing.
- `src/strategies/ema_3_19_15m/artifact_contracts.py` already declares both runtime artifacts as platform-produced:
  - `ema_3_19_15m_signal_state`
  - `ema_3_19_15m_trade_log`
  Both have `producer="moex_runtime"`.
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` currently contains the generic file-backed session responsibilities inline:
  - `_load_state(...)`
  - `_save_state(...)`
  - `_read_last_trade_log_row(...)`
  - `_next_seq(...)`
  - `_append_trade_log_row(...)`
  - runtime-state assembly/update inside `run_registered_runtime_boundary(...)`
- The same responsibility also exists in the legacy runtime path, proving that it is not EMA math and not package-local strategy logic:
  - `src/strategy/realtime/ema_3_19_15m/session_state_ema_3_19_15m.py`
  - `src/strategy/realtime/ema_3_19_15m/trade_logger_ema_3_19_15m.py`
- `src/cli/loop_ema_3_19_15m_realtime.py` still imports legacy session/logging surfaces separately from signal logic, which is further proof that runtime session persistence is its own infra responsibility.

## 3. chosen first extraction unit

Chosen extraction unit:

`file-backed runtime session store`

Exact responsibility of that unit:
- consume already-resolved runtime state contract and runtime trade-log contract
- load current persisted runtime session state
- inspect the last persisted trade-log row when present
- provide next sequence allocation for a new emitted trade event
- persist updated runtime state atomically
- append the runtime trade-log row idempotently

The one-shot runtime runner remains the caller.
The strategy package remains unchanged.

## 4. why this unit is platform-shared and not strategy-local

This unit is platform-shared because:
- artifact ownership is already declared as `producer="moex_runtime"`, not `producer="ema_3_19_15m"`
- the logic is independent of EMA crossover math and independent of strategy parameter semantics
- the same persistence pattern is needed by any live strategy that writes runtime state and execution/trade artifacts
- the target architecture explicitly forbids strategy packages from retaining generic session framework or generic logger/state ownership

This unit is not strategy-local because strategy-local responsibility already stops at:
- signal generation in `signal_engine.py`
- desired-position mapping in `live_adapter.py`

## 5. exact current file scope proven in repo

Exact current source scope for extraction:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`

Exact inline responsibility in that file:
- `_load_state(...)`
- `_save_state(...)`
- `_read_last_trade_log_row(...)`
- `_next_seq(...)`
- `_append_trade_log_row(...)`
- the `updated_state` assembly block in `run_registered_runtime_boundary(...)`
- the position-change branch that writes the runtime trade-log row

Compatibility/source-proof files only:
- `src/strategy/realtime/ema_3_19_15m/session_state_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/trade_logger_ema_3_19_15m.py`

Explicit no-move / no-touch strategy-local files for this extraction:
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/manifest.py`

## 6. exact target layer destination

Exact target destination:
- `src/moex_runtime/state_store/`

Exact target shape to use in the later apply cycle:
- one platform-owned state-store surface under `src/moex_runtime/state_store/`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` remains only a thin caller/orchestrating boundary for the reference path

This spec does not freeze the exact filename beyond the destination layer.
It freezes the destination layer and responsibility boundary.

## 7. exact non-goals

Non-goals for this extraction:
- no multi-strategy runtime orchestration
- no scheduler work
- no lock framework extraction
- no preflight extraction
- no risk-gate extraction
- no notifier extraction
- no broker execution or order-routing work
- no feed loading or feature-materialization migration
- no strategy signal math migration
- no manifest/config semantics change
- no registry-model redesign
- no backtest/research concern mixing
- no legacy realtime loop rewrite

## 8. acceptance criteria for later apply

The later apply is acceptable only if all points below are true:

- a new platform-owned runtime session-store surface exists under `src/moex_runtime/state_store/`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` no longer owns the inline low-level helpers for runtime state JSON and runtime trade-log CSV persistence
- the reference runtime path still starts by the same ids only:
  - `strategy_id = ema_3_19_15m`
  - `portfolio_id = reference_ema_3_19_15m_single`
  - `environment_id = reference_runtime_boundary`
- artifact contract semantics remain unchanged for:
  - `ema_3_19_15m_signal_state`
  - `ema_3_19_15m_trade_log`
- emitted runtime artifact naming and partition semantics remain unchanged
- runtime state persistence remains atomic
- trade-log append remains idempotent for the same last event identity
- `src/strategies/ema_3_19_15m/live_adapter.py` remains free of generic session/log/state IO
- `src/strategies/ema_3_19_15m/signal_engine.py` remains untouched
- legacy files under `src/strategy/realtime/ema_3_19_15m/` remain untouched in this extraction cycle
- one exact end-to-end run of `run_registered_runtime_boundary(...)` still succeeds with no broker/network side effects

## 9. blockers, if any

None.

Current repo state already provides enough proof to freeze this extraction unit without guessing.

## 10. one exact next narrow step after this spec

Implement one narrow apply cycle that extracts the file-backed runtime session store from `src/moex_runtime/engine/run_registered_runtime_boundary.py` into `src/moex_runtime/state_store/` and rewires the runner to call that surface with no behavior change.