# REFERENCE VERTICAL SLICE — EMA 3/19 15m — IMPLEMENTATION WAVE 3

Status: FROZEN NARROW REMEDIATION SPEC  
Project: MOEX Bot  
Reference strategy: `ema_3_19_15m`  
Cycle type: narrow remediation  
Authority: `MOEX_Bot_Target_Architecture_2026_All_In_One.md`, `STRATEGY_SDK_SPEC.md`, `BACKTEST_EXECUTION_SEMANTICS_SPEC.md`, `REGISTRY_CONFIG_MODEL_SPEC.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_SPEC.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_IMPLEMENTATION_WAVE_1.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_WAVE_1_APPLY_REPORT.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_IMPLEMENTATION_WAVE_2.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_WAVE_2_APPLY_REPORT.md`

---

## VERDICT

Wave-3 is the smallest implementable step that must create the **first thin runtime boundary** for `ema_3_19_15m` and stop there.

After wave-3, the repo must be able to:
- start one exact runtime path by ids only
- resolve the existing reference slice records plus one runtime environment record
- materialize the finalized 15m input frame through the already-created platform feature surface
- call `generate_signals(...)`
- call `build_live_decision(...)`
- reconcile `desired_position` vs current runtime position inside one platform-owned boundary
- write the declared runtime state artifact
- write the declared runtime trade-log artifact

Wave-3 must not do:
- broker execution
- realtime loop migration
- lock/preflight/journal migration
- context-gate migration
- scheduler/orchestrator work
- multi-strategy runtime work
- broad runtime/state/logger redesign
- broad legacy cleanup

---

## PROOF

### Frozen SoT sections used

- target architecture freezes the migration order as: SDK/package -> registry-backed backtest -> thin runtime boundary -> broader runtime work
- SDK freezes `live_adapter.py` as the strategy-owned live boundary and forbids generic runtime infra inside the strategy package
- execution semantics freeze runtime ownership at reconcile/fail-closed boundary, not inside the strategy package
- registry/config freezes startup by ids, explicit environment eligibility, and explicit artifact contracts
- reference vertical slice freezes one minimal `reference_runtime_boundary` environment and keeps runtime/state/logging as platform-owned surfaces

### Repo compatibility points used

Current repo already has after wave-2:
- target package `src/strategies/ema_3_19_15m/`
- registry-backed backtest startup
- platform-owned 15m materialization surface
- platform-owned registered backtest runner
- declared strategy-local state artifact

Current repo still shows:
- `live_adapter.py` is explicitly unsupported
- strategy manifest/registry are still backtest-only
- reference portfolio is still not live-eligible
- no thin runtime environment record exists yet
- no platform runtime boundary file exists yet
- legacy realtime loop still owns aggregation, lock, preflight, execution, and journaling responsibilities

### Exact current files used as source material

Used directly and kept as target-owned inputs:
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `src/moex_core/contracts/registry_loader.py`
- `src/moex_features/intraday/si_15m_ohlc_from_5m.py`
- `src/moex_backtest/engine/run_registered_backtest.py`

Used as source material for exact runtime-only compatibility, not as target boundary files:
- `src/strategy/realtime/ema_3_19_15m/session_state_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/executor_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/trade_logger_ema_3_19_15m.py`

Used as explicit legacy/no-touch proof:
- `src/cli/loop_ema_3_19_15m_realtime.py`

### Blockers if any

No blocker for freezing wave-3 scope.

---

## WAVE-3 IMPLEMENTATION SPEC

### Objective

Wave-3 objective is exactly this:

Create the first **registry-backed one-shot runtime boundary** for `ema_3_19_15m`:

`strategy_id + portfolio_id + environment_id`
-> registry runtime resolution
-> explicit dataset/feature/config linkage
-> finalized 15m input materialization
-> `generate_signals(...)`
-> `build_live_decision(...)`
-> platform-owned reconcile of `desired_position` vs current runtime position
-> explicit runtime state write
-> explicit runtime trade-log write

Nothing broader belongs in wave-3.

### Minimal file scope

#### Create

Runtime config:
- `configs/environments/reference_runtime_boundary.json`

Minimal platform runtime code:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`

#### Update

Strategy package:
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`

Registry / config:
- `configs/strategies/ema_3_19_15m.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`

Platform boundary support:
- `src/moex_core/contracts/registry_loader.py`

#### No-touch on this wave

Must remain untouched:
- `src/cli/loop_ema_3_19_15m_realtime.py`
- all `src/strategy/realtime/ema_3_19_15m/*`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/backtest_adapter.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/moex_features/intraday/si_15m_ohlc_from_5m.py`
- `src/moex_backtest/engine/run_registered_backtest.py`
- `configs/strategies/ema_3_19_15m.default.json`
- `src/research/ema/build_ema_pnl_multitimeframe.py`
- `src/research/ema/lib_ema_search.py`
- all other registry records and contracts created in wave-2

### Exact create/update/no-touch list

#### Create-only part

Wave-3 create-only scope is limited to:
- one runtime environment record
- one runtime boundary runner

#### Required update part

Wave-3 cannot stay fully create-only.

Reason:
- `live_adapter.py` is currently fail-closed unsupported
- manifest/strategy registry/portfolio are currently not live-eligible
- strategy artifact declarations do not yet include the runtime trade-log artifact
- registry loader is currently backtest-only

### Source-to-target mapping

#### 1. Current target `live_adapter.py` -> exact live boundary enablement

Source:
- `src/strategies/ema_3_19_15m/live_adapter.py`

Wave-3 transition:
- replace `UnsupportedModeError` stub with minimal desired-position mapping only
- input remains `LiveStrategyInput`
- output remains `LiveAdapterDecision`
- no broker/order side effects
- no lock/session/logger ownership

What transitions now:
- latest signal -> `desired_position`
- machine-readable `reason_code`
- `supports_execution=True` only when boundary inputs are valid
- minimal `state_patch` limited to strategy-local signal metadata only

What stays out:
- order placement
- retry/reject logic
- session framework
- lock framework
- generic logging framework

#### 2. Existing manifest/registry/portfolio -> exact live eligibility flip

Sources:
- `src/strategies/ema_3_19_15m/manifest.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`

Wave-3 transition:
- enable thin runtime boundary eligibility for the same reference ids

What transitions now:
- `supports_live = true`
- `is_live_allowed = true`

What stays out:
- any second portfolio id
- any second strategy id
- any runtime-policy redesign

#### 3. Existing registered backtest boundary -> runtime startup pattern reuse only

Source:
- `src/moex_backtest/engine/run_registered_backtest.py`

Wave-3 transition:
- reuse only the already-proven id-first startup pattern, artifact-root resolution pattern, and feature-materialization call shape

What stays out:
- backtest semantics
- day-metrics output path
- any backtest engine reuse as runtime substitute

#### 4. Existing feature materialization -> reused unchanged

Source:
- `src/moex_features/intraday/si_15m_ohlc_from_5m.py`

Wave-3 transition:
- no code migration
- no strategy ownership change
- runtime runner reuses the same finalized 15m input surface unchanged

#### 5. Legacy runtime files -> compatibility-only source material

Sources:
- `src/strategy/realtime/ema_3_19_15m/session_state_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/executor_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/trade_logger_ema_3_19_15m.py`

What may transition now:
- exact current-position to desired-position delta meaning at the platform runtime boundary
- trade-log naming compatibility with the existing strategy log family
- minimal strategy-local signal metadata field naming compatibility

What stays legacy and untouched:
- session JSON ownership
- realized PnL ownership
- execution-event ownership
- CSV append helper ownership
- any direct file path ownership inside legacy modules

### Exact target surfaces for wave-3

#### Minimal runtime entry boundary

Wave-3 runtime boundary is one importable one-shot platform-owned entry under:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`

Its minimal contract is:
- input: `strategy_id`, `portfolio_id`, `environment_id`
- resolve dependencies by ids only
- materialize finalized 15m inputs through the existing feature surface
- load prior runtime state/trade-log artifacts if present
- call `generate_signals(...)`
- call `build_live_decision(...)`
- reconcile `desired_position` vs current runtime position
- persist updated state artifact
- append runtime trade-log row only when position delta is non-zero

It must not:
- loop forever
- acquire locks
- call broker/exchange APIs
- reuse the legacy realtime CLI
- own generic scheduler/orchestrator design

#### Minimal runtime-facing config/registry surfaces if strictly required

Required exact runtime-facing records after wave-3:
- existing `strategy_id = ema_3_19_15m` updated to live-eligible
- existing `portfolio_id = reference_ema_3_19_15m_single` updated to live-eligible
- new `environment_id = reference_runtime_boundary`

`reference_runtime_boundary` must be minimal only:
- runtime mode record
- explicit required env var names only
- explicit artifact root refs only
- no secrets in repo
- no broker credentials in repo

No new instrument/dataset/feature ids belong in wave-3.

#### Minimal wiring from `src/strategies/ema_3_19_15m/live_adapter.py`

`live_adapter.py` must do only this:
- accept `LiveStrategyInput`
- accept already-generated normalized signal frame context
- emit one `LiveAdapterDecision`
- set `desired_position` from strategy signal meaning only
- set `reason_code`
- set `supports_execution`
- emit only minimal strategy-local `state_patch`

`live_adapter.py` must not do:
- file IO
- path resolution
- current-position reconciliation
- broker/order logic
- trade-log writing
- lock/session/logger ownership

#### Minimal platform runtime surfaces needed for thin boundary only

Wave-3 platform runtime surface is only:
- runtime registry resolution in `src/moex_core/contracts/registry_loader.py`
- one runtime runner in `src/moex_runtime/engine/run_registered_runtime_boundary.py`

No other platform runtime surface is required in wave-3.

### Ordered steps

#### Step 1
Update the exact live-facing strategy and registry surfaces only:
- `live_adapter.py`
- `manifest.py`
- `artifact_contracts.py`
- strategy registry record
- reference portfolio record
- new runtime environment record

Stop condition:
- implementation requires new strategy ids, new portfolio ids, or new dataset/feature ids

#### Step 2
Extend `src/moex_core/contracts/registry_loader.py` with one exact runtime-resolution path.

It must do only:
- load the same reference slice ids plus `reference_runtime_boundary`
- validate live eligibility consistency
- resolve the declared runtime state/trade-log artifacts
- fail closed on missing env/artifact/eligibility mismatch

Stop condition:
- implementation drifts into broad generic runtime framework work

#### Step 3
Create `src/moex_runtime/engine/run_registered_runtime_boundary.py`.

It must do only:
- start by ids
- load runtime state/trade log if present
- materialize finalized 15m inputs
- call the existing strategy package exports
- reconcile position delta inside the platform boundary
- write explicit runtime artifacts

Stop condition:
- implementation tries to become realtime loop, scheduler, broker executor, or platform-wide runtime redesign

#### Step 4
Run import/validation/startup checks for the exact reference runtime path only.

Stop condition:
- validation requires modifying the legacy realtime loop or any legacy runtime module

### Acceptance checks

#### What must import successfully

Updated files:
- `src.moex_core.contracts.registry_loader`
- `src.strategies.ema_3_19_15m.live_adapter`
- `src.strategies.ema_3_19_15m.manifest`
- `src.strategies.ema_3_19_15m.artifact_contracts`

New file:
- `src.moex_runtime.engine.run_registered_runtime_boundary`

#### What must validate successfully

- strategy manifest and strategy registry are consistent with `supports_live = true`
- reference portfolio is consistent with `is_live_allowed = true`
- `reference_runtime_boundary` is active and runtime-eligible
- `build_live_decision(...)` returns a contract-valid `LiveAdapterDecision`
- strategy artifact contracts explicitly declare:
  - `ema_3_19_15m_signal_state`
  - `ema_3_19_15m_trade_log`
- runtime loader fails closed on missing required artifact-root env var or live-eligibility mismatch

#### What must run successfully

One exact start path must run:
- `strategy_id = ema_3_19_15m`
- `portfolio_id = reference_ema_3_19_15m_single`
- `environment_id = reference_runtime_boundary`

That run must:
- resolve dependencies by ids only
- avoid glob/latest-file heuristics
- materialize finalized 15m inputs through the existing feature surface
- produce one `LiveAdapterDecision`
- reconcile `desired_position` vs current runtime position inside the platform runner
- write declared runtime state JSON explicitly
- write declared runtime trade-log CSV explicitly
- perform no broker/network/order side effects

#### Sufficient evidence for completion

Wave-3 is complete when repo-side evidence shows:
- all exact runtime files/records exist
- live eligibility validation succeeds for the reference slice ids
- one exact runtime-boundary start path runs end-to-end
- explicit runtime state and trade-log artifacts are emitted
- no legacy realtime file was modified

### Forbidden shortcuts

Forbidden in wave-3:
- modifying `src/cli/loop_ema_3_19_15m_realtime.py`
- jumping into full runtime rewrite
- jumping into scheduler/orchestrator work
- jumping into broker execution
- jumping into multi-strategy orchestration
- copying generic runtime/state/lock/logger infra into `src/strategies/ema_3_19_15m/`
- copying legacy executor/session/trade-logger modules into the strategy package
- mixing thin runtime boundary with platform-wide runtime redesign
- keeping runtime startup dependent on path guessing or latest-file heuristics
- pulling D-day context gating into the target strategy package
- renaming the target strategy to `ema_3_19_15m_block_adverse`

---

## GITHUB SOT FILE

- exact path: `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_IMPLEMENTATION_WAVE_3.md`
- saved as the frozen minimal wave-3 spec for the first thin runtime boundary

---

## BLOCKERS

None.
