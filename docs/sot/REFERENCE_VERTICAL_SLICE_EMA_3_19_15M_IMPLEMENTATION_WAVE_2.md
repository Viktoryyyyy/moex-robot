# REFERENCE VERTICAL SLICE — EMA 3/19 15m — IMPLEMENTATION WAVE 2

Status: FROZEN NARROW REMEDIATION SPEC  
Project: MOEX Bot  
Reference strategy: `ema_3_19_15m`  
Cycle type: narrow remediation  
Authority: `MOEX_Bot_Target_Architecture_2026_All_In_One.md`, `STRATEGY_SDK_SPEC.md`, `BACKTEST_EXECUTION_SEMANTICS_SPEC.md`, `REGISTRY_CONFIG_MODEL_SPEC.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_SPEC.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_IMPLEMENTATION_WAVE_1.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_WAVE_1_APPLY_REPORT.md`

---

## VERDICT

Wave-2 is the smallest implementable step that must create the **first registry-backed backtest path** for `ema_3_19_15m` and stop there.

After wave-2, the repo must be able to:
- resolve one backtest run by explicit ids only
- validate minimal registry/config linkage for the reference slice
- materialize the required finalized 15m input frame through a platform-owned feature surface
- call the existing target strategy package unchanged
- run canonical backtest semantics through a platform-owned backtest boundary
- emit the declared backtest day-metrics artifact explicitly

Wave-2 must not yet do:
- runtime wiring
- live execution wiring
- CLI loop thinning
- live adapter enablement
- lock/state/logger/reconcile migration
- broad registry rollout beyond the exact reference slice records
- broad platform generalization beyond what is strictly needed for this one backtest path

---

## PROOF

### Frozen SoT sections used

- target architecture freezes the migration order as: SDK -> semantics -> registry/config -> reference slice -> shared infra, and forbids jumping to multi-strategy/runtime work first
- the reference slice freezes one exact registry-backed backtest path for `ema_3_19_15m` before thin runtime work
- SDK v1 already froze the target package shape and required exports; those surfaces now exist after wave-1
- canonical semantics freeze finalized-bar, next-bar-open, open-to-open, reversal-count, commission, and forced-close behavior as platform-owned
- registry/config freezes six registries, explicit ids, explicit artifact linkage, and fail-closed run-start validation

### Repo compatibility points used

Current repo already has after wave-1:
- target package `src/strategies/ema_3_19_15m/`
- `supports_backtest = True`
- `supports_live = False`
- normalized `build_backtest_request(...)` boundary
- explicit strategy-visible artifact declarations

Current repo still does not have:
- registry records for the reference slice
- platform registry loader/validator surface
- platform-owned 15m feature materialization boundary for the slice backtest path
- platform-owned canonical backtest entry boundary for registry startup

### Exact current files used as source material

Used directly and kept as target-owned strategy inputs:
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/backtest_adapter.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`

Used as source material for platform-owned extraction only:
- `src/research/ema/build_ema_pnl_multitimeframe.py`
- `src/research/ema/lib_ema_search.py`

Used as explicit legacy/no-touch proof for this wave:
- `src/cli/loop_ema_3_19_15m_realtime.py`
- `src/strategy/realtime/ema_3_19_15m/signal_engine_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/session_state_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/executor_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/trade_logger_ema_3_19_15m.py`

### Blockers if any

No blocker for freezing wave-2 scope.

---

## WAVE-2 IMPLEMENTATION SPEC

### Objective

Wave-2 objective is exactly this:

Create the first **registry-backed backtest-only** path for `ema_3_19_15m`:

`strategy_id + portfolio_id + environment_id`
-> registry resolution
-> explicit dataset/feature/config linkage
-> platform-owned 15m finalized-bar materialization
-> `generate_signals(...)`
-> `build_backtest_request(...)`
-> canonical backtest semantics
-> explicit `ema_3_19_15m_backtest_day_metrics` output

Nothing broader belongs in wave-2.

### Minimal file scope

#### Create

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

#### Update

None required for wave-2.

#### No-touch on this wave

Must remain untouched:
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/cli/loop_ema_3_19_15m_realtime.py`
- all `src/strategy/realtime/ema_3_19_15m/*`
- `src/research/ema/build_ema_pnl_multitimeframe.py`
- `src/research/ema/lib_ema_search.py`
- any `src/moex_runtime/*`
- any live/backtest CLI entrypoint outside the one new registry-backed backtest boundary

### Exact create/update/no-touch list

#### Create-only rule

Wave-2 should remain create-only.

Reason:
- wave-1 package surfaces already exist
- first backtest path can be added around them
- runtime/live/legacy boundaries do not need modification for this step

#### Exact no-touch rule

Wave-2 must not modify the existing target strategy package except by reading its frozen exports.

### Source-to-target mapping

#### 1. Existing target strategy package -> reused unchanged

Use unchanged:
- `manifest.py` for strategy identity/version/dependency truth
- `config.py` for typed config validation
- `signal_engine.py` for deterministic EMA signal generation
- `backtest_adapter.py` for normalized strategy-to-engine request mapping
- `artifact_contracts.py` for strategy-visible output contract

No strategy-file migration is allowed in wave-2.

#### 2. Current research backtest script -> replaced as system boundary

Source:
- `src/research/ema/build_ema_pnl_multitimeframe.py`

Only the following may transition into wave-2:
- exact backtest-start shape for EMA 3/19 15m on canonical OHLC input
- day-metrics output intent

Must remain legacy and untouched:
- direct script startup as the system boundary
- path glob / latest-file selection
- mixed research/boundary responsibilities

#### 3. Current research EMA library -> split into platform-owned pieces only

Source:
- `src/research/ema/lib_ema_search.py`

Only the following may transition into wave-2:
- finalized 15m resampling logic into `src/moex_features/`
- canonical next-bar-open/open-to-open execution logic into `src/moex_backtest/`
- daily summary logic needed for declared day-metrics output

Must remain legacy and untouched:
- research helper ownership of canonical backtest entry
- reuse of `lib_ema_search.py` as the platform boundary file

#### 4. Legacy realtime path -> explicit hold

Wave-2 does not port any runtime/reconcile/session/logger/executor logic.
All current realtime files remain legacy and untouched.

### Exact target surfaces for wave-2

#### Minimal registry records needed

Required exact ids:
- `instrument_id = si`
- `dataset_id = si_fo_5m_intraday`
- `feature_set_id = si_15m_ohlc_from_5m`
- `strategy_id = ema_3_19_15m`
- `portfolio_id = reference_ema_3_19_15m_single`
- `environment_id = reference_backtest`

Required extra config record:
- default strategy config for `ema_3_19_15m`

#### Minimal config surfaces needed

`configs/strategies/ema_3_19_15m.default.json` must be the only strategy-default config surface in wave-2.

It must contain only:
- `strategy_id`
- `version`
- `params`
- `artifact_bindings`
- `runtime_policy_ref = null`
- `risk_policy_ref = null`

No environment-specific live fields belong there.

#### Minimal backtest entry boundary

Wave-2 backtest boundary is one importable platform-owned entry under:
- `src/moex_backtest/engine/run_registered_backtest.py`

Its minimal contract is:
- input: `strategy_id`, `portfolio_id`, `environment_id`
- resolves all other dependencies from registries and declared artifact contracts only
- no path guessing
- no live/runtime concerns
- no CLI loop reuse

#### Minimal wiring from strategy package to canonical semantics

Exact minimal wiring:
- registry loader resolves strategy/config/dataset/feature/portfolio/environment records
- feature surface materializes finalized 15m bars from the registered 5m dataset
- runner passes the feature frame into `generate_signals(...)`
- runner passes signals into `build_backtest_request(...)`
- canonical backtest logic executes with frozen semantics only
- output writer emits `ema_3_19_15m_backtest_day_metrics`

### Ordered steps

#### Step 1
Create minimal artifact contracts and registry records for the exact reference slice ids only.

Stop condition:
- any required id cannot be linked explicitly
- any record needs path guessing or hidden cwd dependency

#### Step 2
Create `src/moex_core/contracts/registry_loader.py`.

It must do only:
- load the exact required records
- validate cross-links
- validate manifest/registry/config consistency
- fail closed on missing or inactive dependency

Stop condition:
- implementation drifts into broad generic registry framework work

#### Step 3
Create `src/moex_features/intraday/si_15m_ohlc_from_5m.py`.

It must do only:
- load the registered 5m dataset artifact
- produce finalized 15m bars for the reference feature id
- stay outside the strategy package and outside CLI/runtime

Stop condition:
- implementation requires touching runtime loop or strategy package files

#### Step 4
Create `src/moex_backtest/engine/run_registered_backtest.py`.

It must do only:
- start by ids
- call registry loader
- materialize the registered feature frame
- call the existing strategy package exports
- execute canonical semantics
- emit explicit day metrics output

Stop condition:
- implementation tries to enable live mode, runtime wiring, CLI thinning, or broad backtest framework expansion

#### Step 5
Run import/validation/startup checks for the exact reference slice path only.

Stop condition:
- validation requires runtime/live surfaces or any legacy loop modification

### Acceptance checks

#### What must import successfully

New files:
- `src.moex_core.contracts.registry_loader`
- `src.moex_features.intraday.si_15m_ohlc_from_5m`
- `src.moex_backtest.engine.run_registered_backtest`

Existing reused files:
- `src.strategies.ema_3_19_15m.manifest`
- `src.strategies.ema_3_19_15m.config`
- `src.strategies.ema_3_19_15m.signal_engine`
- `src.strategies.ema_3_19_15m.backtest_adapter`
- `src.strategies.ema_3_19_15m.artifact_contracts`

#### What must validate successfully

- strategy registry record matches manifest `strategy_id`, `version`, `timeframe`, `required_dataset_ids`, `required_feature_set_ids`, and support flags
- default strategy config validates through `validate_config(...)`
- portfolio enables exactly `ema_3_19_15m`
- environment is backtest-enabled and not live-enabled
- dataset and feature records are active and resolve through explicit artifact refs only
- feature row semantics are finalized-bar and lookahead-safe

#### What must run successfully

One exact start path must run:
- `strategy_id = ema_3_19_15m`
- `portfolio_id = reference_ema_3_19_15m_single`
- `environment_id = reference_backtest`

That run must:
- resolve dependencies by ids only
- avoid glob/latest-file heuristics
- build finalized 15m inputs
- produce normalized signals
- execute canonical next-bar-open/open-to-open semantics
- write the declared day-metrics artifact explicitly

#### Sufficient evidence for completion

Wave-2 is complete when repo-side evidence shows:
- all exact records and code surfaces exist
- registry validation succeeds for the reference slice ids
- one registry-backed backtest start path runs end-to-end
- output artifact `ema_3_19_15m_backtest_day_metrics` is emitted explicitly
- no runtime/live/legacy files were modified

### Forbidden shortcuts

Forbidden in wave-2:
- jumping into runtime wiring
- jumping into live execution
- enabling `supports_live`
- modifying `src/cli/loop_ema_3_19_15m_realtime.py`
- broad registry rollout beyond the exact reference-slice ids
- keeping `src/research/ema/build_ema_pnl_multitimeframe.py` as the system backtest boundary
- keeping glob/latest-file selection in the new path
- putting 15m aggregation into the strategy package
- putting canonical backtest semantics into the strategy package
- pulling platform responsibilities into `src/strategies/ema_3_19_15m/*`
- touching legacy executor/logger/session/runtime surfaces in this wave

---

## GITHUB SOT FILE

- exact path: `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_IMPLEMENTATION_WAVE_2.md`
- saved as the frozen minimal wave-2 spec for the first registry-backed backtest path

---

## BLOCKERS

None.
