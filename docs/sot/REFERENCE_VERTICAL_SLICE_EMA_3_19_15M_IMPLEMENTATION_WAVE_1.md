# REFERENCE VERTICAL SLICE — EMA 3/19 15m — IMPLEMENTATION WAVE 1

Status: FROZEN NARROW REMEDIATION SPEC  
Project: MOEX Bot  
Reference strategy: `ema_3_19_15m`  
Cycle type: narrow remediation  
Authority: `MOEX_Bot_Target_Architecture_2026_All_In_One.md`, `STRATEGY_SDK_SPEC.md`, `BACKTEST_EXECUTION_SEMANTICS_SPEC.md`, `REGISTRY_CONFIG_MODEL_SPEC.md`, `REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_SPEC.md`

---

## 1. Scope of wave-1

Wave-1 is the smallest real migration step that creates a target-owned strategy unit without pulling runtime, registry, or full backtest platform rewrite into the same change.

Wave-1 includes only:
- minimal SDK base surfaces required by the frozen contracts
- one importable target strategy package at `src/strategies/ema_3_19_15m/`
- extraction of pure EMA strategy identity/config/signal logic into that package
- explicit strategy-local artifact/state declarations for the package boundary
- one normalized backtest adapter boundary surface

Wave-1 explicitly excludes:
- registry wiring
- runtime boundary wiring
- CLI thinning
- 15m aggregation extraction from legacy loop
- canonical backtest runner implementation
- generic runtime state / logging / reconcile framework migration
- broad legacy cleanup

---

## 2. VERDICT

Wave-1 must be package-first, not platform-wide.

The exact wave-1 objective is:
- create the minimal frozen SDK base
- create the first importable target package `src/strategies/ema_3_19_15m/`
- move only pure strategy-owned logic into that package
- stop before registry/runtime/backtest orchestration changes

This is the minimum implementable step that is consistent with the frozen SoT and the current repo state.

---

## 3. PROOF BASIS

### 3.1 Frozen SoT sections used

- target architecture: strategy package is the unit of extension; CLI/runtime/backtest infra remain outside strategy package
- SDK spec: six required strategy files are mandatory; generic infra is forbidden inside the package
- semantics spec: canonical execution semantics remain platform-owned; strategy must not own inline PnL/execution timing logic
- registry/config spec: registry-driven startup is required in target state, but registry/config is a separate contract surface and need not be pulled into the first package-extraction wave
- reference vertical slice spec: final slice includes SDK base, strategy package, registry, feature boundary extraction, backtest boundary, and runtime boundary in that order

### 3.2 Repo compatibility points used

Current repo state shows:
- legacy realtime strategy logic exists already
- target SDK surfaces do not yet exist
- target package `src/strategies/ema_3_19_15m/` does not yet exist
- current repo import style is `src.*`
- current realtime loop still mixes strategy, aggregation, execution, journaling, lock, and preflight responsibilities

### 3.3 Exact current files used as source material

Used as direct source material:
- `src/strategy/realtime/ema_3_19_15m/signal_engine_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/session_state_ema_3_19_15m.py`
- `src/cli/loop_ema_3_19_15m_realtime.py`
- `src/research/ema/lib_ema_search.py`

Used as compatibility-only reference, not as target boundary:
- `src/strategy/realtime/ema_3_19_15m/executor_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/trade_logger_ema_3_19_15m.py`
- `src/research/ema/build_ema_pnl_multitimeframe.py`

### 3.4 Blockers

No blocker for freezing wave-1 scope.

---

## 4. WAVE-1 IMPLEMENTATION SPEC

### 4.1 Objective

After wave-1, the repo must have:
- loadable SDK base types/errors/contracts under `src/moex_strategy_sdk/`
- loadable target package `src/strategies/ema_3_19_15m/`
- pure EMA 3/19 15m signal logic extracted from legacy realtime code into `signal_engine.py`
- typed config and explicit artifact declarations for the target package
- normalized backtest adapter contract surface

After wave-1, the repo must not yet have:
- registry-backed startup
- runtime startup through target package
- CLI loop refactor
- platform runtime reconcile/state/logging migration
- platform feature materialization extraction for 15m bars
- canonical engine wiring beyond normalized request boundary

### 4.2 Exact minimal file scope

#### Create

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

#### Update

None required for wave-1.

#### No-touch on this wave

Must remain untouched:
- `src/cli/loop_ema_3_19_15m_realtime.py`
- `src/strategy/realtime/ema_3_19_15m/signal_engine_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/session_state_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/executor_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/trade_logger_ema_3_19_15m.py`
- `src/research/ema/lib_ema_search.py`
- `src/research/ema/build_ema_pnl_multitimeframe.py`
- any `configs/*` registry records
- any `src/moex_runtime/*` runtime boundary files
- any `src/moex_backtest/*` engine/runner files
- any `src/moex_features/*` aggregation/materialization files

### 4.3 Exact target surfaces for wave-1

#### Minimal `src/moex_strategy_sdk/*` surfaces required now

`manifest.py` must define minimal `StrategyManifest` type.

`interfaces.py` must define minimal frozen boundary types used by wave-1 only:
- `StrategyInputFrame`
- `StrategySignalFrame`
- `BacktestAdapterRequest`
- `LiveStrategyInput`
- `LiveAdapterDecision`

`config_schema.py` must define minimal `BaseStrategyConfig` contract.

`artifact_contracts.py` must define minimal `ArtifactContract` contract and allowed classes/roles.

`errors.py` must define frozen error classes at least:
- `StrategyRegistrationError`
- `StrategyIdMismatchError`
- `ManifestValidationError`
- `ConfigValidationError`
- `InterfaceValidationError`
- `ArtifactContractValidationError`
- `UnsupportedModeError`
- `ForbiddenResponsibilityError`

#### Minimal `src/strategies/ema_3_19_15m/*` surfaces required now

`manifest.py`
- declares `strategy_id = ema_3_19_15m`
- declares `timeframe = 15m`
- declares single-instrument scope
- declares required dataset/feature ids as frozen dependencies
- sets `supports_backtest = true`
- sets `supports_live = false` for wave-1

`config.py`
- defines typed EMA 3/19 15m config
- fixes parameter defaults/bounds
- rejects unknown fields

`signal_engine.py`
- contains only deterministic EMA crossover logic on finalized 15m inputs
- produces normalized desired-position signal rows
- contains no IO, no state persistence, no runtime loop logic

`backtest_adapter.py`
- builds normalized request only
- does not run PnL engine
- does not choose files

`live_adapter.py`
- exports required symbol
- raises `UnsupportedModeError` for wave-1

`artifact_contracts.py`
- declares only strategy-visible artifacts:
  - 15m input frame
  - normalized backtest output contract reference
  - strategy-local signal state

#### Thin runtime entry boundary

Not required in wave-1.

#### Backtest registration boundary

Not required in wave-1.
Only normalized adapter output is required.

### 4.4 Source-to-target mapping for this wave only

#### 1. Legacy signal engine -> target signal engine

Source:
- `src/strategy/realtime/ema_3_19_15m/signal_engine_ema_3_19_15m.py`

Port in wave-1:
- EMA fast/slow update math
- crossover detection semantics
- target desired-position decision meaning

Do not port now:
- dependency on legacy `SessionStateEma31915m`
- direct mutation of legacy persisted state object
- legacy pending-signal bar-end overwrite logic coupled to raw execution loop

#### 2. Legacy session state -> target config/artifact boundary

Source:
- `src/strategy/realtime/ema_3_19_15m/session_state_ema_3_19_15m.py`

Port in wave-1:
- only strategy-local signal state field semantics needed for explicit artifact declaration:
  - `ema_fast`
  - `ema_slow`
  - `pending_target`
  - `pending_signal_bar_end`

Leave legacy and untouched:
- `last_bar_end`
- `pos`
- `entry_price`
- `realized_pnl`
- `cum_pnl`
- `trade_seq`
- file IO / JSON persistence

#### 3. Legacy realtime loop -> naming compatibility only

Source:
- `src/cli/loop_ema_3_19_15m_realtime.py`

Use in wave-1 only for:
- naming compatibility of strategy id family, instrument, timeframe
- proof that current loop is not thin and therefore must stay no-touch in this wave

Do not port now:
- 15m aggregation
- lock handling
- preflight/gating
- journaling
- execution sequencing loop

#### 4. Research EMA library -> backtest adapter compatibility only

Source:
- `src/research/ema/lib_ema_search.py`

Use in wave-1 only for:
- nearest existing repo-compatible signal/output normalization reference
- proof that canonical execution/backtest ownership must stay outside the strategy package

Do not port now:
- `run_point_backtest`
- daily summarization
- resampling utilities

#### 5. Legacy executor/trade logger -> explicit legacy hold

Sources:
- `src/strategy/realtime/ema_3_19_15m/executor_ema_3_19_15m.py`
- `src/strategy/realtime/ema_3_19_15m/trade_logger_ema_3_19_15m.py`

Wave-1 action:
- no code migration
- no target-file creation from these modules
- treat both as legacy runtime-owned material pending later platform extraction

### 4.5 Exact ordered implementation sequence

#### Step 1
Create minimal SDK error/types/contracts under `src/moex_strategy_sdk/`.

Stop condition:
- any required frozen symbol/type cannot be expressed without pulling runtime/registry/backtest orchestration into the same change

#### Step 2
Create `src/strategies/ema_3_19_15m/manifest.py` and `config.py`.

Stop condition:
- manifest/config cannot validate without hidden cwd/env/path dependency

#### Step 3
Create `src/strategies/ema_3_19_15m/signal_engine.py` by extracting only pure EMA crossover logic.

Stop condition:
- implementation requires file IO, legacy session object import, or direct runtime state mutation

#### Step 4
Create `src/strategies/ema_3_19_15m/backtest_adapter.py` as normalized request mapper only.

Stop condition:
- implementation attempts to bring inline PnL engine, path resolution, or resampling logic into the strategy package

#### Step 5
Create `src/strategies/ema_3_19_15m/live_adapter.py` and `artifact_contracts.py`.

Stop condition:
- live adapter needs runtime reconcile/state/logging framework in this same wave

#### Step 6
Run import/validation checks only.

Stop condition:
- validation requires registry wiring, CLI refactor, or server-only artifacts

### 4.6 Exact acceptance checks for wave-1

#### Must import successfully

SDK:
- `src.moex_strategy_sdk.manifest`
- `src.moex_strategy_sdk.interfaces`
- `src.moex_strategy_sdk.config_schema`
- `src.moex_strategy_sdk.artifact_contracts`
- `src.moex_strategy_sdk.errors`

Reference strategy package:
- `src.strategies.ema_3_19_15m.manifest`
- `src.strategies.ema_3_19_15m.config`
- `src.strategies.ema_3_19_15m.signal_engine`
- `src.strategies.ema_3_19_15m.backtest_adapter`
- `src.strategies.ema_3_19_15m.live_adapter`
- `src.strategies.ema_3_19_15m.artifact_contracts`

#### Must validate successfully

- `STRATEGY_MANIFEST.strategy_id == "ema_3_19_15m"`
- `STRATEGY_MANIFEST.timeframe == "15m"`
- `supports_backtest == True`
- `supports_live == False`
- `validate_config(...)` returns `StrategyConfig`
- `generate_signals(...)` is deterministic on repeated identical input
- `build_backtest_request(...)` returns a normalized request object
- `build_live_decision(...)` raises `UnsupportedModeError` deterministically
- `ARTIFACT_CONTRACTS` declares all wave-1 strategy-visible artifacts explicitly

#### Must not yet be required

- no registry records
- no effective config resolution by ids
- no `src/moex_backtest/*` engine wiring
- no `src/moex_runtime/*` runtime boundary wiring
- no update to `src/cli/loop_ema_3_19_15m_realtime.py`
- no 15m feature materialization extraction
- no server artifact creation
- no runtime/trade-log execution proof

#### Sufficient completion evidence

Wave-1 is complete when all created modules import and the following are true from repo-only checks:
- manifest/config/artifact declarations are internally consistent
- signal engine is pure/deterministic
- backtest adapter is normalized-boundary only
- live adapter is explicitly unsupported, not silently partial
- no legacy runtime/research file was modified

### 4.7 Exact forbidden shortcuts

Forbidden in wave-1:
- modifying `src/cli/loop_ema_3_19_15m_realtime.py`
- moving 15m aggregation into the new strategy package
- copying `run_point_backtest` or any inline PnL engine into `src/strategies/ema_3_19_15m/`
- porting executor/trade logger into the new strategy package
- mixing strategy-local signal state with generic execution/session state
- adding registry records before the package contract exists
- introducing path autodetect / latest-file heuristics into new code
- introducing locks, journaling, preflight, Telegram, or broker calls into the strategy package
- declaring `ema_3_19_15m_block_adverse` as the target strategy id
- broad cleanup of legacy realtime or research surfaces outside the exact no-touch list above

---

## 5. Final frozen statement

Wave-1 is complete only when the repo gains one clean target strategy package plus the minimal SDK base, while legacy runtime/research/orchestration surfaces remain untouched.

Anything broader than that is not wave-1.
