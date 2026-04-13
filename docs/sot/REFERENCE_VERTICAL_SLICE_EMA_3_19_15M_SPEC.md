# REFERENCE VERTICAL SLICE — EMA 3/19 15m

Status: FROZEN IMPLEMENTABLE MIGRATION SPEC  
Project: MOEX Bot  
Reference strategy: `ema_3_19_15m`  
Authority: `MOEX_Bot_Target_Architecture_2026_All_In_One.md`, `STRATEGY_SDK_SPEC.md`, `BACKTEST_EXECUTION_SEMANTICS_SPEC.md`, `REGISTRY_CONFIG_MODEL_SPEC.md`

---

## 1. Scope

### Included

- exactly one reference strategy package: `src/strategies/ema_3_19_15m/`
- exactly one instrument scope for the slice
- exactly one registry-backed backtest path for this strategy
- exactly one thin runtime boundary for this strategy
- explicit artifact contracts for backtest inputs, backtest outputs, runtime state, and runtime trade log
- migration of strategy logic out of legacy realtime surfaces into the frozen SDK package shape

### Explicitly excluded

- `ema_3_19_15m_block_adverse` as strategy identity
- D-day context gating as strategy-owned logic
- multi-strategy orchestration
- portfolio netting / capital allocation redesign
- broker-specific execution details
- Telegram / notifier expansion
- experiment registry expansion
- broad legacy cleanup outside surfaces directly mapped by this slice
- migration of any second strategy or instrument

Frozen slice meaning:
- the reference strategy is the pure EMA 3/19 15m strategy package
- context gating, pilot journaling, locks, reconciliation, and execution remain platform/runtime responsibilities

---

## 2. Target package / file shape

### 2.1 Strategy package

Required target package:

- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/backtest_adapter.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`

Optional files are out of scope for the first slice.

### 2.2 Required platform modules touched

Required frozen SDK/platform surfaces touched by this slice:

- `src/moex_strategy_sdk/manifest.py`
- `src/moex_strategy_sdk/interfaces.py`
- `src/moex_strategy_sdk/config_schema.py`
- `src/moex_strategy_sdk/artifact_contracts.py`
- `src/moex_strategy_sdk/errors.py`
- one registry loader/validator surface under the platform config layer
- one canonical backtest runner boundary under `src/moex_backtest/`
- one runtime reconcile/state/logging boundary under `src/moex_runtime/`

### 2.3 Thin runtime entrypoint shape

The legacy realtime CLI boundary may remain at:

- `src/cli/loop_ema_3_19_15m_realtime.py`

But in the target slice it is thin only. It may:
- load ids/config
- call platform runtime boundary
- return result code

It must not keep:
- 15m aggregation logic
- EMA math
- strategy-local session schema
- execution/PnL logic
- artifact naming policy

### 2.4 Backtest entry boundary

Backtest entry boundary for the slice is:

- registry resolution -> strategy `backtest_adapter.py` -> canonical backtest engine

The strategy package must not own any inline backtest engine.

---

## 3. Contracts used

### 3.1 Strategy SDK binding

This slice is bound to the frozen SDK contract:
- `STRATEGY_MANIFEST`
- `StrategyConfig`
- `validate_config`
- `generate_signals`
- `build_backtest_request`
- `build_live_decision`
- `ARTIFACT_CONTRACTS`

### 3.2 Backtest / execution semantics binding

This slice is bound to canonical semantics only:
- finalized-bar signal semantics
- signal on bar `t`, execution no earlier than `t+1`
- canonical backtest mode = `next_bar_open`
- canonical mark basis = open-to-open
- reversal = 2 actions and 2 commissions
- forced terminal close exactly once
- fail-closed on invalid ordering, duplicate bars, missing execution bar, or leakage

### 3.3 Registry / config binding

This slice is registry-driven only:
- explicit ids only
- six canonical registries
- config layering only through frozen registry/config model
- no path guessing
- no hidden cwd dependency
- no override of ids, versions, manifest refs, artifact refs, support flags, or time/session semantics

### 3.4 Artifact contract binding

Every persisted or consumed slice artifact must have one explicit contract.

This slice uses only these artifact classes:
- `external_pattern` for server-resident data/state/log/result artifacts
- `env_contract` only for required runtime secret names if runtime mode needs them

`repo_relative`, `cli_argument`, and undeclared implicit discovery are not required for this slice.

---

## 4. Required registry records

### 4.1 Instrument

Required record:
- `instrument_id = si`

Frozen meaning:
- this slice is single-instrument only
- strategy `instrument_scope = ["si"]`
- registry symbol binding resolves current MOEX futures instrument mapping externally; strategy code does not

### 4.2 Dataset

Required record:
- `dataset_id = si_fo_5m_intraday`

Frozen meaning:
- canonical 5m intraday OHLC input dataset for `si`
- strategy package does not fetch MOEX data directly

### 4.3 Feature

Required record:
- `feature_set_id = si_15m_ohlc_from_5m`

Frozen meaning:
- 15m finalized-bar frame is produced by platform data/features layer
- 15m aggregation is not owned by strategy package and not owned by thin CLI loop

### 4.4 Strategy

Required record:
- `strategy_id = ema_3_19_15m`

Frozen meaning:
- package identity = manifest identity = registry identity
- current context-gated naming remains legacy and is not the target strategy id

### 4.5 Portfolio

Minimal portfolio record is required for the runtime boundary:
- `portfolio_id = reference_ema_3_19_15m_single`

Frozen meaning:
- single enabled strategy only
- no cross-strategy logic
- no portfolio redesign inside this slice

### 4.6 Environment

Two minimal environment records are required:
- `environment_id = reference_backtest`
- `environment_id = reference_runtime_boundary`

Frozen meaning:
- backtest and runtime boundary are both registry-startable
- runtime env declares required adapter ids / required env var names only
- secrets remain outside repo config

---

## 5. Exact artifact set

### 5.1 Required inputs

1. `si_fo_5m_intraday_dataset`
- role: input
- class: `external_pattern`
- producer: platform data layer
- consumer: feature layer / backtest path
- meaning: canonical 5m intraday OHLC dataset for `si`

2. `si_15m_ohlc_feature_frame`
- role: input
- class: `external_pattern`
- producer: platform feature layer
- consumer: `strategies/ema_3_19_15m/signal_engine.py`
- meaning: finalized 15m bars derived from the registered 5m dataset

### 5.2 Required outputs

3. `ema_3_19_15m_backtest_day_metrics`
- role: output
- class: `external_pattern`
- producer: canonical backtest/report path
- consumer: research/backtest verdict layer
- meaning: traceable day-level backtest result for the slice

4. `ema_3_19_15m_trade_log`
- role: output
- class: `external_pattern`
- producer: platform runtime boundary
- consumer: runtime observability / post-run analysis
- meaning: execution/trade log for the slice runtime path

### 5.3 State artifacts

5. `ema_3_19_15m_signal_state`
- role: state
- class: `external_pattern`
- producer: platform runtime boundary via strategy state patch
- consumer: `live_adapter.py`
- meaning: strategy-local persisted state only
- allowed contents: EMA warm state and pending signal state only

### 5.4 Explicitly out of scope for strategy artifact contract

Not strategy-owned artifacts in this slice:
- runtime lock file
- generic execution state store
- pilot journal CSV
- pilot day status CSV
- D-day context artifact

Those may exist as platform or legacy artifacts, but they are not part of the strategy package contract for this slice.

---

## 6. Source-to-target migration mapping

### 6.1 Current repo surfaces that map into the slice

1. `src/strategy/realtime/ema_3_19_15m/signal_engine_ema_3_19_15m.py`
- target: `src/strategies/ema_3_19_15m/signal_engine.py`
- keep: EMA 3/19 crossover math
- remove: dependency on legacy session module naming

2. `src/cli/loop_ema_3_19_15m_realtime.py`
- target: thin runtime adapter only
- remove from this file: 15m bucket build, direct strategy math calls, state schema ownership, trade execution logic, artifact naming policy

3. `src/strategy/realtime/ema_3_19_15m/session_state_ema_3_19_15m.py`
- split target:
  - strategy-local signal state contract remains strategy-visible
  - generic runtime/execution state leaves strategy package

4. `src/strategy/realtime/ema_3_19_15m/executor_ema_3_19_15m.py`
- target owner: platform runtime / execution boundary
- not a strategy package file in the slice

5. `src/strategy/realtime/ema_3_19_15m/trade_logger_ema_3_19_15m.py`
- target owner: platform runtime logging boundary
- not a strategy package file in the slice

6. `src/research/ema/build_ema_pnl_multitimeframe.py`
- target role: migration input only
- the slice must not keep this script as the strategy backtest boundary

7. `src/research/ema/lib_ema_search.py`
- target role: nearest compatible backtest semantics source
- canonical ownership moves to platform backtest layer, not to strategy package

### 6.2 What becomes strategy package

Inside `src/strategies/ema_3_19_15m/` only:
- manifest identity and dependency declaration
- typed config
- EMA signal logic
- mapping into canonical backtest request
- mapping into canonical live desired-position decision
- explicit artifact declarations

### 6.3 What becomes platform responsibility

Platform owns:
- 5m data loading
- 15m aggregation/materialization
- registry loading/validation
- runtime reconcile
- runtime lock/state/logging framework
- canonical backtest engine
- canonical cost/action/forced-close semantics

### 6.4 What stays legacy and out of scope

Out of scope for this slice:
- context preflight module as required strategy dependency
- pilot journaling surfaces
- current block-adverse strategy identity
- broad research script cleanup beyond the exact boundary needed for the slice

---

## 7. Ordered implementation sequence

### Step 1 — SDK base surface

File scope:
- `src/moex_strategy_sdk/*.py`

Goal:
- make the frozen SDK symbols/types/errors loadable

Fail points:
- missing required SDK symbols
- no fail-closed validation surface

### Step 2 — Strategy package shell

File scope:
- `src/strategies/ema_3_19_15m/*`

Goal:
- create required package shape with manifest/config/signal/backtest/live/artifact modules

Fail points:
- package id mismatch
- manifest/config/artifact contract mismatch

### Step 3 — Registry records

File scope:
- one record under each required registry directory:
  - `configs/instruments/`
  - `configs/datasets/`
  - `configs/features/`
  - `configs/strategies/`
  - `configs/portfolios/`
  - `configs/environments/`

Goal:
- make the slice discoverable by ids only

Fail points:
- unresolved dependency ids
- inactive or mode-incompatible record
- hidden override dependency

### Step 4 — Feature boundary extraction

File scope:
- one platform feature materialization surface under `src/moex_features/`
- remove 15m aggregation ownership from `src/cli/loop_ema_3_19_15m_realtime.py`

Goal:
- move 15m bucket construction out of the CLI loop and out of the strategy package

Fail points:
- aggregation still lives in thin CLI
- strategy still depends on raw 5m feed shape directly

### Step 5 — Backtest boundary wiring

File scope:
- `src/strategies/ema_3_19_15m/backtest_adapter.py`
- one canonical runner/boundary surface under `src/moex_backtest/`

Goal:
- run the strategy by registry ids through canonical semantics only

Fail points:
- inline PnL engine remains in strategy path
- path guessing remains in backtest startup

### Step 6 — Runtime boundary wiring

File scope:
- `src/strategies/ema_3_19_15m/live_adapter.py`
- one runtime boundary surface under `src/moex_runtime/`
- `src/cli/loop_ema_3_19_15m_realtime.py`

Goal:
- thin runtime startup resolves ids, materializes inputs, emits desired position, delegates reconcile/state/logging to platform

Fail points:
- CLI still owns execution math or trade logging contract
- strategy package still owns generic session/execution state

### Step 7 — Contract tests / acceptance fixtures

File scope:
- contract/integration tests only for this slice

Goal:
- prove registration, backtest startup, runtime boundary startup, and explicit artifact contracts

Fail points:
- undeclared artifact
- strategy-side hidden dependency
- forbidden responsibility still present in strategy package

---

## 8. Acceptance criteria

### 8.1 Strategy package registration succeeds

Must pass:
- package exists at `src/strategies/ema_3_19_15m/`
- all required SDK symbols validate
- registry `strategy_id` matches manifest/package id
- required dataset/feature ids resolve

### 8.2 One registry-backed backtest run succeeds

Must pass:
- startup is by ids only
- required artifacts resolve through declared contracts only
- canonical semantics are used unchanged
- output artifact `ema_3_19_15m_backtest_day_metrics` is produced explicitly
- no path glob / latest-file heuristic is required

### 8.3 One thin runtime boundary succeeds

Must pass:
- startup is by `strategy_id + portfolio_id + environment_id`
- runtime boundary emits desired position from strategy package
- reconcile/state/logging are platform-owned
- required runtime state/trade log artifacts are explicit
- no strategy-owned lock/session/logger/executor framework remains

### 8.4 Artifact contracts are explicit

Must pass:
- every consumed/persisted slice artifact is declared
- each artifact has exactly one contract class
- strategy package does not emit undeclared files

### 8.5 No forbidden responsibilities remain inside strategy package

Must be absent from `src/strategies/ema_3_19_15m/`:
- generic feed loading
- 15m aggregation
- inline PnL engine
- generic reconcile/execution logic
- generic lock/session/logger framework
- path autodiscovery

---

## 9. Deferred items / forbidden shortcuts

### Forbidden shortcuts

- do not register the reference slice under `ema_3_19_15m_block_adverse`
- do not keep D-day context gating inside the strategy package
- do not keep 15m aggregation in the CLI loop
- do not keep executor/trade logger as strategy-owned modules
- do not preserve mixed strategy-state + execution-state JSON as the target contract
- do not keep backtest startup dependent on path glob or latest-file selection
- do not use pilot journal/day-status artifacts as mandatory slice contracts

### Explicitly deferred

- multi-strategy orchestration
- portfolio netting/allocation redesign
- broker-specific live execution details
- notifier/Telegram work
- experiment registry expansion
- migration of other strategies
- broader legacy cleanup outside exact slice surfaces

---

## 10. Final frozen statement

The first reference vertical slice is frozen as a single-strategy, single-instrument, registry-backed migration slice for `ema_3_19_15m`.

Its purpose is not to preserve the current legacy realtime implementation.
Its purpose is to extract one clean strategy package, bind it to the frozen SDK/semantics/registry contracts, and prove one backtest path plus one thin runtime boundary without carrying forbidden responsibilities inside the strategy package.
