# PHASE 9 Registered Backtest Runner Generalization Spec

## 1. verdict

The correct minimal platform-first next step is:

**generalize `src/moex_backtest/engine/run_registered_backtest.py` so registered backtests resolve their strategy boundary from existing registry state instead of encoding the EMA reference package as the only runnable registered backtest path**

This boundary is intentionally narrow.
It freezes only the shared registered-backtest binding layer needed for future phase-9 migrations.
It does not approve a broader backtest-engine redesign, live/runtime widening, or any additional strategy migration in this cycle.

## 2. repo proof

Current main already proves that the missing piece is a shared platform boundary, not a missing architecture model.

### 2.1 current runner is not strategy-generic

`src/moex_backtest/engine/run_registered_backtest.py` currently hardcodes the EMA reference line at four critical points:

- feature producer import is hardcoded to `src.moex_features.intraday.si_15m_ohlc_from_5m:materialize_feature_frame`
- strategy signal hook import is hardcoded to `src.strategies.ema_3_19_15m.signal_engine:generate_signals`
- strategy backtest adapter import is hardcoded to `src.strategies.ema_3_19_15m.backtest_adapter:build_backtest_request`
- output artifact id is hardcoded as `ema_3_19_15m_backtest_day_metrics`

So the current file is not a generic registry-backed runner.
It is a shared runner shell with one strategy path embedded in it.

### 2.2 current runner also hardcodes EMA-shaped strategy inputs

The same file contains `_to_strategy_inputs()`, which whitelists only:

- `instrument_id`
- `end`
- `open`
- `high`
- `low`
- `close`
- `volume`

That means the generic runner currently strips any other registered feature columns before strategy hooks see them.
This is a second shared-layer blocker because a generic registered runner must not encode one strategy’s feature schema as the only admissible strategy input shape.

### 2.3 the registry model already contains the data needed to generalize this boundary

Current main already contains the required registry-driven inputs:

- feature registry records already declare `producer_ref`
- strategy registry records already declare `package_ref`
- strategy default config already declares `artifact_bindings`
- strategy artifact contracts already declare backtest output artifacts

This is already visible in current main for both the EMA reference line and the newly migrated `usdrubf_large_day_mr` line.

### 2.4 current loader already proves the runtime side of the same pattern

`src/moex_core/contracts/registry_loader.py` already resolves the runtime boundary generically:

- runtime feature builder from `feature_record["producer_ref"]`
- runtime signal hook from `strategy_record["package_ref"] + ".signal_engine:generate_signals"`
- runtime live adapter hook from `strategy_record["package_ref"] + ".live_adapter:build_live_decision"`

So current main already proves the correct platform pattern.
The backtest side is the missing parallel boundary.

## 3. exact blocker boundary frozen in `src/moex_backtest/engine/run_registered_backtest.py`

This spec freezes the blocker boundary exactly as follows.

### 3.1 hardcoded feature producer boundary

Blocked now:
- `run_registered_backtest.py` imports one specific feature producer directly from `src.moex_features.intraday.si_15m_ohlc_from_5m`

Why this is a blocker:
- future registered backtests would require editing the shared runner whenever a different feature producer is needed
- that violates the frozen registry/config model

### 3.2 hardcoded strategy signal hook boundary

Blocked now:
- `run_registered_backtest.py` imports one specific strategy signal hook directly from `src.strategies.ema_3_19_15m.signal_engine`

Why this is a blocker:
- the runner cannot execute another registered strategy without platform-file mutation
- strategy discovery stops being registry-driven in practice

### 3.3 hardcoded strategy backtest adapter boundary

Blocked now:
- `run_registered_backtest.py` imports one specific strategy backtest adapter directly from `src.strategies.ema_3_19_15m.backtest_adapter`

Why this is a blocker:
- the shared platform runner owns a strategy-specific adapter choice
- future registered strategies cannot plug into the shared runner through package registration alone

### 3.4 hardcoded output artifact id boundary

Blocked now:
- `run_registered_backtest.py` resolves the output artifact contract through the literal artifact id `ema_3_19_15m_backtest_day_metrics`

Why this is a blocker:
- the runner writes only the EMA reference output contract unless the shared platform file is edited again
- existing strategy default-config `artifact_bindings` are ignored even though they already exist in current main

### 3.5 hardcoded strategy input shaping boundary

Blocked now:
- `_to_strategy_inputs()` strips the feature frame down to EMA-style OHLC columns

Why this is a blocker:
- the registered feature producer may legally emit additional strategy-visible columns
- a generic shared runner must not discard registered feature payload needed by non-EMA strategies

### 3.6 what is not frozen as the blocker in this cycle

Not frozen here as the platform blocker:
- canonical backtest semantics in `_execute_canonical_backtest()`
- commission semantics
- next-bar-open semantics
- registry/config model itself
- SDK v1 package shape

Those surfaces remain frozen and unchanged in this contract-freeze cycle.

## 4. which parts must become registry-driven

Only the following shared backtest boundary parts must become registry-driven.

### 4.1 feature producer resolution

The shared runner must resolve the feature producer from the active feature registry record, not from a hardcoded module path.

Source of truth:
- `feature_record["producer_ref"]`

### 4.2 strategy signal hook resolution

The shared runner must resolve the strategy signal hook from the registered strategy package, not from a hardcoded EMA import.

Minimal source of truth:
- `strategy_record["package_ref"] + ".signal_engine:generate_signals"`

### 4.3 strategy backtest adapter resolution

The shared runner must resolve the strategy backtest adapter from the registered strategy package, not from a hardcoded EMA import.

Minimal source of truth:
- `strategy_record["package_ref"] + ".backtest_adapter:build_backtest_request"`

### 4.4 output artifact id resolution

The shared runner must resolve the backtest output artifact id from the strategy default config binding, not from a hardcoded literal.

Minimal source of truth:
- `default_strategy_config_record["artifact_bindings"]["output_day_metrics_artifact_id"]`

### 4.5 strategy input payload handoff

The shared runner must stop encoding one strategy-local schema as the only allowed strategy input payload.

Minimal rule:
- the runner passes forward the full row payload produced by the resolved feature producer
- no EMA-specific column whitelist may remain in the generic runner

## 5. minimal target contract

This spec freezes the minimal target contract only for the four required resolution points plus the generic input handoff rule.

## 5.1 feature producer resolution contract

The resolved backtest boundary must expose one callable feature producer.

Frozen source:
- import from `feature_record["producer_ref"]`

Frozen callable contract:
- `materialize_feature_frame(*, dataset_artifact_path: str | Path, instrument_id: str, timezone_name: str) -> pd.DataFrame`

Frozen boundary rule:
- `run_registered_backtest.py` may not import a strategy-specific or feature-specific producer directly
- all producer selection must happen from registry-loaded state

## 5.2 strategy signal hook resolution contract

The resolved backtest boundary must expose one callable signal hook.

Frozen source:
- derive import ref from `strategy_record["package_ref"] + ".signal_engine:generate_signals"`

Frozen callable contract:
- `generate_signals(*, inputs: StrategyInputFrame, config: StrategyConfig) -> StrategySignalFrame`

Frozen boundary rule:
- no strategy-id switch statement inside the shared runner
- no direct import of any concrete strategy signal engine inside `run_registered_backtest.py`

## 5.3 strategy backtest adapter resolution contract

The resolved backtest boundary must expose one callable backtest adapter hook.

Frozen source:
- derive import ref from `strategy_record["package_ref"] + ".backtest_adapter:build_backtest_request"`

Frozen callable contract:
- `build_backtest_request(*, inputs: StrategyInputFrame, signals: StrategySignalFrame, config: StrategyConfig) -> BacktestAdapterRequest`

Frozen boundary rule:
- the shared runner remains platform-owned
- strategy-specific backtest request normalization remains strategy-owned through the registered package hook only
- no concrete strategy adapter import may remain in `run_registered_backtest.py`

## 5.4 output artifact id resolution contract

The resolved backtest boundary must expose one backtest day-metrics output contract.

Frozen source:
- `default_strategy_config_record["artifact_bindings"]["output_day_metrics_artifact_id"]`
- then exact contract resolution from `strategy_artifact_contracts`

Frozen rule:
- the requested artifact id must resolve to exactly one strategy artifact contract
- that contract must remain strategy-owned and producer-scoped to `moex_backtest`
- the shared runner may not contain a concrete strategy output artifact literal

## 5.5 generic strategy input handoff contract

The shared runner must hand off the full feature payload to the strategy hooks.

Frozen minimal rule:
- convert the resolved feature frame to row records without an EMA-specific column whitelist
- preserve all strategy-visible columns produced by the resolved feature producer

What this means:
- the generic runner owns transport only
- the generic runner does not own per-strategy feature-field curation

## 5.6 resolved loader boundary

`load_registered_backtest()` must gain the same generic backtest-resolution role that `load_registered_runtime_boundary()` already plays for runtime.

Minimum acceptable resolved backtest bundle:
- resolved feature producer callable
- resolved strategy signal callable
- resolved strategy backtest adapter callable
- resolved backtest output artifact contract

Exact field names are not frozen here.
Equivalent resolved fields are sufficient.
But the generic backtest boundary must become explicit in the loader result rather than being reconstructed by hardcoded imports inside the runner.

## 6. what must remain unchanged

The future apply-only package for this spec must keep the rest of the shared platform contract unchanged.

### 6.1 canonical backtest semantics remain unchanged

Unchanged:
- finalized-bar semantics
- next-bar-open execution rule
- open-to-open PnL rule
- reversal counting rule
- fixed 2-point commission default
- forced terminal close rule
- fail-closed bar validation rules

This cycle does not approve a new backtest mode and does not reopen `docs/sot/BACKTEST_EXECUTION_SEMANTICS_SPEC.md`.

### 6.2 registry/config model remains unchanged

Unchanged:
- canonical registries under `configs/`
- existing strategy/package registration model
- existing feature registry `producer_ref`
- existing strategy `package_ref`
- existing strategy default-config `artifact_bindings`
- existing portfolio/environment eligibility rules

This cycle does not approve new registries or a new config scope.

### 6.3 strategy SDK v1 remains unchanged

Unchanged:
- package shape
- required module names
- `generate_signals` contract
- `build_backtest_request` contract
- artifact contract declaration model

This cycle does not approve a new SDK hook surface.

### 6.4 existing path-resolution model remains unchanged

Unchanged:
- dataset path still resolves through the declared dataset artifact contract plus environment artifact-root binding
- output path still resolves through the declared strategy artifact contract plus environment artifact-root binding

This cycle does not approve guessed paths, latest-file heuristics, or server-memory discovery.

## 7. exact acceptance boundary for a future apply-only package

A later apply-only package is acceptable only if all points below are true.

### 7.1 minimal platform file scope

The apply-only package must stay narrow.

Expected minimal scope:
- `src/moex_backtest/engine/run_registered_backtest.py`
- `src/moex_core/contracts/registry_loader.py`

Optional narrow additions are acceptable only if strictly required for validation or tests.
No strategy package migration is required in this package.

### 7.2 no hardcoded EMA boundary remains in the shared runner

After apply, `src/moex_backtest/engine/run_registered_backtest.py` must contain none of the following concrete shared-runner bindings:

- direct import from `src.moex_features.intraday.si_15m_ohlc_from_5m`
- direct import from `src.strategies.ema_3_19_15m.signal_engine`
- direct import from `src.strategies.ema_3_19_15m.backtest_adapter`
- literal output artifact id `ema_3_19_15m_backtest_day_metrics`

### 7.3 resolved backtest boundary exists in the loader

After apply, `load_registered_backtest()` must return or otherwise surface an explicit registry-derived backtest boundary that includes:

- feature producer resolution
- signal hook resolution
- backtest adapter resolution
- output artifact contract resolution

Hardcoded reconstruction of these four pieces inside the runner is not acceptable.

### 7.4 generic input handoff is fixed

After apply:
- the generic runner must pass the full feature-row payload into the strategy hooks
- EMA-specific field whitelisting inside `_to_strategy_inputs()` must be removed or replaced with an equivalent generic pass-through rule

### 7.5 current EMA reference path must still remain runnable through registry-derived binding

After apply:
- the EMA registered backtest path must still resolve through the generic boundary
- its output artifact contract must come from registry/config binding, not from a literal hardcoded id

### 7.6 no platform widening is allowed

After apply there must be no widening into:
- runtime/orchestration
- observability
- live boundary logic
- experiment-registry
- portfolio redesign
- environment redesign
- strategy SDK redesign
- canonical backtest semantics redesign

### 7.7 what this apply package is not required to prove

This boundary package is accepted when the shared platform hardcoding is removed.
It is not required in the same cycle to solve every later strategy-local feature/execution-shape issue.

That means this package is **not** required to:
- mutate any concrete strategy package
- widen any registered feature schema in this cycle
- redesign `_execute_canonical_backtest()`
- introduce new strategy-local semantics hooks

Those remain later narrow strategy/apply concerns and must stay within the already frozen semantics model.

## 8. explicit out-of-scope list

Out of scope for this contract-freeze cycle:

- code apply
- server proof
- live/runtime/orchestration changes
- observability changes
- experiment-registry
- broad backtest-engine redesign
- changes to canonical backtest semantics
- new SDK lifecycle hooks
- new config scopes or registries
- migration of any additional strategy in this cycle
- widening the current cycle into a full `usdrubf_large_day_mr` execution repair

## 9. blockers if any

No blocker is evidenced for this spec-only cycle.

Current main already contains:
- the frozen strategy SDK contract
- the frozen registry/config model
- the shared registered runner file that exposes the exact hardcoded blocker boundary
- the runtime loader pattern that already demonstrates the correct generic-resolution model
- active registry records for both EMA and `usdrubf_large_day_mr`
- existing strategy default-config artifact bindings that already provide the correct output-artifact source of truth

So this platform boundary can be frozen now without reopening broader architecture.

## 10. short final statement

Freeze phase 9 platform-first remediation as one narrow generalization of the registered backtest runner boundary so backtest feature producer, signal hook, backtest adapter, and output artifact selection come from existing registry state rather than from EMA-specific hardcoding in the shared platform runner.
