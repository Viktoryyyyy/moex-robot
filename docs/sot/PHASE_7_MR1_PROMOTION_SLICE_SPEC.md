# PHASE 7 MR1 Promotion Slice Spec

## 1. verdict

Blocked at one exact strategy-local contract point.

The smallest correct `mr1` promotion slice is **not** direct legacy restoration and is **not** runtime/orchestration widening.

The frozen target unit is:

- one current-main strategy package under `src/strategies/mr1/`
- one strategy registry entry at `configs/strategies/mr1.json`
- one default config entry at `configs/strategies/mr1.default.json`

But repo proof still leaves one exact blocker before that unit can be frozen as fully runtime-ready:

**legacy repo evidence proves only `mr1` signal-consumption / loop surfaces, not an importable current-main-safe `mr1` signal-generation contract that can be placed inside `src/strategies/mr1/signal_engine.py` under the existing registered runtime boundary.**

## 2. repo proof

Repo proof is exact and narrow:

- target architecture requires every strategy to land as a package with `manifest.py`, `config.py`, `signal_engine.py`, `live_adapter.py`, and `artifact_contracts.py`; direct script sprawl is not the target model.
- current registered runtime loading is package/registry-driven: `configs/strategies/<strategy_id>.json` must provide `package_ref`, `manifest_ref`, `config_schema_ref`, `default_config_ref`, and `artifact_contract_ref`; runtime loading then imports `signal_engine:generate_signals` and `live_adapter:build_live_decision` from that package.
- current runtime boundary also requires exactly one runtime `state` artifact contract and exactly one runtime `output` trade-log artifact contract from the strategy artifact tuple.
- current main proves the reference pattern through `ema_3_19_15m`: `manifest.py`, `config.py`, `signal_engine.py`, `live_adapter.py`, `artifact_contracts.py`, `configs/strategies/ema_3_19_15m.json`, and `configs/strategies/ema_3_19_15m.default.json`.
- current main portfolio scope is still single-strategy only, which confirms that this cycle must stay strategy-local and must not widen into portfolio semantics.
- frozen phase-7 source lineage already established that the legitimate source is legacy `mr1`, not a current-main near-ready package and not the USDRUBF research line.
- repo history proves legacy `mr1` existence only through script-era notification/loop surfaces such as `scripts/signal_mr1.py`, `scripts/loop_signal_mr1.py`, and follow-up `mr1` script commits.
- that legacy `scripts/signal_mr1.py` surface reads an already-produced latest Si CSV, searches for pre-existing signal columns such as `mr1_signal`, `signal_mr1`, or `signal`, and formats / sends the result; it does **not** define a current-main package-safe deterministic `generate_signals(...)` implementation.
- current main does not contain `src/strategies/mr1/manifest.py`, `src/strategies/mr1/config.py`, `src/strategies/mr1/signal_engine.py`, `src/strategies/mr1/live_adapter.py`, `src/strategies/mr1/artifact_contracts.py`, `configs/strategies/mr1.json`, or `configs/strategies/mr1.default.json`.
- current active reference feature registration is `si_15m_ohlc_from_5m`, which is OHLC-oriented and tied to the EMA reference slice; repo proof in this cycle does not establish a current-main feature/input contract that already carries legacy `mr1` signal semantics.

So repo evidence is sufficient to freeze the package destination and loader contract, but insufficient to freeze the exact `mr1` signal/input contract as already repo-proven current-main material.

## 3. chosen promotion slice or blocker

Chosen output for this cycle is the narrow blocker:

**Freeze `mr1` promotion as a strategy-package-only destination, but block full runtime-ready approval until one exact strategy-local contract is proved: the deterministic `mr1` signal-generation/input contract that `src/strategies/mr1/signal_engine.py` will own under current-main registry/runtime rules.**

The smallest non-widening target shape is still exactly:

- `src/strategies/mr1/manifest.py`
- `src/strategies/mr1/config.py`
- `src/strategies/mr1/signal_engine.py`
- `src/strategies/mr1/live_adapter.py`
- `src/strategies/mr1/artifact_contracts.py`
- `configs/strategies/mr1.json`
- `configs/strategies/mr1.default.json`

And the minimum role of each file is fixed already:

- `manifest.py`: declare `strategy_id`, version, instrument scope, timeframe, required dataset ids, required feature ids, `supports_live=True`, and artifact contract version.
- `config.py`: expose typed `StrategyConfig` plus `validate_config(...)` matching the strategy registry and default config.
- `signal_engine.py`: export deterministic `generate_signals(...)` for current runtime inputs; no file discovery, no notifier logic, no loop/session logic.
- `live_adapter.py`: export `build_live_decision(...)` that converts latest signal plus state into `LiveAdapterDecision`; reuse the existing runtime hold/state pattern already proven by EMA.
- `artifact_contracts.py`: declare at minimum one runtime `state` contract and one runtime trade-log `output` contract, plus any strategy input/output contracts required by the chosen `mr1` signal path.
- `configs/strategies/mr1.json`: satisfy the exact `registry_loader.py` schema and point only to the package surfaces above.
- `configs/strategies/mr1.default.json`: provide exact default config object with `strategy_id`, version, params, artifact bindings, and null runtime/risk policy refs.

## 4. why it is the smallest correct promotion unit

This is the smallest correct unit because:

- the runtime boundary, registry loader, and sequential orchestrator are already present and do not need redesign.
- direct restoration of legacy scripts would import wrong-layer responsibilities: notifier logic, loop control, lock handling, implicit latest-file discovery, and message formatting.
- freezing more than the package/config destination would widen into portfolio, scheduler, notifier, or broker work that the repo does not require for this decision.
- freezing less than the package/config destination would be too weak, because current runtime proof already shows the exact loader contract that any second strategy must satisfy.
- the only unresolved point left by repo evidence is the actual `mr1` signal/input contract; that is a real blocker, while runtime/orchestration are not.

## 5. exact current repo surfaces in scope

- `src/moex_core/contracts/registry_loader.py`
- `src/moex_strategy_sdk/interfaces.py`
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/strategies/ema_3_19_15m.default.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `configs/features/si_15m_ohlc_from_5m.json`
- `contracts/features/si_15m_ohlc_from_5m.json`
- `docs/sot/PHASE_7_FIRST_TRUE_MULTI_STRATEGY_REFERENCE_PORTFOLIO_SPEC.md`
- `docs/sot/PHASE_7_FIRST_SECOND_STRATEGY_REFERENCE_SLICE_SPEC.md`
- `docs/sot/PHASE_7_SECOND_STRATEGY_SOURCE_SPEC.md`
- legacy `mr1` lineage commits and files only as source proof:
  - `d3b7156cb3833c27b65db2426bb28cf6caf2a0a7` → `scripts/signal_mr1.py`
  - `37d7c5070e5b245b43ff7a9a20faa39b491d2321` → `scripts/signal_mr1.py`, `scripts/tg_utils.py`
  - `898c43999cca232c956a79232603a9556bbd13a3` → `scripts/loop_signal_mr1.py`
  - `e945b3317aae75713b22c56fa1aca88c3c9e71f0` → follow-up `scripts/signal_mr1.py` compatibility expansion

## 6. exact target layer / config destination

The exact destination remains only:

- `src/strategies/mr1/manifest.py`
- `src/strategies/mr1/config.py`
- `src/strategies/mr1/signal_engine.py`
- `src/strategies/mr1/live_adapter.py`
- `src/strategies/mr1/artifact_contracts.py`
- `configs/strategies/mr1.json`
- `configs/strategies/mr1.default.json`

No other layer destination is approved in this cycle.

## 7. exact non-goals

Non-goals for this spec:

- direct restore of `scripts/signal_mr1.py` or `scripts/loop_signal_mr1.py`
- scheduler / cron / daemon design
- notifier / Telegram migration
- lock model migration
- broker routing redesign
- portfolio apply
- orchestration redesign
- portfolio netting
- capital allocation
- risk-engine expansion
- research promotion of USDRUBF large-day MR
- artifact-model redesign unless the unresolved `mr1` signal/input contract proves it necessary
- registry-model redesign unless the unresolved `mr1` signal/input contract proves it necessary
- invention of new `mr1` alpha beyond repo-proven lineage

## 8. acceptance criteria for later apply

Later apply is acceptable only when all of the following are true in current main:

- `src/strategies/mr1/manifest.py` exists and `validate_strategy_manifest(...)` accepts it.
- `src/strategies/mr1/config.py` exports `StrategyConfig` and `validate_config(...)` with strict `mr1`-specific fields only.
- `src/strategies/mr1/signal_engine.py` exports deterministic `generate_signals(...)` derived from an explicit repo-proven `mr1` signal/input contract rather than from latest-file autodetect or notifier scripts.
- `src/strategies/mr1/live_adapter.py` exports `build_live_decision(...)` returning `LiveAdapterDecision` and reuses existing runtime state semantics instead of adding a new loop/session framework.
- `src/strategies/mr1/artifact_contracts.py` declares a non-empty tuple of validated `ArtifactContract` items including exactly one `artifact_role="state"` contract with producer `moex_runtime` and exactly one runtime trade-log `artifact_role="output"` contract with producer `moex_runtime`.
- `configs/strategies/mr1.json` matches the exact strategy registry shape already used by `registry_loader.py`: `strategy_id`, version, package refs, default config ref, artifact contract ref, required dataset ids, required feature ids, required label ids, instrument scope, timeframe, support flags, schema versions, and `status="active"`.
- `configs/strategies/mr1.default.json` matches the exact default-config object shape already enforced by `registry_loader.py`.
- `load_registered_runtime_boundary(strategy_id="mr1", portfolio_id=<existing or later-approved portfolio>, environment_id=<active runtime env>)` can resolve the package without changing runtime-boundary semantics.
- no scheduler, notifier, broker, portfolio, or orchestration widening is introduced.

## 9. blockers if any

One blocker remains and it is exact:

- current repo proof does not yet expose the deterministic `mr1` signal-generation/input contract in a current-main package-ready form; legacy lineage proves only signal consumption / notification wrappers around an already-produced signal column.

Accordingly:

- runtime boundary reuse is **not** blocked
- registry-loader shape is **not** blocked
- strategy package destination is **not** blocked
- exact `mr1` signal/input contract is the only remaining blocker to freezing a fully runtime-ready promotion slice

## 10. one sentence final scope statement

`mr1` promotion is frozen as a strategy-package-only destination under the existing registered runtime boundary, but full runtime-ready approval remains blocked until repo proof establishes the exact deterministic `mr1` signal/input contract for `src/strategies/mr1/signal_engine.py` without restoring legacy scripts as system boundaries.
