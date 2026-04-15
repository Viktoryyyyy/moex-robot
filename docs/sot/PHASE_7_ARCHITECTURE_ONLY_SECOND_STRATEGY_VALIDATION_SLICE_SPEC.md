# PHASE 7 Architecture-Only Second Strategy Validation Slice Spec

## 1. verdict

The cheapest correct second registered live strategy slice is **a synthetic architecture-only validation strategy package**, not `mr1` revival and not a real research promotion.

More exactly, the next slice should be one minimal current-main package such as `reference_flat_15m_validation` that satisfies the already-applied registered runtime boundary and portfolio orchestrator contracts while intentionally doing no real alpha work.

This is **not blocked** by current runtime, orchestrator, portfolio, registry, or artifact semantics.

## 2. repo proof

Repo proof is exact and narrow:

- the current sequential orchestrator already reads `enabled_strategy_ids` from a portfolio config and delegates each strategy id through `run_registered_runtime_boundary(...)` in order; it returns portfolio-level `delegated_strategy_results` without requiring parallelism, netting, or allocation widening.
- the current runtime boundary already loads a strategy package through registry/config refs, builds features from exactly one registered dataset plus one registered feature set, imports `signal_engine:generate_signals`, imports `live_adapter:build_live_decision`, resolves exactly one runtime `state` contract and exactly one runtime trade-log `output` contract, and returns a per-strategy runtime result.
- the registry loader already defines the minimum contract for any live-capable registered strategy: active strategy registry record, active default config, manifest/config validation, one dataset id, one feature id, one instrument id, package-based signal engine import, package-based live-adapter import, exactly one runtime state contract, and exactly one runtime trade-log contract.
- current main already proves the package pattern through `ema_3_19_15m`, whose registry record points only to package-local `manifest.py`, `config.py`, `signal_engine.py`, `live_adapter.py`, `artifact_contracts.py`, and default config.
- the current reference portfolio is still single-strategy only, with `enabled_strategy_ids` containing only `ema_3_19_15m`; this proves the missing unit is a second registered strategy package, not new orchestration semantics.
- the already-frozen first true multi-strategy portfolio spec states the orchestrator is reusable and the blocker is only absence of a second repo-supported runtime-ready registered live strategy.
- the already-frozen second-strategy source spec and `mr1` promotion slice spec establish that real second-strategy work is currently higher-cost and blocked at the strategy-local signal contract level; therefore real-strategy promotion is not the cheapest architecture-validation path.

So current repo proof resolves into one exact conclusion:

**architecture validation is already supported by the platform if one minimal synthetic registered live strategy package is added; no runtime or portfolio redesign is required.**

## 3. chosen validation slice or blocker

Chosen validation slice:

**Add one synthetic package-scoped second live strategy whose sole purpose is architecture proof.**

Recommended target id:

- `reference_flat_15m_validation`

Required behavior:

- package shape exactly matches the current registered strategy contract
- reuse the already-active `si` / `15m` / `si_fo_5m_intraday` / `si_15m_ohlc_from_5m` surfaces
- `signal_engine.py` is deterministic and intentionally minimal; the cheapest correct form is to emit an empty signal frame
- `live_adapter.py` must return a valid `LiveAdapterDecision` with `desired_position=0.0` and a fixed architecture-validation reason code when no signal exists
- `artifact_contracts.py` must declare strategy-local runtime state and runtime trade-log artifacts with **strategy-specific paths** so there is no collision with `ema_3_19_15m`
- `configs/strategies/reference_flat_15m_validation.json` and `configs/strategies/reference_flat_15m_validation.default.json` must register that package under the existing loader contract

No blocker is evidenced for this synthetic slice.

## 4. why it is the cheapest correct architecture-only unit

This is the cheapest correct unit because:

- it reuses the already-proven package contract instead of reviving blocked legacy logic
- it reuses the already-proven runtime boundary instead of widening runtime semantics
- it reuses the already-proven sequential orchestrator instead of redesigning orchestration
- it reuses the already-proven dataset, feature, instrument, and timeframe surfaces instead of adding new market/data plumbing
- it avoids all real strategy research, alpha decisions, and signal-contract archaeology
- it proves the exact architecture question that remains open: whether a second registered live strategy can coexist end-to-end under the present platform contracts

Any `mr1` path is more expensive in this cycle because current frozen repo evidence says `mr1` is blocked at the exact signal-generation/input contract point, while the synthetic slice has no such blocker.

## 5. exact current repo surfaces in scope

- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
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
- `docs/sot/PHASE_7_FIRST_TRUE_MULTI_STRATEGY_REFERENCE_PORTFOLIO_SPEC.md`
- `docs/sot/PHASE_7_SECOND_STRATEGY_SOURCE_SPEC.md`
- `docs/sot/PHASE_7_MR1_PROMOTION_SLICE_SPEC.md`

## 6. exact target layer / config destination

The exact apply destination for the architecture-only validation slice is only:

- `src/strategies/reference_flat_15m_validation/manifest.py`
- `src/strategies/reference_flat_15m_validation/config.py`
- `src/strategies/reference_flat_15m_validation/signal_engine.py`
- `src/strategies/reference_flat_15m_validation/live_adapter.py`
- `src/strategies/reference_flat_15m_validation/artifact_contracts.py`
- `configs/strategies/reference_flat_15m_validation.json`
- `configs/strategies/reference_flat_15m_validation.default.json`
- one new portfolio config under `configs/portfolios/` whose `enabled_strategy_ids` equals exactly `[
  "ema_3_19_15m",
  "reference_flat_15m_validation"
]`

No other destination layer is required.

## 7. exact non-goals

Non-goals for this spec:

- `mr1` revival
- any real strategy research or alpha design
- any signal-quality optimization
- runtime-boundary redesign
- orchestrator redesign
- portfolio netting
- capital allocation redesign
- scheduler / cron / daemon work
- lock / risk / notifier / telemetry expansion
- broker routing redesign
- feature or dataset contract redesign
- artifact-model redesign beyond adding strategy-local state/trade-log paths for the validation strategy
- registry-model redesign
- multi-instrument widening
- backtest semantics widening

## 8. acceptance criteria for later apply

Later apply is acceptable only when all of the following are true in current main:

- `src/strategies/reference_flat_15m_validation/manifest.py` exists and validates as a live-capable strategy manifest
- `src/strategies/reference_flat_15m_validation/config.py` exports `StrategyConfig` and `validate_config(...)`
- `src/strategies/reference_flat_15m_validation/signal_engine.py` exports deterministic `generate_signals(...)` and does not perform file IO, path discovery, notifier work, loop logic, or network calls
- `src/strategies/reference_flat_15m_validation/live_adapter.py` exports `build_live_decision(...)` returning `LiveAdapterDecision` with valid flat-position behavior under current state semantics
- `src/strategies/reference_flat_15m_validation/artifact_contracts.py` declares exactly one runtime `state` contract with producer `moex_runtime` and exactly one runtime trade-log `output` contract with producer `moex_runtime`, both using strategy-specific artifact ids and paths
- `configs/strategies/reference_flat_15m_validation.json` matches the exact strategy registry shape already enforced by `registry_loader.py`
- `configs/strategies/reference_flat_15m_validation.default.json` matches the exact default-config shape already enforced by `registry_loader.py`
- one new portfolio config under `configs/portfolios/` enables exactly `ema_3_19_15m` and `reference_flat_15m_validation`
- owner-run proof shows `run_registered_portfolio_runtime_orchestrator(...)` returning `enabled_strategy_ids` with both strategy ids and `delegated_strategy_results` containing two per-strategy results in order
- owner-run proof shows the validation strategy resolves its own runtime state path and trade-log path without colliding with `ema_3_19_15m`
- no runtime semantics widening, orchestration widening, or portfolio-semantic widening is introduced

## 9. blockers if any

No architecture blocker is evidenced for the synthetic validation slice.

The only blocker evidenced in current frozen repo history applies to real `mr1` promotion, not to a synthetic validation package.

## 10. one sentence final scope statement

The cheapest correct next unit is one synthetic flat second registered live strategy package that reuses the current `si` 15m feature/runtime contracts and exists only to prove that the already-applied runtime boundary and sequential portfolio orchestrator work end-to-end with `enabled_strategy_ids > 1`.
