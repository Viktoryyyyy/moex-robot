# PHASE 7 First Second-Strategy Reference Slice Spec

## 1. verdict

The smallest correct next second-strategy reference slice is a blocker, not a strategy apply.

Current repo proof does not support a second runtime-ready registered live strategy candidate that can be upgraded with only narrow packaging work.

Therefore the first second-strategy reference slice must be frozen as:

**blocked until one real second strategy candidate exists in current repo main at strategy-package level rather than only as legacy script history or research-only material.**

## 2. repo proof

Repo proof is exact and current-main scoped:

- `src/strategies/` currently resolves only one strategy package: `ema_3_19_15m`.
- `src/strategies/ema_3_19_15m/manifest.py` declares `strategy_id="ema_3_19_15m"` and `supports_live=True`.
- `configs/strategies/` currently resolves only `ema_3_19_15m.default.json` and therefore only one registered strategy config surface in current main.
- `configs/portfolios/` currently resolves only `reference_ema_3_19_15m_single.json`.
- `src/moex_core/contracts/registry_loader.py` proves the runtime boundary now expects a real registered strategy package with current-main registry/config plus importable `signal_engine` and `live_adapter` hooks and exactly one runtime state contract plus exactly one runtime trade-log contract.
- repo history contains historical `mr1` work, but current main does not contain an active `src/strategies/mr1/` package or matching `configs/strategies/mr1*.json` registration surface.
- current main also contains USDRUBF large-day mean-reversion research material under `src/research/build_usdrubf_large_day_mr_day_pairs.py`, but that is research-only and not a runtime-ready registered live strategy slice.

So current repo supports one live registered strategy and two non-qualifying categories only:

- historical legacy `mr1` traces in commit history
- research-only USDRUBF large-day MR material

Neither qualifies as a current runtime-ready registered live strategy candidate.

## 3. chosen second-strategy reference slice or blocker

Chosen output for this cycle is the blocker:

**No current-main second strategy candidate is narrow-enough and repo-ready enough to freeze as the next runtime-ready registered live slice.**

The required blocker statement is:

- no `src/strategies/<second_strategy_id>/` package exists in current main
- no second live-capable strategy manifest exists in current main
- no second strategy config registration exists in `configs/strategies/`
- no second runtime state / trade-log artifact-contract pair exists for a second strategy in current main

Accordingly, the first true multi-strategy reference portfolio remains blocked at strategy level, not orchestration level.

## 4. why it is the smallest correct next strategy-level unit

This is the smallest correct next unit because the already-applied portfolio orchestrator and registered runtime boundary are no longer the blocker.

The blocker is now singular and strategy-local: absence of one second strategy package that satisfies the already-frozen strategy/runtime contract.

Choosing any historical `mr1` commit line or any research-only USDRUBF large-day MR artifact as if it were already near-runtime-ready would widen into unproven migration, strategy redesign, feature-contract creation, instrument/runtime onboarding, and artifact-contract design that current main does not yet freeze.

So the narrow correct result is to freeze the absence of a valid second candidate, rather than to pretend that one already exists.

## 5. exact current repo surfaces in scope

- `src/strategies/`
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/strategies/ema_3_19_15m.default.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `src/moex_core/contracts/registry_loader.py`
- `src/research/build_usdrubf_large_day_mr_day_pairs.py`
- historical repo commit trail containing `mr1` legacy work only as repo-history evidence, not as current-main strategy-package proof

## 6. exact target layer / config destination

The eventual second runtime-ready reference slice must land exactly in the already-frozen target layers below, and nowhere broader:

- one new package under `src/strategies/<second_strategy_id>/`
- one new registry/config entry under `configs/strategies/<second_strategy_id>.json`
- one new default config under `configs/strategies/<second_strategy_id>.default.json`
- any required feature/dataset registration only if strictly required by that chosen second strategy and only at current target contract surfaces

No orchestration-layer destination change is required.

## 7. exact non-goals

Non-goals for this spec:

- applying a second strategy in this cycle
- selecting a fake placeholder second strategy
- upgrading historical `mr1` commit history directly into approval without current-main package proof
- promoting USDRUBF research scripts into live strategy status without separate narrow repo proof
- orchestration redesign
- scheduler / cron / daemon design
- locks / risk / notifier expansion
- broker routing redesign
- portfolio netting
- capital allocation
- artifact-model redesign unless a later candidate-specific blocker proves it is required
- registry-model redesign unless a later candidate-specific blocker proves it is required
- backtest/research mixing

## 8. acceptance criteria for later apply

Later apply is acceptable only when all of the following become true in current main for one real second strategy id:

- `src/strategies/<second_strategy_id>/manifest.py` exists and declares `supports_live=True`
- `src/strategies/<second_strategy_id>/config.py` exists and validates a typed runtime config
- `src/strategies/<second_strategy_id>/signal_engine.py` exists and exports `generate_signals`
- `src/strategies/<second_strategy_id>/live_adapter.py` exists and exports `build_live_decision`
- `src/strategies/<second_strategy_id>/artifact_contracts.py` exists and declares exactly one runtime state contract and exactly one runtime trade-log contract compatible with the current registered runtime boundary
- `configs/strategies/<second_strategy_id>.json` exists and registers manifest/config/artifact refs plus required dataset/feature ids
- `configs/strategies/<second_strategy_id>.default.json` exists
- the second strategy can be loaded by `src/moex_core/contracts/registry_loader.py` without runtime-boundary redesign
- no scheduler, netting, capital-allocation, broker-routing, or orchestration redesign is introduced

## 9. blockers if any

One blocker exists and is exact:

- current main contains no second runtime-ready registered live strategy candidate that already reaches the minimum required package, config, live-hook, and artifact-contract surfaces

There is no blocker in the already-applied sequential orchestrator or in the already-applied registered single-strategy runtime boundary.

## 10. one sentence final scope statement

The next correct move is not a second-strategy apply but the emergence in current main of one real second live-capable registered strategy package that fits the existing runtime boundary without widening runtime or portfolio design.
