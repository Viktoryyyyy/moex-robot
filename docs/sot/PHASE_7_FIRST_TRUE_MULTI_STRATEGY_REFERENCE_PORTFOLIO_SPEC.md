# PHASE 7 First True Multi-Strategy Reference Portfolio Spec

## 1. verdict

Blocked.

The smallest correct next phase-7 portfolio-level unit is not a new `enabled_strategy_ids > 1` reference portfolio config yet.

The current repo still exposes only one runtime-ready registered live strategy for the already-applied sequential orchestrator: `ema_3_19_15m`.

Therefore the first true multi-strategy reference portfolio cannot be frozen as a real portfolio config yet without inventing a second strategy that the repo does not currently support.

## 2. repo proof

Repo proof for the blocker is exact and narrow:

- `src/strategies/ema_3_19_15m/manifest.py` declares `strategy_id="ema_3_19_15m"` and `supports_live=True`.
- `configs/strategies/` currently exposes only EMA strategy config files: `ema_3_19_15m.json` and `ema_3_19_15m.default.json`.
- `configs/portfolios/` currently exposes only one portfolio registry/config file: `reference_ema_3_19_15m_single.json`.
- `src/strategies/` currently resolves only the `ema_3_19_15m` strategy package in repo scope.
- the already-applied orchestrator and runtime boundary are reusable, but reuse alone does not create a second runtime-ready registered strategy.

So the blocker is not orchestration. The blocker is absence of a second repo-supported runtime-ready registered live strategy package plus its matching registered config surface.

## 3. chosen first reference portfolio or blocker

Chosen output for this cycle is the blocker, frozen exactly:

**Current repo does not yet contain a second runtime-ready registered live strategy that can legally join `ema_3_19_15m` inside one reference portfolio with `enabled_strategy_ids > 1`.**

Accordingly, the first true multi-strategy reference portfolio is blocked pending exactly one narrow prerequisite:

- add one second repo-supported runtime-ready registered live strategy package
- add its matching strategy registry/config surface under `configs/strategies/`
- keep orchestration sequential and portfolio-scoped exactly as already applied

Until that prerequisite exists, any multi-strategy portfolio file would be fake rather than reference-correct.

## 4. why it is the smallest correct next portfolio-level unit

This is the smallest correct next unit because the orchestrator itself is already in place and already portfolio-scoped, while the missing piece is now singular and concrete: one second runtime-ready registered strategy.

Freezing anything larger would widen into strategy creation, allocation, risk, broker, netting, or runtime redesign.

Freezing an actual multi-strategy portfolio config now would be incorrect because there is no second repo-backed live strategy to include.

## 5. exact current repo surfaces in scope

- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- `src/moex_core/contracts/registry_loader.py`
- `src/strategies/ema_3_19_15m/manifest.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/strategies/ema_3_19_15m.default.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `configs/environments/reference_runtime_boundary.json`

## 6. exact target layer / config destination

The eventual first true multi-strategy reference portfolio remains a portfolio-registry/config-layer artifact.

Its exact destination is:

- one new portfolio config under `configs/portfolios/`

No other layer destination is required for the first real portfolio proof, provided the missing second runtime-ready registered strategy already exists in repo before that apply.

## 7. exact non-goals

Non-goals for this spec:

- creating the second strategy in this cycle
- widening the registry model
- widening artifact contracts
- scheduler / cron / daemon work
- parallel execution
- locks / notifier / telemetry expansion
- broker routing redesign
- portfolio netting
- cross-strategy capital allocation
- shared risk engine expansion
- backtest or research packaging redesign
- strategy package redesign beyond adding one repo-supported second runtime-ready registered live strategy

## 8. acceptance criteria for later apply

Later apply is acceptable only when all of the following are true:

- repo contains a second runtime-ready registered live strategy package in `src/strategies/<second_strategy_id>/`
- that second strategy has a manifest/config/live-adapter/artifact-contract surface compatible with the current registered runtime boundary
- repo contains its matching strategy config under `configs/strategies/`
- one new portfolio config is added under `configs/portfolios/` with `enabled_strategy_ids` containing exactly `ema_3_19_15m` plus that second registered live strategy id
- the new portfolio keeps one shared `environment_id` and uses the already-applied sequential orchestrator without parallelism or runtime-semantics widening
- owner-run proof shows the orchestrator returning two delegated strategy results in declared order for that portfolio
- no portfolio netting, capital allocation, cross-strategy risk, broker-routing redesign, or artifact-model redesign is introduced

## 9. blockers if any

One blocker exists and is exact:

- missing second repo-supported runtime-ready registered live strategy package with matching registered config surface

There is no additional blocker in orchestrator design, runtime-boundary shape, or current portfolio config semantics.

## 10. one sentence final scope statement

The first true multi-strategy reference portfolio is blocked until one second repo-supported runtime-ready registered live strategy is added, after which the next correct apply is exactly one new `configs/portfolios/` entry that pairs it with `ema_3_19_15m` under the existing sequential orchestrator.
