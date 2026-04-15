# PHASE 7 Second Strategy Source Spec

## 1. verdict

The most legitimate source for the first real second strategy slice is:

**legacy line is the correct source**

More exactly, the least-widening legitimate source is the historical `mr1` strategy line, which should be revived into the already-frozen current-main strategy package contract.

## 2. repo proof

Repo proof is exact and narrow:

- current main contains one live-capable registered strategy package: `src/strategies/ema_3_19_15m/manifest.py` plus `configs/strategies/ema_3_19_15m.json`
- current main portfolio registration still exposes only one enabled strategy id in `configs/portfolios/reference_ema_3_19_15m_single.json`
- current main runtime registration contract requires a real package/config/artifact surface, not loose scripts: package ref, manifest, config schema, artifact contracts, `signal_engine:generate_signals`, `live_adapter:build_live_decision`, exactly one runtime state contract, and exactly one runtime trade-log contract
- current main does not contain `src/strategies/mr1/manifest.py`
- current main does not contain `src/strategies/usdrubf_large_day_mr/manifest.py`
- current main does contain `src/research/build_usdrubf_large_day_mr_day_pairs.py`, but that file is research-only and produces day-pair research output rather than a runtime-ready registered strategy surface
- repo history contains an actual `mr1` strategy lineage with strategy-specific signal and loop surfaces, including `scripts/signal_mr1.py` and `scripts/loop_signal_mr1.py`, which proves legacy strategy existence even though it is not present in current main package form

So the repo evidence resolves into four exact facts:

- no current-main near-ready second strategy package exists
- a real legacy strategy line does exist: `mr1`
- a research line does exist: USDRUBF large-day MR
- among the non-current-main sources, the legacy `mr1` line is the narrower promotion source

## 3. chosen source or blocker

Chosen source:

**Revive the historical `mr1` strategy line as the first real second strategy source.**

This is a source-spec only.

It does **not** approve direct restore of old scripts into current main.
It freezes only the lineage choice:

- source lineage = legacy `mr1`
- promotion target = current-main strategy package contract
- promotion standard = same registered runtime boundary model already used by `ema_3_19_15m`

## 4. why this is the least-widening legitimate source

This is the least-widening legitimate source because:

- current-main has no second near-ready candidate, so option 1 is disproven
- `mr1` already exists as a real repo strategy lineage rather than only as an analytical research artifact
- `mr1` legacy evidence already shows strategy-local signal and loop surfaces, which is closer to strategy revival than to research promotion
- the USDRUBF large-day MR line in current main is still only a research builder and would widen into research-to-runtime promotion, instrument/runtime promotion, and new strategy contract freezing at the same time
- choosing “no source” would be too strong, because a legitimate historical source lineage does exist in repo history

So the narrowest correct answer is not “no source” and not “research”.
It is:

**legacy `mr1` should be the source lineage, but only via controlled re-entry into current-main package form.**

## 5. exact current repo surfaces in scope

- `src/strategies/ema_3_19_15m/manifest.py`
- `configs/strategies/ema_3_19_15m.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `src/moex_core/contracts/registry_loader.py`
- `src/research/build_usdrubf_large_day_mr_day_pairs.py`
- historical repo commit lineage for `mr1`:
  - `d3b7156cb3833c27b65db2426bb28cf6caf2a0a7`
  - `898c43999cca232c956a79232603a9556bbd13a3`
  - `e945b3317aae75713b22c56fa1aca88c3c9e71f0`

## 6. exact downstream destination if later promoted

If later promoted, the chosen legacy source must land only at the already-frozen target surfaces below:

- `src/strategies/mr1/manifest.py`
- `src/strategies/mr1/config.py`
- `src/strategies/mr1/signal_engine.py`
- `src/strategies/mr1/live_adapter.py`
- `src/strategies/mr1/artifact_contracts.py`
- `configs/strategies/mr1.json`
- `configs/strategies/mr1.default.json`

If that promotion later succeeds, the first true multi-strategy portfolio destination is then exactly:

- one new portfolio config under `configs/portfolios/` with `enabled_strategy_ids` containing exactly `ema_3_19_15m` and `mr1`

## 7. exact non-goals

Non-goals for this spec:

- direct legacy script restoration into current main
- second-strategy apply in this cycle
- research promotion of USDRUBF large-day MR in this cycle
- invention of a new second strategy idea
- scheduler / orchestration redesign
- locks / risk / notifier expansion
- broker routing redesign
- portfolio netting
- capital allocation
- artifact model redesign unless a later candidate-specific blocker proves it is required
- registry model redesign unless a later candidate-specific blocker proves it is required
- broad audit of all historical strategy work beyond the exact `mr1` source lineage proof

## 8. acceptance criteria for later promotion into a real second strategy slice

Later promotion from the chosen source is acceptable only when all of the following are true in current main:

- `src/strategies/mr1/manifest.py` exists and declares `supports_live=True`
- `src/strategies/mr1/config.py` exists and validates a typed runtime config
- `src/strategies/mr1/signal_engine.py` exists and exports `generate_signals`
- `src/strategies/mr1/live_adapter.py` exists and exports `build_live_decision`
- `src/strategies/mr1/artifact_contracts.py` exists and declares exactly one runtime state contract and exactly one runtime trade-log contract compatible with the current registered runtime boundary
- `configs/strategies/mr1.json` exists and registers manifest/config/artifact refs plus required dataset/feature ids
- `configs/strategies/mr1.default.json` exists
- `mr1` loads through `src/moex_core/contracts/registry_loader.py` without runtime-boundary redesign
- promotion does not widen into USDRUBF onboarding, research-to-runtime mixing, scheduler redesign, portfolio redesign, broker redesign, or capital-allocation work

## 9. blockers if any

Exact blockers:

- the chosen `mr1` source exists only as legacy repo lineage today, not as current-main package/config registration
- legacy `mr1` surfaces are script-era and therefore cannot be reused as-is
- one narrow promotion slice is still required to re-enter `mr1` into current-main package form under the existing registry/runtime contract

There is no blocker in the already-applied sequential orchestrator itself.

## 10. one sentence final scope statement

The first real second strategy should come from the legacy `mr1` line, revived narrowly into current-main strategy package form under the existing registered runtime boundary, rather than from a research-only USDRUBF line or from an invented new candidate.
