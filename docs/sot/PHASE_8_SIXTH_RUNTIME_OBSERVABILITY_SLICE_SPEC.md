# PHASE 8 Sixth Runtime Observability Slice Spec

## 1. verdict

The smallest correct next phase-8 unit after the completed fifth slice is **one compact whole-run delegated outcome summary embedded inside the already existing orchestrator-owned portfolio runtime run report**.

More exactly:
- keep the existing persisted portfolio runtime run report as the only persisted observability artifact in scope
- do not add a second report artifact, dashboard surface, notifier surface, or experiment-registry write path
- extend only the portfolio runtime run report with one top-level `delegated_outcome_summary` section for compact portfolio-run delegated strategy outcomes
- keep `execution_summary` exactly as already frozen and applied
- keep `failure_summary` exactly as already frozen and applied
- keep `run_timing_summary` exactly as already frozen and applied
- keep `data_freshness_summary` exactly as already frozen and applied

A second persisted artifact would be broader than necessary.
A per-strategy delegated outcome list would also be wrong, because the approved need is only one compact normalized whole-run section inside the already existing report surface.

## 2. repo proof

Current repo proof is exact:

- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 first slice introduced exactly one orchestrator-owned JSON runtime run report per portfolio run and explicitly did **not** introduce separate per-strategy persisted report artifacts.
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 second slice added delegated success-path `execution_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 third slice added delegated failure-path `failure_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `docs/sot/PHASE_8_FOURTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FOURTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 fourth slice added top-level `run_timing_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `docs/sot/PHASE_8_FIFTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FIFTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 fifth slice added top-level `data_freshness_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py` already proves that the orchestrator remains the single persisted portfolio-run report producer and that the current report already carries top-level `portfolio_run_schema_version = 5`, `execution_summary`, `failure_summary`, `run_timing_summary`, and `data_freshness_summary` as the approved normalized whole-run observability surface.
- the same orchestrator-owned portfolio-run flow already distinguishes delegated successes from delegated failures at run aggregation time, which is the exact existing repo-backed basis needed to derive one compact delegated portfolio-run outcome summary without introducing duplicated per-strategy payload persistence.

So the current repo already proves five important facts:

1. the persisted report surface is already correct and must remain the only persisted artifact in scope
2. delegated success and delegated failure information already exist at portfolio-run aggregation time
3. the orchestrator already persists both successful and delegated failure reports, so deterministic top-level summary presence can be frozen now
4. the missing observability unit is not per-strategy detail but one compact normalized whole-run delegated outcome summary
5. the next slice can stay narrow because the summary can be derived from already available orchestrator-owned run aggregation facts

That means the correct next step is **not** a new persisted artifact surface, **not** a redesign of success/failure/timing/freshness reporting, and **not** a duplicated per-strategy delegated result payload list.
It is one compact top-level delegated outcome summary on top of the already persisted report.

## 3. chosen observability slice or blocker

Chosen slice:

**one top-level `delegated_outcome_summary` object embedded in the existing portfolio runtime run report and populated on every completed report write, for both successful and failed portfolio runs**

Frozen addition:
- `delegated_outcome_summary`

Frozen `delegated_outcome_summary` fields:
- `delegated_outcome_summary_schema_version`
- `delegated_scope`
- `enabled_strategy_count`
- `delegated_success_count`
- `delegated_failure_count`
- `delegated_outcome_status`

Frozen semantics:
- `delegated_outcome_summary` is a top-level sibling section of the existing portfolio runtime run report
- `delegated_outcome_summary` is portfolio-run level only and never a second persisted per-strategy delegated outcome artifact
- `delegated_outcome_summary` must be present on every completed persisted portfolio runtime run report, including both success and delegated failure paths
- `delegated_scope` is frozen to `portfolio_registered_strategies`
- `enabled_strategy_count` is the count of enabled strategies included in the completed portfolio run
- `delegated_success_count` is the count of enabled strategies whose delegated runtime execution completed successfully and contributed a usable delegated success result
- `delegated_failure_count` is the count of enabled strategies whose delegated runtime execution ended in delegated failure for the completed portfolio run
- `delegated_outcome_status` is a compact normalized whole-run classification derived only from the three counts above
- `delegated_outcome_summary` must not duplicate per-strategy delegated payloads as a list
- `delegated_outcome_summary` must not introduce strategy-specific metrics beyond compact outcome counts
- `delegated_outcome_summary` must not duplicate `execution_summary`, `failure_summary`, `run_timing_summary`, or `data_freshness_summary` semantics

Frozen `delegated_outcome_status` values:
- `all_succeeded` = `enabled_strategy_count > 0`, `delegated_success_count = enabled_strategy_count`, and `delegated_failure_count = 0`
- `partial_failure` = `delegated_success_count > 0` and `delegated_failure_count > 0`
- `all_failed` = `enabled_strategy_count > 0`, `delegated_success_count = 0`, and `delegated_failure_count = enabled_strategy_count`
- `none_enabled` = `enabled_strategy_count = 0`, `delegated_success_count = 0`, and `delegated_failure_count = 0`

Frozen invariants:
- `enabled_strategy_count`, `delegated_success_count`, and `delegated_failure_count` are JSON integer values greater than or equal to `0`
- `delegated_success_count + delegated_failure_count = enabled_strategy_count`
- `delegated_success_count <= enabled_strategy_count`
- `delegated_failure_count <= enabled_strategy_count`
- `delegated_outcome_status` must be exactly one of `all_succeeded`, `partial_failure`, `all_failed`, `none_enabled`

Frozen versioning:
- portfolio-level `portfolio_run_schema_version` advances from `5` to `6`
- delegated success result `runtime_result_schema_version` remains unchanged
- `failure_summary_schema_version` remains unchanged
- `run_timing_summary_schema_version` remains unchanged
- `data_freshness_summary_schema_version` remains unchanged
- `delegated_outcome_summary_schema_version` starts at `1`

No blocker is evidenced for this slice.

## 4. why this is the correct next narrow cycle

This is the correct next narrow cycle because it closes the smallest remaining platform-level delegated outcome observability gap without reopening prior slices and without introducing a second persisted artifact.

Why smaller is insufficient:
- relying only on existing delegated success and failure sections leaves whole-run delegated outcome classification ad hoc because every consumer would need to recompute normalized counts and portfolio-run status itself
- leaving delegated outcome normalization to a future notifier or dashboard would weaken the platform-owned runtime observability contract

Why broader is unnecessary:
- a second persisted delegated outcome artifact would duplicate the already-correct orchestrator-owned report surface
- a per-strategy delegated payload list would widen into detail duplication rather than the approved compact platform report contract
- widening strategy-specific metrics beyond compact counts would reopen already sufficient delegated result surfaces without a repo-backed blocker
- any redesign of `execution_summary`, `failure_summary`, `run_timing_summary`, or `data_freshness_summary` would reopen already frozen slices for no justified reason

So the cheapest correct unit is:
**keep the existing persisted report exactly as the only persisted surface and add one compact top-level `delegated_outcome_summary` for normalized whole-run delegated strategy outcomes.**

## 5. exact current repo surfaces in scope

In scope only:
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`
- `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`
- `docs/sot/PHASE_8_FOURTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_FOURTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`
- `docs/sot/PHASE_8_FIFTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_FIFTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

Not in scope:
- `execution_summary` redesign
- `failure_summary` redesign
- `run_timing_summary` redesign
- `data_freshness_summary` redesign
- delegated runtime boundary contract widening unless a real repo-backed blocker is proven later
- strategy signal math
- strategy live adapters
- strategy artifact ownership
- runtime-boundary registration rules
- portfolio enablement semantics
- scheduler / daemon surfaces
- broker / risk / notifier surfaces
- experiment-registry surfaces
- dashboarding or monitoring-stack surfaces
- per-step telemetry
- external telemetry coupling

## 6. exact target layer / artifact destination

Exact owner layer:
- delegated-outcome-summary construction and persistence remain owned by `src/moex_runtime/orchestrator/`

Exact persisted artifact destination:
- unchanged from the first, second, third, fourth, and fifth slices:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`

Meaning:
- no new runtime report artifact path is introduced
- no second JSON artifact is introduced
- the new observability unit lives inside the existing portfolio runtime run report
- the orchestrator remains the single persisted report producer

## 7. exact non-goals

- no second persisted runtime report artifact
- no separate per-strategy persisted delegated outcome report file
- no `execution_summary` redesign
- no `failure_summary` redesign
- no `run_timing_summary` redesign
- no `data_freshness_summary` redesign
- no notifier integration or notifier contract change
- no dashboarding or metrics-stack redesign
- no experiment-registry integration or write path
- no delegated success payload list duplication
- no strategy-specific metric widening beyond compact outcome counts
- no scheduler / cron / daemon redesign
- no runtime-boundary redesign beyond using current orchestration facts as delegated outcome inputs
- no broker / risk expansion
- no portfolio netting
- no capital allocation
- no research/backtest observability widening
- no per-step profiling
- no external telemetry coupling

## 8. acceptance criteria for later apply

Later apply is acceptable only if all points below are true:

- `run_registered_portfolio_runtime_orchestrator(...)` still remains the single portfolio-run aggregation and persisted-report producer
- persisted report path remains exactly:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`
- portfolio-level `portfolio_run_schema_version` is `6`
- on fully successful runs, top-level `delegated_outcome_summary` is present
- on delegated runtime failure, top-level `delegated_outcome_summary` is present
- top-level `delegated_outcome_summary` contains exactly:
  - `delegated_outcome_summary_schema_version`
  - `delegated_scope`
  - `enabled_strategy_count`
  - `delegated_success_count`
  - `delegated_failure_count`
  - `delegated_outcome_status`
- `delegated_outcome_summary_schema_version = 1`
- `delegated_scope = "portfolio_registered_strategies"`
- `enabled_strategy_count`, `delegated_success_count`, and `delegated_failure_count` are JSON integer values greater than or equal to `0`
- `delegated_success_count + delegated_failure_count = enabled_strategy_count`
- `delegated_outcome_status` is one of `all_succeeded`, `partial_failure`, `all_failed`, `none_enabled`
- `delegated_outcome_status = "all_succeeded"` only when `enabled_strategy_count > 0`, `delegated_success_count = enabled_strategy_count`, and `delegated_failure_count = 0`
- `delegated_outcome_status = "partial_failure"` only when `delegated_success_count > 0` and `delegated_failure_count > 0`
- `delegated_outcome_status = "all_failed"` only when `enabled_strategy_count > 0`, `delegated_success_count = 0`, and `delegated_failure_count = enabled_strategy_count`
- `delegated_outcome_status = "none_enabled"` only when all three counts are `0`
- `delegated_outcome_summary` does not introduce per-strategy lists
- `delegated_outcome_summary` does not introduce per-step fields
- delegated success result `runtime_result_schema_version` remains unchanged
- `failure_summary_schema_version` remains unchanged
- `run_timing_summary_schema_version` remains unchanged
- `data_freshness_summary_schema_version` remains unchanged
- no new report artifact path is introduced
- no notifier, dashboard, registry, broker, risk, scheduler, or strategy-package widening is introduced

## 9. blockers if any

No blocker is evidenced from current repo proof.

The current repo already proves:
- the persisted portfolio run report surface exists
- the orchestrator already persists both successful and failed reports
- compact delegated whole-run counts can be derived at orchestrator aggregation time
- one normalized whole-run delegated outcome summary can therefore be added without introducing a second artifact or reopening delegated payload contracts

So this slice is supportable without new artifact ownership, notifier coupling, external telemetry coupling, or strategy-payload widening.

## 10. one sentence final scope statement

Freeze phase-8 sixth as one compact top-level `delegated_outcome_summary` for normalized whole-run delegated strategy outcomes inside the already existing orchestrator-owned portfolio runtime run report, so delegated portfolio-run outcomes become normalized by default without adding a second report artifact or widening runtime scope.
