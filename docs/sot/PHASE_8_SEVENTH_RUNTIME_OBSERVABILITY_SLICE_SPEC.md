# PHASE 8 Seventh Runtime Observability Slice Spec

## 1. verdict

The smallest correct next phase-8 unit after the completed sixth slice is **one compact whole-run signal activity summary embedded inside the already existing orchestrator-owned portfolio runtime run report**.

More exactly:
- keep the existing persisted portfolio runtime run report as the only persisted observability artifact in scope
- do not add a second report artifact, dashboard surface, notifier surface, or experiment-registry write path
- extend only the portfolio runtime run report with one top-level `signal_activity_summary` section for compact portfolio-run signal activity
- keep `execution_summary` exactly as already frozen and applied
- keep `failure_summary` exactly as already frozen and applied
- keep `run_timing_summary` exactly as already frozen and applied
- keep `data_freshness_summary` exactly as already frozen and applied
- keep `delegated_outcome_summary` exactly as already frozen and applied

A second persisted artifact would be broader than necessary.
A per-strategy signal activity list would also be wrong, because the approved need is only one compact normalized whole-run section inside the already existing report surface.

## 2. repo proof

Current repo proof is exact:

- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 first slice introduced exactly one orchestrator-owned JSON runtime run report per portfolio run and explicitly did **not** introduce separate per-strategy persisted report artifacts.
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 second slice added delegated success-path `execution_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 third slice added delegated failure-path `failure_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `docs/sot/PHASE_8_FOURTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FOURTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 fourth slice added top-level `run_timing_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `docs/sot/PHASE_8_FIFTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FIFTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 fifth slice added top-level `data_freshness_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `docs/sot/PHASE_8_SIXTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_SIXTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 sixth slice added top-level `delegated_outcome_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py` already proves that the orchestrator remains the single persisted portfolio-run report producer and that the current report already carries top-level `portfolio_run_schema_version = 6`, `failure_summary`, `run_timing_summary`, `data_freshness_summary`, and `delegated_outcome_summary` as the approved normalized whole-run observability surface.
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` already proves that delegated successful runtime results already carry compact signal activity primitives as `signal_count`, `position_changed`, and `action`, which is the exact existing repo-backed basis needed to derive one compact whole-run signal activity summary without widening strategy payload contracts or introducing duplicated per-strategy persistence.

So the current repo already proves five important facts:

1. the persisted report surface is already correct and must remain the only persisted artifact in scope
2. compact per-strategy signal activity primitives already exist in delegated successful runtime results
3. the orchestrator already persists both successful and delegated failure reports, so deterministic top-level summary presence can be frozen now
4. the missing observability unit is not per-strategy detail but one compact normalized whole-run signal activity summary
5. the next slice can stay narrow because the summary can be derived from already available orchestrator-owned run aggregation facts and delegated successful runtime result fields

That means the correct next step is **not** a new persisted artifact surface, **not** a redesign of success/failure/timing/freshness/delegated-outcome reporting, and **not** a duplicated per-strategy signal payload list.
It is one compact top-level signal activity summary on top of the already persisted report.

## 3. chosen observability slice or blocker

Chosen slice:

**one top-level `signal_activity_summary` object embedded in the existing portfolio runtime run report and populated on every completed report write, for both successful and failed portfolio runs**

Frozen addition:
- `signal_activity_summary`

Frozen `signal_activity_summary` fields:
- `signal_activity_summary_schema_version`
- `signal_activity_scope`
- `signal_coverage_status`
- `reporting_strategy_count`
- `active_strategy_count`
- `total_signal_count`
- `signal_activity_status`

Frozen semantics:
- `signal_activity_summary` is a top-level sibling section of the existing portfolio runtime run report
- `signal_activity_summary` is portfolio-run level only and never a second persisted per-strategy signal artifact
- `signal_activity_summary` must be present on every completed persisted portfolio runtime run report, including both success and delegated failure paths
- `signal_activity_scope` is frozen to `portfolio_runtime_signals`
- signal aggregation is derived only from already available delegated successful runtime results that contain usable integer `result.signal_count`
- `reporting_strategy_count` is the count of delegated successful runtime results contributing usable integer `signal_count` to the summary
- `active_strategy_count` is the count of contributing delegated successful runtime results whose usable `signal_count` is strictly greater than `0`
- `total_signal_count` is the sum of usable delegated successful `signal_count` values contributing to the summary
- `signal_activity_status` is a compact normalized whole-run classification derived only from `reporting_strategy_count`, `active_strategy_count`, and `total_signal_count`
- `signal_activity_summary` must not duplicate per-strategy payloads as a list
- `signal_activity_summary` must not widen strategy-specific metrics beyond compact signal counts and status
- `signal_activity_summary` must not duplicate `execution_summary`, `failure_summary`, `run_timing_summary`, `data_freshness_summary`, or `delegated_outcome_summary` semantics

Frozen coverage status values:
- `complete` = all enabled strategies for the completed portfolio run contributed delegated successful runtime results with usable integer `signal_count`
- `partial` = at least one delegated successful runtime result contributed usable integer `signal_count`, but the completed portfolio run does not contain usable signal counts for all enabled strategies
- `unavailable` = no delegated successful runtime result contributed usable integer `signal_count`

Frozen `signal_activity_status` values:
- `active` = `reporting_strategy_count > 0`, `active_strategy_count > 0`, and `total_signal_count > 0`
- `idle` = `reporting_strategy_count > 0`, `active_strategy_count = 0`, and `total_signal_count = 0`
- `unavailable` = `reporting_strategy_count = 0`, `active_strategy_count = 0`, and `total_signal_count = 0`

Frozen invariants:
- `reporting_strategy_count`, `active_strategy_count`, and `total_signal_count` are JSON integer values greater than or equal to `0`
- `active_strategy_count <= reporting_strategy_count`
- `signal_coverage_status = "unavailable"` if and only if `reporting_strategy_count = 0`
- `signal_activity_status = "unavailable"` if and only if `reporting_strategy_count = 0`, `active_strategy_count = 0`, and `total_signal_count = 0`
- `signal_activity_status = "idle"` if and only if `reporting_strategy_count > 0`, `active_strategy_count = 0`, and `total_signal_count = 0`
- `signal_activity_status = "active"` if and only if `reporting_strategy_count > 0`, `active_strategy_count > 0`, and `total_signal_count > 0`

Frozen versioning:
- portfolio-level `portfolio_run_schema_version` advances from `6` to `7`
- delegated success result `runtime_result_schema_version` remains unchanged
- `failure_summary_schema_version` remains unchanged
- `run_timing_summary_schema_version` remains unchanged
- `data_freshness_summary_schema_version` remains unchanged
- `delegated_outcome_summary_schema_version` remains unchanged
- `signal_activity_summary_schema_version` starts at `1`

No blocker is evidenced for this slice.

## 4. why this is the correct next narrow cycle

This is the correct next narrow cycle because it closes the smallest remaining platform-level signal activity observability gap without reopening prior slices and without introducing a second persisted artifact.

Why smaller is insufficient:
- relying only on delegated successful per-strategy `signal_count` leaves whole-run signal activity interpretation ad hoc because every consumer would need to aggregate reporting coverage, activity, and total counts itself
- leaving signal activity normalization to a future notifier or dashboard would weaken the platform-owned runtime observability contract

Why broader is unnecessary:
- a second persisted signal activity artifact would duplicate the already-correct orchestrator-owned report surface
- a per-strategy signal activity list would widen into detail duplication rather than the approved compact platform report contract
- widening strategy-specific metrics beyond compact counts and normalized status would reopen already sufficient delegated result surfaces without a repo-backed blocker
- any redesign of `execution_summary`, `failure_summary`, `run_timing_summary`, `data_freshness_summary`, or `delegated_outcome_summary` would reopen already frozen slices for no justified reason

So the cheapest correct unit is:
**keep the existing persisted report exactly as the only persisted surface and add one compact top-level `signal_activity_summary` for normalized whole-run signal activity.**

## 5. exact current repo surfaces in scope

In scope only:
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` strictly as existing repo proof for already available delegated `signal_count`
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
- `docs/sot/PHASE_8_SIXTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_SIXTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

Not in scope:
- `execution_summary` redesign
- `failure_summary` redesign
- `run_timing_summary` redesign
- `data_freshness_summary` redesign
- `delegated_outcome_summary` redesign
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
- per-step signal telemetry
- external telemetry coupling

## 6. exact target layer / artifact destination

Exact owner layer:
- signal-activity-summary construction and persistence remain owned by `src/moex_runtime/orchestrator/`

Exact persisted artifact destination:
- unchanged from the first, second, third, fourth, fifth, and sixth slices:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`

Meaning:
- no new runtime report artifact path is introduced
- no second JSON artifact is introduced
- the new observability unit lives inside the existing portfolio runtime run report
- the orchestrator remains the single persisted report producer

## 7. exact non-goals

- no second persisted runtime report artifact
- no separate per-strategy persisted signal activity report file
- no `execution_summary` redesign
- no `failure_summary` redesign
- no `run_timing_summary` redesign
- no `data_freshness_summary` redesign
- no `delegated_outcome_summary` redesign
- no notifier integration or notifier contract change
- no dashboarding or metrics-stack redesign
- no experiment-registry integration or write path
- no delegated success payload widening unless a real repo-backed blocker is proven later
- no strategy-specific signal metric widening beyond compact signal counts and normalized status
- no scheduler / cron / daemon redesign
- no runtime-boundary redesign beyond using current delegated successful results as signal-activity inputs
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
- portfolio-level `portfolio_run_schema_version` is `7`
- on fully successful runs, top-level `signal_activity_summary` is present
- on delegated runtime failure, top-level `signal_activity_summary` is present
- top-level `signal_activity_summary` contains exactly:
  - `signal_activity_summary_schema_version`
  - `signal_activity_scope`
  - `signal_coverage_status`
  - `reporting_strategy_count`
  - `active_strategy_count`
  - `total_signal_count`
  - `signal_activity_status`
- `signal_activity_summary_schema_version = 1`
- `signal_activity_scope = "portfolio_runtime_signals"`
- `signal_coverage_status` is one of `complete`, `partial`, `unavailable`
- `signal_activity_status` is one of `active`, `idle`, `unavailable`
- `reporting_strategy_count`, `active_strategy_count`, and `total_signal_count` are JSON integer values greater than or equal to `0`
- `active_strategy_count <= reporting_strategy_count`
- when `signal_coverage_status = "complete"`, all enabled strategies in the completed portfolio run contributed usable integer delegated `result.signal_count`
- when `signal_coverage_status = "partial"`, at least one but not all enabled strategies in the completed portfolio run contributed usable integer delegated `result.signal_count`
- when `signal_coverage_status = "unavailable"`, `reporting_strategy_count = 0`, `active_strategy_count = 0`, and `total_signal_count = 0`
- `signal_activity_status = "active"` only when `reporting_strategy_count > 0`, `active_strategy_count > 0`, and `total_signal_count > 0`
- `signal_activity_status = "idle"` only when `reporting_strategy_count > 0`, `active_strategy_count = 0`, and `total_signal_count = 0`
- `signal_activity_status = "unavailable"` only when `reporting_strategy_count = 0`, `active_strategy_count = 0`, and `total_signal_count = 0`
- `signal_activity_summary` does not introduce per-strategy lists
- `signal_activity_summary` does not introduce per-step fields
- delegated success result `runtime_result_schema_version` remains unchanged
- `failure_summary_schema_version` remains unchanged
- `run_timing_summary_schema_version` remains unchanged
- `data_freshness_summary_schema_version` remains unchanged
- `delegated_outcome_summary_schema_version` remains unchanged
- no new report artifact path is introduced
- no notifier, dashboard, registry, broker, risk, scheduler, or strategy-package widening is introduced

## 9. blockers if any

No blocker is evidenced from current repo proof.

The current repo already proves:
- the persisted portfolio run report surface exists
- the orchestrator already persists both successful and failed reports
- successful delegated runtime results already carry `signal_count`
- compact whole-run signal activity aggregation can therefore be added without introducing a second artifact or reopening delegated success payload contracts

So this slice is supportable without new artifact ownership, notifier coupling, external telemetry coupling, or strategy-payload widening.

## 10. one sentence final scope statement

Freeze phase-8 seventh as one compact top-level `signal_activity_summary` for normalized whole-run signal activity inside the already existing orchestrator-owned portfolio runtime run report, so portfolio-level signal activity becomes normalized by default without adding a second report artifact or widening runtime scope.
