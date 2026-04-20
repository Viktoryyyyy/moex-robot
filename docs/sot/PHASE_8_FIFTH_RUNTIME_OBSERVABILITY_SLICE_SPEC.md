# PHASE 8 Fifth Runtime Observability Slice Spec

## 1. verdict

The smallest correct next phase-8 unit after the completed fourth slice is **one compact whole-run data freshness summary embedded inside the already existing orchestrator-owned portfolio runtime run report**.

More exactly:
- keep the existing persisted portfolio runtime run report as the only persisted observability artifact in scope
- do not add a second report artifact, dashboard surface, notifier surface, or experiment-registry write path
- extend only the portfolio runtime run report with one top-level `data_freshness_summary` section for compact portfolio-run input freshness
- keep `execution_summary` exactly as already frozen and applied
- keep `failure_summary` exactly as already frozen and applied
- keep `run_timing_summary` exactly as already frozen and applied

A second persisted artifact would be broader than necessary.
A per-step or per-strategy freshness artifact would also be wrong, because the approved need is only one compact normalized whole-run freshness section inside the already existing report surface.

## 2. repo proof

Current repo proof is exact:

- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 first slice introduced exactly one orchestrator-owned JSON runtime run report per portfolio run and explicitly did **not** introduce separate per-strategy persisted report artifacts.
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 second slice added delegated success-path `execution_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 third slice added delegated failure-path `failure_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `docs/sot/PHASE_8_FOURTH_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FOURTH_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 fourth slice added top-level `run_timing_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py` already proves that the orchestrator remains the single persisted portfolio-run report producer and that the current report already carries top-level `portfolio_run_schema_version = 4`, `started_at_utc`, `completed_at_utc`, and `run_timing_summary` on both success and delegated failure paths.
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` already proves that delegated successful runtime results already carry per-strategy `latest_bar_end`, which is the exact existing repo-backed signal needed to derive a compact portfolio-level freshness summary without widening strategy payload contracts.

So the current repo already proves four important facts:

1. the persisted report surface is already correct and must remain the only persisted artifact in scope
2. compact per-strategy freshness anchors already exist in successful delegated results as `latest_bar_end`
3. the orchestrator already persists both successful and delegated failure reports, so deterministic top-level summary presence can be frozen now
4. the next missing observability unit is one compact normalized whole-run freshness summary derived from already persisted run-level and delegated-result data

That means the correct next step is **not** a new persisted artifact surface, **not** a redesign of success/failure/timing reporting, and **not** strategy-specific payload widening unless a real blocker is proven later.
It is one compact top-level freshness summary on top of the already persisted report.

## 3. chosen observability slice or blocker

Chosen slice:

**one top-level `data_freshness_summary` object embedded in the existing portfolio runtime run report and populated on every completed report write, for both successful and failed portfolio runs**

Frozen addition:
- `data_freshness_summary`

Frozen `data_freshness_summary` fields:
- `data_freshness_summary_schema_version`
- `freshness_scope`
- `freshness_coverage_status`
- `oldest_latest_bar_end_utc`
- `newest_latest_bar_end_utc`
- `latest_bar_end_span_seconds`

Frozen semantics:
- `data_freshness_summary` is a top-level sibling section of the existing portfolio runtime run report
- `data_freshness_summary` is portfolio-run level only and never a second persisted per-strategy freshness artifact
- `data_freshness_summary` must be present on every completed persisted portfolio runtime run report, including both success and delegated failure paths
- `freshness_scope` is frozen to `portfolio_runtime_inputs`
- freshness aggregation is derived only from already available delegated successful runtime results that contain `result.latest_bar_end`
- `oldest_latest_bar_end_utc` is the earliest delegated successful `latest_bar_end` contributing to the summary
- `newest_latest_bar_end_utc` is the latest delegated successful `latest_bar_end` contributing to the summary
- `latest_bar_end_span_seconds` is the non-negative elapsed seconds between `oldest_latest_bar_end_utc` and `newest_latest_bar_end_utc`
- `latest_bar_end_span_seconds` is stored as a JSON numeric value in seconds
- `data_freshness_summary` must not duplicate per-strategy `latest_bar_end` values as a list
- `data_freshness_summary` must not duplicate `execution_summary`, `failure_summary`, or `run_timing_summary` semantics

Frozen coverage status values:
- `complete` = all enabled strategies for the portfolio run contributed delegated successful runtime results with usable `latest_bar_end`
- `partial` = at least one delegated successful runtime result contributed usable `latest_bar_end`, but the completed portfolio run does not contain usable freshness anchors for all enabled strategies
- `unavailable` = no delegated successful runtime result contributed usable `latest_bar_end`

Frozen nullability rules:
- when `freshness_coverage_status = "complete"` or `"partial"`, `oldest_latest_bar_end_utc` and `newest_latest_bar_end_utc` must be populated valid UTC ISO-8601 timestamps and `latest_bar_end_span_seconds` must be a non-negative JSON numeric value
- when `freshness_coverage_status = "unavailable"`, `oldest_latest_bar_end_utc = null`, `newest_latest_bar_end_utc = null`, and `latest_bar_end_span_seconds = null`

Frozen versioning:
- portfolio-level `portfolio_run_schema_version` advances from `4` to `5`
- delegated success result `runtime_result_schema_version` remains unchanged
- `failure_summary_schema_version` remains unchanged
- `run_timing_summary_schema_version` remains unchanged
- `data_freshness_summary_schema_version` starts at `1`

No blocker is evidenced for this slice.

## 4. why this is the correct next narrow cycle

This is the correct next narrow cycle because it closes the smallest remaining platform-level data freshness observability gap without reopening prior slices and without introducing a second persisted artifact.

Why smaller is insufficient:
- relying only on per-strategy delegated `latest_bar_end` leaves whole-run freshness interpretation ad hoc because every consumer would need to aggregate spread and coverage itself
- leaving freshness normalization to a future notifier or dashboard would weaken the platform-owned runtime observability contract

Why broader is unnecessary:
- a second persisted freshness artifact would duplicate the already-correct orchestrator-owned report surface
- per-step freshness telemetry would widen into instrumentation rather than the approved compact platform report contract
- widening delegated success payloads would reopen already sufficient per-strategy surfaces without a repo-backed blocker
- any redesign of `execution_summary`, `failure_summary`, or `run_timing_summary` would reopen already frozen slices for no justified reason

So the cheapest correct unit is:
**keep the existing persisted report exactly as the only persisted surface and add one compact top-level `data_freshness_summary` for whole-run runtime input freshness.**

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
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` strictly as existing repo proof for already available delegated `latest_bar_end`

Not in scope:
- `execution_summary` redesign
- `failure_summary` redesign
- `run_timing_summary` redesign
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
- per-step freshness telemetry
- external telemetry coupling

## 6. exact target layer / artifact destination

Exact owner layer:
- freshness-summary construction and persistence remain owned by `src/moex_runtime/orchestrator/`

Exact persisted artifact destination:
- unchanged from the first, second, third, and fourth slices:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`

Meaning:
- no new runtime report artifact path is introduced
- no second JSON artifact is introduced
- the new observability unit lives inside the existing portfolio runtime run report
- the orchestrator remains the single persisted report producer

## 7. exact non-goals

- no second persisted runtime report artifact
- no separate per-strategy persisted freshness report file
- no `execution_summary` redesign
- no `failure_summary` redesign
- no `run_timing_summary` redesign
- no notifier integration or notifier contract change
- no dashboarding or metrics-stack redesign
- no experiment-registry integration or write path
- no delegated success payload widening unless a real repo-backed blocker is proven later
- no strategy-specific freshness payload persistence beyond existing delegated `latest_bar_end`
- no scheduler / cron / daemon redesign
- no runtime-boundary redesign beyond using current delegated successful results as freshness inputs
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
- portfolio-level `portfolio_run_schema_version` is `5`
- on fully successful runs, top-level `data_freshness_summary` is present
- on delegated runtime failure, top-level `data_freshness_summary` is present
- top-level `data_freshness_summary` contains exactly:
  - `data_freshness_summary_schema_version`
  - `freshness_scope`
  - `freshness_coverage_status`
  - `oldest_latest_bar_end_utc`
  - `newest_latest_bar_end_utc`
  - `latest_bar_end_span_seconds`
- `data_freshness_summary_schema_version = 1`
- `freshness_scope = "portfolio_runtime_inputs"`
- `freshness_coverage_status` is one of `complete`, `partial`, `unavailable`
- when `freshness_coverage_status = "complete"`, the summary is derived from all enabled strategies in the completed portfolio run and `latest_bar_end_span_seconds` is a non-negative JSON numeric value
- when `freshness_coverage_status = "partial"`, the summary is derived only from delegated successful runtime results with usable `result.latest_bar_end`
- when `freshness_coverage_status = "unavailable"`, `oldest_latest_bar_end_utc = null`, `newest_latest_bar_end_utc = null`, and `latest_bar_end_span_seconds = null`
- `oldest_latest_bar_end_utc` and `newest_latest_bar_end_utc` are valid UTC ISO-8601 timestamps whenever populated
- `latest_bar_end_span_seconds` equals the elapsed seconds between `oldest_latest_bar_end_utc` and `newest_latest_bar_end_utc` whenever populated
- `data_freshness_summary` does not introduce per-strategy lists
- `data_freshness_summary` does not introduce per-step fields
- delegated success result `runtime_result_schema_version` remains unchanged
- `failure_summary_schema_version` remains unchanged
- `run_timing_summary_schema_version` remains unchanged
- no new report artifact path is introduced
- no notifier, dashboard, registry, broker, risk, scheduler, or strategy-package widening is introduced

## 9. blockers if any

No blocker is evidenced from current repo proof.

The current repo already proves:
- the persisted portfolio run report surface exists
- the orchestrator already persists both successful and failed reports
- successful delegated runtime results already carry `latest_bar_end`
- compact whole-run freshness aggregation can therefore be added without introducing a second artifact or reopening delegated success payload contracts

So this slice is supportable without new artifact ownership, notifier coupling, external telemetry coupling, or strategy-payload widening.

## 10. one sentence final scope statement

Freeze phase-8 fifth as one compact top-level `data_freshness_summary` for whole-run runtime input freshness inside the already existing orchestrator-owned portfolio runtime run report, so portfolio-level freshness becomes normalized by default without adding a second report artifact or widening runtime scope.
