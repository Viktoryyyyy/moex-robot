# PHASE 8 Fourth Runtime Observability Slice Spec

## 1. verdict

The smallest correct next phase-8 unit after the completed third slice is **one compact whole-run timing summary embedded inside the already existing orchestrator-owned portfolio runtime run report**.

More exactly:
- keep the existing persisted portfolio runtime run report as the only persisted observability artifact in scope
- do not add a second report artifact, dashboard surface, notifier surface, or experiment-registry write path
- extend only the portfolio runtime run report with one top-level `run_timing_summary` section for whole-run timing
- keep `execution_summary` exactly as already frozen and applied
- keep `failure_summary` exactly as already frozen and applied
- keep fail-closed stop behavior exactly as already frozen and applied

A second persisted artifact would be broader than necessary.
A step-level timing surface would also be wrong, because the approved need is only one compact normalized whole-run timing section inside the already existing report surface.

## 2. repo proof

Current repo proof is exact:

- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 first slice introduced exactly one orchestrator-owned JSON runtime run report per portfolio run and explicitly did **not** introduce separate per-strategy persisted report artifacts.
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 second slice added delegated success-path `execution_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 third slice added delegated failure-path `failure_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py` already proves that the orchestrator remains the single persisted portfolio-run report producer and that the current report already carries top-level `started_at_utc` and `completed_at_utc` on both success and failure paths.
- That same orchestrator file also proves that the current portfolio report schema version is `3`, that the third-slice `failure_summary` is already present on delegated failure, and that failed reports are still written with partial delegated results accumulated before stop.
- `src/moex_runtime/telemetry/summarize_runtime_trade_log_execution.py` already proves that compact normalized observability helpers under `src/moex_runtime/telemetry/` are already the accepted target home for a narrow runtime summary helper.

So the current repo already proves three important facts:

1. the persisted report surface is already correct and must remain the only persisted artifact in scope
2. the raw timing anchors already exist at the report top level as `started_at_utc` and `completed_at_utc`
3. the next missing observability unit is one compact normalized whole-run timing summary derived from those already persisted anchors

That means the correct next step is **not** a new persisted artifact surface, **not** a redesign of success/failure reporting, and **not** step-level profiling.
It is one compact top-level timing summary on top of the already persisted report.

## 3. chosen observability slice or blocker

Chosen slice:

**one top-level `run_timing_summary` object embedded in the existing portfolio runtime run report and populated on every completed report write, for both successful and failed portfolio runs**

Frozen addition:
- `run_timing_summary`

Frozen `run_timing_summary` fields:
- `run_timing_summary_schema_version`
- `timing_scope`
- `wall_clock_duration_seconds`

Frozen semantics:
- `run_timing_summary` is a top-level sibling section of the existing portfolio runtime run report
- `run_timing_summary` is whole-run only and never per-step, per-strategy, or per-retry
- `run_timing_summary` must be present on every completed persisted portfolio runtime run report, including both success and delegated failure paths
- `timing_scope` is frozen to `portfolio_run_wall_clock`
- `wall_clock_duration_seconds` is the non-negative elapsed wall-clock duration of the portfolio run from the already persisted top-level `started_at_utc` to the already persisted top-level `completed_at_utc`
- `wall_clock_duration_seconds` is stored as a JSON numeric value in seconds
- `wall_clock_duration_seconds` must not encode per-step, per-strategy, retry, queue, notifier, or dashboard timing
- `run_timing_summary` must not duplicate `started_at_utc` or `completed_at_utc` inside itself because those anchors already exist at the portfolio-report top level

Frozen versioning:
- portfolio-level `portfolio_run_schema_version` advances from `3` to `4`
- delegated success result `runtime_result_schema_version` remains unchanged
- `failure_summary_schema_version` remains unchanged
- `run_timing_summary_schema_version` starts at `1`

No blocker is evidenced for this slice.

## 4. why this is the correct next narrow cycle

This is the correct next narrow cycle because it closes the smallest remaining platform-level timing observability gap without reopening prior slices and without introducing a second persisted artifact.

Why smaller is insufficient:
- relying only on raw top-level `started_at_utc` and `completed_at_utc` leaves whole-run timing interpretation partially ad hoc because every consumer would need to derive elapsed duration itself
- leaving duration derivation to a future notifier or dashboard would weaken the platform-owned runtime observability contract

Why broader is unnecessary:
- a second persisted timing artifact would duplicate the already-correct orchestrator-owned report surface
- step-level profiling would widen into runtime instrumentation rather than the approved compact platform report contract
- retry timing would widen into retry semantics that are explicitly out of scope
- any redesign of `execution_summary` or `failure_summary` would reopen already frozen slices for no justified reason

So the cheapest correct unit is:
**keep the existing persisted report exactly as the only persisted surface and add one compact top-level `run_timing_summary` for whole-run elapsed wall-clock timing.**

## 5. exact current repo surfaces in scope

In scope only:
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `src/moex_runtime/telemetry/` as the target home for one new run-timing normalization helper module if needed
- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`
- `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_THIRD_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

Not in scope:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` success-path contract redesign
- `failure_summary` redesign
- strategy signal math
- strategy live adapters
- strategy artifact ownership
- runtime-boundary registration rules
- portfolio enablement semantics
- scheduler / daemon surfaces
- broker / risk / notifier surfaces
- experiment-registry surfaces
- dashboarding or monitoring-stack surfaces
- step-level profiling or retry tracing

## 6. exact target layer / artifact destination

Exact owner layer:
- run-timing normalization lives under `src/moex_runtime/telemetry/` if a helper is introduced
- run-timing attachment and persistence remain owned by `src/moex_runtime/orchestrator/`

Exact persisted artifact destination:
- unchanged from the first, second, and third slices:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`

Meaning:
- no new runtime report artifact path is introduced
- no second JSON artifact is introduced
- the new observability unit lives inside the existing portfolio runtime run report
- the orchestrator remains the single persisted report producer

## 7. exact non-goals

- no second persisted runtime report artifact
- no separate per-strategy persisted report file
- no `execution_summary` redesign
- no `failure_summary` redesign
- no notifier integration or notifier contract change
- no dashboarding or metrics-stack redesign
- no experiment-registry integration or write path
- no retry ledger
- no stack-trace persistence
- no strategy-specific timing payload persistence
- no scheduler / cron / daemon redesign
- no runtime-boundary redesign beyond leaving current runtime call behavior intact
- no broker / risk expansion
- no portfolio netting
- no capital allocation
- no research/backtest observability widening
- no step-level profiling

## 8. acceptance criteria for later apply

Later apply is acceptable only if all points below are true:

- `run_registered_portfolio_runtime_orchestrator(...)` still remains the single portfolio-run aggregation and persisted-report producer
- persisted report path remains exactly:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`
- portfolio-level `portfolio_run_schema_version` is `4`
- on fully successful runs, top-level `run_timing_summary` is present
- on delegated runtime failure, top-level `run_timing_summary` is present
- top-level `run_timing_summary` contains exactly:
  - `run_timing_summary_schema_version`
  - `timing_scope`
  - `wall_clock_duration_seconds`
- `run_timing_summary_schema_version = 1`
- `timing_scope = "portfolio_run_wall_clock"`
- `wall_clock_duration_seconds` is a non-negative JSON numeric value derived from the report top-level `started_at_utc` and `completed_at_utc`
- `run_timing_summary` does not repeat `started_at_utc`
- `run_timing_summary` does not repeat `completed_at_utc`
- `run_timing_summary` does not introduce per-step fields
- `run_timing_summary` does not introduce per-strategy timing fields
- `run_timing_summary` does not introduce retry timing fields
- delegated success result `runtime_result_schema_version` remains unchanged
- `failure_summary_schema_version` remains unchanged
- no new report artifact path is introduced
- no notifier, dashboard, registry, broker, risk, scheduler, or strategy-package widening is introduced

## 9. blockers if any

No blocker is evidenced from current repo proof.

The current repo already proves:
- the persisted portfolio run report surface exists
- the orchestrator already writes `started_at_utc` and `completed_at_utc`
- the orchestrator already persists both successful and failed reports
- compact normalized observability helper logic already has an accepted home under `src/moex_runtime/telemetry/`

So this slice is supportable without new artifact ownership, notifier coupling, retry semantics, or experiment-registry widening.

## 10. one sentence final scope statement

Freeze phase-8 fourth as one compact top-level `run_timing_summary` for whole-run elapsed wall-clock timing inside the already existing orchestrator-owned portfolio runtime run report, so timing observability becomes normalized by default without adding a second report artifact or widening runtime scope.
