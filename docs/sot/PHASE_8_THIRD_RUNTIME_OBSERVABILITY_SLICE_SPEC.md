# PHASE 8 Third Runtime Observability Slice Spec

## 1. verdict

The smallest correct next phase-8 unit after the completed second slice is **one compact delegated failure-path summary embedded inside the already existing orchestrator-owned portfolio runtime run report**.

More exactly:
- keep the existing persisted portfolio runtime run report as the only persisted observability artifact in scope
- do not add a second report artifact, dashboard surface, notifier surface, or experiment-registry write path
- extend only the portfolio runtime run report with one optional top-level `failure_summary` section for delegated runtime failure
- keep success-path delegated `execution_summary` exactly as already frozen and applied
- keep fail-closed stop behavior exactly as already frozen and applied

A second persisted artifact would be broader than necessary.
A broader failure ledger would also be wrong, because the current approved need is only first terminal delegated failure visibility inside the already existing report surface.

## 2. repo proof

Current repo proof is exact:

- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 first slice introduced exactly one orchestrator-owned JSON runtime run report per portfolio run and explicitly did **not** introduce separate per-strategy persisted report artifacts.
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 second slice added delegated success-path `execution_summary` inside that same orchestrator-owned report surface without adding a second persisted artifact.
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py` already proves that the orchestrator remains the single persisted portfolio-run report producer, that it still fails closed on first delegated failure, and that it already writes a failed portfolio report with partial delegated results accumulated before stop.
- That same orchestrator file also proves that the current delegated failure payload is still only:
  - `strategy_id`
  - `ok`
  - `error_type`
  - `error`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` already proves that the delegated success path has already been widened to `runtime_result_schema_version = 3` with one embedded `execution_summary`, so the current remaining gap is **not** success-path execution observability.
- `src/moex_runtime/telemetry/summarize_runtime_trade_log_execution.py` already proves that compact normalized observability helpers under `src/moex_runtime/telemetry/` are already the accepted target home for a narrow runtime summary helper.

So the current repo already proves two important facts:

1. the persisted report surface is already correct and must remain the only persisted artifact in scope
2. the next missing observability unit is a compact normalized failure-path summary for the first terminal delegated runtime failure

That means the correct next step is **not** a new persisted artifact surface and **not** a success-path redesign.
It is one optional failure-path summary section on top of the already persisted report.

## 3. chosen observability slice or blocker

Chosen slice:

**one optional top-level `failure_summary` object embedded in the existing portfolio runtime run report and populated only when a delegated runtime call fails terminally**

Frozen addition:
- `failure_summary`

Frozen `failure_summary` fields:
- `failure_summary_schema_version`
- `failure_scope`
- `failed_strategy_id`
- `failed_at_utc`
- `error_type`
- `error_message`

Frozen semantics:
- `failure_summary` is a top-level sibling section of the existing portfolio runtime run report
- `failure_summary` appears only when portfolio status is `failed` because of a delegated runtime exception
- `failure_scope` is frozen to `delegated_runtime`
- `failed_strategy_id` is the exact delegated strategy that raised the terminal exception
- `failed_at_utc` is the UTC timestamp captured by the orchestrator at the terminal delegated failure point
- `error_type` is the exception class name already captured by the orchestrator
- `error_message` is the compact string form of the exception already captured by the orchestrator
- first terminal captured failure wins
- no later failure capture is attempted because the orchestrator already stops fail-closed on first delegated failure
- no stack trace is persisted
- no retry history is persisted
- no strategy-specific payload is persisted

Frozen versioning:
- portfolio-level `portfolio_run_schema_version` advances from `2` to `3`
- delegated success result `runtime_result_schema_version` remains `3`
- `failure_summary_schema_version` starts at `1`

No blocker is evidenced for this slice.

## 4. why this is the correct next narrow cycle

This is the correct next narrow cycle because it closes the smallest remaining platform-level observability gap without reopening prior slices and without introducing a second persisted artifact.

Why smaller is insufficient:
- relying only on the current delegated failure item leaves failure-path interpretation partially ad hoc because the report has no normalized top-level failure section for the terminal delegated stop event
- leaving failure normalization to a future notifier or dashboard would weaken the platform-owned runtime observability contract

Why broader is unnecessary:
- a second persisted report artifact would duplicate the already-correct orchestrator-owned report surface
- a retry ledger would widen into runtime recovery semantics rather than observability of the already existing fail-closed path
- stack-trace persistence would widen storage and payload scope beyond the approved compact normalized contract
- success-path redesign is unnecessary because the second slice already froze and applied delegated `execution_summary`

So the cheapest correct unit is:
**keep the existing persisted report exactly as the only persisted surface and add one compact top-level `failure_summary` for the first terminal delegated runtime failure.**

## 5. exact current repo surfaces in scope

In scope only:
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `src/moex_runtime/telemetry/` as the target home for one new failure-summary normalization helper module if needed
- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_SECOND_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

Not in scope:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` success-path contract redesign
- strategy signal math
- strategy live adapters
- strategy artifact ownership
- runtime-boundary registration rules
- portfolio enablement semantics
- scheduler / daemon surfaces
- broker / risk / notifier surfaces
- experiment-registry surfaces
- dashboarding or monitoring-stack surfaces

## 6. exact target layer / artifact destination

Exact owner layer:
- failure-path normalization lives under `src/moex_runtime/telemetry/` if a helper is introduced
- failure-summary attachment and persistence remain owned by `src/moex_runtime/orchestrator/`

Exact persisted artifact destination:
- unchanged from the first and second slices:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`

Meaning:
- no new runtime report artifact path is introduced
- no second JSON artifact is introduced
- the new observability unit lives inside the existing portfolio runtime run report
- the orchestrator remains the single persisted report producer

## 7. exact non-goals

- no second persisted runtime report artifact
- no separate per-strategy persisted report file
- no success-path `execution_summary` redesign
- no notifier integration or notifier contract change
- no dashboarding or metrics-stack redesign
- no experiment-registry integration or write path
- no retry ledger
- no stack-trace persistence
- no strategy-specific failure payload persistence
- no scheduler / cron / daemon redesign
- no runtime-boundary redesign beyond leaving current failure raising behavior intact
- no broker / risk expansion
- no portfolio netting
- no capital allocation
- no research/backtest observability widening

## 8. acceptance criteria for later apply

Later apply is acceptable only if all points below are true:

- `run_registered_portfolio_runtime_orchestrator(...)` still remains the single portfolio-run aggregation and persisted-report producer
- persisted report path remains exactly:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`
- portfolio-level `portfolio_run_schema_version` is `3`
- on fully successful runs, top-level `failure_summary` is absent
- on delegated runtime failure, top-level `failure_summary` is present and contains exactly:
  - `failure_summary_schema_version`
  - `failure_scope`
  - `failed_strategy_id`
  - `failed_at_utc`
  - `error_type`
  - `error_message`
- on delegated runtime failure, `failure_scope = "delegated_runtime"`
- on delegated runtime failure, `failed_strategy_id` matches the delegated strategy that raised the terminal exception
- on delegated runtime failure, `error_type` matches the exception class name
- on delegated runtime failure, `error_message` matches the compact string form of the exception
- on delegated runtime failure, the report still contains partial delegated results accumulated before stop
- first terminal captured failure wins
- no retry history field is introduced
- no stack-trace field is introduced
- no strategy-specific payload fields are introduced
- delegated success result `runtime_result_schema_version` remains `3`
- no new report artifact path is introduced
- no notifier, dashboard, registry, broker, risk, scheduler, or strategy-package widening is introduced

## 9. blockers if any

No blocker is evidenced from current repo proof.

The current repo already proves:
- the persisted portfolio run report surface exists
- the orchestrator already stops on first delegated failure
- the orchestrator already writes failed reports with partial delegated results
- compact normalized observability helper logic already has an accepted home under `src/moex_runtime/telemetry/`

So this slice is supportable without new artifact ownership, retry semantics, notifier coupling, or experiment-registry widening.

## 10. one sentence final scope statement

Freeze phase-8 third as one compact top-level `failure_summary` for first terminal delegated runtime failure inside the already existing orchestrator-owned portfolio runtime run report, so failure-path observability becomes normalized by default without adding a second report artifact or widening runtime scope.
