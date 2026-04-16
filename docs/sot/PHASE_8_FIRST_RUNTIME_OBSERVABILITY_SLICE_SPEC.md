# PHASE 8 First Runtime Observability Slice Spec

## 1. verdict

The smallest correct phase-8 unit is **one platform-owned structured runtime result contract plus one explicit portfolio-run JSON report artifact written by the existing portfolio orchestrator**.

More exactly:
- extend the existing per-strategy runtime result so each delegated run carries the minimum missing traceability fields
- keep one portfolio-level aggregated result at the orchestrator surface
- persist that aggregated result by default as one runtime report artifact per portfolio run
- do **not** add separate per-strategy report files, experiment-registry writes, dashboarding, or telemetry-stack redesign in this cycle

A result-contract-only freeze is **not sufficient** here, because current runtime/orchestrator results are return-value-only and therefore do not become traceable by default after process exit.

## 2. repo proof

Current repo proof is exact:

- `src/moex_runtime/engine/run_registered_runtime_boundary.py` already returns a structured success result for each delegated strategy run, but that result currently contains only `strategy_id`, `portfolio_id`, `environment_id`, `dataset_path`, `state_path`, `trade_log_path`, `signal_count`, `current_position`, `desired_position`, `position_changed`, and `action`.
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py` already aggregates one portfolio-level result with `portfolio_id`, `environment_id`, `status`, `ok`, `enabled_strategy_ids`, and `delegated_strategy_results`, including fail-closed partial results on first delegated failure.
- Those two surfaces prove that the platform already has the correct runtime return boundaries for observability, but they are still **ephemeral return payloads** rather than an explicit persisted runtime report surface.
- `src/strategies/ema_3_19_15m/artifact_contracts.py` and `src/strategies/reference_flat_15m_validation/artifact_contracts.py` prove that current runtime artifacts are still limited to strategy-local state and trade-log outputs; there is no existing runtime run/report artifact for the orchestrator path.
- `src/moex_strategy_sdk/artifact_contracts.py` already proves that `report` is an allowed artifact role, so repo evidence does **not** show an artifact-model blocker for adding one explicit observability/report surface.
- `configs/environments/reference_runtime_boundary.json` already proves that runtime artifacts are expected to resolve under the environment artifact root contract, so a persisted runtime report can be introduced without scheduler, registry, or experiment-registry widening.

So current repo proof resolves into one exact conclusion:

**the next missing observability unit is not new orchestration semantics, but default persistence of the already-existing runtime result boundary with a minimal traceability field extension.**

## 3. chosen observability slice or blocker

Chosen slice:

**one portfolio-owned runtime run report surface, backed by a minimally extended delegated runtime result contract**

Exact scope of that slice:
- keep the existing orchestrator and runtime boundary ownership unchanged
- extend the delegated per-strategy success result with only the missing fields needed for traceability/comparison:
  - `runtime_result_schema_version`
  - `strategy_version`
  - `instrument_id`
  - `trade_date`
  - `latest_bar_end`
  - `reason_code`
- extend the portfolio-level result with only the missing run-identity fields needed for default traceability:
  - `portfolio_run_schema_version`
  - `portfolio_run_id`
  - `started_at_utc`
  - `completed_at_utc`
- persist exactly one JSON report per portfolio run containing:
  - the full portfolio-level result
  - one delegated record per strategy in execution order
  - partial delegated results on fail-closed stop

No blocker is evidenced for this slice.

## 4. why it is the smallest correct next phase-8 unit

This is the smallest correct phase-8 step because it closes the exact current gap while staying inside already-applied runtime boundaries.

Why smaller is insufficient:
- a result-contract-only freeze would still leave runtime observability as process-local return data or ad hoc stdout, so runs would still not be traceable by default
- separate per-strategy report files would be broader than necessary because the orchestrator already owns the only surface that sees one full portfolio run plus all delegated results
- experiment-registry integration would be broader than necessary because the current missing problem is first-order runtime traceability, not research/run catalog integration

Why broader is unnecessary:
- the orchestrator already has the correct aggregation point
- the runtime boundary already has the correct per-strategy result point
- the environment already has the correct artifact-root contract

So the cheapest correct unit is:
**return-contract extension plus one orchestrator-owned persisted report artifact, and nothing more.**

## 5. exact current repo surfaces in scope

In scope only:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `src/moex_strategy_sdk/artifact_contracts.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `src/strategies/reference_flat_15m_validation/artifact_contracts.py`
- `configs/environments/reference_runtime_boundary.json`

Not in scope:
- strategy signal math
- runtime-boundary registration rules
- portfolio enablement semantics
- experiment-registry surfaces
- scheduler / daemon surfaces

## 6. exact target layer / artifact destination

Exact owner layer:
- `src/moex_runtime/orchestrator/`

Exact artifact destination:
- one platform-owned external-pattern JSON runtime report under the runtime artifact root
- exact path pattern to freeze now:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`

Meaning:
- per-strategy delegated run observability remains embedded inside the orchestrator-owned portfolio run report
- the runtime boundary remains the producer of structured delegated result payloads
- the orchestrator becomes the single producer of the persisted runtime run report artifact
- no strategy package owns this artifact
- no experiment-registry surface owns this artifact in this cycle

## 7. exact non-goals

- no experiment-registry integration or write path
- no registry-model redesign
- no strategy-package artifact-contract redesign for runtime observability ownership
- no separate per-strategy persisted report files
- no dashboarding or monitoring-stack redesign
- no latency/metrics stack expansion beyond the frozen result/report fields above
- no scheduler / cron / daemon redesign
- no runtime-boundary redesign
- no orchestrator control-flow redesign
- no broker / risk / notifier expansion
- no portfolio netting
- no capital allocation
- no research/backtest observability widening
- no historical aggregation service

## 8. acceptance criteria for later apply

Later apply is acceptable only if all points below are true:

- `run_registered_runtime_boundary(...)` still remains a single-strategy callable boundary and still returns one structured success result per delegated run
- that delegated success result now includes exactly the current fields plus the frozen minimum observability additions:
  - `runtime_result_schema_version`
  - `strategy_version`
  - `instrument_id`
  - `trade_date`
  - `latest_bar_end`
  - `reason_code`
- `run_registered_portfolio_runtime_orchestrator(...)` still remains the single portfolio-run aggregation point and still fails closed on delegated failure
- the portfolio-level result now includes exactly the current fields plus the frozen minimum run-trace additions:
  - `portfolio_run_schema_version`
  - `portfolio_run_id`
  - `started_at_utc`
  - `completed_at_utc`
- the orchestrator writes exactly one JSON artifact per portfolio run to:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`
- that JSON artifact is resolved from the environment artifact root contract, not from a hardcoded absolute server path
- the JSON artifact contains the full portfolio-level result and the delegated per-strategy records in declared execution order
- on delegated failure, the JSON artifact is still written and contains `status = "failed"`, `ok = false`, and the partial delegated results accumulated before stop
- no separate per-strategy report artifact is introduced
- no experiment-registry, scheduler, broker, risk, notifier, netting, or allocation work is introduced

## 9. blockers if any

No blocker is evidenced from current repo proof.

The current repo already proves:
- the runtime boundary exists
- the orchestrator aggregation point exists
- report is a valid artifact role
- the environment artifact-root contract already exists

So this slice is supportable without artifact-model redesign or experiment-registry widening.

## 10. one sentence final scope statement

Freeze phase-8 first as one orchestrator-owned persisted portfolio runtime-run JSON report, backed by a minimally extended delegated runtime result contract, so that one portfolio run and each delegated per-strategy run become traceable by default without widening runtime, orchestration, portfolio semantics, or experiment-registry scope.
