# PHASE 8 Second Runtime Observability Slice Spec

## 1. verdict

The smallest correct next phase-8 unit after the completed first slice is **one trade-log-derived delegated execution summary embedded inside the already existing orchestrator-owned portfolio runtime run report**.

More exactly:
- keep the existing persisted portfolio runtime run report as the only persisted observability artifact in scope
- do not add a second report artifact, dashboard surface, notifier surface, or experiment-registry write path
- extend only the delegated per-strategy runtime success result with one compact execution-summary object derived from the already existing runtime trade log
- let the existing portfolio run report persist that delegated summary automatically through the already frozen orchestrator-owned report surface

A new report artifact would be broader than necessary.
A contract-only statement without default persistence would also be wrong, but that gap was already closed by the first slice.

## 2. repo proof

Current repo proof is exact:

- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_SPEC.md` and `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md` already prove that phase-8 first slice introduced exactly one orchestrator-owned JSON runtime run report per portfolio run and explicitly did **not** introduce separate per-strategy persisted report artifacts.
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py` already proves that the orchestrator remains the single persisted portfolio-run report producer and that delegated strategy results are embedded inside that one report.
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` already proves that each delegated success result includes `trade_log_path`, `trade_date`, `latest_bar_end`, `reason_code`, `signal_count`, `current_position`, `desired_position`, `position_changed`, and `action`.
- `src/moex_runtime/state_store/file_backed_runtime_session_store.py` already proves that the runtime trade log row schema is exactly:
  - `trade_date`
  - `seq`
  - `bar_end`
  - `action`
  - `prev_pos`
  - `new_pos`
  - `price`
  - `reason_code`
- `src/strategies/ema_3_19_15m/artifact_contracts.py` already proves that the runtime trade log is a declared platform runtime output artifact partitioned by `trade_date`.

So the current repo already proves two important facts:

1. the persistence/traceability gap is already closed by the first slice
2. the next missing narrow observability unit is a compact default execution summary derived from the already declared execution log

That means the correct next step is **not** a new persisted artifact surface.
It is a summary surface on top of the already persisted one.

## 3. chosen observability slice or blocker

Chosen slice:

**one delegated `execution_summary` object derived from the existing per-trade-date runtime trade log and embedded in each delegated success result**

Frozen delegated addition:
- `execution_summary`

Frozen `execution_summary` fields:
- `execution_summary_schema_version`
- `execution_event_count_day`
- `last_execution_seq`
- `last_execution_bar_end`
- `last_execution_action`
- `last_closed_trade_pnl_points`
- `current_day_realized_pnl_points`

Frozen semantics:
- summary source is only the delegated strategy `trade_log_path` already returned by the runtime boundary
- summary scope is only the current `trade_date` partition already used by that trade log
- `execution_event_count_day` = number of data rows currently present in that trade-date trade log
- `last_execution_*` fields come from the last trade-log row if at least one row exists; otherwise they are `null`
- `last_closed_trade_pnl_points` = realized points for the most recent closed leg derivable from the trade log; `null` if no close has yet occurred in that trade-date partition
- `current_day_realized_pnl_points` = sum of realized points for all closed legs derivable from that same trade-date partition; `0.0` if no close has yet occurred
- point semantics are raw realized execution-log price-delta points only for this slice; no commission, slippage, MTM, or drawdown overlay is added here
- a reversal event closes the prior position leg at the reversal price and opens the opposite leg at that same logged price; only the close component contributes to realized points at that event

Frozen result/report versioning:
- delegated success result `runtime_result_schema_version` advances from `2` to `3`
- portfolio-level `portfolio_run_schema_version` advances from `1` to `2`
- no other version surface changes in this slice

No blocker is evidenced for this slice.

## 4. why this is the correct next narrow cycle

This is the correct next narrow cycle because it uses the exact boundary created by the first slice and adds the smallest missing operator-facing execution summary without widening ownership or platform surfaces.

Why smaller is insufficient:
- doing nothing but rely on raw `trade_log_path` still leaves execution-count and realized-points interpretation to downstream ad hoc readers
- pushing this responsibility to a later notifier or dashboard consumer would weaken the platform-owned observability contract

Why broader is unnecessary:
- a second persisted report artifact would duplicate the already-correct orchestrator-owned report surface
- experiment-registry integration is broader than needed because the missing problem is runtime execution summary, not cataloging or research lineage
- mark-to-market, drawdown, and historical aggregation would widen into runtime analytics rather than the next narrow observability increment

So the cheapest correct unit is:
**keep the first-slice persisted report exactly as the only persisted surface and embed one deterministic trade-log-derived delegated execution summary inside it.**

## 5. exact current repo surfaces in scope

In scope only:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `src/moex_runtime/state_store/file_backed_runtime_session_store.py`
- `src/moex_runtime/telemetry/` as the target home for one new summary helper module
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_SPEC.md`
- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

Not in scope:
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
- delegated summary computation lives under `src/moex_runtime/telemetry/`
- delegated summary attachment happens at `src/moex_runtime/engine/`
- persisted ownership remains unchanged at `src/moex_runtime/orchestrator/`

Exact persisted artifact destination:
- unchanged from the first slice:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`

Meaning:
- no new runtime report artifact path is introduced
- no second JSON artifact is introduced
- the new observability unit lives inside the existing delegated success result and is therefore persisted inside the already frozen portfolio run report

## 7. exact non-goals

- no second persisted runtime report artifact
- no separate per-strategy persisted report file
- no experiment-registry integration or write path
- no notifier integration or notifier contract change
- no dashboarding or metrics-stack redesign
- no mark-to-market PnL
- no drawdown computation
- no commission or slippage overlay on runtime execution-summary points
- no historical aggregation service
- no scheduler / cron / daemon redesign
- no runtime-boundary redesign beyond the frozen field addition
- no portfolio netting
- no capital allocation
- no research/backtest observability widening

## 8. acceptance criteria for later apply

Later apply is acceptable only if all points below are true:

- `run_registered_runtime_boundary(...)` still remains the single-strategy callable runtime boundary
- delegated success result now includes one additional top-level field:
  - `execution_summary`
- delegated success result `runtime_result_schema_version` is `3`
- `execution_summary` contains exactly:
  - `execution_summary_schema_version`
  - `execution_event_count_day`
  - `last_execution_seq`
  - `last_execution_bar_end`
  - `last_execution_action`
  - `last_closed_trade_pnl_points`
  - `current_day_realized_pnl_points`
- `execution_summary` is derived only from the delegated `trade_log_path` and the current delegated `trade_date`
- if the delegated trade log does not yet exist or has no rows for that trade-date partition:
  - `execution_event_count_day = 0`
  - `last_execution_seq = null`
  - `last_execution_bar_end = null`
  - `last_execution_action = null`
  - `last_closed_trade_pnl_points = null`
  - `current_day_realized_pnl_points = 0.0`
- reversal handling inside the summary treats the reversal event as closing the prior leg and opening the opposite leg at the same logged price; realized points are credited only to the closed leg component
- `run_registered_portfolio_runtime_orchestrator(...)` remains the single persisted report producer
- portfolio-level `portfolio_run_schema_version` is `2`
- persisted report path remains exactly:
  - `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`
- no new report artifact path is introduced
- no notifier, dashboard, scheduler, registry, broker, risk, or strategy-package widening is introduced

## 9. blockers if any

No blocker is evidenced from current repo proof.

The current repo already proves:
- the persisted portfolio run report surface exists
- delegated strategy results already flow into that surface
- the runtime trade log already exists as a declared artifact
- the trade log already contains the minimal ordered fields required to compute execution counts and realized closed-leg points

So this slice is supportable without new artifact ownership, notifier coupling, or experiment-registry widening.

## 10. one sentence final scope statement

Freeze phase-8 second as one trade-log-derived delegated execution summary embedded into the already existing orchestrator-owned portfolio runtime run report, so execution counts and realized closed-leg day summary become platform-owned by default without adding a second report artifact or widening runtime scope.
