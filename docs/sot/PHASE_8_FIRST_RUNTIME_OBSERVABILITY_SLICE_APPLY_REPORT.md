# PHASE 8 First Runtime Observability Slice Apply Report

## Verdict

Phase-8 first runtime observability slice is now fully reflected in repo SoT.

The original code apply remains the already accepted GitHub code apply commit:
- `8f92df5e32f93badad77998176ec8626c456f9f1`

This report-only cleanup records the already observed owner-run server sync/proof outcome for that code apply and does not change code, config, runtime behavior, or architecture scope.

The original apply stayed narrow:
- delegated per-strategy runtime success result was minimally extended only
- portfolio-level runtime result was minimally extended only
- exactly one orchestrator-owned JSON runtime run report artifact was added per portfolio run
- no separate per-strategy persisted report artifact was introduced
- no scheduler, registry, broker, risk, notifier, or experiment-registry widening was introduced

## Exact file scope changed for the original code apply

Changed:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- `src/moex_runtime/orchestrator/run_registered_portfolio_runtime_orchestrator.py`
- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

## Exact file scope not changed for the original code apply

Not changed:
- `configs/environments/reference_runtime_boundary.json`
- `src/moex_strategy_sdk/artifact_contracts.py`
- `src/strategies/ema_3_19_15m/*`
- `src/strategies/reference_flat_15m_validation/*`
- strategy math
- runtime-boundary registration rules
- portfolio enablement semantics
- experiment-registry surfaces
- scheduler / daemon surfaces
- broker / risk / notifier surfaces

## Exact file scope changed for this report-only cleanup

Changed:
- `docs/sot/PHASE_8_FIRST_RUNTIME_OBSERVABILITY_SLICE_APPLY_REPORT.md`

Not changed:
- all code files
- all config files
- all runtime/orchestrator files
- all strategy files
- all architecture/spec files other than this existing apply report

## Code apply commit SHA

- `8f92df5e32f93badad77998176ec8626c456f9f1`

## Report commit SHA

- report-only cleanup commit separate from code apply commit

## Proof delegated runtime result contract was minimally extended only

`run_registered_runtime_boundary(...)` still remains the single-strategy callable runtime boundary.

Pre-existing delegated success result fields were preserved:
- `strategy_id`
- `portfolio_id`
- `environment_id`
- `dataset_path`
- `state_path`
- `trade_log_path`
- `signal_count`
- `current_position`
- `desired_position`
- `position_changed`
- `action`

Frozen additions applied exactly:
- `runtime_result_schema_version`
- `strategy_version`
- `instrument_id`
- `trade_date`
- `latest_bar_end`
- `reason_code`

No other delegated result fields were introduced.

## Proof portfolio result contract was minimally extended only

`run_registered_portfolio_runtime_orchestrator(...)` still remains the single portfolio aggregation point.

Pre-existing portfolio result fields were preserved:
- `portfolio_id`
- `environment_id`
- `status`
- `ok`
- `enabled_strategy_ids`
- `delegated_strategy_results`

Frozen additions applied exactly:
- `portfolio_run_schema_version`
- `portfolio_run_id`
- `started_at_utc`
- `completed_at_utc`

No other portfolio result fields were introduced.

## Proof exactly one persisted report artifact per portfolio run now exists

The orchestrator now resolves and writes exactly one JSON report per portfolio run at:
- `data/runtime/portfolio_runs/{portfolio_id}/runtime_run_{portfolio_run_id}.json`

The report is written by the orchestrator after successful completion and also on delegated fail-closed stop with partial delegated results accumulated before stop.

The persisted JSON contains:
- the full portfolio-level result
- delegated per-strategy records in declared execution order
- partial delegated results on delegated failure

## Proof no separate per-strategy persisted report artifact was introduced

No strategy package was changed.

No per-strategy report write path was introduced.

Delegated per-strategy observability remains embedded inside the single orchestrator-owned portfolio runtime run report.

## Owner-run server sync proof

This chat did not perform server mutation.

Owner-run server sync to the accepted code apply commit was observed in project flow:
- synced commit: `8f92df5e32f93badad77998176ec8626c456f9f1`

This preserves the project distinction:
- GitHub / repo code apply = architectural Source of Truth
- owner-run server sync = applied state only

## Owner-run server proof result

Owner-run server proof PASS was observed for the frozen validation portfolio:
- `portfolio_id=reference_ema_3_19_15m_with_flat_validation`
- `environment_id=reference_runtime_boundary`

Observed PASS scope:
- orchestrator returned portfolio result successfully
- returned portfolio result included `portfolio_run_schema_version`, `portfolio_run_id`, `started_at_utc`, `completed_at_utc`
- delegated per-strategy result payloads included `runtime_result_schema_version`, `strategy_version`, `instrument_id`, `trade_date`, `latest_bar_end`, `reason_code`
- exactly one JSON runtime run report was written under the runtime artifact root contract
- JSON contained both delegated strategy records in declared order
- no separate per-strategy persisted report artifact existed

## Blockers

No repo blocker remains for the already completed phase-8 first observability slice.

This report-only cleanup exists only to align repo SoT with the already observed owner-run server sync/proof PASS.
