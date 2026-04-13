## 1. verdict

APPLY COMPLETED

The already-approved first phase-6 extraction was applied in narrow scope.

The file-backed runtime session-store responsibility was moved out of `src/moex_runtime/engine/run_registered_runtime_boundary.py` into `src/moex_runtime/state_store/` with no intended behavior change.

## 2. exact file scope changed

Code apply changed exactly these files:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`
- `src/moex_runtime/state_store/file_backed_runtime_session_store.py`

Report-only commit changed exactly this file:
- `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_PHASE_6_FIRST_SHARED_RUNTIME_INFRA_APPLY_REPORT.md`

## 3. exact file scope not changed

Explicitly unchanged in this cycle:
- `src/strategies/ema_3_19_15m/`
- `src/strategy/realtime/ema_3_19_15m/`
- `configs/environments/reference_runtime_boundary.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `configs/strategies/ema_3_19_15m.json`
- `src/moex_core/contracts/registry_loader.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- all registry/config/manifest files

## 4. code apply commit sha

`d8163684c192fec5cdb02cc608b5f9d2f0a326eb`

## 5. report commit sha

This report is written in a separate report-only commit.

## 6. proof that runtime boundary is thinner after apply

Before apply, `src/moex_runtime/engine/run_registered_runtime_boundary.py` owned inline low-level persistence helpers for:
- runtime state JSON load
- runtime state JSON atomic save
- runtime trade-log last-row read
- next sequence allocation
- runtime trade-log append

After apply, those helpers are no longer defined inline in the runtime boundary file.

The runtime boundary now imports platform-owned state-store functions from:
- `src/moex_runtime/state_store/file_backed_runtime_session_store.py`

The runtime boundary remains the thin caller that:
- resolves contracts
- materializes the feature frame
- calls strategy signal/live adapter logic
- computes position change/action
- delegates persistence and trade-log IO to the platform state-store surface

## 7. proof that strategy package stayed untouched

The code apply changed only two files, both under `src/moex_runtime/`.

No file under `src/strategies/ema_3_19_15m/` was modified.

## 8. proof that artifact contract semantics stayed unchanged

Artifact contract semantics stayed unchanged because:
- `src/strategies/ema_3_19_15m/artifact_contracts.py` was not modified
- the runtime still resolves the same runtime state contract and runtime trade-log contract
- the runtime still uses the same ids only:
  - `strategy_id=ema_3_19_15m`
  - `portfolio_id=reference_ema_3_19_15m_single`
  - `environment_id=reference_runtime_boundary`
- state artifact locator resolution remains unchanged
- trade-log artifact locator resolution remains unchanged
- trade-date derivation remains unchanged
- state save remains atomic via temp-file replace
- trade-log append keeps current repo behavior semantics unchanged

No artifact ids, locator refs, or partition semantics were changed.

## 9. blockers

None.

## 10. acceptance summary

Accepted against the frozen narrow scope:
- new platform-owned state-store surface now exists under `src/moex_runtime/state_store/`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` no longer owns inline low-level persistence helpers
- `src/strategies/ema_3_19_15m/` remained untouched
- artifact ids / locator refs / partition semantics remained unchanged
- no broker/network side effects were introduced in this apply
