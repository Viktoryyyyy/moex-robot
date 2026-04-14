## 1. verdict

APPLY COMPLETED

The already-approved third phase-6 extraction was applied in narrow scope.

The explicit external-pattern artifact locator / env-contract path resolution was moved out of `src/moex_runtime/engine/run_registered_runtime_boundary.py` into `src/moex_core/contracts/external_pattern_artifact_path_resolver.py` with no intended behavior change and no contract redesign.

## 2. exact file scope changed

Code apply changed exactly these files:
- `src/moex_core/contracts/external_pattern_artifact_path_resolver.py`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`

Report-only commit changed exactly this file:
- `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_PHASE_6_THIRD_SHARED_RUNTIME_INFRA_APPLY_REPORT.md`

## 3. exact file scope not changed

Explicitly unchanged in this cycle:
- `src/strategies/ema_3_19_15m/`
- `src/moex_runtime/state_store/file_backed_runtime_session_store.py`
- `src/moex_runtime/execution/runtime_position_transition.py`
- `configs/environments/reference_runtime_boundary.json`
- `configs/portfolios/reference_ema_3_19_15m_single.json`
- `configs/strategies/ema_3_19_15m.json`
- `src/moex_core/contracts/registry_loader.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`
- all registry/config/manifest files except this new report file

## 4. code apply commit sha

`5a23435c5621f2c93aabaaa3935705adcffa5d91`

## 5. report commit sha

This report is written in a separate report-only commit.

## 6. proof that runtime boundary is thinner after apply

Before apply, `src/moex_runtime/engine/run_registered_runtime_boundary.py` owned inline external-pattern artifact path resolution for:
- env-contract based artifact root resolution
- validation of `artifact_root_refs` shape for this runtime boundary
- formatting `locator_ref` into absolute artifact paths
- dataset/state/trade-log explicit path resolution

After apply, those responsibilities are delegated to the platform-owned contract surface:
- `src/moex_core/contracts/external_pattern_artifact_path_resolver.py`

The runtime boundary remains the thin caller that:
- loads the registered runtime boundary
- materializes the feature frame
- calls strategy signal/live adapter logic
- allocates the next trade sequence
- delegates position-transition execution
- appends the trade-log row when a position change occurs
- persists the returned runtime state

## 7. proof that strategy package stayed untouched

The code apply changed only files under `src/moex_core/contracts/` and `src/moex_runtime/`.

No file under `src/strategies/ema_3_19_15m/` was modified.

## 8. proof that contracts stayed unchanged

Contracts stayed unchanged because:
- `src/strategies/ema_3_19_15m/artifact_contracts.py` was not modified
- `configs/environments/reference_runtime_boundary.json` was not modified
- `artifact_root_refs` stayed unchanged
- `required_env_vars` stayed unchanged
- `contract_class` values stayed unchanged
- `locator_ref` values stayed unchanged
- the runtime still starts from the same ids only:
  - `strategy_id=ema_3_19_15m`
  - `portfolio_id=reference_ema_3_19_15m_single`
  - `environment_id=reference_runtime_boundary`
- dataset/state/trade-log absolute path formatting semantics remain unchanged

No artifact ids, locator refs, partition semantics, or env contracts were changed.

## 9. server sync proof

Direct server access was unavailable in this cycle.

Owner-run sync/proof command is required to confirm that server applied state matches GitHub SoT at the resulting commit chain.

## 10. server proof result

PENDING OWNER-RUN

The exact owner-run command was produced after GitHub apply so the same cycle can continue without server-first editing.

## 11. blockers if any

No repo blocker.

Direct server access is unavailable from this sub-chat, so server sync/proof must be owner-run from the exact command provided in the cycle response.
