## 1. verdict

The remaining explicit artifact locator / env-contract path resolution in `src/moex_runtime/engine/run_registered_runtime_boundary.py` is already shared platform contract infra.

It is the correct next narrow phase-6 extraction unit.

This spec approves extraction of that unit into `src/moex_core/contracts/` only.

## 2. repo proof

- `src/moex_runtime/engine/run_registered_runtime_boundary.py` still owns the candidate unit inline via `_resolve_external_pattern_path(...)`.
- That helper currently performs all four platform-contract steps in one place:
  - validate `locator_ref`
  - validate that `environment_record["artifact_root_refs"]` is exactly one non-empty root ref for this runtime boundary
  - resolve the required artifact-root env var
  - format `locator_ref` into an absolute path via `Path(artifact_root) / locator_ref.format(**format_kwargs)`
- The same helper is already reused for three different artifact classes in the same runtime call path:
  - dataset path
  - runtime state path
  - runtime trade-log path
  This proves it is not strategy-local file handling.
- `configs/environments/reference_runtime_boundary.json` declares `artifact_root_refs=["MOEX_BOT_ARTIFACT_ROOT"]` and `required_env_vars=["MOEX_BOT_ARTIFACT_ROOT"]`, so artifact-root selection is already an environment contract rather than EMA logic.
- `src/moex_core/contracts/registry_loader.py` already owns adjacent contract validation:
  - `_validate_registry_artifact_contract(...)` requires `contract_class` and `locator_ref`
  - `_require_runtime_env_vars(...)` enforces runtime env presence from the environment record
  This proves that path/env contract enforcement is already partially rooted in `moex_core/contracts`.
- `src/strategies/ema_3_19_15m/artifact_contracts.py` declares all three relevant runtime-boundary artifacts as `external_pattern` contracts with explicit `locator_ref` values:
  - feature dataset input
  - runtime signal state
  - runtime trade log
  The strategy package declares the contract strings, but does not own absolute-path resolution.
- `src/strategies/ema_3_19_15m/live_adapter.py` and `src/strategies/ema_3_19_15m/signal_engine.py` are outside this concern, which confirms that locator/env resolution is not strategy math and not strategy-local runtime behavior.

## 3. chosen extraction unit or blocker

Chosen extraction unit:

`external-pattern runtime artifact contract resolver`

Exact responsibility of that unit:
- consume `locator_ref`, `environment_record`, and formatting kwargs
- validate the `artifact_root_refs` shape required by this registered runtime boundary
- resolve the artifact-root env contract used by the runtime boundary
- format the declared `locator_ref` into an absolute artifact path
- serve dataset/state/trade-log explicit path resolution from one platform-owned contract surface

No blocker is present in current repo proof.

## 4. why it is platform-shared and not strategy-local

This unit is platform-shared because it operates only on generic contract concepts:
- environment record
- env var contract
- artifact root reference
- `external_pattern` locator string
- absolute artifact path materialization

It is not strategy-local because EMA-specific responsibility already ends before this boundary at signal generation and live decision production. The strategy declares artifact contracts; the platform resolves those contracts into concrete runtime paths.

## 5. exact current file scope

Exact current source scope approved for later extraction:
- `src/moex_runtime/engine/run_registered_runtime_boundary.py`

Exact inline responsibility inside that file:
- `_resolve_external_pattern_path(...)`
- dataset path resolution call site
- runtime state path resolution call site
- runtime trade-log path resolution call site

Adjacent source-proof files only:
- `configs/environments/reference_runtime_boundary.json`
- `src/moex_core/contracts/registry_loader.py`
- `src/strategies/ema_3_19_15m/artifact_contracts.py`

Explicit no-touch strategy-local files for this extraction:
- `src/strategies/ema_3_19_15m/signal_engine.py`
- `src/strategies/ema_3_19_15m/live_adapter.py`
- `src/strategies/ema_3_19_15m/config.py`
- `src/strategies/ema_3_19_15m/manifest.py`

## 6. exact target layer destination

Exact target destination:
- `src/moex_core/contracts/`

Exact target boundary to freeze:
- one platform-owned contract-resolution surface under `src/moex_core/contracts/`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` remains the thin caller/orchestrating boundary for the reference path

This spec freezes the destination layer and responsibility boundary only.

## 7. exact non-goals

Non-goals for this extraction:
- no artifact model redesign
- no registry shape redesign
- no multi-root artifact framework beyond the currently proven single-root contract for this runtime boundary
- no state-store redesign
- no execution transition redesign
- no feature materialization migration
- no strategy signal generation changes
- no live adapter changes
- no scheduler work
- no locks work
- no risk/notifier work
- no broker routing work
- no backtest/research mixing
- no multi-strategy orchestration
- no legacy broad audit

## 8. acceptance criteria for later apply

The later apply is acceptable only if all points below are true:

- a new platform-owned contract-resolution surface exists under `src/moex_core/contracts/`
- `src/moex_runtime/engine/run_registered_runtime_boundary.py` no longer owns inline external-pattern artifact path resolution
- the reference runtime path still starts by the same ids only:
  - `strategy_id = ema_3_19_15m`
  - `portfolio_id = reference_ema_3_19_15m_single`
  - `environment_id = reference_runtime_boundary`
- `artifact_root_refs`, `required_env_vars`, `contract_class`, and `locator_ref` values remain unchanged
- the runtime boundary still requires exactly one artifact-root ref for this reference path
- dataset/state/trade-log absolute path results remain identical for the same environment record and formatting kwargs
- strategy-local files under `src/strategies/ema_3_19_15m/` remain unchanged
- one exact end-to-end run of `run_registered_runtime_boundary(...)` still succeeds with no broker/network side effects

## 9. blockers if any

None.

Current repo state is sufficient to freeze this extraction unit without widening scope.

## 10. one sentence final scope statement

Freeze only the extraction of explicit external-pattern artifact locator / env-contract path resolution out of `src/moex_runtime/engine/run_registered_runtime_boundary.py` into `src/moex_core/contracts/`, with no behavior change and no contract redesign.
