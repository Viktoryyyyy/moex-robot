## 1. verdict

PASS — current `origin/main` preserves one contract-correct registered runtime-boundary path after the first phase-6 state-store extraction.

What is repo-proven:
- id-only startup semantics are preserved
- the extracted state-store responsibility remains in `src/moex_runtime/state_store/`
- no registry/config/artifact contract drift is present in the validated path
- no code drift has occurred in the validated runtime path since the code apply commit

What is not repo-proven:
- a fresh post-extraction runtime execution actually ran end-to-end
- live filesystem outputs were written in a real run
- side-effect-free execution was observed in practice

## 2. repo proof

- `src/moex_runtime/engine/run_registered_runtime_boundary.py` still exposes the runtime boundary as one function that starts only from `strategy_id`, `portfolio_id`, and `environment_id`.
- The runtime boundary no longer defines inline file-backed session helpers. It imports `load_runtime_state`, `read_last_trade_log_row`, `next_trade_seq`, `append_trade_log_row`, and `save_runtime_state` from `src/moex_runtime/state_store/file_backed_runtime_session_store.py`.
- Apply commit `d8163684c192fec5cdb02cc608b5f9d2f0a326eb` shows a one-for-one structural move only:
  - inline JSON/CSV persistence helpers removed from the runtime boundary
  - equivalent helpers added under `src/moex_runtime/state_store/file_backed_runtime_session_store.py`
  - call sites rewired to the extracted surface
- Comparing `d8163684c192fec5cdb02cc608b5f9d2f0a326eb..main` shows current `main` is ahead only by the report file `docs/sot/REFERENCE_VERTICAL_SLICE_EMA_3_19_15M_PHASE_6_FIRST_SHARED_RUNTIME_INFRA_APPLY_REPORT.md`. No later code drift is present in the validated runtime path.

## 3. id-only startup path validation

- `run_registered_runtime_boundary(*, strategy_id: str, portfolio_id: str, environment_id: str)` still accepts ids only.
- The first step inside the function is still `load_registered_runtime_boundary(strategy_id=strategy_id, portfolio_id=portfolio_id, environment_id=environment_id)`.
- `src/moex_core/contracts/registry_loader.py` still resolves the runtime boundary entirely from those ids and validates:
  - active strategy registry record
  - active live-enabled portfolio record with exactly the requested strategy id enabled
  - active live-enabled runtime-boundary environment record
  - required runtime env vars declared by the environment record
- The validated registered path remains exactly:
  - `strategy_id = ema_3_19_15m`
  - `portfolio_id = reference_ema_3_19_15m_single`
  - `environment_id = reference_runtime_boundary`
- `configs/environments/reference_runtime_boundary.json` still declares:
  - `mode = runtime_boundary`
  - `is_backtest = false`
  - `is_live = true`
  - `artifact_root_refs = ["MOEX_BOT_ARTIFACT_ROOT"]`
  - `required_env_vars = ["MOEX_BOT_ARTIFACT_ROOT"]`
  - `broker_adapter_ref = null`
  - `market_data_adapter_ref = null`

## 4. artifact contract drift check

- `src/strategies/ema_3_19_15m/artifact_contracts.py` still declares the same runtime artifacts:
  - `ema_3_19_15m_signal_state`
  - `ema_3_19_15m_trade_log`
- Their locator refs remain unchanged:
  - `data/state/ema_3_19_15m_signal_state_{trade_date}.json`
  - `data/signals/ema_3_19_15m_realtime_{trade_date}.csv`
- `run_registered_runtime_boundary(...)` still resolves `state_path` and `trade_log_path` from the same artifact contracts and the same `trade_date` derived from the latest finalized bar end.
- The return payload still exposes the same artifact path outputs:
  - `dataset_path`
  - `state_path`
  - `trade_log_path`
- No registry/config file in the id-only path changed after the code apply commit.

## 5. behavior-preservation check

- The extracted helper functions preserve the removed inline behavior for:
  - runtime state load
  - runtime state atomic save via temp-file replace
  - last trade-log row read
  - next sequence allocation
  - trade-log append with header-on-create behavior
- The runtime boundary still performs the same caller-owned logic for:
  - dataset path resolution
  - feature materialization
  - signal generation
  - current-position reconciliation from runtime state / last trade-log row
  - live decision generation
  - action derivation
  - updated state assembly
  - returned summary payload
- Current repo code therefore supports a behavior-preservation verdict for the extraction itself.
- Limitation: explicit same-event dedup is not separately evidenced in repo code. The trade-log append surface is a plain append and is only validated here as preserved current behavior, not as a newly proven replay guard.

## 6. what is validated vs what is only reported

Validated from repo:
- the current registered runtime-boundary path remains structurally contract-correct
- id-only startup semantics remain intact
- artifact ids, locator refs, and trade-date partitioning did not drift
- the state-store extraction remained narrow and platform-local
- current `main` did not introduce post-apply code drift into the validated path
- the environment record for this path contains no broker adapter or market-data adapter refs

Only reported, not repo-proven:
- that a fresh post-extraction end-to-end execution was performed
- that runtime artifacts were actually materialized on disk after extraction
- that such a run completed with no broker/network side effects in practice
- that replay/idempotency behavior was exercised by runtime execution rather than inferred from unchanged code shape

## 7. blockers if any

None for repo-first validation.

There is one proof boundary, not a repo blocker:
- real runtime execution evidence after extraction is not present as repo-native proof and therefore cannot be upgraded from reported claim to validated fact in this report

## 8. one exact next narrow step

Add one repo-level contract/integration test for `run_registered_runtime_boundary(strategy_id="ema_3_19_15m", portfolio_id="reference_ema_3_19_15m_single", environment_id="reference_runtime_boundary")` against fixed local artifacts, asserting:
- id-only registry resolution
- unchanged artifact locator resolution
- atomic state persistence surface
- no duplicate trade-log row on deterministic replay
