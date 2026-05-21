# futures_universal_daily_refresh_manifest_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: json
schema_version: futures_universal_daily_refresh_manifest.v1

purpose: Canonical universal futures daily refresh manifest for all-universe eligibility-driven orchestration. This contract replaces Slice 1 whitelist semantics as the canonical daily refresh scope while preserving the legacy Slice 1 runner as compatibility-only.

producer: src/moex_data/futures/universal_daily_refresh_runner.py
consumer:
- futures_data_lake_pm_review
- futures_daily_refresh_quality_consumer
- futures_daily_refresh_scheduler

path_pattern: ${MOEX_DATA_ROOT}/futures/runs/universal_daily_refresh/run_date={run_date}/manifest.json
primary_key:
- run_id

required_fields:
- schema_version
- run_id
- run_date
- snapshot_date
- refresh_from
- refresh_till
- started_ts
- completed_ts
- canonical_stage_order
- executed_stage_order
- debug_controls
- selection_model
- slice1_whitelist_semantics
- prerequisite_artifacts
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- child_component_status
- artifact_validation_status
- universal_daily_refresh_result_verdict
- blockers
- output_artifacts

canonical_stage_order:
1. registry_refresh
2. all_universe_eligibility_snapshot
3. raw_5m_refresh
4. futoi_raw_refresh
5. raw_d1_derivation
6. continuous_eligibility_refinement
7. expiration_map
8. roll_map
9. continuous_5m
10. continuous_d1
11. continuous_w1
12. quality_reports
13. unified_manifest

selection_policy:
- selection_model must equal eligibility_snapshot_driven.
- Slice 1 whitelist semantics are forbidden as canonical daily refresh scope.
- The runner must not use DEFAULT_WHITELIST or SHORT_HISTORY_ALLOWED from slice1_common as canonical universe controls.
- Stage-specific narrowing by --family or --secid is debug/orchestration-only and must not redefine included/deferred/excluded semantics.
- all-universe eligibility snapshot is the canonical source for universe status.

allowed_debug_controls:
- --reuse-prerequisites
- --stop-after <stage>
- --stage <stage>
- --family <family_code>
- --secid <secid>
- --from YYYY-MM-DD
- --till YYYY-MM-DD

debug_control_policy:
- debug_controls.semantics_effect must equal orchestration_only_no_universe_or_eligibility_redefinition.
- Debug controls must not change universe semantics.
- Debug controls must not change eligibility semantics.
- Debug controls must not change schema.
- Debug controls must not change roll policy.
- Debug controls must not change adjustment policy.
- Debug controls must not change quality rules.
- Debug controls must not change included/deferred/excluded rules.

validation_rules:
- schema_version must equal futures_universal_daily_refresh_manifest.v1.
- canonical_stage_order must exactly match this contract.
- executed_stage_order must be a prefix of canonical_stage_order unless --stage is used.
- --stage execution must still report the full canonical_stage_order and debug_controls.stage.
- roll_policy_id must equal expiration_minus_1_trading_session_v1.
- adjustment_policy_id must equal unadjusted_v1.
- adjustment_factor must equal 1.0.
- universal_daily_refresh_result_verdict=pass is allowed only when all executed child components pass and artifact_validation_status=pass.
- If any required canonical all-universe stage is unavailable, the runner must fail closed and write blocker details in the manifest.
- Missing canonical all-universe FUTOI producer must be reported as canonical_all_universe_futoi_raw_producer_missing; the runner must not silently call the Slice 1 whitelist FUTOI loader as replacement.
- Missing canonical family discovery for W1 must be reported as blocker; the runner must not silently infer families from ad hoc latest-file discovery.
- Previous valid artifacts must remain available if the current run fails.
- No global cleanup is allowed.

legacy_boundary:
- src/moex_data/futures/daily_refresh_runner.py remains compatibility-only.
- The scheduler canonical entrypoint must point to moex_data.futures.universal_daily_refresh_runner.
- Legacy futures_daily_data_refresh_manifest.v1 may remain for compatibility but must not define the future canonical universe scope.

forbidden_scope:
- no strategy changes
- no research result generation
- no runtime trading logic changes
- no FUTOI pre-join into OHLCV
- no materialized 15m, 30m, 1h, or 4h outputs
- no change to roll_policy_id=expiration_minus_1_trading_session_v1
- no change to adjustment_policy_id=unadjusted_v1
- no server-first code edits

blocking_conditions:
- canonical all-universe raw producer missing
- canonical all-universe FUTOI producer missing
- eligibility snapshot missing or invalid
- required stage artifact missing or invalid
- child component return code non-zero
- stage order mismatch
- Slice 1 whitelist semantics used as canonical scope
- debug controls change universe or eligibility semantics
- scheduler contract points canonical daily refresh to legacy runner
