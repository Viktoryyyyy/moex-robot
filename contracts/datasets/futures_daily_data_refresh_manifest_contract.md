# futures_daily_data_refresh_manifest_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: json
schema_version: futures_daily_data_refresh_manifest.v1

purpose: Top-level daily data refresh run manifest that orchestrates registry refresh, raw 5m futures loader, FUTOI raw loader, derived D1 OHLCV builder, and Continuous Futures v1 derived outputs as fail-closed child components while preserving independent raw, FUTOI, raw D1, roll-map, continuous 5m, continuous D1, manifest, and quality storage zones.

producer: src/moex_data/futures/daily_refresh_runner.py
consumer:
- futures_data_lake_pm_review
- futures_daily_refresh_quality_consumer
- later_scheduler_or_cron_wrapper
- later_futures_continuous_builder_integration

path_pattern: ${MOEX_DATA_ROOT}/futures/runs/daily_refresh/run_date={run_date}/manifest.json
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
- runner_whitelist_applied
- excluded_instruments_confirmed
- roll_policy_id
- adjustment_policy_id
- adjustment_factor
- component_execution_order
- child_component_status
- child_manifest_references
- continuous_child_artifact_references
- per_instrument_status
- short_history_flag_check
- excluded_instruments_check
- artifact_validation_status
- daily_refresh_result_verdict
- blockers
- output_artifacts

optional_all_universe_fields:
- eligibility_snapshot_id
- registry_snapshot_id
- chunk_id
- dataset_stage
- backfill_selection_status
- backfill_selection_reason

nullable_fields:
- refresh_from
- refresh_till
- child_manifest_references.expiration_map_builder.manifest_path
- child_manifest_references.continuous_roll_map_builder.manifest_path
- child_manifest_references.continuous_5m_builder.manifest_path
- child_manifest_references.continuous_d1_builder.manifest_path
- eligibility_snapshot_id
- registry_snapshot_id
- chunk_id

status_fields:
- daily_refresh_result_verdict
- artifact_validation_status
- child_component_status.status
- child_component_status.validation_status
- short_history_flag_check.status
- excluded_instruments_check.status

validation_rules:
- schema_version must equal futures_daily_data_refresh_manifest.v1.
- runner_whitelist_applied must equal SiM6, SiU6, SiU7, SiZ6, USDRUBF for accepted Slice 1 closeout.
- For later all-universe daily refresh expansion, selection must be driven by eligibility_snapshot_id and stage-specific eligibility flags rather than hardcoded whitelist logic.
- PM L3-2 first executable slice must not implement or schedule daily unattended refresh.
- The Slice 1 whitelist remains a compatibility baseline, not the all-universe selection rule.
- excluded_instruments_confirmed must include SiH7 and SiM7 for Slice 1 closeout.
- roll_policy_id must equal expiration_minus_1_trading_session_v1.
- adjustment_policy_id must equal unadjusted_v1.
- adjustment_factor must equal 1.0.
- component_execution_order must equal registry_refresh_runner, raw_5m_loader, futoi_raw_loader, derived_d1_ohlcv_builder, expiration_map_builder, continuous_roll_map_builder, continuous_5m_builder, continuous_d1_builder, continuous_builder_manifest, continuous_quality_report for the current daily refresh implementation.
- daily_refresh_result_verdict must be pass only when every child component status is pass, artifact_validation_status is pass, and the continuous quality report has zero fail rows.
- child_manifest_references must include manifest references for registry_refresh_runner, raw_5m_loader, futoi_raw_loader, derived_d1_ohlcv_builder, continuous_builder_manifest, and the continuous_quality_report gate.
- continuous_child_artifact_references must include expiration_map, continuous_roll_map, continuous_5m_root, continuous_d1_root, continuous_builder_manifest, and continuous_quality_report.
- registry_refresh_runner must execute before raw_5m_loader.
- raw_5m_loader and futoi_raw_loader must receive the same snapshot_date that registry_refresh_runner refreshed.
- continuous components must execute only after registry_refresh_runner, raw_5m_loader, futoi_raw_loader, and derived_d1_ohlcv_builder have passed.
- all child output_artifacts and partition_paths_created must exist at validation time where the child manifest contract declares them.
- per_instrument_status must contain only accepted whitelist instruments for raw 5m, FUTOI, and raw D1 components in Slice 1 compatibility runs.
- excluded_instruments_check.status must equal pass.
- short_history_flag_check.status must equal pass and must confirm SiU7 short_history_flag=true across downstream raw/FUTOI/D1 data components in Slice 1 compatibility runs.
- no child manifest or partition path may include secid=SiH7 or secid=SiM7 in downstream loader/builder outputs for Slice 1 compatibility runs.
- continuous roll map, continuous 5m, continuous D1, continuous builder manifest, and continuous quality report must preserve adjustment_policy_id=unadjusted_v1 and adjustment_factor=1.0.
- USDRUBF must remain a perpetual identity in continuous outputs.
- partial Si-chain gaps caused by excluded SiH7 or SiM7 must remain explicit and must not be silently bridged.
- if any child component fails, the runner must fail closed and must not execute later components.

blocking_conditions:
- any required child manifest contract is missing from repo.
- registry_refresh_runner fails or its manifest verdict is not pass.
- raw_5m_loader fails or its manifest verdict is not pass.
- futoi_raw_loader fails or its manifest verdict is not pass.
- derived_d1_ohlcv_builder fails or its manifest verdict is not pass.
- expiration_map_builder fails or does not produce a valid futures_expiration_map.v1 artifact.
- continuous_roll_map_builder fails or does not produce a valid futures_continuous_roll_map.v1 artifact.
- continuous_5m_builder fails or does not produce futures_continuous_5m.v1 partitions.
- continuous_d1_builder fails or does not produce futures_continuous_d1.v1 partitions.
- continuous_builder_manifest fails or its manifest verdict is not pass.
- continuous_quality_report is missing, invalid, or has any check_status=fail row.
- any child manifest is stale relative to the child process execution.
- any accepted whitelist instrument is missing from raw/FUTOI/D1 child instrument_summaries in Slice 1 compatibility runs.
- any eligibility-selected instrument is missing from stage-specific all-universe child outputs in eligibility-snapshot-driven runs.
- any excluded instrument appears in downstream child summaries, partition paths, roll map, or continuous output source fields.
- SiU7 short_history_flag is not true for raw/FUTOI/D1 child components in Slice 1 compatibility runs.
- child output artifacts or created partitions are missing.
- USDRUBF identity validation fails.
- adjustment_factor is not 1.0.
