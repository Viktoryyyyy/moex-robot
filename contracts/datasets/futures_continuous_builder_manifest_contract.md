# futures_continuous_builder_manifest_contract

status: design_contract
project: MOEX Bot
artifact_class: external_pattern
format: json
schema_version: futures_continuous_builder_manifest.v1

purpose: Run manifest for continuous futures v1 roll-map, 5m, and D1 build steps.

producer: src/moex_data/futures/continuous_series_builder.py
consumer:
- futures_daily_data_refresh_manifest
- futures_data_lake_pm_review
- futures_continuous_quality_report_consumer

path_pattern: ${MOEX_DATA_ROOT}/futures/runs/continuous_series_builder/run_date={run_date}/manifest.json
primary_key:
- run_id

required_fields:
- schema_version
- run_id
- run_date
- snapshot_date
- started_ts
- completed_ts
- builder_whitelist_applied
- excluded_instruments_confirmed
- roll_policy_id
- adjustment_policy_id
- calendar_status
- input_artifacts
- output_artifacts
- roll_map_artifact
- continuous_5m_partitions_created
- continuous_d1_partitions_created
- family_summaries
- roll_boundary_summary
- partial_chain_gap_summary
- usdrubf_identity_check
- source_lineage_check
- quality_status_counts
- builder_result_verdict
- blockers

nullable_fields:
- none

status_fields:
- builder_result_verdict
- calendar_status
- usdrubf_identity_check.status
- source_lineage_check.status
- partial_chain_gap_summary.status

validation_rules:
- schema_version must equal futures_continuous_builder_manifest.v1.
- builder_whitelist_applied must equal SiM6, SiU6, SiU7, SiZ6, USDRUBF for Slice 1 continuous v1.
- excluded_instruments_confirmed must include SiH7 and SiM7.
- roll_policy_id must equal expiration_minus_1_trading_session_v1.
- adjustment_policy_id must equal unadjusted_v1.
- calendar_status must equal canonical_apim_futures_xml.
- input_artifacts must reference normalized registry, expiration map, futures_raw_5m, and futures_derived_d1_ohlcv as applicable.
- roll_map_artifact must reference futures_continuous_roll_map.v1.
- output_artifacts must include roll_map, continuous_5m_partition_root, continuous_d1_partition_root, quality_report, and manifest.
- builder_result_verdict must be pass only when quality_status_counts.fail is absent or zero.
- family_summaries must include Si and USDRUBF.
- partial_chain_gap_summary must explicitly list Slice 1 chain gaps caused by excluded SiH7 or SiM7 when applicable.
- usdrubf_identity_check.status must equal pass for pass verdict.
- source_lineage_check.status must equal pass for pass verdict.
- no continuous_5m_partitions_created or continuous_d1_partitions_created path may include secid=SiH7 or secid=SiM7.
- if any required input artifact is missing or stale, the builder must fail closed and must not publish pass manifest.

blocking_conditions:
- manifest missing after builder run.
- missing required input artifact.
- roll map missing or invalid.
- unresolved decision_source for buildable ordinary row.
- invalid calendar_status.
- missing raw source partition.
- ambiguous active source contract.
- overlapping roll-map windows.
- missing next contract not represented as explicit partial-chain gap.
- USDRUBF identity validation fails.
- adjustment_factor not equal to 1.0.
- unexpected included instrument.
- excluded instrument included.
- source lineage completeness fails.
