# futures_continuous_w1_quality_report_contract

status: design_contract
project: MOEX Bot
artifact_class: external_pattern
format: json
schema_version: futures_continuous_w1_quality_report.v1

purpose: Per-run quality and lineage validation report for futures_continuous_w1.v1 outputs.

producer: src/moex_data/futures/continuous_w1_builder.py
consumer:
- futures_data_lake_pm_review
- later_daily_refresh_quality_gate

path_pattern: ${MOEX_DATA_ROOT}/futures/quality/continuous_w1_builder/run_date={run_date}/quality_report.json
partitioning:
- run_date
primary_key:
- run_id
- family_code

required_fields:
- schema_version
- run_id
- run_date
- family_code
- started_at
- finished_at
- source_artifact_contract
- output_artifact_contract
- input_d1_partitions
- output_w1_partitions
- row_counts
- checks
- quality_report_status
- blockers

required_checks:
- source_is_d1_only
- w1_row_count
- d1_row_count
- w1_primary_key_unique
- w1_lineage_completeness
- no_raw_5m_read
- no_futoi_join
- no_materialized_intraday_timeframes

validation_rules:
- schema_version must equal futures_continuous_w1_quality_report.v1.
- source_artifact_contract must equal futures_continuous_d1.v1.
- output_artifact_contract must equal futures_continuous_w1.v1.
- quality_report_status allowed values are pass and fail.
- all required_checks must be present.
- any failed required check blocks quality_report_status=pass.

blocking_conditions:
- quality report missing after builder run.
- any required check missing.
- any required check_status=fail.
- source_artifact_contract not futures_continuous_d1.v1.
- output_artifact_contract not futures_continuous_w1.v1.
