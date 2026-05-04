# futures_continuous_quality_report_contract

status: design_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_continuous_quality_report.v1

purpose: Per-run quality and roll-boundary validation report for continuous futures v1 outputs.

producer: src/moex_data/futures/continuous_series_builder.py
consumer:
- futures_continuous_builder_manifest
- futures_data_lake_pm_review
- later_daily_refresh_quality_gate

path_pattern: ${MOEX_DATA_ROOT}/futures/quality/continuous_series_builder/run_date={run_date}/futures_continuous_quality_report.parquet
partitioning:
- run_date
primary_key:
- quality_report_id
- run_id
- family_code
- check_id

required_fields:
- quality_report_id
- run_id
- run_date
- snapshot_date
- family_code
- continuous_symbol
- check_id
- dataset_id
- schema_version
- roll_policy_id
- adjustment_policy_id
- calendar_status
- check_status
- affected_source_secid
- affected_trade_date
- observed_value
- expected_value
- review_notes

nullable_fields:
- affected_source_secid
- affected_trade_date
- observed_value
- expected_value
- review_notes

status_fields:
- check_status
- calendar_status

required_checks:
- missing_normalized_registry
- missing_expiration_anchor
- unresolved_decision_source
- invalid_calendar_status
- duplicate_timestamps
- missing_raw_source_partitions
- ambiguous_active_source_contract
- overlapping_roll_map_windows
- missing_next_contract
- explicit_partial_chain_gap_for_excluded_SiH7_SiM7
- usdrubf_identity_validation
- adjustment_factor_not_1
- unexpected_included_instruments
- excluded_instruments_included
- continuous_output_row_source_lineage_completeness

validation_rules:
- schema_version must equal futures_continuous_quality_report.v1.
- dataset_id must be one of futures_continuous_roll_map, futures_continuous_5m, futures_continuous_d1, futures_continuous_series_v1.
- roll_policy_id must equal expiration_minus_1_trading_session_v1.
- adjustment_policy_id must equal unadjusted_v1.
- calendar_status must equal canonical_apim_futures_xml for pass rows.
- every required check must be represented for each relevant run/family, either as pass, fail, not_applicable, or explicit_gap.
- check_status allowed values are pass, fail, not_applicable, explicit_gap.
- any fail check_status blocks builder_result_verdict=pass.
- explicit_partial_chain_gap_for_excluded_SiH7_SiM7 must be explicit_gap or pass, never omitted, when family_code=Si.
- usdrubf_identity_validation must be pass for family_code=USDRUBF.
- continuous_output_row_source_lineage_completeness must be pass for all published output rows.
- adjustment_factor_not_1 must be pass, meaning no row has adjustment_factor other than 1.0.
- excluded_instruments_included must be pass, meaning SiH7 and SiM7 are absent from outputs.

blocking_conditions:
- quality report missing after builder run.
- any required check missing.
- any required check_status=fail.
- calendar_status not canonical_apim_futures_xml.
- USDRUBF identity validation not pass.
- source lineage completeness not pass.
- excluded instrument included.
