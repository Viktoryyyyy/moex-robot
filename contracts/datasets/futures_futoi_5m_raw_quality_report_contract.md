# futures_futoi_5m_raw_quality_report_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_futoi_5m_raw_quality_report.v1

purpose: Per-run quality report for Slice 1 FUTOI raw loader outputs.
producer: src/moex_data/futures/futoi_raw_loader.py
consumer:
- futures_data_lake_pm_review
- futures_futoi_5m_raw_loader_manifest
- later_daily_refresh_quality_gate

path_pattern: ${MOEX_DATA_ROOT}/futures/quality/futoi_raw_loader/run_date={run_date}/futures_futoi_5m_raw_quality_report.parquet
primary_key:
- quality_report_id
- run_id
- secid

required_fields:
- quality_report_id
- run_id
- run_date
- snapshot_date
- board
- secid
- family_code
- source_ticker
- source_scope
- dataset_id
- schema_version
- requested_from
- requested_till
- source_endpoint_url
- fetch_status
- rows
- trade_dates
- min_ts
- max_ts
- clgroups_json
- duplicate_key_count
- null_required_count
- invalid_position_count
- off_calendar_date_count
- missing_expected_trading_days
- partition_count
- calendar_denominator_status
- futoi_availability_status
- futoi_probe_status
- history_depth_status
- liquidity_status
- short_history_flag
- data_gap_status
- quality_status
- review_notes
- mapped_columns_json
- observed_columns_json

nullable_fields:
- source_endpoint_url
- fetch_error
- normalization_error
- min_ts
- max_ts
- missing_expected_trading_days

status_fields:
- fetch_status
- quality_status
- data_gap_status
- calendar_denominator_status
- futoi_availability_status
- futoi_probe_status
- history_depth_status
- liquidity_status
- short_history_flag

validation_rules:
- schema_version must equal futures_futoi_5m_raw_quality_report.v1.
- dataset_id must equal futures_futoi_5m_raw.
- one row must exist for each accepted Slice 1 whitelist instrument.
- quality_status must be pass for operational closeout.
- fetch_status must be completed for operational closeout.
- futoi_availability_status must be available for operational closeout.
- calendar_denominator_status must equal canonical_apim_futures_xml.
- duplicate_key_count, null_required_count, invalid_position_count, and off_calendar_date_count must equal zero.
- rows and partition_count must be greater than zero for each accepted whitelist instrument.
- SiU7 must have short_history_flag=true.
- SiM6, SiU6, SiZ6, and USDRUBF must have short_history_flag=false.
- SiH7 and SiM7 must not appear in secid.
- data_gap_status must report whether calendar gaps were detected, but calendar gaps are reported as data availability diagnostics rather than silently ignored.

blocking_conditions:
- quality report missing after loader run.
- missing accepted whitelist instrument.
- quality_status fail or review_required for any accepted whitelist instrument.
- excluded instrument appears in report.
- APIM futures calendar validation not canonical.
- invalid position values, duplicate primary-key rows, null required values, or off-calendar dates detected.
