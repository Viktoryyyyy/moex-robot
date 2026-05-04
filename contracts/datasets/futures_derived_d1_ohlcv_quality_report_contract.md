# futures_derived_d1_ohlcv_quality_report_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: parquet
schema_version: futures_derived_d1_ohlcv_quality_report.v1

purpose: Quality report for Slice 1 derived D1 OHLCV builder, validating one-row-per-secid/trade_date output from futures_raw_5m and preserving short-history handling.
producer: src/moex_data/futures/derived_d1_ohlcv_builder.py
consumer:
- futures_data_lake_pm_review
- later_daily_refresh_runner
- later_data_quality_dashboard

path_pattern: ${MOEX_DATA_ROOT}/futures/quality/derived_d1_ohlcv_builder/run_date={run_date}/futures_derived_d1_ohlcv_quality_report.parquet
primary_key:
- quality_report_id

required_fields:
- quality_report_id
- run_id
- run_date
- secid
- dataset_id
- schema_version
- quality_status
- review_notes
- short_history_flag
- raw_5m_rows
- raw_trade_dates
- d1_rows
- partition_count
- calendar_denominator_status

nullable_fields:
- none

status_fields:
- quality_status
- short_history_flag
- calendar_denominator_status

validation_rules:
- schema_version must equal futures_derived_d1_ohlcv_quality_report.v1.
- dataset_id must equal futures_derived_d1_ohlcv.
- one report row must exist for each accepted whitelist instrument.
- quality_status must be pass for accepted closeout.
- calendar_denominator_status must equal canonical_apim_futures_xml.
- d1_rows must equal raw_trade_dates for each secid.
- partition_count must equal d1_rows for each secid.
- raw_5m_rows must be greater than zero for each accepted whitelist instrument.
- SiU7 must have short_history_flag=true.
- all accepted instruments except SiU7 must have short_history_flag=false.
- no report row may exist for excluded instruments SiH7 or SiM7.

blocking_conditions:
- missing report.
- missing accepted whitelist instrument.
- quality_status other than pass.
- d1_rows does not equal raw_trade_dates.
- partition_count does not equal d1_rows.
- non-canonical calendar_denominator_status.
- report row exists for excluded instruments SiH7 or SiM7.
