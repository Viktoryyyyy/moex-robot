# futures_derived_d1_ohlcv_manifest_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: json
schema_version: futures_derived_d1_ohlcv_manifest.v1

purpose: Run manifest for Slice 1 derived D1 OHLCV builder execution, including applied whitelist, excluded instruments, input raw 5m partitions, output D1 artifacts, per-instrument summaries, and source-to-output row checks.
producer: src/moex_data/futures/derived_d1_ohlcv_builder.py
consumer:
- futures_data_lake_pm_review
- futures_derived_d1_ohlcv_quality_report_consumer
- later_daily_refresh_runner

path_pattern: ${MOEX_DATA_ROOT}/futures/runs/derived_d1_ohlcv_builder/run_date={run_date}/manifest.json
primary_key:
- run_id

required_fields:
- schema_version
- run_id
- run_date
- ingest_ts
- builder_whitelist_applied
- excluded_instruments_confirmed
- input_artifacts
- output_artifacts
- partition_paths_created
- instrument_summaries
- quality_status_counts
- source_to_output_row_check
- short_history_handling
- calendar_validation_summary
- builder_result_verdict

nullable_fields:
- none

status_fields:
- builder_result_verdict
- quality_status_counts
- calendar_validation_summary.calendar_denominator_status

validation_rules:
- schema_version must equal futures_derived_d1_ohlcv_manifest.v1.
- builder_whitelist_applied must equal SiM6, SiU6, SiU7, SiZ6, USDRUBF for accepted Slice 1 closeout.
- excluded_instruments_confirmed must include SiH7 and SiM7.
- builder_result_verdict must be pass only when quality_status_counts.fail is absent or zero.
- calendar_validation_summary.calendar_denominator_status must equal canonical_apim_futures_xml.
- input_artifacts.raw_5m_partition_root must reference the futures_raw_5m external pattern.
- partition_paths_created must contain no path with secid=SiH7 or secid=SiM7.
- short_history_handling must include SiU7 and its per-instrument short_history_flag must be true.
- output_artifacts must include derived_d1_partition_root, quality_report, and manifest.
- source_to_output_row_check.missing_d1_row_count must equal zero.

blocking_conditions:
- manifest missing after builder run.
- builder_result_verdict is fail.
- missing accepted whitelist instrument from instrument_summaries.
- excluded instrument appears in partition_paths_created.
- APIM futures calendar validation not canonical in source rows.
- source_to_output_row_check shows missing D1 rows.
- SiU7 short_history_flag not true in short_history_handling.
