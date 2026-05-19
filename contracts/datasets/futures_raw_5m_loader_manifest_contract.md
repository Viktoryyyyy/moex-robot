# futures_raw_5m_loader_manifest_contract

status: implemented_contract
project: MOEX Bot
artifact_class: external_pattern
format: json
schema_version: futures_raw_5m_loader_manifest.v1

purpose: Run manifest for raw 5m futures loader execution, including applied whitelist or eligibility-snapshot-driven selection, excluded instruments, input artifacts, output artifacts, partition list, per-instrument summaries, and APIM futures calendar validation.
producer: src/moex_data/futures/raw_5m_loader.py
consumer:
- futures_data_lake_pm_review
- futures_raw_5m_quality_report_consumer
- later_daily_refresh_runner
- futures_all_universe_raw_5m_backfill_slice

path_pattern: ${MOEX_DATA_ROOT}/futures/runs/raw_5m_loader/run_date={run_date}/manifest.json
primary_key:
- run_id

required_fields:
- schema_version
- run_id
- run_date
- snapshot_date
- ingest_ts
- loader_whitelist_applied
- excluded_instruments_confirmed
- input_artifacts
- output_artifacts
- partition_paths_created
- instrument_summaries
- quality_status_counts
- calendar_validation_summary
- short_history_handling
- loader_result_verdict

optional_all_universe_fields:
- eligibility_snapshot_id
- registry_snapshot_id
- chunk_id
- dataset_stage
- backfill_selection_status
- backfill_selection_reason

nullable_fields:
- eligibility_snapshot_id
- registry_snapshot_id
- chunk_id

status_fields:
- loader_result_verdict
- quality_status_counts
- calendar_validation_summary.calendar_denominator_status

validation_rules:
- schema_version must equal futures_raw_5m_loader_manifest.v1.
- loader_whitelist_applied must equal SiM6, SiU6, SiU7, SiZ6, USDRUBF for accepted Slice 1 closeout.
- For PM L3-2 all-universe slices, raw 5m selection may be driven by eligibility_snapshot_id and raw_5m_eligible=true instead of the Slice 1 whitelist.
- The Slice 1 whitelist remains a compatibility baseline, not the all-universe selection rule.
- excluded_instruments_confirmed must include SiH7 and SiM7 for Slice 1 closeout.
- loader_result_verdict must be pass only when quality_status_counts.fail is absent or zero.
- calendar_validation_summary.calendar_denominator_status must equal canonical_apim_futures_xml.
- partition_paths_created must contain no path with secid=SiH7 or secid=SiM7 for Slice 1 compatibility runs.
- short_history_handling must include SiU7 and its per-instrument short_history_flag must be true for Slice 1 compatibility runs.
- input_artifacts must reference normalized registry, liquidity screen, and history-depth screen artifacts for the same snapshot_date when those screens are required by the active slice.
- output_artifacts must include raw_5m_partition_root, quality_report, and manifest.

blocking_conditions:
- manifest missing after loader run.
- loader_result_verdict is fail.
- missing accepted whitelist instrument from instrument_summaries in Slice 1 compatibility runs.
- missing eligibility-selected instrument from instrument_summaries in eligibility-snapshot-driven runs.
- excluded instrument appears in partition_paths_created.
- APIM futures calendar validation not canonical.
- SiU7 short_history_flag not true in short_history_handling for Slice 1 compatibility runs.
