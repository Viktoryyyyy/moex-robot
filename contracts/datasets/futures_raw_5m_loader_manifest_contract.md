# futures_raw_5m_loader_manifest_contract

status: legacy_runtime_compatibility
project: MOEX Bot
architecture_proof_allowed: false
new_legacy_writes_authorized: false
canonical_replacement: contracts/datasets/futures_data_refresh_manifest.v1.yaml
canonical_ingestion_runbook: docs/data/futures_data_ingestion_runbook.md
artifact_class: external_pattern
format: json
schema_version: futures_raw_5m_loader_manifest.v1

purpose: Compatibility-only manifest schema for legacy raw_5m_loader and legacy daily-refresh code. It is not the canonical Stage 2 refresh manifest contract.
producer: src/moex_data/futures/raw_5m_loader.py
consumer:
- legacy_daily_refresh_runner
- legacy_raw_5m_quality_consumers

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

compatibility_rules:
- canonical Stage 2 ingestion contracts live in YAML and are referenced from configs/datasets/futures_data_lake.v1.yaml.
- this file exists only to avoid breaking legacy runtime prerequisite checks while scheduler and research remain blocked.
- it does not authorize canonical ingestion, accepted pointer creation, scheduler enablement, or new legacy-root onboarding.
