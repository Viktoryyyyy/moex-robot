# futures_futoi_5m_raw_loader_manifest_contract

status: legacy_runtime_compatibility
project: MOEX Bot
architecture_proof_allowed: false
new_legacy_writes_authorized: false
canonical_dataset_replacement: contracts/datasets/futures_futoi_raw.v1.yaml
canonical_manifest_replacement: contracts/datasets/futures_futoi_refresh_manifest.v1.yaml
canonical_ingestion_runbook: docs/data/futures_data_ingestion_runbook.md
artifact_class: external_pattern
format: json
schema_version: futures_futoi_5m_raw_loader_manifest.v1

purpose: Compatibility-only manifest schema for legacy futoi_raw_loader, expansion, and daily-refresh code. Canonical FUTOI Stage 2 uses authenticated APIM and the YAML contracts above.
producer: src/moex_data/futures/futoi_raw_loader.py
consumer:
- legacy_daily_refresh_runner
- legacy_futoi_raw_backfill_expansion

path_pattern: ${MOEX_DATA_ROOT}/futures/runs/futoi_raw_loader/run_date={run_date}/manifest.json
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
- futoi_source_scope_note
- short_history_handling
- loader_result_verdict

compatibility_rules:
- this file is not canonical ingestion Source of Truth.
- canonical FUTOI transport is authenticated APIM only; public ISS and public ISS fallback remain forbidden.
- this file does not authorize new legacy-root writes, scheduler enablement, accepted pointer creation, or research readiness.
- legacy runner migration is a separate workstream after canonical raw acceptance.
