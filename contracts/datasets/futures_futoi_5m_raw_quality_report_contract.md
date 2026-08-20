# futures_futoi_5m_raw_quality_report_contract

status: legacy_runtime_compatibility
project: MOEX Bot
architecture_proof_allowed: false
new_legacy_writes_authorized: false
canonical_dataset_replacement: contracts/datasets/futures_futoi_raw.v1.yaml
canonical_quality_replacement: contracts/datasets/futures_futoi_quality_report.v1.yaml
canonical_ingestion_runbook: docs/data/futures_data_ingestion_runbook.md
artifact_class: external_pattern
format: parquet
schema_version: futures_futoi_5m_raw_quality_report.v1

purpose: Compatibility-only quality schema for legacy FUTOI loaders. Canonical Stage 2 FUTOI quality is defined by the YAML replacement contract.
producer: src/moex_data/futures/futoi_raw_loader.py
consumer:
- legacy_futoi_raw_backfill_expansion
- legacy_daily_refresh_quality_gate

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
- fetch_status
- rows
- trade_dates
- duplicate_key_count
- null_required_count
- invalid_position_count
- partition_count
- calendar_denominator_status
- quality_status

compatibility_rules:
- this file is not canonical ingestion Source of Truth.
- canonical FUTOI raw identity and quality rules are defined by contracts/datasets/futures_futoi_raw.v1.yaml and contracts/datasets/futures_futoi_quality_report.v1.yaml.
- public ISS transport or fallback is forbidden.
- this file does not authorize new legacy-root materialization, accepted pointer creation, scheduler enablement, or research readiness.
