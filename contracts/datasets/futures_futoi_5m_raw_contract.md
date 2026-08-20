# futures_futoi_5m_raw_contract

status: legacy_research_compatibility_pending_migration
project: MOEX Bot
architecture_proof_allowed: false
new_legacy_writes_authorized: false
canonical_replacement: contracts/datasets/futures_futoi_raw.v1.yaml
canonical_quality_replacement: contracts/datasets/futures_futoi_quality_report.v1.yaml
canonical_manifest_replacement: contracts/datasets/futures_futoi_refresh_manifest.v1.yaml
canonical_ingestion_runbook: docs/data/futures_data_ingestion_runbook.md

purpose: Compatibility-only reference retained for the pending phase8.7A FUTOI experiment contract. It is not the canonical FUTOI dataset contract and must be migrated to the canonical YAML contract before that experiment is implemented or executed.

legacy_consumer:
- contracts/experiments/usdrubf_phase8_7a_futoi_si_source_and_feature_contract_v1.json

legacy_path_pattern: ${MOEX_DATA_ROOT}/futures/futoi_raw/trade_date={trade_date}/family={family_code}/secid={secid}/part.parquet

canonical_requirements:
- canonical dataset identity and storage are defined only by contracts/datasets/futures_futoi_raw.v1.yaml.
- canonical FUTOI transport is authenticated APIM only; public ISS and public ISS fallback are forbidden.
- canonical raw source-record identity preserves trade_date, sess_id, seqnum, secid, and clgroup.
- systime is publication/archive metadata and is not the canonical historical raw event timestamp.
- this compatibility file authorizes no materialization, accepted pointer, scheduler, D1/W1 derivation, or research consumption.
- phase8.7A implementation is blocked until its experiment contract is explicitly migrated to the canonical FUTOI dataset contract.
