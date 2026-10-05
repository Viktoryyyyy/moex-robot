# futures_raw_5m_contract

status: legacy_runtime_compatibility
project: MOEX Bot
architecture_proof_allowed: false
new_legacy_writes_authorized: false
canonical_replacement: contracts/datasets/futures_raw_5m.v1.yaml
canonical_ingestion_runbook: docs/data/futures_data_ingestion_runbook.md
artifact_class: external_pattern
format: parquet
schema_version: futures_raw_5m.v1

purpose: Compatibility-only path contract for legacy continuous/D1 readers that still consume the retired Slice 1 raw layout. It is not the canonical ingestion contract and must not be used for new loading or onboarding.
producer: src/moex_data/futures/raw_5m_loader.py
consumer:
- legacy_continuous_series_builder
- legacy_continuous_quality_report
- legacy_derived_d1_ohlcv_builder

path_pattern: ${MOEX_DATA_ROOT}/futures/raw_5m/trade_date={trade_date}/family={family_code}/secid={secid}/part.parquet
partitioning:
- trade_date
- family_code
- secid
primary_key:
- trade_date
- ts
- secid

required_fields:
- trade_date
- ts
- end
- session_date
- board
- secid
- family_code
- open
- high
- low
- close
- volume
- source
- ingest_ts
- schema_version
- short_history_flag
- calendar_denominator_status

nullable_fields:
- value
- num_trades
- source_endpoint_url
- source_seqnum

status_fields:
- short_history_flag
- calendar_denominator_status

compatibility_rules:
- canonical ingestion remains contracts/datasets/futures_raw_5m.v1.yaml under ${MOEX_DATA_ROOT}/market.
- this file exists only so legacy readers can resolve already-existing historical compatibility data.
- no new instrument onboarding, canonical materialization, accepted pointer, scheduler enablement, or research readiness may be inferred from this file.
- migration of legacy readers to canonical accepted datasets is a separate workstream.

date_source_read_compatibility:
- D1/continuous readers may consume canonical_apim_futures_xml or authoritative_observed_algopack_tradestats only after retained admission metadata validation described in docs/FUTURES_D1_DATE_PROVENANCE_COMPATIBILITY_V1.md.
- A label alone is insufficient. Exact instrument/family/date/partition identity, source endpoint, raw quality, matching producer manifest and chronology are required. source_endpoint_url is mandatory for admission even though older artifacts may store null.
- The old raw_5m_loader and all_universe_raw_5m_backfill evidence layouts retain their original meanings; this extension does not authorize new legacy ingestion.
- Mixed histories across dates are valid; contradictory labels within one secid/trade_date are rejected. Observed dates never establish a complete exchange calendar.
- Missing retained evidence is a blocker. Readers do not repair labels, rewrite raw, fabricate receipts or fetch replacement history.

recovery_versioning:
- Task futures_daily_history_expiry_recovery_v1 explicitly authorizes targeted recovery through the existing producer for the full existing scheduler scope; this does not authorize a new collector, universe expansion or canonical accepted pointers.
- Before replacing any current raw partition, the producer preserves both previous and new exact bytes in futures/raw_5m_admission/objects/{sha256}, with immutable logical-path/version records. A failed publication must retain the previous version.
- Successful producer cohorts retain their complete partition version mapping and immutable quality/manifest pair. Both current aliases and retained witnesses must match the exact recorded raw version; matching timestamps and counts alone are insufficient.
- Old unversioned witnesses retain strict full-cohort checks. Lost historical versions cannot be reconstructed by accepting a mixture of current partitions. Such history requires explicit new source acquisition and admission, with originals retained.
- Reacquisition records its actual ingest timestamp and observed date-source status. Current publication receipts do not establish historical point-in-time availability, completeness of the exchange calendar, or a forecast evaluation right.
