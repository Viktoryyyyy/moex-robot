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
