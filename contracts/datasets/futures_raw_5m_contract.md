# futures_raw_5m_contract — DEPRECATED

status: deprecated_compatibility_tombstone
replacement_contract: `contracts/datasets/futures_raw_5m.v1.yaml`
canonical_source_contract: `contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml`
canonical_runbook: `docs/data/moex_market_data_ingestion_runbook.v1.md`

This historical Slice1 contract is not architecture proof and must not be used for new loading, backfill, field definitions, storage paths, quality gates, or instrument selection.

The former `${MOEX_DATA_ROOT}/futures/raw_5m/...` layout and family/SECID storage identity are deprecated. Canonical ingestion uses the replacement contract and `${MOEX_DATA_ROOT}/market/raw/...`.
