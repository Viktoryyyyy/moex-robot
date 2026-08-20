# futures_futoi_5m_raw_contract — DEPRECATED

status: deprecated_compatibility_tombstone
replacement_contract: `contracts/datasets/futures_futoi_raw.v1.yaml`
canonical_source_contract: `contracts/sources/futures/moex_algopack_futoi.v1.yaml`
canonical_runbook: `docs/data/moex_market_data_ingestion_runbook.v1.md`

This historical Slice1 FUTOI contract is not architecture proof and must not be used for loading, backfill, field definitions, storage paths, timestamp semantics, duplicate keys or instrument selection.

The former `${MOEX_DATA_ROOT}/futures/futoi_raw/...` layout, family storage identity, nullable `sess_id`/`seqnum`, and `trade_date+ts+secid+clgroup` raw key are deprecated. Current raw identity and timestamp semantics are defined only by the replacement contract.
