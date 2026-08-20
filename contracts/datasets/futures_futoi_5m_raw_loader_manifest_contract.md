# futures_futoi_5m_raw_loader_manifest_contract — DEPRECATED

status: deprecated_compatibility_tombstone
canonical_dataset_contract: `contracts/datasets/futures_futoi_raw.v1.yaml`
canonical_refresh_manifest_contract: `contracts/datasets/futures_futoi_refresh_manifest.v1.yaml`
canonical_runbook: `docs/data/moex_market_data_ingestion_runbook.v1.md`

This historical Slice1 loader-manifest contract must not be used to infer current FUTOI paths, whitelist logic, metadata, timestamp semantics or loader behavior. Use the canonical contracts and current materializer/backfill modules referenced by the runbook.
