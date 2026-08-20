# MOEX Bot Data Layer

Start here for market-data work.

## Canonical read order

1. `contracts/architecture/moex_data_access_canon_v1.yaml`
2. `configs/datasets/futures_data_lake.v1.yaml`
3. `docs/data/futures_data_ingestion_runbook.md`
4. dataset contract referenced by the data-lake config
5. source contract referenced by the dataset/data-lake config
6. exact instrument binding from `configs/instruments/forts_instrument_registry.v1.yaml`
7. producer code referenced by the active contract

## Rules

- GitHub/repository is Source of Truth.
- Server is Applied State only.
- Do not infer architecture from server files.
- Do not invent paths, field names, SECIDs, FUTOI tickers, source ids, or CLI arguments.
- `MOEX_DATA_ROOT` is the only external storage-root contract.
- New writes use `${MOEX_DATA_ROOT}/market/...` only.
- FUTOI uses authenticated MOEX APIM only; public `iss.moex.com` is forbidden for FUTOI.
- Accepted pointers, scheduler enablement, and research enablement require their explicit architecture gates.

For loading or backfill execution, follow `docs/data/futures_data_ingestion_runbook.md`.
