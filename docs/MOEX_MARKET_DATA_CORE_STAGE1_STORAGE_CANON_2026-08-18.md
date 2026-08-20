# MOEX Market Data Core — Stage 1 Storage Canon

status: canonical_storage_architecture_applied
updated_at: 2026-08-20
source_of_truth: GitHub repository `Viktoryyyyy/moex-robot`
architecture_contract: `contracts/architecture/moex_data_access_canon_v1.yaml`
canonical_ingestion_runbook: `docs/data/moex_market_data_ingestion_runbook.v1.md`

## Result

The canonical market-data storage architecture is now implemented by the active Stage2 Quotes and FUTOI producers.

Canonical new writes use `${MOEX_DATA_ROOT}/market`:

- Quotes → `${MOEX_DATA_ROOT}/market/raw/...`
- FUTOI → `${MOEX_DATA_ROOT}/market/supplementary/...`

Accepted pointers use `dataset_id + instrument_id`:

`${MOEX_DATA_ROOT}/state/datasets/dataset_id={DATASET_ID}/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json`

Server filesystem remains Applied State only. `MOEX_DATA_ROOT` is an environment contract and no server-specific data-root value is architectural canon.

## Canonical contracts

Quotes:

- `contracts/datasets/futures_raw_5m.v1.yaml`
- `contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml`

FUTOI:

- `contracts/datasets/futures_futoi_raw.v1.yaml`
- `contracts/sources/futures/moex_algopack_futoi.v1.yaml`

Instrument bindings:

- `configs/instruments/forts_instrument_registry.v1.yaml`

Data-lake scope/readiness:

- `configs/datasets/futures_data_lake.v1.yaml`

## Legacy storage

The following are compatibility-only and forbidden for new ingestion:

- `${MOEX_DATA_ROOT}/forts/...`
- `${MOEX_DATA_ROOT}/futures/raw_5m/...`
- `${MOEX_DATA_ROOT}/futures/futoi_raw/...`
- artifact_id/SECID accepted-pointer partitioning
- family as canonical storage identity

Legacy compatibility modules may remain until a separate removal task proves they have no required consumers. Their contracts are not architecture proof.

## Current boundary

Storage architecture and canonical writers are applied. Raw-history validation for Stage2 core lanes is recorded in `configs/datasets/futures_data_lake.v1.yaml` and the Stage2 status note.

Accepted-pointer creation, observed-source refresh, scheduler, D1/W1 and research enablement remain separate later gates.
