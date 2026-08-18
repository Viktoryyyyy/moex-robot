# MOEX Market Data Core — Stage 1 Storage Canon

status: architecture_fixed_migration_pending
task_id: market_data_core_stage1_storage_canon_20260818
source_of_truth: GitHub repository `Viktoryyyyy/moex-robot`
architecture_contract: `contracts/architecture/moex_data_access_canon_v1.yaml`

## Result

Stage 1 fixes one architecture for future MOEX market-data materialization without moving existing server data.

Canonical writes use `${MOEX_DATA_ROOT}/market`. Quotes use the raw lane. FUTOI uses the supplementary lane. FUTOI is never embedded into quote partitions and has its own quality result, manifest and accepted pointer.

New instrument onboarding and new dataset creation must not extend the existing `${MOEX_DATA_ROOT}/forts` or `${MOEX_DATA_ROOT}/futures` storage roots.

## Canonical contracts

Quotes are defined by `contracts/datasets/futures_raw_5m.v1.yaml`.

FUTOI is defined by `contracts/datasets/futures_futoi_raw.v1.yaml`.

Accepted pointers use the canonical `dataset_id + instrument_id` model defined by `contracts/architecture/moex_data_access_canon_v1.yaml` and implemented by `src/moex_data/futures/accepted_manifest.py`.

## Known storage drift

Compatibility-only migration targets identified from repository code:

- `src/moex_data/futures/materialize_forts_raw_5m_tradestats.py` uses the legacy `forts/raw_5m/tradestats` root.
- `src/moex_data/futures/materialize_forts_raw_5m_instrument.py` uses the legacy `forts/raw_5m/tradestats` root.
- `src/moex_data/futures/raw_5m_loader.py` uses the legacy `futures/raw_5m` root.
- `src/moex_data/futures/futoi_raw_loader.py` uses the legacy `futures/futoi_raw` root.
- `src/moex_data/futures/refresh_forts_raw_5m_incremental_pointer.py` uses a legacy pointer structure based on `artifact_id` and `secid`.

These implementations remain compatibility-only until migrated. They are not authorized for onboarding new instruments.

## Stage 1 boundaries

Stage 1 does not move or delete server data, enable registry entries, change schedulers, invent source identities, or declare FUTOI availability without source evidence.

Server filesystem remains Applied State only and is not architecture proof.

## Stage 2 entry gate

Before a FORTS instrument is enabled, Stage 2 must establish its exact registry and source identity from GitHub evidence. If GitHub does not contain the required fact, use an explicit server/source probe and do not invent request-field names.

Stage 2 then migrates the active FORTS Quotes and FUTOI producers to the canonical `market` paths and canonical accepted-pointer model before enabling new instruments.
