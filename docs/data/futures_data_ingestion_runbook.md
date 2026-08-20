# MOEX Bot Futures Data Ingestion Runbook

## Authority

This runbook is the operational entry point for futures market-data loading in MOEX Bot.

Source of Truth order:

1. `contracts/architecture/moex_data_access_canon_v1.yaml`
2. `configs/datasets/futures_data_lake.v1.yaml`
3. dataset contract under `contracts/datasets/`
4. source contract under `contracts/sources/futures/`
5. instrument binding under `configs/instruments/forts_instrument_registry.v1.yaml`
6. producer code referenced by the active contracts

GitHub/repository is Source of Truth. Server filesystem is Applied State only and must not be used to infer architecture, paths, field names, source selection, or CLI semantics.

## Canonical external root

`MOEX_DATA_ROOT` is mandatory.

Hardcoded server data paths are forbidden.

Canonical storage roots:

- quotes: `${MOEX_DATA_ROOT}/market/raw/...`
- supplementary datasets: `${MOEX_DATA_ROOT}/market/supplementary/...`
- refresh state: `${MOEX_DATA_ROOT}/state/refresh/...`
- quality state: `${MOEX_DATA_ROOT}/state/quality/...`
- accepted dataset pointer: `${MOEX_DATA_ROOT}/state/datasets/dataset_id={DATASET_ID}/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json`

## Canonical Stage 2 datasets

### `futures_raw_5m`

Contract: `contracts/datasets/futures_raw_5m.v1.yaml`

Source contract: `contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml`

Producer: `src/moex_data/futures/materialize_forts_raw_5m_instrument.py`

Canonical source id: `moex_algopack_fo_tradestats_5m`

Transport: authenticated MOEX AlgoPack APIM.

Authentication: `MOEX_API_KEY` bearer token.

Base URL: `MOEX_API_URL`, default from source contract.

Canonical historical Stage 2 instruments:

- `usdrubf_futures_family` / `USDRUBF`
- `cnyrubf_futures_family` / `CNYRUBF`

Reference-only current-expiry quote instruments in Stage 2:

- `si_futures_family` / current explicit `Si` contract from registry
- `cr_futures_family` / current explicit `CR` contract from registry

Do not perform multi-year historical quote backfill using one fixed current-expiry Si or CR SECID. Historical Si/CR quotes require a separate continuous-roll-chain dataset.

Storage pattern:

`${MOEX_DATA_ROOT}/market/raw/timeframe=5m/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`

Canonical identity:

- internal: `instrument_id`
- source: `source_id`, `secid`, `board`, `market`, `engine`

Primary key:

- `instrument_id`
- `ts`
- `source_id`

Required stored columns are defined only by `contracts/datasets/futures_raw_5m.v1.yaml`. Do not reconstruct a schema from legacy markdown contracts.

Required loader inputs:

- `MOEX_DATA_ROOT`
- `MOEX_API_KEY`
- explicit `instrument_id`
- explicit `secid`
- explicit `source_id`
- explicit trade date/range
- explicit run/artifact id required by producer CLI

Source selection, SECID selection, and latest-date autodetection are not implicit.

### `futures_futoi_raw`

Contract: `contracts/datasets/futures_futoi_raw.v1.yaml`

Source contract: `contracts/sources/futures/moex_algopack_futoi.v1.yaml`

Producer: `src/moex_data/futures/materialize_futoi_instrument.py`

Controlled range backfill producer: `src/moex_data/futures/backfill_futoi_instrument.py`

Canonical source id: `moex_algopack_futoi`

Transport: authenticated MOEX APIM only.

Public `iss.moex.com` transport and public-ISS fallback are forbidden for FUTOI.

Authentication: `MOEX_API_KEY` bearer token.

Ticker binding must come from `configs/instruments/forts_instrument_registry.v1.yaml` and must never be guessed.

Stage 2 raw FUTOI historical datasets accepted by physical audit:

- `si_futures_family`: `2020-01-03` through `2026-08-17`
- `cr_futures_family`: `2022-04-21` through `2026-08-17`

Storage pattern:

`${MOEX_DATA_ROOT}/market/supplementary/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`

Raw grain: one source record by participant group.

Canonical source-record key:

- `trade_date`
- `sess_id`
- `seqnum`
- `secid`
- `clgroup`

Timestamp semantics:

- `ts = tradedate + tradetime`
- `moment = ts`
- `systime` is MOEX source publication/archive metadata and is not historical raw event identity

Historical APIM may return the same later `systime` on many old source records. Never use `systime` as the raw historical 5-minute key.

Same `moment` with different source-record identity is preserved in raw. Analytical revision resolution belongs in a separate derived layer.

Required stored columns and hard quality checks are defined only by:

- `contracts/datasets/futures_futoi_raw.v1.yaml`
- `contracts/datasets/futures_futoi_quality_report.v1.yaml`
- `contracts/datasets/futures_futoi_refresh_manifest.v1.yaml`

## Instrument lookup

Before loading any partition, read `configs/instruments/forts_instrument_registry.v1.yaml`.

Never invent:

- `instrument_id`
- `secid`
- FUTOI `ticker`
- `board`
- `market`
- `engine`
- source id

If a required binding is not in GitHub, stop and obtain explicit source evidence before changing the registry.

## Load procedure

### Quotes

1. Read `futures_raw_5m.v1.yaml`.
2. Read `moex_algopack_fo_tradestats_5m.v1.yaml`.
3. Read exact instrument binding from `forts_instrument_registry.v1.yaml`.
4. Use the producer CLI exactly as implemented in GitHub; do not invent CLI arguments.
5. Load one explicit date first when onboarding or changing semantics.
6. Validate status, quality status, row count, identity, and resulting partition.
7. Only then run the approved historical range.
8. Audit physical partitions against the aggregate manifest before acceptance.

### FUTOI

1. Read `futures_futoi_raw.v1.yaml`.
2. Read `moex_algopack_futoi.v1.yaml`.
3. Read exact FUTOI ticker binding from `forts_instrument_registry.v1.yaml`.
4. Use authenticated APIM only.
5. Run one explicit-date materialization after any schema, identity, timestamp, or duplicate-policy change.
6. Validate source-record key and timestamp semantics on the physical parquet.
7. Run controlled range backfill only after the one-day validation passes.
8. Reconcile `partitions_written + partitions_skipped` against the requested calendar range.
9. Require `failed_dates=[]` and hard-quality counts equal to zero.
10. Do not create or update an accepted pointer unless the active acceptance gate explicitly authorizes it.

## Quality and acceptance

Data existence is not acceptance.

Minimum acceptance evidence:

- physical partition count reconciles to manifest
- required fields exist
- required identity fields equal registry binding
- required timestamps parse
- duplicate key count is zero under the active dataset key
- hard quality checks pass
- skipped dates do not overlap physical partitions
- requested range reconciles to written + skipped dates
- aggregate backfill has no failed dates
- accepted pointer remains absent until the architecture gate enables it

Current Stage 2 raw history evidence as of 2026-08-20:

### Quotes

- `usdrubf_futures_family`: 1100 partitions, historical core, source coverage from `2022-04-26` through `2026-08-17`
- `cnyrubf_futures_family`: 1100 partitions, historical core, source coverage from `2022-04-26` through `2026-08-17`

### FUTOI

- `si_futures_family`: 1757 physical partitions, 597650 raw rows, 662 source-empty dates, physical audit PASS
- `cr_futures_family`: 1177 physical partitions, 390474 raw rows, 403 source-empty dates, physical audit PASS

These facts describe validated Applied State evidence. They do not independently authorize scheduler, research consumption, or accepted-pointer creation.

## Runtime state layout

Quote refresh state currently follows the generic refresh-state contract and producer implementation. Do not infer quote state by assuming a `dataset_id=` directory component unless the active contract/producer says so.

FUTOI state uses dataset-scoped paths defined by the FUTOI quality/refresh contracts.

Always follow producer output references for `manifest_reference` and `quality_report_reference`; do not reconstruct state paths from memory.

## Prohibited legacy behavior

Do not use any of the following as current architecture:

- `${MOEX_DATA_ROOT}/futures/raw_5m/...`
- `${MOEX_DATA_ROOT}/futures/futoi_raw/...`
- `${MOEX_DATA_ROOT}/forts/raw_5m/...` for new writes
- family-based storage identity
- implicit SECID selection
- fixed current Si/CR contract as multi-year historical series
- public `iss.moex.com` for FUTOI
- FUTOI key `(trade_date, ts, secid, clgroup)`
- FUTOI `ts=systime`
- legacy Slice 1 whitelists as current Stage 2 architecture

## When contracts disagree

The current YAML architecture/source/dataset contracts and producer code take precedence over legacy or research documentation.

Do not silently reconcile conflicting contracts. Update GitHub first through an isolated branch and PR, then apply merged `main` to the server.
