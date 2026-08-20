# MOEX Market Data Ingestion Runbook v1

status: current_canonical_ingestion_entrypoint
project: MOEX_Bot
source_of_truth: GitHub repository `Viktoryyyyy/moex-robot`
management_canon: `docs/MOEX_BOT_MANAGEMENT_CANON.md`
architecture_canon: `contracts/architecture/moex_data_access_canon_v1.yaml`
data_lake_config: `configs/datasets/futures_data_lake.v1.yaml`
instrument_registry: `configs/instruments/forts_instrument_registry.v1.yaml`

## 1. Purpose

This is the operational entry point for any chat or agent that must load, backfill, validate, or inspect canonical MOEX FORTS market data.

Do not reconstruct ingestion from old chats, server directory discovery, legacy CSV files, research contracts, or deprecated Slice1 contracts. GitHub contracts and current producer code define request fields, schemas, identities, paths and failure semantics.

Server filesystem is Applied State only. It may prove that an approved GitHub producer ran, but it never defines architecture.

## 2. Mandatory read order

Before changing or running ingestion, read in this order:

1. `docs/MOEX_BOT_MANAGEMENT_CANON.md`
2. `contracts/architecture/moex_data_access_canon_v1.yaml`
3. `configs/datasets/futures_data_lake.v1.yaml`
4. `configs/instruments/forts_instrument_registry.v1.yaml`
5. the dataset contract for the requested lane
6. the source contract for the requested lane
7. the current producer/backfill module named by this runbook

If a field, CLI argument, ticker, SECID or path is not present in GitHub, do not invent it. Obtain explicit source/server evidence and then update GitHub first.

## 3. Canonical server shell context

Canonical server command prefix:

```text
cd ~/moex_bot && source venv/bin/activate && cd moex-robot
```

Repository path:

```text
/home/trader/moex_bot/moex-robot
```

Data root is never hardcoded by ingestion code. It is read from:

```text
MOEX_DATA_ROOT
```

For the canonical producers below, use `--env-file .env` so the module loads the approved environment file before resolving `MOEX_DATA_ROOT` and `MOEX_API_KEY`.

Forbidden repository paths:

```text
/home/trader/moex_bot/moex_robot
~/moex_bot/moex_robot
```

## 4. Canonical ingestion lanes

### 4.1 Quotes — `futures_raw_5m`

Dataset contract:

```text
contracts/datasets/futures_raw_5m.v1.yaml
```

Source contract:

```text
contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml
```

Source identity:

```text
source_id=moex_algopack_fo_tradestats_5m
host=https://apim.moex.com
endpoint=/iss/datashop/algopack/fo/tradestats.json
auth=Bearer MOEX_API_KEY
```

Canonical storage:

```text
${MOEX_DATA_ROOT}/market/raw/timeframe=5m/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet
```

Canonical state:

```text
manifest=${MOEX_DATA_ROOT}/state/refresh/run_date={YYYY-MM-DD}/run_id={RUN_ID}/manifest.json
quality=${MOEX_DATA_ROOT}/state/quality/run_date={YYYY-MM-DD}/run_id={RUN_ID}/quality_report.json
accepted_pointer=${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_raw_5m/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json
```

Canonical single-partition producer:

```text
moex_data.futures.materialize_forts_raw_5m_instrument
```

Canonical controlled historical backfill producer:

```text
moex_data.futures.backfill_stage2_forts_raw_5m_instrument
```

### 4.2 FUTOI — `futures_futoi_raw`

Dataset contract:

```text
contracts/datasets/futures_futoi_raw.v1.yaml
```

Source contract:

```text
contracts/sources/futures/moex_algopack_futoi.v1.yaml
```

Source identity:

```text
source_id=moex_algopack_futoi
host=https://apim.moex.com
endpoint=/iss/analyticalproducts/futoi/securities/{TICKER}.json
auth=Bearer MOEX_API_KEY
```

The `/iss/...` path above is an APIM namespace on `apim.moex.com`. Public `iss.moex.com` transport is forbidden for FUTOI.

Canonical storage:

```text
${MOEX_DATA_ROOT}/market/supplementary/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet
```

Canonical state:

```text
manifest=${MOEX_DATA_ROOT}/state/refresh/dataset_id=futures_futoi_raw/run_date={YYYY-MM-DD}/run_id={RUN_ID}/manifest.json
quality=${MOEX_DATA_ROOT}/state/quality/dataset_id=futures_futoi_raw/run_date={YYYY-MM-DD}/run_id={RUN_ID}/quality_report.json
accepted_pointer=${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json
```

Canonical single-partition producer:

```text
moex_data.futures.materialize_futoi_instrument
```

Canonical controlled historical backfill producer:

```text
moex_data.futures.backfill_futoi_instrument
```

## 5. Stage2 instrument bindings

The only authoritative bindings are in `configs/instruments/forts_instrument_registry.v1.yaml`.

Current Stage2 bindings:

| instrument_id | Quotes SECID | Quotes role | FUTOI ticker |
|---|---|---|---|
| `usdrubf_futures_family` | `USDRUBF` | historical core | `usdrubf` |
| `cnyrubf_futures_family` | `CNYRUBF` | historical core | `cnyrubf` |
| `si_futures_family` | `SiU6` | current explicit reference | `si` |
| `cr_futures_family` | `CRU6` | current explicit reference | `cr` |

`SiU6` and `CRU6` are current explicit contracts only. Automatic roll selection is not enabled. Multi-year fixed-contract Quotes backfill for those SECIDs is not part of Stage2.

## 6. Exact Quotes request contract

The canonical APIM request uses only fields declared by the source contract:

```text
date={YYYY-MM-DD}
from={YYYY-MM-DD}
till={YYYY-MM-DD}
secid={SECID}
start={PAGINATION_OFFSET}
iss.meta=off
iss.only=tradestats
```

Do not use implicit SECID selection or latest autodetection.

The current normalizer accepts these source column names/aliases:

```text
trade date: tradedate | date
trade time: tradetime | time | moment
identity: secid
open: pr_open | open
high: pr_high | high
low: pr_low | low
close: pr_close | close
volume: vol | volume | qty
value: val | value | turnover
trades: trades | num_trades | numtrades
```

Required source data are trade date, SECID, OHLC and volume. `value` and `num_trades` may be absent and normalize to null.

## 7. Canonical Quotes partition schema

Required stored columns:

```text
instrument_id
trade_date
ts
session_date
secid
board
market
engine
source_id
open
high
low
close
volume
value
num_trades
source
ingest_ts
```

Primary key:

```text
instrument_id + ts + source_id
```

Hard quality gates include duplicate key, monotonic timestamp, OHLC validity and non-negative volume/value/trade counts.

Execution/result metadata are not partition columns. Canonical producer output includes:

```text
status
dataset_id
source_id
source_contract_ref
storage_partition_path
manifest_reference
quality_report_reference
quality_status
row_count
instrument_id_scope
secid_scope
storage_pattern
latest_autodetect_used
hardcoded_server_path_used
```

## 8. Exact FUTOI request contract

Request fields:

```text
from={YYYY-MM-DD}
till={YYYY-MM-DD}
latest=0
start={PAGINATION_OFFSET}
```

Response block:

```text
futoi
```

Required source columns after case normalization:

```text
sess_id
seqnum
tradedate
tradetime
ticker
clgroup
pos
pos_long
pos_short
pos_long_num
pos_short_num
systime
```

Required participant groups:

```text
FIZ
YUR
```

## 9. Canonical FUTOI partition schema and time semantics

Required stored columns:

```text
instrument_id
trade_date
ts
moment
systime
sess_id
seqnum
secid
board
market
engine
source_id
source_ticker
clgroup
pos
pos_long
pos_short
pos_long_num
pos_short_num
availability_ts_utc
ingest_ts
```

Time semantics:

```text
ts = moment = tradedate + tradetime
```

`ts` means the time of the last trade included in the FUTOI calculation.

```text
systime = MOEX source publication metadata
```

Historical APIM rows may share a later archival/republication `systime`. Therefore `systime` is not a historical event identity and must not replace `ts`.

Canonical raw source-record key:

```text
trade_date + sess_id + seqnum + secid + clgroup
```

Same `moment` with different source-record identity is preserved in raw. Analytical revision resolution belongs in a separate derived layer.

Execution/result metadata include:

```text
status
dataset_id
instrument_id
source_id
secid
futoi_ticker
trade_date
row_count
quality_status
storage_partition_path
quality_report_reference
manifest_reference
accepted_manifest_pointer_reference
source_contract_ref
latest_autodetect_used
hardcoded_server_path_used
timestamp_semantics
source_record_key_fields
exact_duplicate_rows_dropped
```

## 10. Canonical commands

### Quotes — one explicit date

```text
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && PYTHONPATH=src python -m moex_data.futures.materialize_forts_raw_5m_instrument --trade-date YYYY-MM-DD --instrument-id INSTRUMENT_ID --secid SECID --artifact-version RUN_ID --env-file .env
```

### Quotes — controlled historical range

```text
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && PYTHONPATH=src python -m moex_data.futures.backfill_stage2_forts_raw_5m_instrument --date-start YYYY-MM-DD --date-end YYYY-MM-DD --instrument-id INSTRUMENT_ID --secid SECID --artifact-version RUN_ID --env-file .env --progress-every 25
```

This full-range runner is Stage2-authorized only for the historical-core Quotes instrument IDs in `configs/datasets/futures_data_lake.v1.yaml`.

### FUTOI — one explicit date

```text
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && PYTHONPATH=src python -m moex_data.futures.materialize_futoi_instrument --trade-date YYYY-MM-DD --instrument-id INSTRUMENT_ID --run-id RUN_ID --env-file .env
```

### FUTOI — controlled historical range

```text
cd ~/moex_bot && source venv/bin/activate && cd moex-robot && PYTHONPATH=src python -m moex_data.futures.backfill_futoi_instrument --date-start YYYY-MM-DD --date-end YYYY-MM-DD --instrument-id INSTRUMENT_ID --run-id RUN_ID --env-file .env --progress-every 25
```

Do not add `--create-accepted-pointer` unless the current acceptance task explicitly authorizes pointer creation after full quality/coverage validation.

## 11. Current validated Stage2 raw-history baseline

Server results below are recorded execution evidence, not architecture definitions.

Quotes:

| instrument_id | range | partitions | rows | audit |
|---|---:|---:|---:|---|
| `usdrubf_futures_family` | 2022-04-26 → 2026-08-17 | 1100 | 181139 | PASS |
| `cnyrubf_futures_family` | 2022-04-26 → 2026-08-17 | 1100 | 174825 | PASS |

FUTOI raw source-record history:

| instrument_id | range | partitions | rows | source-empty dates | audit |
|---|---:|---:|---:|---:|---|
| `si_futures_family` | 2020-01-03 → 2026-08-17 | 1757 | 597650 | 662 | PASS |
| `cr_futures_family` | 2022-04-21 → 2026-08-17 | 1177 | 390474 | 403 | PASS |

For Si and CR FUTOI, physical audits found zero bad partitions and zero duplicate source-record keys. The one 2-row partition on `2025-01-02` was explicitly validated as one FIZ and one YUR source record.

Current acceptance boundary:

```text
accepted_pointer_ready=false
scheduler_ready=false
d1_materialization_ready=false
research_ready=false
```

FUTOI EOD must be derived deterministically from accepted raw source-record history in a separate contracted layer; do not refetch EOD with `latest=1` as the final source of truth.

## 12. Acceptance checklist

For each ingestion run, verify all applicable items:

1. exact GitHub source/dataset/instrument contracts are current;
2. source and instrument identities are explicit;
3. no fallback/latest/glob selection was used;
4. every written partition uses the canonical `${MOEX_DATA_ROOT}/market` path;
5. manifest exists at the contract path;
6. quality report exists and passes;
7. partition schema matches the canonical dataset contract;
8. identity values match the registry;
9. duplicate/null/sign/OHLC checks are zero as applicable;
10. requested range reconciles to written partitions plus explicitly skipped empty-source dates;
11. accepted pointer is unchanged unless pointer creation was explicitly authorized and the aggregate acceptance gate passed.

## 13. New instrument onboarding

For a new FORTS instrument:

1. prove exact source identity and availability;
2. add an explicit row to `configs/instruments/forts_instrument_registry.v1.yaml` with execution flags still false;
3. bind only approved source IDs and source contracts;
4. run one-date canonical pilot;
5. validate partition + manifest + quality;
6. promote evidence without enabling generic runtime;
7. run only the backfill scope explicitly authorized in `configs/datasets/futures_data_lake.v1.yaml`;
8. perform full physical acceptance audit;
9. create accepted pointer only after explicit acceptance authority;
10. enable observed-source refresh/scheduler only in a later authorized workstream.

If runtime/server evidence is missing, collect all required facts in one explicit server command where practical. Do not request piecemeal filesystem exploration.

## 14. Forbidden legacy ingestion

Never use the following as current ingestion architecture:

```text
${MOEX_DATA_ROOT}/forts/...
${MOEX_DATA_ROOT}/futures/raw_5m/...
${MOEX_DATA_ROOT}/futures/futoi_raw/...
family as canonical storage identity
secid as accepted-pointer partition
public iss.moex.com for FUTOI
latest=1 as final FUTOI raw history
implicit latest/glob file selection
```

Compatibility-only modules are not authorized for new ingestion:

```text
src/moex_data/futures/raw_5m_loader.py
src/moex_data/futures/futoi_raw_loader.py
src/moex_data/futures/materialize_forts_raw_5m_tradestats.py
src/moex_data/futures/refresh_forts_raw_5m_incremental_pointer.py
```

Historical/research contracts and CSV files are not ingestion Source of Truth.

Deprecated contract files may remain as tombstones so old references fail safely toward this runbook and the canonical contracts; their historical schemas and paths must not be reused.
