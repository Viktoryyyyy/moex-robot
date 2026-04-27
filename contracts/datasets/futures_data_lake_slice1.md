# MOEX Bot — Dataset Contracts — Futures Data Lake Slice 1

Status: implemented contract
Project: MOEX Bot
Scope: Slice 1 server-side MOEX futures data lake
Schema contract version: futures_data_lake_slice1.v1

## Boundary

GitHub stores code, dataset contracts, and dataset config. Server stores data artifacts only.

All external data paths are resolved only through `MOEX_DATA_ROOT` or explicit `--data-root`.
No absolute server data path is embedded in implementation logic.

Slice 1 includes:
- Si family
- USDRUBF perpetual
- no optional third family
- no continuous futures
- no strategy, research, runtime trading, Telegram, or scheduler installation changes

## Source policy

Canonical OHLCV base:
- MOEX ISS futures candles endpoint where available
- 5 minute interval
- dataset: `futures_raw_5m`

FUTOI:
- MOEX ISS analyticalproducts/futoi endpoint
- stored separately as raw enrichment
- dataset: `futures_futoi_5m_raw`

TradeStats / Super Candles:
- excluded from canonical OHLCV base in Slice 1
- may be added later as separate enrichment

ISS calendar:
- `/iss/calendars.json`
- used for trading-day/session validation in quality report

## External storage root

Artifact class: `env_contract`

```text
MOEX_DATA_ROOT
```

CLI override:
```text
--data-root
```

## Dataset contracts

### instrument_registry_snapshot

```text
dataset_id: instrument_registry_snapshot
artifact_class: external_pattern
path_pattern: ${MOEX_DATA_ROOT}/futures/registry/snapshot_date={snapshot_date}/instrument_registry_snapshot.parquet
producer: src.moex_data.futures.futures_data_lake
consumer: futures data refresh runner, validation sub-chat, later data access layer
format: parquet
schema_version: instrument_registry_snapshot.v1
partitioning: snapshot_date
```

Required normalized fields:
- secid
- family
- board
- market
- is_perpetual
- selected
- selection_error
- registry_snapshot_date
- registry_source
- schema_version

Raw MOEX registry fields may also be preserved.

### futures_raw_5m

```text
dataset_id: futures_raw_5m
artifact_class: external_pattern
path_pattern: ${MOEX_DATA_ROOT}/futures/raw_5m/trade_date={trade_date}/family={family}/secid={secid}/part.parquet
producer: src.moex_data.futures.futures_data_lake
consumer: D1 resampler, future query/access layer, validation sub-chat
format: parquet
schema_version: futures_raw_5m.v1
partitioning: trade_date, family, secid
```

Fields:
- trade_date
- ts
- session_date
- secid
- family
- open
- high
- low
- close
- volume
- value
- source
- ingest_ts
- schema_version

### futures_futoi_5m_raw

```text
dataset_id: futures_futoi_5m_raw
artifact_class: external_pattern
path_pattern: ${MOEX_DATA_ROOT}/futures/futoi_raw/trade_date={trade_date}/family={family}/secid={secid}/part.parquet
producer: src.moex_data.futures.futures_data_lake
consumer: future enrichment joins, validation sub-chat
format: parquet
schema_version: futures_futoi_5m_raw.v1
partitioning: trade_date, family, secid
```

Required normalized fields:
- trade_date
- ts
- session_date
- secid
- family
- source
- ingest_ts
- schema_version

Raw FUTOI columns are preserved when returned by MOEX.

### futures_derived_d1

```text
dataset_id: futures_derived_d1
artifact_class: external_pattern
path_pattern: ${MOEX_DATA_ROOT}/futures/derived_d1/series_type={series_type}/family={family}/secid={secid}/part.parquet
producer: src.moex_data.futures.futures_data_lake
consumer: research/backtest/runtime data access layer
format: parquet
schema_version: futures_derived_d1.v1
partitioning: series_type, family, secid
```

Fields:
- session_date
- secid
- family
- open
- high
- low
- close
- volume
- bar_count
- min_ts
- max_ts
- ingest_ts
- schema_version

Slice 1 D1 rule:
- grouped from canonical raw 5m bars by `session_date`
- `session_date` is calendar date of timestamp in Europe/Moscow
- ISS calendar status is reported separately in quality report
- evening-session remapping is not introduced in Slice 1

### futures_data_quality_report

```text
dataset_id: futures_data_quality_report
artifact_class: external_pattern
path_pattern: ${MOEX_DATA_ROOT}/runs/futures_data_refresh/run_date={run_date}/run_id={run_id}/quality_report.json
producer: src.moex_data.futures.futures_data_lake
consumer: validation sub-chat, PM closeout
format: json
schema_version: futures_data_quality_report.v1
partitioning: run_date, run_id
```

Fields:
- run_id
- schema_version
- started_at
- finished_at
- day_from
- day_till
- rows
- errors
- quality_status

Per-row quality fields:
- dataset_id
- trade_date
- secid
- family
- rows
- min_ts
- max_ts
- duplicate_ts_count
- null_ohlc_count
- invalid_ohlc_count
- calendar_status
- quality_status
- notes

### futures_data_refresh_manifest

```text
dataset_id: futures_data_refresh_manifest
artifact_class: external_pattern
path_pattern: ${MOEX_DATA_ROOT}/runs/futures_data_refresh/run_date={run_date}/run_id={run_id}/manifest.json
producer: src.moex_data.futures.futures_data_lake
consumer: validation sub-chat, PM closeout, future scheduler
format: json
schema_version: futures_data_refresh_manifest.v1
partitioning: run_date, run_id
```

Fields:
- run_id
- schema_version
- started_at
- finished_at
- day_from
- day_till
- config_path
- data_root_env
- selected_secids
- output_paths
- error_count
- errors
- status

## Idempotency

The writer overwrites affected dataset files atomically through temporary files plus `os.replace`.
Repeated runs for the same date range do not append duplicate rows to the same partition file.

## CLI contract

Thin wrapper:

```text
src/cli/run_futures_data_refresh_daily.py
```

Canonical module:

```text
src.moex_data.futures.futures_data_lake
```

Arguments:
- `--config`
- `--data-root`
- `--from YYYY-MM-DD`
- `--till YYYY-MM-DD`
- `--fail-on-empty`

## Known Slice 1 gaps

- continuous futures are excluded
- all-futures backfill is excluded
- production scheduler installation is excluded
- FUTOI is stored separately; materialized join is excluded
- evening-session remapping is not introduced in Slice 1
