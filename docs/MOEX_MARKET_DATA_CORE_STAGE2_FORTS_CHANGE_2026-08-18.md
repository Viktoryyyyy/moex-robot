# MOEX Market Data Core — Stage 2 FORTS Change

Date: 2026-08-18
Status: change_complete_validation_pending
Branch: `agent/market-data-core-stage2-forts`
Base main SHA: `c15e82e9c0638c4199093c7049fc36c5105cb906`

## Scope

Stage 2 binds and prepares canonical FORTS Quotes and FUTOI lanes for:

- `usdrubf_futures_family` → `USDRUBF`;
- `cnyrubf_futures_family` → `CNYRUBF`;
- `si_futures_family` → explicit current contract `SiU6`;
- `cr_futures_family` → explicit current contract `CRU6`.

For `SiU6` and `CRU6`, the observed expiration and last-trade date is `2026-09-17`. Automatic roll selection is not enabled in this stage.

## Source evidence

Server source probes on 2026-08-18 proved AlgoPack tradestats availability for:

- `USDRUBF`;
- `CNYRUBF`;
- `SiU6`;
- `CRU6`.

The same evidence confirmed `MOEX_API_KEY` is loaded when `.env` is loaded. The successful source route is MOEX AlgoPack FORTS tradestats.

Server source probes also proved FUTOI availability for explicit tickers:

- `usdrubf`;
- `cnyrubf`;
- `si`;
- `cr`.

A historical research artifact used `moex_algopack_cnyrubf_fo_tradestats_5m` for CNYRUBF. Stage 2 does not elevate that instrument-specific research identifier into the Market Data Core canon.

## Canonical source identities introduced

Quotes:

- source_id: `moex_algopack_fo_tradestats_5m`;
- contract: `contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml`;
- source_id scope: shared source identity, not instrument-specific;
- APIM Bearer token env: `MOEX_API_KEY`.

FUTOI:

- source_id: `moex_iss_futoi`;
- contract: `contracts/sources/futures/moex_iss_futoi.v1.yaml`;
- exact ticker binding is explicit in the FORTS registry;
- fallback endpoint selection is forbidden for the canonical Stage 2 writer.

## Quotes lane

Canonical raw storage remains:

`${MOEX_DATA_ROOT}/market/raw/timeframe=5m/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`

`src/moex_data/futures/materialize_forts_raw_5m_instrument.py` now routes through the canonical `futures_raw_5m` materializer instead of writing a `${MOEX_DATA_ROOT}/forts/...` partition. It injects the required APIM Bearer header from `MOEX_API_KEY` and fails closed if the token is absent.

`src/moex_data/futures/backfill_forts_raw_5m_instrument.py` now aggregates canonical state manifests/quality reports and uses `moex_data.futures.accepted_manifest.write_accepted_manifest_pointer` for an explicitly authorized accepted-pointer write.

Canonical Quotes pointer:

`${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_raw_5m/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json`

No `secid` pointer partition is allowed.

## FUTOI lane

Canonical supplementary storage:

`${MOEX_DATA_ROOT}/market/supplementary/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`

The new registry-driven pilot producer is:

`src/moex_data/futures/materialize_futoi_instrument.py`

It uses the explicit registry ticker binding and writes FUTOI separately from Quotes. Stage 2 introduces independent FUTOI quality and refresh-manifest contracts:

- `contracts/datasets/futures_futoi_quality_report.v1.yaml`;
- `contracts/datasets/futures_futoi_refresh_manifest.v1.yaml`.

Canonical FUTOI pointer contract:

`${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json`

The FUTOI registry materialization flag remains disabled until pilot validation passes.

## Fail-closed state after this change

The change does not authorize production runtime:

- `enabled_for_update: false` for all four FORTS identities;
- D1 derivation disabled;
- research disabled;
- scheduler disabled;
- automatic Si/CR roll selection disabled;
- server apply not performed.

## Next action

Separate `VALIDATE` action:

1. repository contract tests;
2. exact-head tests for the Stage 2 branch;
3. one-date server pilot for Quotes and FUTOI using the branch code only after repository validation allows it;
4. inspect actual canonical partition schemas and state artifacts;
5. only after pilot PASS, advance FUTOI/backfill readiness and continue to full backfill + observed-source refresh acceptance.
