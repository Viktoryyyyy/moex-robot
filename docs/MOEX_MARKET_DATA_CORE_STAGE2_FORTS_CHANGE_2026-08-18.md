# MOEX Market Data Core — Stage 2 FORTS Change

Date: 2026-08-20
Status: raw_history_validated_acceptance_pending
Source of Truth: GitHub repository `Viktoryyyyy/moex-robot`
Canonical ingestion runbook: `docs/data/moex_market_data_ingestion_runbook.v1.md`

## Scope

Stage2 bindings remain explicit:

- `usdrubf_futures_family` → Quotes `USDRUBF`, FUTOI ticker `usdrubf`;
- `cnyrubf_futures_family` → Quotes `CNYRUBF`, FUTOI ticker `cnyrubf`;
- `si_futures_family` → Quotes current explicit reference `SiU6`, FUTOI ticker `si`;
- `cr_futures_family` → Quotes current explicit reference `CRU6`, FUTOI ticker `cr`.

`SiU6` and `CRU6` expire/last-trade on `2026-09-17`. Automatic roll selection remains disabled.

## Quotes

Canonical source:

- `source_id=moex_algopack_fo_tradestats_5m`
- contract `contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml`
- APIM host `apim.moex.com`
- Bearer `MOEX_API_KEY`
- endpoint `/iss/datashop/algopack/fo/tradestats.json`

Canonical dataset:

- `contracts/datasets/futures_raw_5m.v1.yaml`
- `${MOEX_DATA_ROOT}/market/raw/timeframe=5m/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`

Validated historical core:

| instrument_id | range | partitions | rows | status |
|---|---|---:|---:|---|
| `usdrubf_futures_family` | 2022-04-26 → 2026-08-17 | 1100 | 181139 | PASS |
| `cnyrubf_futures_family` | 2022-04-26 → 2026-08-17 | 1100 | 174825 | PASS |

Physical history audits found zero bad partitions. Market spot-checks against official MOEX delayed snapshots passed.

`SiU6` and `CRU6` remain reference SECIDs only in Stage2; multi-year fixed-contract Quotes backfill is not required.

## FUTOI

Canonical source:

- `source_id=moex_algopack_futoi`
- contract `contracts/sources/futures/moex_algopack_futoi.v1.yaml`
- APIM host `apim.moex.com`
- Bearer `MOEX_API_KEY`
- endpoint namespace `/iss/analyticalproducts/futoi/securities/{TICKER}.json`
- public `iss.moex.com` transport/fallback forbidden
- `latest=0`

Canonical dataset:

- `contracts/datasets/futures_futoi_raw.v1.yaml`
- `${MOEX_DATA_ROOT}/market/supplementary/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`

### Corrected time and revision semantics

Official/source revalidation established:

- `tradetime` is the last-trade time included in the calculation;
- canonical raw `ts = moment = tradedate + tradetime`;
- `systime` is source publication metadata;
- historical APIM rows can share a later archival/republication `systime`, so `systime` is not the historical event identity;
- `seqnum` is a technical source package number.

Canonical raw source-record key:

`trade_date + sess_id + seqnum + secid + clgroup`

Same `moment` with a different source-record identity is preserved in raw. Revision resolution is deferred to a separate derived analytical layer.

### Validated FUTOI histories

| instrument_id | range | partitions | rows | source-empty dates | status |
|---|---|---:|---:|---:|---|
| `si_futures_family` | 2020-01-03 → 2026-08-17 | 1757 | 597650 | 662 | PASS |
| `cr_futures_family` | 2022-04-21 → 2026-08-17 | 1177 | 390474 | 403 | PASS |

Both physical audits found:

- `BAD_PARTITIONS=0`;
- source-record duplicates `0`;
- FIZ/YUR identity valid;
- requested calendar range fully reconciled as written partitions + explicit source-empty dates.

The only 2-row partition, `2025-01-02`, was explicitly validated as one FIZ and one YUR source record.

FUTOI availability is also proven for `usdrubf` and `cnyrubf`, but their full corrected raw-history materialization is not asserted by this Stage2 validated-history baseline.

## Canonical producers

Quotes single partition:

`moex_data.futures.materialize_forts_raw_5m_instrument`

Quotes controlled historical backfill:

`moex_data.futures.backfill_stage2_forts_raw_5m_instrument`

FUTOI single partition:

`moex_data.futures.materialize_futoi_instrument`

FUTOI controlled historical backfill:

`moex_data.futures.backfill_futoi_instrument`

Exact current CLI and field definitions are maintained in `docs/data/moex_market_data_ingestion_runbook.v1.md` and the referenced source/dataset contracts.

## Current fail-closed state

The internal `stage2_forts_source_bindings.status=all_pilots_passed_backfill_ready` string is retained in `configs/datasets/futures_data_lake.v1.yaml` because current controlled backfill runners use it as an authorization gate. It must not be interpreted as the current evidence summary; validated-history evidence is recorded separately.

Current gates:

- raw-history validation: PASS for USDRUBF Quotes, CNYRUBF Quotes, Si FUTOI, CR FUTOI;
- accepted pointer readiness: false;
- observed-source refresh readiness: false;
- scheduler: disabled;
- D1/W1 derivation: disabled;
- research: disabled;
- automatic Si/CR roll: disabled.

## EOD policy

Final FUTOI EOD must be derived deterministically from canonical corrected raw source-record history. Do not refetch final EOD using `latest=1`.

## Next acceptance sequence

1. reconcile canonical raw dataset manifests/quality evidence;
2. implement/validate any missing aggregate acceptance layer without manually crafting pointers;
3. create per-instrument accepted pointers only after explicit acceptance authority and PASS;
4. run observed-source refresh checks;
5. only then consider scheduler, D1/W1 derivation and research enablement.
