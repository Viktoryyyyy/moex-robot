# MOEX Market Data Core — Stage 2 FORTS Change

Date: 2026-08-18
Status: all_pilots_passed_backfill_ready

## Scope

Stage 2 canonical FORTS lanes:

- `usdrubf_futures_family` → Quotes `USDRUBF`, FUTOI `usdrubf`;
- `cnyrubf_futures_family` → Quotes `CNYRUBF`, FUTOI `cnyrubf`;
- `si_futures_family` → Quotes explicit current contract `SiU6`, FUTOI family ticker `si`;
- `cr_futures_family` → Quotes explicit current contract `CRU6`, FUTOI family ticker `cr`.

For `SiU6` and `CRU6`, observed expiration and last-trade date is `2026-09-17`. Automatic roll selection is not enabled.

## Quotes — pilot accepted

Canonical source:

- source_id: `moex_algopack_fo_tradestats_5m`;
- contract: `contracts/sources/futures/moex_algopack_fo_tradestats_5m.v1.yaml`;
- host: `apim.moex.com`;
- authentication: Bearer `MOEX_API_KEY`;
- canonical fetch route: `/iss/datashop/algopack/fo/tradestats.json`;
- exact partition request uses the explicit `date`, `from`, `till`, `secid`, `start`, `iss.meta`, and `iss.only` fields defined by the source contract.

The one-date server pilot for `2026-08-17` passed for all four identities with 203 rows per instrument and zero duplicate, gap, null-OHLC, or invalid-OHLC counts.

Canonical Quotes storage:

`${MOEX_DATA_ROOT}/market/raw/timeframe=5m/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`

Canonical Quotes pointer:

`${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_raw_5m/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json`

### Quotes coverage status

A later server coverage probe using the per-SECID availability route with a broad interval returned `null` for all four Quotes identities. This result is not accepted as history evidence because it did not reproduce the canonical materializer request shape. The canonical writer uses the generic `/tradestats.json` route with an explicit date partition request.

Therefore:

- Quotes pilot status: passed;
- Quotes backfill materialization: enabled after pilot acceptance;
- Quotes historical first-available date: pending canonical exact-route reprobe;
- the invalid broad per-SECID coverage result must not be used as evidence of absent history.

Canonical coverage probe:

`moex_data.futures.probe_forts_tradestats_coverage`

It uses the same generic APIM tradestats endpoint and explicit request fields as the canonical writer and fails closed on `ERROR_MESSAGE` payloads.

## FUTOI — public ISS invalidated, APIM accepted

The original public `iss.moex.com` FUTOI availability conclusion was invalid because a one-row `ERROR_MESSAGE` payload had been interpreted as data availability. Public ISS transport and fallback are now forbidden for FUTOI by repository guard.

Canonical FUTOI source:

- source_id: `moex_algopack_futoi`;
- contract: `contracts/sources/futures/moex_algopack_futoi.v1.yaml`;
- host: `apim.moex.com`;
- authentication: Bearer `MOEX_API_KEY`;
- route namespace: `/iss/analyticalproducts/futoi/securities/{TICKER}.json` on the APIM host;
- response block: `futoi`;
- public ISS transport: forbidden;
- public ISS fallback: forbidden;
- `ERROR_MESSAGE` payload: fail closed.

Authenticated APIM revalidation on 2026-08-18 proved the required FUTOI schema for `usdrubf`, `cnyrubf`, `si`, and `cr`. The one-date canonical materialization pilot for `2026-08-17` then passed for all four identities with `quality_status=pass`, 2 rows each, no hardcoded path, no latest autodetect, and separate canonical supplementary partitions.

Canonical FUTOI storage:

`${MOEX_DATA_ROOT}/market/supplementary/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/trade_date={YYYY-MM-DD}/source={SOURCE_ID}/part.parquet`

Canonical FUTOI pointer:

`${MOEX_DATA_ROOT}/state/datasets/dataset_id=futures_futoi_raw/instrument_id={INSTRUMENT_ID}/current_accepted_manifest.json`

### Proven FUTOI historical coverage

Server APIM coverage evidence through `2026-08-17`:

- `si`: `2020-01-03` → `2026-08-17`;
- `cr`: `2022-04-21` → `2026-08-17`;
- `usdrubf`: `2022-12-30` → `2026-08-17`;
- `cnyrubf`: `2022-12-30` → `2026-08-17`.

Canonical full-range runner:

`moex_data.futures.backfill_futoi_instrument`

Rules:

- explicit `date_start`, `date_end`, `instrument_id`, and `run_id` only;
- registry FUTOI materialization must be enabled;
- non-trading empty dates may be skipped;
- network/schema/`ERROR_MESSAGE` failures fail the run;
- accepted pointer may be written only when aggregate quality is `pass` and refresh status is `succeeded`.

## Current fail-closed state

After pilot acceptance:

- Quotes raw materialization: enabled for controlled backfill;
- FUTOI materialization: enabled for controlled backfill;
- backfill readiness: true;
- accepted pointer readiness: false until full-range validation;
- observed-source refresh readiness: false;
- scheduler: disabled;
- D1/W1 derivation: disabled;
- research: disabled;
- automatic Si/CR roll: disabled.

## Next acceptance sequence

1. merge the pilot-promotion/backfill tooling after CI PASS;
2. apply exact merged SHA on the server;
3. determine Quotes historical coverage with the canonical exact-route coverage probe;
4. run controlled full backfill for Quotes and FUTOI over proven ranges;
5. validate aggregate quality/manifests;
6. create per-instrument accepted pointers only after PASS;
7. run observed-source refresh checks;
8. only then consider scheduler, D1/W1, and research enablement.
